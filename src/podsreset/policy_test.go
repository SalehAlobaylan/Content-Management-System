package podsreset

import (
	"strings"
	"testing"
	"time"

	"content-management-system/src/models"
	"github.com/google/uuid"
)

func TestDecisionRegistryCoversEveryDisposition(t *testing.T) {
	decisions := Decisions(false)
	seenClasses := map[string]bool{}
	seenDispositions := map[DataDisposition]bool{}
	for _, decision := range decisions {
		if decision.Class == "" || decision.Owner == "" || decision.Reason == "" || decision.Postcondition == "" {
			t.Fatalf("incomplete reset decision: %#v", decision)
		}
		if seenClasses[decision.Class] {
			t.Fatalf("duplicate data class %q", decision.Class)
		}
		seenClasses[decision.Class] = true
		seenDispositions[decision.Disposition] = true
	}
	for _, expected := range []DataDisposition{DeleteOwnedPayload, PreserveShared, RetainIdentity, DetachRecompute, Protected, Blocked} {
		if !seenDispositions[expected] {
			t.Errorf("decision registry does not represent disposition %q", expected)
		}
	}
	sharedTranscriptFound := false
	for _, decision := range Decisions(true) {
		if decision.Class == "transcript_payload" && decision.Disposition != PreserveShared {
			t.Fatalf("shared transcript disposition = %q, want %q", decision.Disposition, PreserveShared)
		}
		sharedTranscriptFound = sharedTranscriptFound || decision.Class == "transcript_payload"
	}
	if !sharedTranscriptFound {
		t.Fatal("shared transcript decision is missing")
	}
}

func TestRelationshipPolicyRegistryIsExplicitAndUnambiguous(t *testing.T) {
	seen := map[string]bool{}
	for _, policy := range RelationshipPolicies() {
		key := policy.Table + "." + policy.Column
		if policy.Table == "" || policy.Column == "" || policy.Class == "" || policy.Owner == "" || policy.Reason == "" || policy.Postcondition == "" {
			t.Fatalf("incomplete relationship disposition: %#v", policy)
		}
		if seen[key] {
			t.Fatalf("duplicate relationship disposition for %s", key)
		}
		seen[key] = true
		if found, ok := RelationshipPolicyFor(policy.Table, policy.Column); !ok || found != policy {
			t.Fatalf("policy lookup does not resolve %s", key)
		}
	}
	if _, ok := RelationshipPolicyFor("future_table", "content_item_id"); ok {
		t.Fatal("unknown content relationship had an implicit disposition")
	}
	for _, key := range []string{
		"media_supply_action_previews.target_id",
		"media_circulation_recommendations.subject_id",
		"feed_recovery_plan_targets.target_id",
		"experience_events.content_id",
		"enrichment_autopilot_actions.content_id",
		"news_month_archive_story_sources.original_content_id",
		"news_ingest_tombstones.original_content_id",
		"retention_compaction_batches.target_ids",
		"retention_compaction_manifests.anchor_content_ids",
		"retention_compaction_manifests.protected_content_ids",
		"retention_compaction_manifests.retire_content_ids",
	} {
		table, column, ok := strings.Cut(key, ".")
		if !ok {
			t.Fatalf("invalid test relationship key %q", key)
		}
		if _, exists := RelationshipPolicyFor(table, column); !exists {
			t.Errorf("known content relationship %s lacks an explicit disposition", key)
		}
	}
}

func TestStorageBindingValidationIncludesEveryConfiguredTier(t *testing.T) {
	primary := StorageBinding{StorageTier: "primary", Bucket: "hot", EndpointFingerprint: strings.Repeat("a", 64)}
	cold := StorageBinding{StorageTier: "cold", Bucket: "archive", EndpointFingerprint: strings.Repeat("b", 64)}
	if err := ValidateStorageBindings([]string{"primary", "cold"}, []StorageBinding{primary, cold}); err != nil {
		t.Fatalf("complete binding set rejected: %v", err)
	}
	if err := ValidateStorageBindings([]string{"primary", "cold"}, []StorageBinding{primary}); err == nil {
		t.Fatal("empty-tier bucket/account binding was not required")
	}
	if SameStorageBindings([]StorageBinding{primary}, []StorageBinding{{StorageTier: "primary", Bucket: "hot", EndpointFingerprint: strings.Repeat("c", 64)}}) {
		t.Fatal("changed account binding matched frozen identity")
	}
}

func TestSnapshotIsDeterministicAndExcludesPayload(t *testing.T) {
	item := models.ContentItem{TenantID: "default", PublicID: uuid.New(), Type: models.ContentTypePodcast, Status: models.ContentStatusReady, UpdatedAt: time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC), BodyText: stringPointer("private transcript-like body"), Title: stringPointer("title")}
	a, err := Hash(SnapshotFor(item))
	if err != nil {
		t.Fatal(err)
	}
	b, err := Hash(SnapshotFor(item))
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatal("same lifecycle snapshot produced different hashes")
	}
	encoded, err := Hash(SnapshotFor(item))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(encoded, "private") {
		t.Fatal("snapshot hash leaked source payload")
	}
}

func TestManifestHashSortsExplicitItemSnapshots(t *testing.T) {
	first := Snapshot{TenantID: "default", PublicID: "a", Type: models.ContentTypeVideo, Status: models.ContentStatusReady}
	second := Snapshot{TenantID: "default", PublicID: "b", Type: models.ContentTypePodcast, Status: models.ContentStatusReady}
	decisions := map[string][]DataClassDecision{"a": Decisions(false), "b": Decisions(true)}
	objects := map[string][]ObjectIdentity{"a": {{StorageTier: "primary", Bucket: "bucket", ObjectKey: "content/a/one", ETag: "etag", SizeBytes: 1}}, "b": {}}
	left, err := ManifestHash([]Snapshot{first, second}, decisions, objects, "schema")
	if err != nil {
		t.Fatal(err)
	}
	right, err := ManifestHash([]Snapshot{second, first}, decisions, objects, "schema")
	if err != nil {
		t.Fatal(err)
	}
	if left != right {
		t.Fatalf("item ordering changed manifest hash: %s != %s", left, right)
	}
}

func TestValidateObjectSetRequiresExactOwnedStableIdentities(t *testing.T) {
	valid := []ObjectIdentity{{StorageTier: "primary", Bucket: "primary-bucket", ObjectKey: "content/item-id/processed.v2.mp4", ETag: "fingerprint", SizeBytes: 12}}
	if err := ValidateObjectSet("item-id", valid); err != nil {
		t.Fatalf("valid identity rejected: %v", err)
	}
	tests := []struct {
		name   string
		object []ObjectIdentity
	}{
		{name: "outside item namespace", object: []ObjectIdentity{{StorageTier: "primary", Bucket: "b", ObjectKey: "content/another-id/file", ETag: "e", SizeBytes: 1}}},
		{name: "empty fingerprint", object: []ObjectIdentity{{StorageTier: "primary", Bucket: "b", ObjectKey: "content/item-id/file", SizeBytes: 1}}},
		{name: "unknown tier", object: []ObjectIdentity{{StorageTier: "archive", Bucket: "b", ObjectKey: "content/item-id/file", ETag: "e", SizeBytes: 1}}},
		{name: "duplicate object", object: []ObjectIdentity{valid[0], valid[0]}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := ValidateObjectSet("item-id", test.object); err == nil {
				t.Fatal("invalid identity was accepted")
			}
		})
	}
}

func TestAdvanceVerificationProbeRequiresDelayedSecondAbsence(t *testing.T) {
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	due := now.Add(time.Minute)
	if count, code := AdvanceVerificationProbe(1, true, now, due); count != 1 || code != "verification_probe_not_due" {
		t.Fatalf("early verification advanced: count=%d code=%q", count, code)
	}
	if count, code := AdvanceVerificationProbe(1, false, due, due); count != 1 || code != "verification_objects_present" {
		t.Fatalf("present object was accepted: count=%d code=%q", count, code)
	}
	if count, code := AdvanceVerificationProbe(1, true, due, due); count != 2 || code != "" {
		t.Fatalf("second absence was not accepted: count=%d code=%q", count, code)
	}
	if count, code := AdvanceVerificationProbe(0, true, due, due); count != 0 || code != "verification_probe_state_invalid" {
		t.Fatalf("invalid probe state was accepted: count=%d code=%q", count, code)
	}
}

func TestArtifactManifestCoverageBlocksUnknownAndChangedObjects(t *testing.T) {
	object := ObjectIdentity{StorageTier: "primary", Bucket: "media", ObjectKey: "content/item/original.mp4", ETag: "etag", SizeBytes: 10}
	owner := ArtifactOwnership{ContentItemID: "item", StorageTier: "primary", Bucket: "media", ObjectKey: object.ObjectKey, ETag: "etag", SizeBytes: 10, ArtifactRole: "source", ProducerEventID: "event-1", State: "verified"}
	if err := ValidateArtifactManifestCoverage("item", []ObjectIdentity{object}, []ArtifactOwnership{owner}); err != nil {
		t.Fatalf("exact artifact owner was rejected: %v", err)
	}
	parentOwned := owner
	parentOwned.ContentItemID = ""
	parentOwned.ParentContentItemID = "item"
	if err := ValidateArtifactManifestCoverage("item", []ObjectIdentity{object}, []ArtifactOwnership{parentOwned}); err != nil {
		t.Fatalf("parent-owned artifact with no child owner was rejected: %v", err)
	}
	if err := ValidateArtifactManifestCoverage("item", []ObjectIdentity{object}, nil); err == nil {
		t.Fatal("unregistered provider object was treated as owned")
	}
	changed := owner
	changed.ETag = "stale-etag"
	if err := ValidateArtifactManifestCoverage("item", []ObjectIdentity{object}, []ArtifactOwnership{changed}); err == nil {
		t.Fatal("changed artifact fingerprint was accepted")
	}
	unmapped := owner
	unmapped.ArtifactRole = "legacy_unknown"
	if err := ValidateArtifactManifestCoverage("item", []ObjectIdentity{object}, []ArtifactOwnership{unmapped}); err == nil {
		t.Fatal("unmapped artifact role was accepted")
	}
	if err := ValidateArtifactManifestCoverage("item", []ObjectIdentity{object}, []ArtifactOwnership{owner, owner}); err == nil {
		t.Fatal("a key with multiple live owner records was treated as exclusively owned")
	}
	sharedOnly := owner
	sharedOnly.ContentItemID = "another-item"
	sharedOnly.ParentContentItemID = "item"
	if err := ValidateArtifactManifestCoverage("item", []ObjectIdentity{object}, []ArtifactOwnership{sharedOnly}); err == nil {
		t.Fatal("child-owned artifact was treated as a parent-owned object")
	}
}

func stringPointer(value string) *string { return &value }
