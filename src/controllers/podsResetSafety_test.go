package controllers

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/podsreset"
	"github.com/google/uuid"
	"gorm.io/datatypes"
)

func TestPodsResetInventoryDriftFailsClosedButAllowsPreviouslyAbsentKeys(t *testing.T) {
	expected := []podsreset.ObjectIdentity{
		{StorageTier: "primary", Bucket: "hot", ObjectKey: "content/abc/original.mp4", ETag: "etag-1", SizeBytes: 100},
		{StorageTier: "cold", Bucket: "cold", ObjectKey: "content/abc/hls/seg-1.m4s", ETag: "etag-2", SizeBytes: 25},
	}
	wire := []podsResetObjectWire{
		{StorageTier: "primary", Bucket: "hot", ObjectKey: "content/abc/original.mp4", ETag: "etag-1", SizeBytes: 100},
		{StorageTier: "cold", Bucket: "cold", ObjectKey: "content/abc/hls/seg-1.m4s", ETag: "etag-2", SizeBytes: 25},
	}
	if podsResetInventoryChanged(expected, wire[:1]) {
		t.Fatal("a missing previously approved key should remain an idempotent absence outcome")
	}
	changed := append([]podsResetObjectWire(nil), wire...)
	changed[0].ETag = "replacement"
	if !podsResetInventoryChanged(expected, changed) {
		t.Fatal("a replaced key must invalidate the approved inventory")
	}
	unlisted := append(append([]podsResetObjectWire(nil), wire...), podsResetObjectWire{StorageTier: "primary", Bucket: "hot", ObjectKey: "content/abc/new.mp4", ETag: "new", SizeBytes: 1})
	if !podsResetInventoryChanged(expected, unlisted) {
		t.Fatal("a newly discovered key must invalidate the approved inventory")
	}
}

func TestPodsResetProviderOutcomeMustExactlyMatchFrozenKeys(t *testing.T) {
	expected := []models.PodsResetObject{
		{StorageTier: "primary", Bucket: "hot", ObjectKey: "content/abc/original.mp4", ETag: "etag-1", SizeBytes: 100},
		{StorageTier: "cold", Bucket: "cold", ObjectKey: "content/abc/hls/seg-1.m4s", ETag: "etag-2", SizeBytes: 25},
	}
	deleted := []podsResetObjectWire{{StorageTier: "primary", Bucket: "hot", ObjectKey: "content/abc/original.mp4", ETag: "etag-1", SizeBytes: 100}}
	absent := []podsResetObjectWire{{StorageTier: "cold", Bucket: "cold", ObjectKey: "content/abc/hls/seg-1.m4s", ETag: "etag-2", SizeBytes: 25}}
	if err := podsResetValidateObjectOutcomes(expected, deleted, absent, 1); err != nil {
		t.Fatalf("complete approved outcome rejected: %v", err)
	}
	if err := podsResetValidateObjectOutcomes(expected, deleted, nil, 1); err == nil {
		t.Fatal("provider response omitting a frozen key was accepted")
	}
	extra := append(deleted, podsResetObjectWire{StorageTier: "primary", Bucket: "hot", ObjectKey: "content/abc/unapproved.mp4", ETag: "extra", SizeBytes: 4})
	if err := podsResetValidateObjectOutcomes(expected, extra, absent, 2); err == nil {
		t.Fatal("provider response containing an unapproved key was accepted")
	}
	duplicate := append(deleted, deleted[0])
	if err := podsResetValidateObjectOutcomes(expected, duplicate, absent, 2); err == nil {
		t.Fatal("duplicate provider outcome was accepted")
	}
}

func TestPodsResetTierSnapshotIsOrderIndependent(t *testing.T) {
	if !podsResetSameStrings([]string{"primary", "cold"}, []string{"cold", "primary"}) {
		t.Fatal("tier ordering must not invalidate a stable inventory")
	}
	if podsResetSameStrings([]string{"primary"}, []string{"primary", "cold"}) {
		t.Fatal("a changed configured tier set must invalidate the manifest")
	}
}

func TestPodsResetVerificationPendingItemsRemainResumable(t *testing.T) {
	for _, state := range []string{"fenced", "verification_pending", "objects_deleted"} {
		if !podsResetItemNeedsProcessing(state) {
			t.Errorf("state %q was skipped by the execution resume filter", state)
		}
	}
	for _, state := range []string{"planned", "blocked", "complete", "unknown"} {
		if podsResetItemNeedsProcessing(state) {
			t.Errorf("state %q unexpectedly entered irreversible processing", state)
		}
	}
}

func TestPodsResetStorageBindingsCoverEmptyConfiguredTiers(t *testing.T) {
	bindings := []podsreset.StorageBinding{
		{StorageTier: "primary", Bucket: "hot", EndpointFingerprint: strings.Repeat("a", 64)},
		{StorageTier: "cold", Bucket: "archive", EndpointFingerprint: strings.Repeat("b", 64)},
	}
	if err := podsreset.ValidateStorageBindings([]string{"primary", "cold"}, bindings); err != nil {
		t.Fatalf("complete primary/cold bindings rejected: %v", err)
	}
	if err := podsreset.ValidateStorageBindings([]string{"primary", "cold"}, bindings[:1]); err == nil {
		t.Fatal("missing empty cold-tier binding was accepted")
	}
	changed := append([]podsreset.StorageBinding(nil), bindings...)
	changed[0].EndpointFingerprint = strings.Repeat("c", 64)
	if podsreset.SameStorageBindings(bindings, changed) {
		t.Fatal("changed provider account binding was treated as the approved endpoint")
	}
}

func TestPodsResetFenceTriggerNamesRespectPostgresIdentifierLimit(t *testing.T) {
	short := "content_stage_events"
	if got, want := podsResetFenceTriggerName(short), "pods_reset_fence_"+short; got != want {
		t.Fatalf("short trigger name = %q, want %q", got, want)
	}
	long := strings.Repeat("a", 60)
	name := podsResetFenceTriggerName(long)
	if len(name) != len("pods_reset_fence_")+32 || !strings.HasPrefix(name, "pods_reset_fence_") {
		t.Fatalf("long trigger name is not safely bounded: %q (%d bytes)", name, len(name))
	}
	if name != podsResetFenceTriggerName(long) {
		t.Fatal("long trigger name is not deterministic")
	}
}

func TestPodsResetSchemaFingerprintRequiresCorrectActiveFenceTriggers(t *testing.T) {
	valid := `CREATE TRIGGER reset_guard BEFORE INSERT OR UPDATE ON content_flags FOR EACH ROW EXECUTE FUNCTION public.enforce_pods_reset_retirement_fence('content_item_id')`
	if !podsResetTriggerDefinitionUsable("O", valid, "enforce_pods_reset_retirement_fence", "BEFORE INSERT OR UPDATE") {
		t.Fatal("correct origin-mode write fence should count as enabled")
	}
	for name, tc := range map[string]struct {
		enabled    string
		definition string
	}{
		"disabled":       {"D", valid},
		"replica-only":   {"R", valid},
		"wrong-function": {"O", `CREATE TRIGGER reset_guard BEFORE INSERT OR UPDATE ON content_flags FOR EACH ROW EXECUTE FUNCTION unrelated_guard()`},
		"wrong-events":   {"O", `CREATE TRIGGER reset_guard BEFORE DELETE ON content_flags FOR EACH ROW EXECUTE FUNCTION enforce_pods_reset_retirement_fence('content_item_id')`},
	} {
		t.Run(name, func(t *testing.T) {
			if podsResetTriggerDefinitionUsable(tc.enabled, tc.definition, "enforce_pods_reset_retirement_fence", "BEFORE INSERT OR UPDATE") {
				t.Fatal("misconfigured trigger incorrectly qualified retirement writes")
			}
		})
	}
}

func TestPodsResetManifestTargetUsesStrongItemIdentity(t *testing.T) {
	id := uuid.New()
	target := podsResetManifestTarget{ID: id, StorageTiers: []string{"primary"}, StorageVersionModel: "cloudflare-r2-current-key-delete-v1"}
	if target.ID == uuid.Nil || target.ID.String() != id.String() {
		t.Fatal("reset manifest did not preserve the selected item UUID")
	}
	manifest, err := json.Marshal(map[string]any{"targets": []podsResetManifestTarget{target}})
	if err != nil {
		t.Fatal(err)
	}
	decoded := itemStorageTarget(datatypes.JSON(manifest), id)
	if decoded == nil || decoded.ID != id || decoded.StorageVersionModel != target.StorageVersionModel || !podsResetSameStrings(decoded.StorageTiers, target.StorageTiers) {
		t.Fatalf("persisted item target could not be recovered: %#v", decoded)
	}
}

func TestPodsResetVerificationEvidenceAppendsDistinctProbes(t *testing.T) {
	firstAt := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	evidence, err := podsResetAppendVerificationProbe(datatypes.JSON([]byte(`{"probes":[]}`)), podsResetVerificationProbe{
		Number: 1, ObservedAt: firstAt, ObjectsAbsent: true, ObservedObjectCount: 0,
		StorageVersionModel: "cloudflare-r2-current-key-delete-v1", StorageTiers: []string{"primary"},
	})
	if err != nil {
		t.Fatal(err)
	}
	evidence, err = podsResetAppendVerificationProbe(evidence, podsResetVerificationProbe{
		Number: 2, ObservedAt: firstAt.Add(time.Minute), ObjectsAbsent: true, ObservedObjectCount: 0,
		StorageVersionModel: "cloudflare-r2-current-key-delete-v1", StorageTiers: []string{"primary"},
	})
	if err != nil {
		t.Fatal(err)
	}
	var recorded podsResetVerificationEvidence
	if err := json.Unmarshal(evidence, &recorded); err != nil {
		t.Fatal(err)
	}
	if len(recorded.Probes) != 2 || recorded.Probes[0].Number != 1 || recorded.Probes[1].Number != 2 || !recorded.Probes[0].ObservedAt.Before(recorded.Probes[1].ObservedAt) {
		t.Fatalf("verification evidence is not two distinct ordered probes: %#v", recorded.Probes)
	}
}

func TestPodsResetDeleteResponseDecodesAggregationWireContract(t *testing.T) {
	const response = `{"data":{"content_item_id":"00000000-0000-4000-8000-000000000001","objectsAbsent":true,"deletedCount":1,"freedBytes":123,"deletedObjects":[{"storage_tier":"primary","bucket":"media","object_key":"content/00000000-0000-4000-8000-000000000001/original.mp4","etag":"etag","size_bytes":123}],"alreadyAbsentObjects":[],"fencing_token":"00000000-0000-4000-8000-000000000002"}}`
	var decoded podsResetDeleteResponse
	if err := json.Unmarshal([]byte(response), &decoded); err != nil {
		t.Fatal(err)
	}
	if !decoded.Data.ObjectsAbsent || decoded.Data.DeletedCount != 1 || decoded.Data.FreedBytes != 123 || len(decoded.Data.DeletedObjects) != 1 || decoded.Data.FencingToken == "" {
		t.Fatalf("Aggregation deletion receipt did not decode: %#v", decoded.Data)
	}
}
