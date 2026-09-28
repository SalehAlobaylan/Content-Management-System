package podsreset

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"content-management-system/src/models"
)

const (
	PolicyVersion             = 2
	MaxItems                  = 30
	MaxObjects                = 1000
	MaxRunObjects             = 1000
	MaxRunBytes               = int64(25 * 1024 * 1024 * 1024)
	MaxBatchItems             = 3
	PlanTTL                   = 30 * time.Minute
	VerificationProbeInterval = time.Minute
)

type DataDisposition string

const (
	DeleteOwnedPayload DataDisposition = "delete_owned_payload"
	PreserveShared     DataDisposition = "preserve_shared"
	RetainIdentity     DataDisposition = "retain_identity"
	DetachRecompute    DataDisposition = "detach_or_recompute"
	Protected          DataDisposition = "protected"
	Blocked            DataDisposition = "blocked"
)

// DataClassDecision is deliberately explicit and versioned. Reset does not
// infer deletion rights from an LLM, a cascade, or a missing reference.
type DataClassDecision struct {
	Class         string          `json:"class"`
	Disposition   DataDisposition `json:"disposition"`
	Owner         string          `json:"owner"`
	Reason        string          `json:"reason"`
	Postcondition string          `json:"postcondition"`
}

func Decisions(transcriptShared bool) []DataClassDecision {
	transcriptDisposition := DeleteOwnedPayload
	transcriptReason := "transcript has no consumers outside the approved selection"
	if transcriptShared {
		transcriptDisposition = PreserveShared
		transcriptReason = "transcript is referenced by an item outside the approved selection"
	}
	return []DataClassDecision{
		{Class: "content_payload", Disposition: DeleteOwnedPayload, Owner: "cms", Reason: "legacy item payload is expendable after retirement", Postcondition: "text, metadata, media references, embeddings, and item tags are cleared"},
		{Class: "topic_membership", Disposition: DeleteOwnedPayload, Owner: "cms", Reason: "item-to-topic edges are derived, item-owned projections", Postcondition: "only the selected item's topic memberships are removed; shared topic catalog rows remain"},
		{Class: "transcript_payload", Disposition: transcriptDisposition, Owner: "media/cms", Reason: transcriptReason, Postcondition: "shared transcript stays intact; exclusively owned transcript text and segments are cleared"},
		{Class: "editorial_transcript_or_chapter", Disposition: Protected, Owner: "cms/editorial", Reason: "approved human captions and manual chapter metadata are not disposable derived payload", Postcondition: "items with approved transcript text or manual chapters are blocked before storage effects"},
		{Class: "artifact_objects", Disposition: DeleteOwnedPayload, Owner: "aggregation", Reason: "only the frozen exact-key inventory may be removed", Postcondition: "all approved keys are absent in every configured storage tier"},
		{Class: "artifact_and_delivery_projections", Disposition: DetachRecompute, Owner: "aggregation/cms", Reason: "manifest, rendition, and HLS access rows must stop advertising deleted origin objects", Postcondition: "manifests are marked deleted, rendition generations superseded, HLS access points superseded, and packages failed"},
		{Class: "transcription_and_atomization_payload", Disposition: DetachRecompute, Owner: "media/cms", Reason: "owned generated transcript segments and chapter plans are no longer eligible for repair or serving", Postcondition: "owned generations/units are superseded and payload-bearing text/plans are cleared"},
		{Class: "content_identity", Disposition: RetainIdentity, Owner: "cms", Reason: "stage, audit, and restrictive references require a stable identity", Postcondition: "minimal archived row and permanent retirement tombstone remain"},
		{Class: "processing_ledger", Disposition: RetainIdentity, Owner: "cms", Reason: "append-only stage and artifact evidence is retained", Postcondition: "terminal ledger history is unchanged and stale writes are rejected"},
		{Class: "storage_and_lifecycle_audit", Disposition: RetainIdentity, Owner: "cms", Reason: "storage operation sagas and artifact event evidence are retained for reconciliation", Postcondition: "terminal audit rows remain linked to the retired identity; unresolved sagas block reset"},
		{Class: "source_configuration", Disposition: PreserveShared, Owner: "cms", Reason: "source definitions, credentials, schedules, and checkpoints are outside reset scope", Postcondition: "source configuration and checkpoints are unchanged"},
		{Class: "feed_projection", Disposition: DetachRecompute, Owner: "cms", Reason: "retired items must not remain in active or candidate Pods membership", Postcondition: "all target memberships are detached before object deletion"},
		{Class: "user_or_moderation_data", Disposition: Protected, Owner: "cms", Reason: "interactions, holds, open reports, or flags protect the item", Postcondition: "no protected item is modified"},
		{Class: "redundancy_evidence", Disposition: Blocked, Owner: "cms", Reason: "redundancy family, pair, or fingerprint relationships need their owner to resolve them first", Postcondition: "no target remains in unresolved redundancy evidence"},
		{Class: "active_work", Disposition: Blocked, Owner: "aggregation/media/enrichment", Reason: "nonterminal work must be drained by its owner before reset", Postcondition: "no active producer, upload, job, or lease can republish the target"},
		{Class: "unknown_dependency", Disposition: Blocked, Owner: "unmapped", Reason: "absence of an owner or disposition is not deletion authorization", Postcondition: "every relationship and artifact role is explicitly mapped before approval"},
	}
}

// Snapshot is a bounded, deterministic read set used to invalidate approval
// whenever the lifecycle or media ownership inputs change.
type Snapshot struct {
	TenantID             string               `json:"tenant_id"`
	PublicID             string               `json:"public_id"`
	Type                 models.ContentType   `json:"type"`
	Status               models.ContentStatus `json:"status"`
	IdempotencyKey       string               `json:"idempotency_key"`
	ProcessingGeneration int64                `json:"processing_generation"`
	ProcessingDigest     string               `json:"processing_digest"`
	UpdatedAt            time.Time            `json:"updated_at"`
	ParentContentItemID  string               `json:"parent_content_item_id,omitempty"`
	TranscriptID         string               `json:"transcript_id,omitempty"`
	PlaybackURL          string               `json:"playback_url,omitempty"`
	MediaURL             string               `json:"media_url,omitempty"`
	ThumbnailURL         string               `json:"thumbnail_url,omitempty"`
	RenditionDigest      string               `json:"rendition_digest"`
}

func SnapshotFor(item models.ContentItem) Snapshot {
	value := Snapshot{
		TenantID: item.TenantID, PublicID: item.PublicID.String(), Type: item.Type,
		Status: item.Status, ProcessingGeneration: item.ProcessingGeneration,
		RenditionDigest: item.RenditionDigest, UpdatedAt: item.UpdatedAt.UTC(),
	}
	if item.IdempotencyKey != nil {
		value.IdempotencyKey = strings.TrimSpace(*item.IdempotencyKey)
	}
	if item.ProcessingInputDigest != nil {
		value.ProcessingDigest = *item.ProcessingInputDigest
	}
	if item.ParentContentItemID != nil {
		value.ParentContentItemID = item.ParentContentItemID.String()
	}
	if item.TranscriptID != nil {
		value.TranscriptID = item.TranscriptID.String()
	}
	if item.PlaybackURL != nil {
		value.PlaybackURL = *item.PlaybackURL
	}
	if item.MediaURL != nil {
		value.MediaURL = *item.MediaURL
	}
	if item.ThumbnailURL != nil {
		value.ThumbnailURL = *item.ThumbnailURL
	}
	return value
}

func Hash(value any) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

func ManifestHash(items []Snapshot, decisions map[string][]DataClassDecision, objects map[string][]ObjectIdentity, schemaFingerprint string) (string, error) {
	sorted := append([]Snapshot(nil), items...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].PublicID < sorted[j].PublicID })
	manifest := struct {
		PolicyVersion     int                            `json:"policy_version"`
		SchemaFingerprint string                         `json:"schema_fingerprint"`
		Items             []Snapshot                     `json:"items"`
		Decisions         map[string][]DataClassDecision `json:"decisions"`
		Objects           map[string][]ObjectIdentity    `json:"objects"`
	}{PolicyVersion, schemaFingerprint, sorted, decisions, objects}
	return Hash(manifest)
}

type ObjectIdentity struct {
	StorageTier string `json:"storage_tier"`
	Bucket      string `json:"bucket"`
	ObjectKey   string `json:"object_key"`
	ETag        string `json:"etag"`
	SizeBytes   int64  `json:"size_bytes"`
}

// ArtifactOwnership is the selected item's current CMS registry proof for a
// provider object. Prefix membership alone is intentionally insufficient.
type ArtifactOwnership struct {
	ContentItemID       string
	ParentContentItemID string
	StorageTier         string
	Bucket              string
	ObjectKey           string
	ETag                string
	SizeBytes           int64
	ArtifactRole        string
	ProducerEventID     string
	State               string
}

var knownArtifactRoles = map[string]bool{
	"source": true, "analysis_audio": true, "chapter_media": true,
	"chapter_hls": true, "thumbnail": true, "transcript_segment": true,
	"playback_audio": true, "playback_mp4": true, "delivery_audio": true,
	"delivery_progressive": true, "hls_master": true, "hls_access_master": true,
	"hls_playlist": true, "hls_init": true, "hls_segment": true,
}

func ValidateArtifactManifestCoverage(contentID string, objects []ObjectIdentity, manifests []ArtifactOwnership) error {
	approved := make(map[string]ObjectIdentity, len(objects))
	for _, object := range objects {
		approved[object.StorageTier+"\n"+object.Bucket+"\n"+object.ObjectKey] = object
	}
	covered := make(map[string]bool, len(objects))
	for _, manifest := range manifests {
		if manifest.State == "deleted" {
			continue
		}
		// A manifest directly owned by a child may also carry its source parent
		// for lineage. That parent is not a second object owner. A nil content
		// owner with an explicit parent is the only parent-scoped ownership form.
		ownsItem := manifest.ContentItemID == contentID || (manifest.ContentItemID == "" && manifest.ParentContentItemID == contentID)
		if !ownsItem {
			continue
		}
		identity := manifest.StorageTier + "\n" + manifest.Bucket + "\n" + manifest.ObjectKey
		object, listed := approved[identity]
		if !listed {
			return fmt.Errorf("artifact manifest references an object outside the provider inventory")
		}
		if !knownArtifactRoles[strings.TrimSpace(manifest.ArtifactRole)] || strings.TrimSpace(manifest.ProducerEventID) == "" {
			return fmt.Errorf("artifact manifest has an unmapped owner role or lacks a producer identity")
		}
		if strings.Trim(manifest.ETag, `"`) == "" || strings.Trim(manifest.ETag, `"`) != object.ETag || manifest.SizeBytes != object.SizeBytes {
			return fmt.Errorf("artifact manifest fingerprint differs from provider inventory")
		}
		if covered[identity] {
			return fmt.Errorf("object identity has more than one live artifact owner record")
		}
		covered[identity] = true
	}
	for identity := range approved {
		if !covered[identity] {
			return fmt.Errorf("provider object lacks an exact registered owner")
		}
	}
	return nil
}

func ValidateObjectSet(contentID string, objects []ObjectIdentity) error {
	if len(objects) > MaxObjects {
		return fmt.Errorf("object_count_exceeds_limit")
	}
	seen := make(map[string]struct{}, len(objects))
	for _, object := range objects {
		if object.StorageTier != "primary" && object.StorageTier != "cold" {
			return fmt.Errorf("unknown_storage_tier")
		}
		if strings.TrimSpace(object.Bucket) == "" || strings.TrimSpace(object.ETag) == "" || object.SizeBytes < 0 {
			return fmt.Errorf("incomplete_object_identity")
		}
		prefix := "content/" + contentID + "/"
		if !strings.HasPrefix(object.ObjectKey, prefix) || len(object.ObjectKey) == len(prefix) {
			return fmt.Errorf("object_outside_content_prefix")
		}
		key := object.StorageTier + "\n" + object.Bucket + "\n" + object.ObjectKey
		if _, ok := seen[key]; ok {
			return fmt.Errorf("duplicate_object_identity")
		}
		seen[key] = struct{}{}
	}
	return nil
}

// AdvanceVerificationProbe requires two durable, distinct absence observations.
// Probe one is recorded with the exact deletion receipt; probe two is only
// valid after a delay so late asynchronous writes have a chance to surface.
func AdvanceVerificationProbe(probeCount int, absent bool, now, notBefore time.Time) (int, string) {
	if probeCount != 1 {
		return probeCount, "verification_probe_state_invalid"
	}
	if !absent {
		return probeCount, "verification_objects_present"
	}
	if now.Before(notBefore) {
		return probeCount, "verification_probe_not_due"
	}
	return 2, ""
}
