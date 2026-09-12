package controllers

import (
	"content-management-system/src/models"
	"github.com/google/uuid"
	"time"
)

// Claim credentials are emitted only at the capability-protected claim endpoint,
// never from model serialization used by list/trace APIs. Keep these DTOs
// explicit: embedding a GORM model here has repeatedly reintroduced secret
// fields into ordinary responses when the model gained a new column.
type atomizationUnitClaim struct {
	ID                    uuid.UUID `json:"id"`
	TenantID              string    `json:"tenant_id"`
	GenerationID          uuid.UUID `json:"generation_id"`
	UnitIndex             int       `json:"unit_index"`
	StartMs               int64     `json:"start_ms"`
	EndMs                 int64     `json:"end_ms"`
	PlanDigest            string    `json:"plan_digest"`
	TranscriptSliceDigest string    `json:"transcript_slice_digest"`
	State                 string    `json:"state"`
	ClaimToken            uuid.UUID `json:"claim_token"`
	UnitFenceToken        uuid.UUID `json:"unit_fence_token"`
	// fence_token is retained as a compatibility alias for deployed workers;
	// both values are the same unit-scoped capability, never the outer fence.
	FenceToken     uuid.UUID `json:"fence_token"`
	LeaseExpiresAt time.Time `json:"lease_expires_at"`
	AttemptCount   int       `json:"attempt_count"`
}
type transcriptionUnitClaim struct {
	ID                 uuid.UUID  `json:"id"`
	TenantID           string     `json:"tenant_id"`
	GenerationID       uuid.UUID  `json:"generation_id"`
	SegmentIndex       int        `json:"segment_index"`
	StartMs            int64      `json:"start_ms"`
	EndMs              int64      `json:"end_ms"`
	OverlapMs          int64      `json:"overlap_ms"`
	SourceDigest       string     `json:"source_digest"`
	SegmentDigest      string     `json:"segment_digest"`
	ArtifactManifestID *uuid.UUID `json:"artifact_manifest_id,omitempty"`
	State              string     `json:"state"`
	ClaimToken         uuid.UUID  `json:"claim_token"`
	UnitFenceToken     uuid.UUID  `json:"unit_fence_token"`
	FenceToken         uuid.UUID  `json:"fence_token"`
	LeaseExpiresAt     time.Time  `json:"lease_expires_at"`
	AttemptCount       int        `json:"attempt_count"`
}

func newAtomizationUnitClaim(unit models.AtomizationChapterUnit) atomizationUnitClaim {
	return atomizationUnitClaim{
		ID: unit.PublicID, TenantID: unit.TenantID, GenerationID: unit.GenerationID,
		UnitIndex: unit.UnitIndex, StartMs: unit.StartMs, EndMs: unit.EndMs,
		PlanDigest: unit.PlanDigest, TranscriptSliceDigest: unit.TranscriptSliceDigest,
		State: unit.State, ClaimToken: valueUUID(unit.ClaimToken),
		UnitFenceToken: valueUUID(unit.FenceToken), FenceToken: valueUUID(unit.FenceToken),
		LeaseExpiresAt: valueTime(unit.LeaseExpiresAt), AttemptCount: unit.AttemptCount,
	}
}

func newTranscriptionUnitClaim(unit models.TranscriptionSegmentUnit) transcriptionUnitClaim {
	return transcriptionUnitClaim{
		ID: unit.PublicID, TenantID: unit.TenantID, GenerationID: unit.GenerationID,
		SegmentIndex: unit.SegmentIndex, StartMs: unit.StartMs, EndMs: unit.EndMs,
		OverlapMs: unit.OverlapMs, SourceDigest: unit.SourceDigest,
		SegmentDigest: unit.SegmentDigest, ArtifactManifestID: unit.ArtifactManifestID,
		State: unit.State, ClaimToken: valueUUID(unit.ClaimToken),
		UnitFenceToken: valueUUID(unit.FenceToken), FenceToken: valueUUID(unit.FenceToken),
		LeaseExpiresAt: valueTime(unit.LeaseExpiresAt), AttemptCount: unit.AttemptCount,
	}
}

func valueUUID(value *uuid.UUID) uuid.UUID {
	if value == nil {
		return uuid.Nil
	}
	return *value
}

func valueTime(value *time.Time) time.Time {
	if value == nil {
		return time.Time{}
	}
	return *value
}
