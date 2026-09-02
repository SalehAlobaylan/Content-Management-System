package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

// MediaArtifactManifest is the current ownership projection for one immutable
// object-store artifact. MediaStorageArtifactEvent remains the append-only
// history; this row is the state used by reconciliation and cleanup.
type MediaArtifactManifest struct {
	ID                         uint           `gorm:"primaryKey" json:"-"`
	PublicID                   uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID                   string         `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	ContentItemID              *uuid.UUID     `gorm:"type:uuid;index" json:"content_item_id,omitempty"`
	ParentContentItemID        *uuid.UUID     `gorm:"type:uuid;index" json:"parent_content_item_id,omitempty"`
	AtomizationGenerationID    *uuid.UUID     `gorm:"type:uuid;index" json:"atomization_generation_id,omitempty"`
	AtomizationChapterUnitID   *uuid.UUID     `gorm:"type:uuid;index" json:"atomization_chapter_unit_id,omitempty"`
	TranscriptionGenerationID  *uuid.UUID     `gorm:"type:uuid;index" json:"transcription_generation_id,omitempty"`
	TranscriptionSegmentUnitID *uuid.UUID     `gorm:"type:uuid;index" json:"transcription_segment_unit_id,omitempty"`
	AttemptID                  *uuid.UUID     `gorm:"type:uuid;index" json:"attempt_id,omitempty"`
	ArtifactRole               string         `gorm:"type:varchar(32);not null;index" json:"artifact_role"`
	PackageManifestID          *uuid.UUID     `gorm:"type:uuid;index" json:"package_manifest_id,omitempty"`
	StorageTier                string         `gorm:"type:varchar(16);not null;default:'primary'" json:"storage_tier"`
	Bucket                     string         `gorm:"type:varchar(255);not null" json:"bucket"`
	ObjectKey                  string         `gorm:"type:text;not null" json:"object_key"`
	PublicURL                  string         `gorm:"type:text" json:"public_url,omitempty"`
	ContentType                string         `gorm:"type:varchar(255)" json:"content_type,omitempty"`
	CacheControl               string         `gorm:"type:varchar(255)" json:"cache_control,omitempty"`
	SizeBytes                  int64          `gorm:"type:bigint;not null;default:0" json:"size_bytes"`
	ETag                       string         `gorm:"column:etag;type:varchar(255)" json:"etag,omitempty"`
	SHA256                     string         `gorm:"type:char(64)" json:"sha256,omitempty"`
	DurationMs                 *int64         `gorm:"type:bigint" json:"duration_ms,omitempty"`
	CreatorRole                string         `gorm:"type:varchar(64);not null" json:"creator_role"`
	ProducerEventID            uuid.UUID      `gorm:"type:uuid;not null" json:"producer_event_id"`
	FenceToken                 *uuid.UUID     `gorm:"type:uuid" json:"fence_token,omitempty"`
	InputDigest                string         `gorm:"type:char(64);not null" json:"input_digest"`
	State                      string         `gorm:"type:varchar(24);not null;index" json:"state"`
	RecoveryClass              string         `gorm:"type:varchar(32);not null;default:'recoverable'" json:"recovery_class"`
	VerificationEvidence       datatypes.JSON `gorm:"type:jsonb" json:"verification_evidence,omitempty"`
	TerminalProof              datatypes.JSON `gorm:"type:jsonb" json:"terminal_proof,omitempty"`
	CleanupEligibleAt          *time.Time     `json:"cleanup_eligible_at,omitempty"`
	VerifiedAt                 *time.Time     `json:"verified_at,omitempty"`
	DeletedAt                  *time.Time     `json:"deleted_at,omitempty"`
	CreatedAt                  time.Time      `json:"created_at"`
	UpdatedAt                  time.Time      `json:"updated_at"`
}

func (MediaArtifactManifest) TableName() string { return "media_artifact_manifests" }

type TranscriptionGeneration struct {
	ID                      uint           `gorm:"primaryKey" json:"-"`
	PublicID                uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID                string         `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	ContentItemID           uuid.UUID      `gorm:"type:uuid;not null;index" json:"content_item_id"`
	TranscriptionJobID      *uuid.UUID     `gorm:"type:uuid;index" json:"transcription_job_id,omitempty"`
	InputDigest             string         `gorm:"type:char(64);not null" json:"input_digest"`
	AnalysisAudioManifestID *uuid.UUID     `gorm:"type:uuid;index" json:"analysis_audio_manifest_id,omitempty"`
	Provider                string         `gorm:"type:varchar(64)" json:"provider,omitempty"`
	Model                   string         `gorm:"type:varchar(128)" json:"model,omitempty"`
	Language                string         `gorm:"type:varchar(16)" json:"language,omitempty"`
	State                   string         `gorm:"type:varchar(24);not null;index" json:"state"`
	TotalSegments           int            `gorm:"not null;default:0" json:"total_segments"`
	CompletedSegments       int            `gorm:"not null;default:0" json:"completed_segments"`
	MergedTranscriptID      *uuid.UUID     `gorm:"type:uuid;index" json:"merged_transcript_id,omitempty"`
	ClaimOwner              string         `json:"claim_owner,omitempty"`
	ClaimToken              *uuid.UUID     `gorm:"type:uuid" json:"-"`
	FenceToken              *uuid.UUID     `gorm:"type:uuid" json:"fence_token,omitempty"`
	ClaimExpiresAt          *time.Time     `json:"claim_expires_at,omitempty"`
	TerminalProof           datatypes.JSON `gorm:"type:jsonb" json:"terminal_proof,omitempty"`
	FailureClass            string         `json:"failure_class,omitempty"`
	CreatedAt               time.Time      `json:"created_at"`
	UpdatedAt               time.Time      `json:"updated_at"`
}

func (TranscriptionGeneration) TableName() string { return "transcription_generations" }

type TranscriptionSegmentUnit struct {
	ID                 uint           `gorm:"primaryKey" json:"-"`
	PublicID           uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID           string         `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	GenerationID       uuid.UUID      `gorm:"type:uuid;not null;index" json:"generation_id"`
	SegmentIndex       int            `gorm:"not null" json:"segment_index"`
	StartMs            int64          `gorm:"type:bigint;not null" json:"start_ms"`
	EndMs              int64          `gorm:"type:bigint;not null" json:"end_ms"`
	OverlapMs          int64          `gorm:"type:bigint;not null;default:0" json:"overlap_ms"`
	SourceDigest       string         `gorm:"type:char(64);not null" json:"source_digest"`
	SegmentDigest      string         `gorm:"type:char(64);not null" json:"segment_digest"`
	ArtifactManifestID *uuid.UUID     `gorm:"type:uuid;index" json:"artifact_manifest_id,omitempty"`
	State              string         `gorm:"type:varchar(24);not null;index" json:"state"`
	NotBeforeAt        *time.Time     `json:"not_before_at,omitempty"`
	ClaimOwner         string         `json:"claim_owner,omitempty"`
	ClaimToken         *uuid.UUID     `gorm:"type:uuid" json:"-"`
	FenceToken         *uuid.UUID     `gorm:"type:uuid" json:"fence_token,omitempty"`
	LeaseExpiresAt     *time.Time     `json:"lease_expires_at,omitempty"`
	AttemptCount       int            `gorm:"not null;default:0" json:"attempt_count"`
	FailureClass       string         `json:"failure_class,omitempty"`
	TranscriptText     string         `gorm:"type:text" json:"transcript_text,omitempty"`
	TranscriptSegments datatypes.JSON `gorm:"type:jsonb" json:"transcript_segments,omitempty"`
	ResultDigest       string         `gorm:"type:char(64)" json:"result_digest,omitempty"`
	TerminalProof      datatypes.JSON `gorm:"type:jsonb" json:"terminal_proof,omitempty"`
	CreatedAt          time.Time      `json:"created_at"`
	UpdatedAt          time.Time      `json:"updated_at"`
}

func (TranscriptionSegmentUnit) TableName() string { return "transcription_segment_units" }

type AtomizationGeneration struct {
	ID                  uint           `gorm:"primaryKey" json:"-"`
	PublicID            uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID            string         `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	ParentContentItemID uuid.UUID      `gorm:"type:uuid;not null;index" json:"parent_content_item_id"`
	WorkRequestID       uuid.UUID      `gorm:"type:uuid;not null;index" json:"work_request_id"`
	GenerationNumber    int            `gorm:"not null" json:"generation_number"`
	TranscriptDigest    string         `gorm:"type:char(64);not null" json:"transcript_digest"`
	PolicyDigest        string         `gorm:"type:char(64);not null" json:"policy_digest"`
	InputDigest         string         `gorm:"type:char(64);not null" json:"input_digest"`
	PlanDigest          string         `gorm:"type:char(64);not null" json:"plan_digest"`
	ExpectedUnits       int            `gorm:"not null;default:0" json:"expected_units"`
	CompletedUnits      int            `gorm:"not null;default:0" json:"completed_units"`
	CoverageDigest      string         `gorm:"type:char(64)" json:"coverage_digest,omitempty"`
	State               string         `gorm:"type:varchar(24);not null;index" json:"state"`
	ActivationAt        *time.Time     `json:"activation_at,omitempty"`
	TerminalProof       datatypes.JSON `gorm:"type:jsonb" json:"terminal_proof,omitempty"`
	CreatedAt           time.Time      `json:"created_at"`
	UpdatedAt           time.Time      `json:"updated_at"`
}

func (AtomizationGeneration) TableName() string { return "atomization_generations" }

type AtomizationChapterUnit struct {
	ID                     uint           `gorm:"primaryKey" json:"-"`
	PublicID               uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID               string         `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	GenerationID           uuid.UUID      `gorm:"type:uuid;not null;index" json:"generation_id"`
	UnitIndex              int            `gorm:"not null" json:"unit_index"`
	StartMs                int64          `gorm:"type:bigint;not null" json:"start_ms"`
	EndMs                  int64          `gorm:"type:bigint;not null" json:"end_ms"`
	PlanDigest             string         `gorm:"type:char(64);not null" json:"plan_digest"`
	TranscriptSliceDigest  string         `gorm:"type:char(64);not null" json:"transcript_slice_digest"`
	State                  string         `gorm:"type:varchar(24);not null;index" json:"state"`
	NotBeforeAt            *time.Time     `json:"not_before_at,omitempty"`
	ClaimOwner             string         `json:"claim_owner,omitempty"`
	ClaimToken             *uuid.UUID     `gorm:"type:uuid" json:"-"`
	FenceToken             *uuid.UUID     `gorm:"type:uuid" json:"fence_token,omitempty"`
	LeaseExpiresAt         *time.Time     `json:"lease_expires_at,omitempty"`
	AttemptCount           int            `gorm:"not null;default:0" json:"attempt_count"`
	FailureClass           string         `json:"failure_class,omitempty"`
	ArtifactManifestIDs    datatypes.JSON `gorm:"type:jsonb" json:"artifact_manifest_ids,omitempty"`
	CandidateContentItemID *uuid.UUID     `gorm:"type:uuid;index" json:"candidate_content_item_id,omitempty"`
	Result                 datatypes.JSON `gorm:"type:jsonb" json:"result,omitempty"`
	TerminalProof          datatypes.JSON `gorm:"type:jsonb" json:"terminal_proof,omitempty"`
	CreatedAt              time.Time      `json:"created_at"`
	UpdatedAt              time.Time      `json:"updated_at"`
}

func (AtomizationChapterUnit) TableName() string { return "atomization_chapter_units" }
