package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

const (
	PipelineRepairAwaitingApproval = "awaiting_approval"
	PipelineRepairQueued           = "queued"
	PipelineRepairClaimed          = "claimed"
	PipelineRepairRunning          = "running"
	PipelineRepairVerifying        = "verifying"
	PipelineRepairSucceeded        = "succeeded"
	PipelineRepairFailed           = "failed"
	PipelineRepairCancelled        = "cancelled"
	PipelineRepairUncertain        = "uncertain"

	PipelineStageMediaDownload  = "media_download"
	PipelineStageMediaTranscode = "media_transcode"
	PipelineStageMediaThumbnail = "media_thumbnail"
	PipelineStageTextEmbedding  = "text_embedding"
)

// PipelineRepairRequest is the CMS-authoritative command for one exact
// content-item version and stage. It is intentionally not a retry of a row or
// queue: the prior worker's evidence and lease decide whether it is admissible.
type PipelineRepairRequest struct {
	ID                       uint           `gorm:"primaryKey" json:"-"`
	PublicID                 uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID                 string         `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	ContentItemID            uuid.UUID      `gorm:"type:uuid;not null;index" json:"content_item_id"`
	ActionRequestID          *uuid.UUID     `gorm:"type:uuid;uniqueIndex" json:"action_request_id,omitempty"`
	ExpectedItemUpdatedAt    time.Time      `gorm:"type:timestamptz;not null" json:"expected_item_updated_at"`
	ExpectedStatus           string         `gorm:"type:varchar(20);not null" json:"expected_status"`
	Stage                    string         `gorm:"type:varchar(48);not null" json:"stage"`
	SourceRunRequestID       *uint          `json:"-"`
	PriorStageEvidenceDigest string         `gorm:"type:varchar(128);not null" json:"prior_stage_evidence_digest"`
	RepairClass              string         `gorm:"type:varchar(32);not null" json:"repair_class"`
	IdempotencyKey           string         `gorm:"type:varchar(255);not null" json:"idempotency_key"`
	DeterministicJobID       string         `gorm:"type:varchar(255);not null" json:"deterministic_job_id"`
	EffectInputDigest        string         `gorm:"type:varchar(128)" json:"effect_input_digest,omitempty"`
	EffectProducerEventID    *uuid.UUID     `gorm:"type:uuid" json:"effect_producer_event_id,omitempty"`
	State                    string         `gorm:"type:varchar(32);not null;index" json:"state"`
	ApprovedBy               string         `gorm:"type:varchar(128)" json:"approved_by,omitempty"`
	ApprovedAt               *time.Time     `gorm:"type:timestamptz" json:"approved_at,omitempty"`
	ClaimOwner               string         `gorm:"type:varchar(128)" json:"claim_owner,omitempty"`
	ClaimToken               *uuid.UUID     `gorm:"type:uuid" json:"-"`
	ClaimEpoch               int64          `gorm:"not null" json:"claim_epoch"`
	ClaimExpiresAt           *time.Time     `gorm:"type:timestamptz" json:"claim_expires_at,omitempty"`
	CancellationRequestedAt  *time.Time     `gorm:"type:timestamptz" json:"cancellation_requested_at,omitempty"`
	BeforeEffects            datatypes.JSON `gorm:"type:jsonb" json:"before_effects"`
	PlannedEffects           datatypes.JSON `gorm:"type:jsonb" json:"planned_effects"`
	AfterEffects             datatypes.JSON `gorm:"type:jsonb" json:"after_effects"`
	VerifiedEffects          datatypes.JSON `gorm:"type:jsonb" json:"verified_effects"`
	AffectedSubjects         datatypes.JSON `gorm:"type:jsonb" json:"affected_subjects"`
	DeepLinks                datatypes.JSON `gorm:"type:jsonb" json:"deep_links"`
	FailureClass             string         `gorm:"type:varchar(64)" json:"failure_class,omitempty"`
	TerminalProof            datatypes.JSON `gorm:"type:jsonb" json:"terminal_proof"`
	CreatedAt                time.Time      `json:"created_at"`
	UpdatedAt                time.Time      `json:"updated_at"`
	FinishedAt               *time.Time     `gorm:"type:timestamptz" json:"finished_at,omitempty"`
}

func (PipelineRepairRequest) TableName() string { return "pipeline_repair_requests" }

type PipelineRepairAttempt struct {
	ID              uint       `gorm:"primaryKey" json:"-"`
	PublicID        uuid.UUID  `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID        string     `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	RepairRequestID uuid.UUID  `gorm:"type:uuid;not null;index" json:"repair_request_id"`
	AttemptNumber   int        `gorm:"not null" json:"attempt_number"`
	State           string     `gorm:"type:varchar(32);not null" json:"state"`
	FenceToken      uuid.UUID  `gorm:"type:uuid;not null" json:"fence_token"`
	OwnerProtocol   string     `gorm:"type:varchar(64);not null" json:"owner_protocol"`
	StartedAt       *time.Time `gorm:"type:timestamptz" json:"started_at,omitempty"`
	EffectStartedAt *time.Time `gorm:"type:timestamptz" json:"effect_started_at,omitempty"`
	FinishedAt      *time.Time `gorm:"type:timestamptz" json:"finished_at,omitempty"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
}

func (PipelineRepairAttempt) TableName() string { return "pipeline_repair_attempts" }

type PipelineRepairEvent struct {
	ID              uint           `gorm:"primaryKey" json:"-"`
	PublicID        uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID        string         `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	RepairRequestID uuid.UUID      `gorm:"type:uuid;not null;index" json:"repair_request_id"`
	AttemptID       *uuid.UUID     `gorm:"type:uuid" json:"attempt_id,omitempty"`
	Sequence        int64          `gorm:"not null" json:"sequence"`
	EventKey        string         `gorm:"type:varchar(255);not null" json:"event_key"`
	EventType       string         `gorm:"type:varchar(48);not null" json:"event_type"`
	Payload         datatypes.JSON `gorm:"type:jsonb;not null" json:"payload"`
	OccurredAt      time.Time      `gorm:"type:timestamptz;not null" json:"occurred_at"`
	CreatedAt       time.Time      `json:"created_at"`
}

func (PipelineRepairEvent) TableName() string { return "pipeline_repair_events" }

type PipelineStageLease struct {
	ID                 uint       `gorm:"primaryKey" json:"-"`
	PublicID           uuid.UUID  `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID           string     `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	ContentItemID      uuid.UUID  `gorm:"type:uuid;not null;index" json:"content_item_id"`
	ItemUpdatedAt      time.Time  `gorm:"type:timestamptz;not null" json:"item_updated_at"`
	Stage              string     `gorm:"type:varchar(48);not null" json:"stage"`
	ExecutionOwner     string     `gorm:"type:varchar(64);not null" json:"execution_owner"`
	RepairRequestID    *uuid.UUID `gorm:"type:uuid" json:"repair_request_id,omitempty"`
	DeterministicJobID string     `gorm:"type:varchar(255);not null" json:"deterministic_job_id"`
	State              string     `gorm:"type:varchar(24);not null" json:"state"`
	LeaseToken         uuid.UUID  `gorm:"type:uuid;not null" json:"-"`
	FenceToken         uuid.UUID  `gorm:"type:uuid;not null" json:"fence_token"`
	LeaseEpoch         int64      `gorm:"not null" json:"lease_epoch"`
	LeaseExpiresAt     time.Time  `gorm:"type:timestamptz;not null" json:"lease_expires_at"`
	HeartbeatAt        time.Time  `gorm:"type:timestamptz;not null" json:"heartbeat_at"`
	EffectStartedAt    *time.Time `gorm:"type:timestamptz" json:"effect_started_at,omitempty"`
	TerminalAt         *time.Time `gorm:"type:timestamptz" json:"terminal_at,omitempty"`
	CreatedAt          time.Time  `json:"created_at"`
	UpdatedAt          time.Time  `json:"updated_at"`
}

func (PipelineStageLease) TableName() string { return "pipeline_stage_leases" }
