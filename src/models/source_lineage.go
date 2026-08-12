package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

const (
	SourceRunRequested            = "requested"
	SourceRunAccepted             = "accepted"
	SourceRunRunning              = "running"
	SourceRunVerificationRequired = "verification_required"
	SourceRunCompleted            = "completed" // legacy read-compatible terminal state
	SourceRunSucceeded            = "succeeded"
	SourceRunPartial              = "partial"
	SourceRunBlocked              = "blocked"
	SourceRunFailed               = "failed"
	SourceRunCancelled            = "cancelled"
	SourceRunExpired              = "expired"

	SourceRunManifestOpen    = "open"
	SourceRunManifestSealing = "sealing"
	SourceRunManifestSealed  = "sealed"
)

// SourceRunRequest is the CMS-owned handoff record. It is created before CMS
// asks Aggregation to enqueue work, so a missing acceptance is observable and
// never inferred from a dashboard label or a direct BullMQ read.
type SourceRunRequest struct {
	ID                    uint           `gorm:"primaryKey" json:"-"`
	PublicID              uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID              string         `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	ContentSourceID       uuid.UUID      `gorm:"type:uuid;not null;index" json:"content_source_id"`
	SourceSuggestionID    *uuid.UUID     `gorm:"type:uuid;index" json:"source_suggestion_id,omitempty"`
	RequestedBy           string         `json:"requested_by"`
	RequestedByActorID    string         `json:"requested_by_actor_id,omitempty"`
	State                 string         `json:"state"`
	AggregationJobID      string         `json:"aggregation_job_id,omitempty"`
	OperatorPlanID        *uuid.UUID     `gorm:"type:uuid;index" json:"operator_plan_id,omitempty"`
	OperatorStepID        *uuid.UUID     `gorm:"type:uuid;index" json:"operator_step_id,omitempty"`
	CorrelationID         string         `json:"correlation_id"`
	IdempotencyKey        string         `json:"idempotency_key"`
	Lane                  string         `gorm:"type:varchar(32);not null;default:'legacy'" json:"lane"`
	Purpose               string         `gorm:"type:varchar(48);not null;default:'legacy'" json:"purpose"`
	ParentRequestID       *uuid.UUID     `gorm:"type:uuid;index" json:"parent_request_id,omitempty"`
	FailedScope           datatypes.JSON `gorm:"type:jsonb;not null;default:'{}'" json:"failed_scope"`
	PolicyFingerprint     string         `gorm:"type:varchar(128)" json:"policy_fingerprint,omitempty"`
	EvidenceFingerprint   string         `gorm:"type:varchar(128)" json:"evidence_fingerprint,omitempty"`
	ArgumentFingerprint   string         `gorm:"type:varchar(128)" json:"argument_fingerprint,omitempty"`
	CadenceWindowStart    *time.Time     `gorm:"type:timestamptz" json:"cadence_window_start,omitempty"`
	NotBeforeAt           *time.Time     `gorm:"type:timestamptz;index" json:"not_before_at,omitempty"`
	ExpiresAt             *time.Time     `gorm:"type:timestamptz;index" json:"expires_at,omitempty"`
	DeadlineAt            *time.Time     `gorm:"type:timestamptz;index" json:"deadline_at,omitempty"`
	NextDispatchAt        *time.Time     `gorm:"type:timestamptz;index" json:"next_dispatch_at,omitempty"`
	RootExecutionUnitID   *uuid.UUID     `gorm:"type:uuid;index" json:"root_execution_unit_id,omitempty"`
	ManifestState         string         `gorm:"type:varchar(16);not null;default:'open'" json:"manifest_state"`
	ManifestVersion       int64          `gorm:"not null;default:0" json:"manifest_version"`
	ManifestSealedAt      *time.Time     `gorm:"type:timestamptz" json:"manifest_sealed_at,omitempty"`
	ExpectedUnitCount     int            `gorm:"not null;default:0" json:"expected_unit_count"`
	CompletedUnitCount    int            `gorm:"not null;default:0" json:"completed_unit_count"`
	ExpectedPageCount     int            `gorm:"not null;default:0" json:"expected_page_count"`
	CompletedPageCount    int            `gorm:"not null;default:0" json:"completed_page_count"`
	ExpectedBatchCount    int            `gorm:"not null;default:0" json:"expected_batch_count"`
	CompletedBatchCount   int            `gorm:"not null;default:0" json:"completed_batch_count"`
	WorkloadCap           int            `gorm:"not null;default:0" json:"workload_cap"`
	ItemCap               int            `gorm:"not null;default:0" json:"item_cap"`
	ByteCap               int64          `gorm:"not null;default:0" json:"byte_cap"`
	ProviderCallCap       int            `gorm:"not null;default:0" json:"provider_call_cap"`
	ReservedWorkload      int            `gorm:"not null;default:0" json:"reserved_workload"`
	ReservedItems         int            `gorm:"not null;default:0" json:"reserved_items"`
	ReservedBytes         int64          `gorm:"not null;default:0" json:"reserved_bytes"`
	ReservedProviderCalls int            `gorm:"not null;default:0" json:"reserved_provider_calls"`
	ConsumedWorkload      int            `gorm:"not null;default:0" json:"consumed_workload"`
	ConsumedItems         int            `gorm:"not null;default:0" json:"consumed_items"`
	ConsumedBytes         int64          `gorm:"not null;default:0" json:"consumed_bytes"`
	ConsumedProviderCalls int            `gorm:"not null;default:0" json:"consumed_provider_calls"`
	ReleasedWorkload      int            `gorm:"not null;default:0" json:"released_workload"`
	ReleasedItems         int            `gorm:"not null;default:0" json:"released_items"`
	ReleasedBytes         int64          `gorm:"not null;default:0" json:"released_bytes"`
	ReleasedProviderCalls int            `gorm:"not null;default:0" json:"released_provider_calls"`
	BudgetState           string         `gorm:"type:varchar(24);not null;default:'legacy_unknown'" json:"budget_state"`
	BudgetSettledAt       *time.Time     `gorm:"type:timestamptz" json:"budget_settled_at,omitempty"`
	FinalizedAt           *time.Time     `gorm:"type:timestamptz" json:"finalized_at,omitempty"`
	VerifiedAt            *time.Time     `gorm:"type:timestamptz" json:"verified_at,omitempty"`
	EvidenceState         string         `gorm:"type:varchar(32);not null;default:'legacy_unknown'" json:"evidence_state"`
	RequestedAt           time.Time      `json:"requested_at"`
	AcceptedAt            *time.Time     `json:"accepted_at,omitempty"`
	StartedAt             *time.Time     `json:"started_at,omitempty"`
	FinishedAt            *time.Time     `json:"finished_at,omitempty"`
	FailureClass          string         `json:"failure_class,omitempty"`
	FailureSummary        string         `json:"failure_summary,omitempty"`
	Metadata              datatypes.JSON `gorm:"type:jsonb" json:"metadata"`
	CreatedAt             time.Time      `json:"created_at"`
	UpdatedAt             time.Time      `json:"updated_at"`
}

func (SourceRunRequest) TableName() string { return "source_run_requests" }

// ContentProcessingEvent is a normalized, immutable cross-service lineage
// event. It intentionally has no cascade foreign keys: processing evidence
// survives routine lifecycle cleanup and cannot be rewritten in place.
type ContentProcessingEvent struct {
	ID                 uint       `gorm:"primaryKey" json:"-"`
	PublicID           uuid.UUID  `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID           string     `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	ContentSourceID    *uuid.UUID `gorm:"type:uuid;index" json:"content_source_id,omitempty"`
	SourceRunRequestID *uint      `json:"-"`
	ContentItemID      *uuid.UUID `gorm:"type:uuid;index" json:"content_item_id,omitempty"`
	Stage              string     `json:"stage"`
	State              string     `json:"state"`
	Producer           string     `json:"producer"`
	JobID              string     `json:"job_id,omitempty"`
	CorrelationID      string     `json:"correlation_id,omitempty"`
	IdempotencyKey     string     `json:"idempotency_key,omitempty"`
	// Pipeline repair correlation is optional for normal lifecycle evidence.
	// When present, all fields are an inseparable CMS-issued effect identity.
	PipelineRepairRequestID  *uuid.UUID     `gorm:"type:uuid;index" json:"pipeline_repair_request_id,omitempty"`
	PipelineRepairAttemptID  *uuid.UUID     `gorm:"type:uuid;index" json:"pipeline_repair_attempt_id,omitempty"`
	PipelineRepairFenceToken *uuid.UUID     `gorm:"type:uuid" json:"pipeline_repair_fence_token,omitempty"`
	ExpectedItemUpdatedAt    *time.Time     `gorm:"type:timestamptz" json:"expected_item_updated_at,omitempty"`
	ProducerEventID          *uuid.UUID     `gorm:"type:uuid" json:"producer_event_id,omitempty"`
	EffectInputDigest        string         `gorm:"type:varchar(128)" json:"effect_input_digest,omitempty"`
	ExecutionOwner           string         `gorm:"type:varchar(64)" json:"execution_owner,omitempty"`
	EventClass               string         `json:"event_class"`
	ErrorClass               string         `json:"error_class,omitempty"`
	Payload                  datatypes.JSON `gorm:"type:jsonb" json:"payload"`
	OccurredAt               time.Time      `json:"occurred_at"`
	CreatedAt                time.Time      `json:"created_at"`
}

func (ContentProcessingEvent) TableName() string { return "content_processing_events" }
