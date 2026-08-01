package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

const (
	SourceRunRequested = "requested"
	SourceRunAccepted  = "accepted"
	SourceRunRunning   = "running"
	SourceRunCompleted = "completed"
	SourceRunFailed    = "failed"
)

// SourceRunRequest is the CMS-owned handoff record. It is created before CMS
// asks Aggregation to enqueue work, so a missing acceptance is observable and
// never inferred from a dashboard label or a direct BullMQ read.
type SourceRunRequest struct {
	ID                 uint           `gorm:"primaryKey" json:"-"`
	PublicID           uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID           string         `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	ContentSourceID    uuid.UUID      `gorm:"type:uuid;not null;index" json:"content_source_id"`
	SourceSuggestionID *uuid.UUID     `gorm:"type:uuid;index" json:"source_suggestion_id,omitempty"`
	RequestedBy        string         `json:"requested_by"`
	RequestedByActorID string         `json:"requested_by_actor_id,omitempty"`
	State              string         `json:"state"`
	AggregationJobID   string         `json:"aggregation_job_id,omitempty"`
	OperatorPlanID     *uuid.UUID     `gorm:"type:uuid;index" json:"operator_plan_id,omitempty"`
	OperatorStepID     *uuid.UUID     `gorm:"type:uuid;index" json:"operator_step_id,omitempty"`
	CorrelationID      string         `json:"correlation_id"`
	IdempotencyKey     string         `json:"idempotency_key"`
	RequestedAt        time.Time      `json:"requested_at"`
	AcceptedAt         *time.Time     `json:"accepted_at,omitempty"`
	StartedAt          *time.Time     `json:"started_at,omitempty"`
	FinishedAt         *time.Time     `json:"finished_at,omitempty"`
	FailureClass       string         `json:"failure_class,omitempty"`
	FailureSummary     string         `json:"failure_summary,omitempty"`
	Metadata           datatypes.JSON `gorm:"type:jsonb" json:"metadata"`
	CreatedAt          time.Time      `json:"created_at"`
	UpdatedAt          time.Time      `json:"updated_at"`
}

func (SourceRunRequest) TableName() string { return "source_run_requests" }

// ContentProcessingEvent is a normalized, immutable cross-service lineage
// event. It intentionally has no cascade foreign keys: processing evidence
// survives routine lifecycle cleanup and cannot be rewritten in place.
type ContentProcessingEvent struct {
	ID                 uint           `gorm:"primaryKey" json:"-"`
	PublicID           uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID           string         `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	ContentSourceID    *uuid.UUID     `gorm:"type:uuid;index" json:"content_source_id,omitempty"`
	SourceRunRequestID *uint          `json:"-"`
	ContentItemID      *uuid.UUID     `gorm:"type:uuid;index" json:"content_item_id,omitempty"`
	Stage              string         `json:"stage"`
	State              string         `json:"state"`
	Producer           string         `json:"producer"`
	JobID              string         `json:"job_id,omitempty"`
	CorrelationID      string         `json:"correlation_id,omitempty"`
	IdempotencyKey     string         `json:"idempotency_key,omitempty"`
	EventClass         string         `json:"event_class"`
	ErrorClass         string         `json:"error_class,omitempty"`
	Payload            datatypes.JSON `gorm:"type:jsonb" json:"payload"`
	OccurredAt         time.Time      `json:"occurred_at"`
	CreatedAt          time.Time      `json:"created_at"`
}

func (ContentProcessingEvent) TableName() string { return "content_processing_events" }
