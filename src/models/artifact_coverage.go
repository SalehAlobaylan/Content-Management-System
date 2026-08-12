package models

import (
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"time"
)

type ArtifactCoverageRequest struct {
	ID                      uint           `gorm:"primaryKey" json:"-"`
	PublicID                uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID                string         `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	ContentItemID           uuid.UUID      `gorm:"type:uuid;not null;index" json:"content_item_id"`
	ItemUpdatedAt           time.Time      `gorm:"type:timestamptz;not null" json:"item_updated_at"`
	Artifact                string         `gorm:"type:varchar(48);not null" json:"artifact"`
	Owner                   string         `gorm:"type:varchar(16);not null" json:"owner"`
	State                   string         `gorm:"type:varchar(24);not null;index" json:"state"`
	ActionRequestID         *uuid.UUID     `gorm:"type:uuid;index" json:"action_request_id,omitempty"`
	ApprovedBy              string         `json:"approved_by,omitempty"`
	ApprovedAt              *time.Time     `json:"approved_at,omitempty"`
	EvidenceDigest          string         `json:"evidence_digest"`
	InputDigest             string         `json:"input_digest"`
	IdempotencyKey          string         `json:"idempotency_key"`
	ClaimOwner              string         `json:"claim_owner,omitempty"`
	ClaimToken              *uuid.UUID     `json:"-"`
	FenceToken              *uuid.UUID     `json:"-"`
	ClaimEpoch              int64          `json:"claim_epoch"`
	ClaimExpiresAt          *time.Time     `json:"claim_expires_at,omitempty"`
	EffectStartedAt         *time.Time     `json:"effect_started_at,omitempty"`
	CancellationRequestedAt *time.Time     `json:"cancellation_requested_at,omitempty"`
	AcceptedAt              *time.Time     `json:"accepted_at,omitempty"`
	VerifiedAt              *time.Time     `json:"verified_at,omitempty"`
	AcceptanceProof         datatypes.JSON `gorm:"type:jsonb" json:"acceptance_proof"`
	TerminalProof           datatypes.JSON `gorm:"type:jsonb" json:"terminal_proof"`
	AffectedSubjects        datatypes.JSON `gorm:"type:jsonb" json:"affected_subjects"`
	DeepLinks               datatypes.JSON `gorm:"type:jsonb" json:"deep_links"`
	FailureClass            string         `json:"failure_class,omitempty"`
	CreatedAt               time.Time      `json:"created_at"`
	UpdatedAt               time.Time      `json:"updated_at"`
	FinishedAt              *time.Time     `json:"finished_at,omitempty"`
}

func (ArtifactCoverageRequest) TableName() string { return "artifact_coverage_requests" }

type ArtifactCoverageAttempt struct {
	ID                 uint       `gorm:"primaryKey" json:"-"`
	PublicID           uuid.UUID  `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID           string     `json:"tenant_id"`
	RequestID          uuid.UUID  `json:"request_id"`
	AttemptNumber      int        `json:"attempt_number"`
	Owner              string     `json:"owner"`
	State              string     `json:"state"`
	ClaimToken         uuid.UUID  `json:"-"`
	FenceToken         uuid.UUID  `json:"fence_token"`
	InputDigest        string     `json:"input_digest"`
	DeterministicJobID string     `json:"deterministic_job_id"`
	LeaseExpiresAt     time.Time  `json:"lease_expires_at"`
	HeartbeatAt        time.Time  `json:"heartbeat_at"`
	EffectStartedAt    *time.Time `json:"effect_started_at,omitempty"`
	AcceptedAt         *time.Time `json:"accepted_at,omitempty"`
	FinishedAt         *time.Time `json:"finished_at,omitempty"`
	AdoptionCount      int        `json:"adoption_count"`
	LastAdoptedAt      *time.Time `json:"last_adopted_at,omitempty"`
	CreatedAt          time.Time  `json:"created_at"`
	UpdatedAt          time.Time  `json:"updated_at"`
}

func (ArtifactCoverageAttempt) TableName() string { return "artifact_coverage_attempts" }

type ArtifactCoverageReceipt struct {
	ID              uint           `gorm:"primaryKey" json:"-"`
	PublicID        uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID        string         `json:"tenant_id"`
	RequestID       uuid.UUID      `json:"request_id"`
	AttemptID       uuid.UUID      `json:"attempt_id"`
	Owner           string         `json:"owner"`
	Artifact        string         `json:"artifact"`
	ProducerEventID string         `json:"producer_event_id"`
	FenceToken      uuid.UUID      `json:"fence_token"`
	Outcome         string         `json:"outcome"`
	PayloadDigest   string         `json:"payload_digest"`
	ObservedAt      time.Time      `json:"observed_at"`
	Payload         datatypes.JSON `gorm:"type:jsonb" json:"payload"`
	CreatedAt       time.Time      `json:"created_at"`
}

func (ArtifactCoverageReceipt) TableName() string { return "artifact_coverage_receipts" }

type ArtifactCoverageEvent struct {
	ID         uint           `gorm:"primaryKey" json:"-"`
	PublicID   uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID   string         `json:"tenant_id"`
	RequestID  uuid.UUID      `json:"request_id"`
	Sequence   int64          `json:"sequence"`
	EventType  string         `json:"event_type"`
	Payload    datatypes.JSON `gorm:"type:jsonb" json:"payload"`
	OccurredAt time.Time      `json:"occurred_at"`
}

func (ArtifactCoverageEvent) TableName() string { return "artifact_coverage_events" }

type ArtifactCoverageBudgetReservation struct {
	ID             uint       `gorm:"primaryKey" json:"-"`
	PublicID       uuid.UUID  `json:"id"`
	TenantID       string     `json:"tenant_id"`
	RequestID      uuid.UUID  `json:"request_id"`
	ActionKey      string     `json:"action_key"`
	Unit           string     `json:"unit"`
	ReservedAmount float64    `json:"reserved_amount"`
	SettledAmount  *float64   `json:"settled_amount,omitempty"`
	State          string     `json:"state"`
	EvidenceDigest string     `json:"evidence_digest"`
	ReservedAt     time.Time  `json:"reserved_at"`
	SettledAt      *time.Time `json:"settled_at,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
}

func (ArtifactCoverageBudgetReservation) TableName() string {
	return "artifact_coverage_budget_reservations"
}
