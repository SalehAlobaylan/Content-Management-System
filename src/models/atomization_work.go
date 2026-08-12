package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

type AtomizationWorkRequest struct {
	ID                      uint           `gorm:"primaryKey" json:"-"`
	PublicID                uuid.UUID      `json:"id"`
	TenantID                string         `json:"tenant_id"`
	ParentContentItemID     uuid.UUID      `json:"parent_content_item_id"`
	ParentUpdatedAt         time.Time      `json:"parent_updated_at"`
	TranscriptID            uuid.UUID      `json:"transcript_id"`
	TranscriptFingerprint   string         `json:"transcript_fingerprint"`
	PolicyHash              string         `json:"policy_hash"`
	InputFingerprint        string         `json:"input_fingerprint"`
	ActionRequestID         *uuid.UUID     `json:"action_request_id,omitempty"`
	State                   string         `json:"state"`
	ClaimOwner              string         `json:"claim_owner,omitempty"`
	ClaimToken              *uuid.UUID     `json:"-"`
	FenceToken              *uuid.UUID     `json:"-"`
	ClaimEpoch              int64          `json:"claim_epoch"`
	ClaimExpiresAt          *time.Time     `json:"claim_expires_at,omitempty"`
	EffectStartedAt         *time.Time     `json:"effect_started_at,omitempty"`
	CancellationRequestedAt *time.Time     `json:"cancellation_requested_at,omitempty"`
	Checkpoints             datatypes.JSON `json:"checkpoints"`
	TerminalProof           datatypes.JSON `json:"terminal_proof"`
	FailureClass            string         `json:"failure_class,omitempty"`
	ApprovedBy              string         `json:"approved_by"`
	ApprovedAt              time.Time      `json:"approved_at"`
	FinishedAt              *time.Time     `json:"finished_at,omitempty"`
	CreatedAt               time.Time      `json:"created_at"`
	UpdatedAt               time.Time      `json:"updated_at"`
}

func (AtomizationWorkRequest) TableName() string { return "atomization_work_requests" }

type AtomizationWorkAttempt struct {
	ID                 uint       `gorm:"primaryKey" json:"-"`
	PublicID           uuid.UUID  `json:"id"`
	TenantID           string     `json:"tenant_id"`
	RequestID          uuid.UUID  `json:"request_id"`
	AttemptNumber      int        `json:"attempt_number"`
	State              string     `json:"state"`
	ClaimToken         uuid.UUID  `json:"-"`
	FenceToken         uuid.UUID  `json:"fence_token"`
	DeterministicJobID string     `json:"deterministic_job_id"`
	LeaseExpiresAt     time.Time  `json:"lease_expires_at"`
	HeartbeatAt        time.Time  `json:"heartbeat_at"`
	EffectStartedAt    *time.Time `json:"effect_started_at,omitempty"`
	FinishedAt         *time.Time `json:"finished_at,omitempty"`
	CreatedAt          time.Time  `json:"created_at"`
	UpdatedAt          time.Time  `json:"updated_at"`
}

func (AtomizationWorkAttempt) TableName() string { return "atomization_work_attempts" }

type AtomizationWorkEvent struct {
	ID         uint           `gorm:"primaryKey" json:"-"`
	PublicID   uuid.UUID      `json:"id"`
	TenantID   string         `json:"tenant_id"`
	RequestID  uuid.UUID      `json:"request_id"`
	Sequence   int64          `json:"sequence"`
	EventType  string         `json:"event_type"`
	Payload    datatypes.JSON `json:"payload"`
	OccurredAt time.Time      `json:"occurred_at"`
}

func (AtomizationWorkEvent) TableName() string { return "atomization_work_events" }

type StudioClearanceRequest struct {
	ID                      uint           `gorm:"primaryKey" json:"-"`
	PublicID                uuid.UUID      `json:"id"`
	TenantID                string         `json:"tenant_id"`
	AtomizationRequestID    uuid.UUID      `json:"atomization_request_id"`
	ChildIDs                datatypes.JSON `json:"child_ids"`
	ChildSetDigest          string         `json:"child_set_digest"`
	State                   string         `json:"state"`
	ClaimOwner              string         `json:"claim_owner,omitempty"`
	ClaimToken              *uuid.UUID     `json:"-"`
	FenceToken              *uuid.UUID     `json:"-"`
	ClaimEpoch              int64          `json:"claim_epoch"`
	ClaimExpiresAt          *time.Time     `json:"claim_expires_at,omitempty"`
	EffectStartedAt         *time.Time     `json:"effect_started_at,omitempty"`
	CancellationRequestedAt *time.Time     `json:"cancellation_requested_at,omitempty"`
	Decisions               datatypes.JSON `json:"decisions"`
	TerminalProof           datatypes.JSON `json:"terminal_proof"`
	FailureClass            string         `json:"failure_class,omitempty"`
	CreatedAt               time.Time      `json:"created_at"`
	UpdatedAt               time.Time      `json:"updated_at"`
	FinishedAt              *time.Time     `json:"finished_at,omitempty"`
}

func (StudioClearanceRequest) TableName() string { return "studio_clearance_requests" }

type StudioClearanceAttempt struct {
	ID              uint       `gorm:"primaryKey" json:"-"`
	PublicID        uuid.UUID  `json:"id"`
	TenantID        string     `json:"tenant_id"`
	RequestID       uuid.UUID  `json:"request_id"`
	AttemptNumber   int        `json:"attempt_number"`
	State           string     `json:"state"`
	ClaimToken      uuid.UUID  `json:"-"`
	FenceToken      uuid.UUID  `json:"fence_token"`
	LeaseExpiresAt  time.Time  `json:"lease_expires_at"`
	HeartbeatAt     time.Time  `json:"heartbeat_at"`
	EffectStartedAt *time.Time `json:"effect_started_at,omitempty"`
	FinishedAt      *time.Time `json:"finished_at,omitempty"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
}

func (StudioClearanceAttempt) TableName() string { return "studio_clearance_attempts" }

type StudioClearanceEvent struct {
	ID         uint           `gorm:"primaryKey" json:"-"`
	PublicID   uuid.UUID      `json:"id"`
	TenantID   string         `json:"tenant_id"`
	RequestID  uuid.UUID      `json:"request_id"`
	Sequence   int64          `json:"sequence"`
	EventType  string         `json:"event_type"`
	Payload    datatypes.JSON `json:"payload"`
	OccurredAt time.Time      `json:"occurred_at"`
	CreatedAt  time.Time      `json:"created_at"`
}

func (StudioClearanceEvent) TableName() string { return "studio_clearance_events" }
