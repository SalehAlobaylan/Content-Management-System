package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

const (
	SourceRunAttemptAuthorized           = "authorized"
	SourceRunAttemptClaimed              = "claimed"
	SourceRunAttemptRunning              = "running"
	SourceRunAttemptVerificationRequired = "verification_required"
	SourceRunAttemptSucceeded            = "succeeded"
	SourceRunAttemptPartial              = "partial"
	SourceRunAttemptBlocked              = "blocked"
	SourceRunAttemptFailed               = "failed"
	SourceRunAttemptCancelled            = "cancelled"
	SourceRunAttemptExpired              = "expired"

	SourceRunUnitAuthorized           = "authorized"
	SourceRunUnitAccepted             = "accepted"
	SourceRunUnitRunning              = "running"
	SourceRunUnitVerificationRequired = "verification_required"
	SourceRunUnitSucceeded            = "succeeded"
	SourceRunUnitFailed               = "failed"
	SourceRunUnitCancelled            = "cancelled"
	SourceRunUnitExpired              = "expired"

	SourceRunVerificationTaskQueued   = "queued"
	SourceRunVerificationTaskClaimed  = "claimed"
	SourceRunVerificationTaskRunning  = "running"
	SourceRunVerificationTaskTerminal = "terminal"
)

// SourceRunAttempt is a fenced provider-effecting attempt. Dispatch ownership
// is renewable; FenceToken is immutable for the attempt and is carried through
// every authorized execution unit and receipt.
type SourceRunAttempt struct {
	ID                       uint       `gorm:"primaryKey" json:"-"`
	PublicID                 uuid.UUID  `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID                 string     `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	SourceRunRequestID       uuid.UUID  `gorm:"type:uuid;not null;index" json:"source_run_request_id"`
	ContentSourceID          uuid.UUID  `gorm:"type:uuid;not null;index" json:"content_source_id"`
	AttemptNumber            int        `gorm:"not null" json:"attempt_number"`
	State                    string     `gorm:"type:varchar(32);not null" json:"state"`
	FenceToken               uuid.UUID  `gorm:"type:uuid;not null" json:"fence_token"`
	DispatcherOwner          string     `gorm:"type:varchar(128)" json:"dispatcher_owner,omitempty"`
	DispatcherToken          *uuid.UUID `gorm:"type:uuid" json:"-"`
	DispatcherEpoch          int64      `gorm:"not null;default:0" json:"dispatcher_epoch"`
	DispatcherLeaseExpiresAt *time.Time `gorm:"type:timestamptz;index" json:"dispatcher_lease_expires_at,omitempty"`
	HeartbeatAt              *time.Time `gorm:"type:timestamptz" json:"heartbeat_at,omitempty"`
	RootExecutionUnitID      *uuid.UUID `gorm:"type:uuid;index" json:"root_execution_unit_id,omitempty"`
	StartedAt                *time.Time `gorm:"type:timestamptz" json:"started_at,omitempty"`
	FinishedAt               *time.Time `gorm:"type:timestamptz" json:"finished_at,omitempty"`
	VerificationRequiredAt   *time.Time `gorm:"type:timestamptz" json:"verification_required_at,omitempty"`
	FailureClass             string     `gorm:"type:varchar(100)" json:"failure_class,omitempty"`
	FailureSummary           string     `gorm:"type:varchar(1000)" json:"failure_summary,omitempty"`
	CreatedAt                time.Time  `json:"created_at"`
	UpdatedAt                time.Time  `json:"updated_at"`
}

func (SourceRunAttempt) TableName() string { return "source_run_attempts" }

// SourceRunExecutionUnit is the only CMS-authorized provider or pipeline work
// identity. A worker must hold its current lease token before it can begin an
// effect or emit a receipt for the unit.
type SourceRunExecutionUnit struct {
	ID                      uint       `gorm:"primaryKey" json:"-"`
	PublicID                uuid.UUID  `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID                string     `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	SourceRunRequestID      uuid.UUID  `gorm:"type:uuid;not null;index" json:"source_run_request_id"`
	SourceRunAttemptID      uuid.UUID  `gorm:"type:uuid;not null;index" json:"source_run_attempt_id"`
	ContentSourceID         uuid.UUID  `gorm:"type:uuid;not null;index" json:"content_source_id"`
	ParentUnitID            *uuid.UUID `gorm:"type:uuid;index" json:"parent_unit_id,omitempty"`
	UnitType                string     `gorm:"type:varchar(32);not null" json:"unit_type"`
	UnitKey                 string     `gorm:"type:varchar(255);not null" json:"unit_key"`
	PageID                  string     `gorm:"type:varchar(128)" json:"page_id,omitempty"`
	BatchID                 string     `gorm:"type:varchar(128)" json:"batch_id,omitempty"`
	JobID                   string     `gorm:"type:varchar(255);not null" json:"job_id"`
	AttemptFenceToken       uuid.UUID  `gorm:"type:uuid;not null" json:"attempt_fence_token"`
	State                   string     `gorm:"type:varchar(32);not null" json:"state"`
	ExecutionOwner          string     `gorm:"type:varchar(128)" json:"execution_owner,omitempty"`
	ExecutionLeaseToken     *uuid.UUID `gorm:"type:uuid" json:"-"`
	ExecutionLeaseEpoch     int64      `gorm:"not null;default:0" json:"execution_lease_epoch"`
	ExecutionLeaseExpiresAt *time.Time `gorm:"type:timestamptz;index" json:"execution_lease_expires_at,omitempty"`
	HeartbeatAt             *time.Time `gorm:"type:timestamptz" json:"heartbeat_at,omitempty"`
	EffectStartedAt         *time.Time `gorm:"type:timestamptz" json:"effect_started_at,omitempty"`
	CancellationRequestedAt *time.Time `gorm:"type:timestamptz" json:"cancellation_requested_at,omitempty"`
	VerificationRequired    bool       `gorm:"not null;default:false" json:"verification_required"`
	DeclaredChildCount      int        `gorm:"not null;default:0" json:"declared_child_count"`
	DeclaredChildDigest     string     `gorm:"type:varchar(128)" json:"declared_child_digest,omitempty"`
	TerminalOutcome         string     `gorm:"type:varchar(48)" json:"terminal_outcome,omitempty"`
	StartedAt               *time.Time `gorm:"type:timestamptz" json:"started_at,omitempty"`
	FinishedAt              *time.Time `gorm:"type:timestamptz" json:"finished_at,omitempty"`
	CreatedAt               time.Time  `json:"created_at"`
	UpdatedAt               time.Time  `json:"updated_at"`
}

func (SourceRunExecutionUnit) TableName() string { return "source_run_execution_units" }

// SourceRunReceipt preserves producer evidence before reducers build current
// state. It is append-only and safe to redeliver by producer event key.
type SourceRunReceipt struct {
	ID                  uint           `gorm:"primaryKey" json:"-"`
	PublicID            uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID            string         `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	ProducerEventKey    string         `gorm:"type:varchar(255);not null" json:"producer_event_key"`
	SourceRunRequestID  uuid.UUID      `gorm:"type:uuid;not null;index" json:"source_run_request_id"`
	SourceRunAttemptID  uuid.UUID      `gorm:"type:uuid;not null;index" json:"source_run_attempt_id"`
	ExecutionUnitID     uuid.UUID      `gorm:"type:uuid;not null;index" json:"execution_unit_id"`
	ContentSourceID     uuid.UUID      `gorm:"type:uuid;not null;index" json:"content_source_id"`
	ContentItemID       *uuid.UUID     `gorm:"type:uuid;index" json:"content_item_id,omitempty"`
	UnitJobID           string         `gorm:"type:varchar(255);not null" json:"unit_job_id"`
	AttemptFenceToken   uuid.UUID      `gorm:"type:uuid;not null" json:"attempt_fence_token"`
	ExecutionLeaseToken uuid.UUID      `gorm:"type:uuid;not null" json:"-"`
	SchemaVersion       string         `gorm:"type:varchar(64);not null" json:"schema_version"`
	Producer            string         `gorm:"type:varchar(32);not null" json:"producer"`
	Stage               string         `gorm:"type:varchar(48);not null" json:"stage"`
	EventType           string         `gorm:"type:varchar(64);not null" json:"event_type"`
	Outcome             string         `gorm:"type:varchar(48);not null" json:"outcome"`
	Sequence            int64          `gorm:"not null;default:0" json:"sequence"`
	PageID              string         `gorm:"type:varchar(128)" json:"page_id,omitempty"`
	BatchID             string         `gorm:"type:varchar(128)" json:"batch_id,omitempty"`
	FinalPage           bool           `gorm:"not null;default:false" json:"final_page"`
	CausationID         string         `gorm:"type:varchar(255)" json:"causation_id,omitempty"`
	Payload             datatypes.JSON `gorm:"type:jsonb;not null" json:"payload"`
	PayloadDigest       string         `gorm:"type:varchar(128);not null" json:"payload_digest"`
	ProducedAt          time.Time      `gorm:"type:timestamptz;not null" json:"produced_at"`
	ObservedAt          time.Time      `gorm:"type:timestamptz;not null" json:"observed_at"`
	CreatedAt           time.Time      `json:"created_at"`
}

func (SourceRunReceipt) TableName() string { return "source_run_receipts" }

// SourceRunRetainedReceipt is Aggregation's durable, CMS-addressable copy of
// a receipt awaiting delivery.  It is intentionally separate from the receipt
// ledger: retaining evidence never advances a unit, and delivery remains the
// only path that may create a SourceRunReceipt/projection work item.
type SourceRunRetainedReceipt struct {
	ID                 uint           `gorm:"primaryKey" json:"-"`
	PublicID           uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID           string         `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	ProducerEventKey   string         `gorm:"type:varchar(255);not null" json:"producer_event_key"`
	SourceRunRequestID uuid.UUID      `gorm:"type:uuid;not null;index" json:"source_run_request_id"`
	SourceRunAttemptID uuid.UUID      `gorm:"type:uuid;not null;index" json:"source_run_attempt_id"`
	ExecutionUnitID    uuid.UUID      `gorm:"type:uuid;not null;index" json:"execution_unit_id"`
	PayloadDigest      string         `gorm:"type:varchar(128);not null" json:"payload_digest"`
	Receipt            datatypes.JSON `gorm:"type:jsonb;not null" json:"-"`
	State              string         `gorm:"type:varchar(24);not null;index" json:"state"`
	DeliveredReceiptID *uuid.UUID     `gorm:"type:uuid;index" json:"delivered_receipt_id,omitempty"`
	CreatedAt          time.Time      `json:"created_at"`
	UpdatedAt          time.Time      `json:"updated_at"`
}

func (SourceRunRetainedReceipt) TableName() string { return "source_run_retained_receipts" }

type SourceRunReconciliationEvent struct {
	ID                    uint           `gorm:"primaryKey" json:"-"`
	PublicID              uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID              string         `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	EventKey              string         `gorm:"type:varchar(255);not null" json:"event_key"`
	SourceRunRequestID    uuid.UUID      `gorm:"type:uuid;not null;index" json:"source_run_request_id"`
	SourceRunAttemptID    *uuid.UUID     `gorm:"type:uuid;index" json:"source_run_attempt_id,omitempty"`
	ExecutionUnitID       *uuid.UUID     `gorm:"type:uuid;index" json:"execution_unit_id,omitempty"`
	ContentSourceID       uuid.UUID      `gorm:"type:uuid;not null;index" json:"content_source_id"`
	AttemptFenceToken     *uuid.UUID     `gorm:"type:uuid" json:"attempt_fence_token,omitempty"`
	EffectIdentity        string         `gorm:"type:varchar(255);not null" json:"effect_identity"`
	ScopeType             string         `gorm:"type:varchar(48);not null" json:"scope_type"`
	ScopeID               string         `gorm:"type:varchar(255);not null" json:"scope_id"`
	Stage                 string         `gorm:"type:varchar(48);not null" json:"stage"`
	Verdict               string         `gorm:"type:varchar(16);not null" json:"verdict"`
	EvidenceSnapshot      string         `gorm:"type:varchar(255);not null" json:"evidence_snapshot"`
	VerifierSchemaVersion string         `gorm:"type:varchar(64);not null" json:"verifier_schema_version"`
	VerificationTaskID    uuid.UUID      `gorm:"type:uuid;not null;index" json:"verification_task_id"`
	VerifierLeaseToken    uuid.UUID      `gorm:"type:uuid;not null" json:"-"`
	CausationID           string         `gorm:"type:varchar(255);not null" json:"causation_id"`
	ProvenanceDigest      string         `gorm:"type:varchar(128);not null" json:"provenance_digest"`
	Payload               datatypes.JSON `gorm:"type:jsonb;not null" json:"payload"`
	ObservedAt            time.Time      `gorm:"type:timestamptz;not null" json:"observed_at"`
	CreatedAt             time.Time      `json:"created_at"`
}

func (SourceRunReconciliationEvent) TableName() string { return "source_run_reconciliation_events" }

type SourceRunVerificationTask struct {
	ID                    uint       `gorm:"primaryKey" json:"-"`
	PublicID              uuid.UUID  `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID              string     `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	TaskKey               string     `gorm:"type:varchar(255);not null" json:"task_key"`
	SourceRunRequestID    uuid.UUID  `gorm:"type:uuid;not null;index" json:"source_run_request_id"`
	SourceRunAttemptID    *uuid.UUID `gorm:"type:uuid;index" json:"source_run_attempt_id,omitempty"`
	ExecutionUnitID       *uuid.UUID `gorm:"type:uuid;index" json:"execution_unit_id,omitempty"`
	ContentSourceID       uuid.UUID  `gorm:"type:uuid;not null;index" json:"content_source_id"`
	EffectIdentity        string     `gorm:"type:varchar(255);not null" json:"effect_identity"`
	ScopeType             string     `gorm:"type:varchar(48);not null" json:"scope_type"`
	ScopeID               string     `gorm:"type:varchar(255);not null" json:"scope_id"`
	Stage                 string     `gorm:"type:varchar(48);not null" json:"stage"`
	EvidenceBoundary      string     `gorm:"type:varchar(255);not null" json:"evidence_boundary"`
	CausationID           string     `gorm:"type:varchar(255);not null" json:"causation_id"`
	VerifierName          string     `gorm:"type:varchar(100);not null" json:"verifier_name"`
	VerifierSchemaVersion string     `gorm:"type:varchar(64);not null" json:"verifier_schema_version"`
	State                 string     `gorm:"type:varchar(24);not null;default:'queued'" json:"state"`
	ClaimOwner            string     `gorm:"type:varchar(128)" json:"claim_owner,omitempty"`
	ClaimToken            *uuid.UUID `gorm:"type:uuid" json:"-"`
	ClaimEpoch            int64      `gorm:"not null;default:0" json:"claim_epoch"`
	ClaimExpiresAt        *time.Time `gorm:"type:timestamptz;index" json:"claim_expires_at,omitempty"`
	HeartbeatAt           *time.Time `gorm:"type:timestamptz" json:"heartbeat_at,omitempty"`
	AttemptCount          int        `gorm:"not null;default:0" json:"attempt_count"`
	NotBeforeAt           *time.Time `gorm:"type:timestamptz;index" json:"not_before_at,omitempty"`
	DeadlineAt            *time.Time `gorm:"type:timestamptz;index" json:"deadline_at,omitempty"`
	TerminalVerdict       string     `gorm:"type:varchar(16)" json:"terminal_verdict,omitempty"`
	TerminalEventID       *uuid.UUID `gorm:"type:uuid" json:"terminal_event_id,omitempty"`
	CreatedAt             time.Time  `json:"created_at"`
	UpdatedAt             time.Time  `json:"updated_at"`
}

func (SourceRunVerificationTask) TableName() string { return "source_run_verification_tasks" }

type SourceRunProjectionWork struct {
	ID             uint       `gorm:"primaryKey" json:"-"`
	PublicID       uuid.UUID  `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID       string     `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	EvidenceKind   string     `gorm:"type:varchar(48);not null" json:"evidence_kind"`
	EvidenceID     uuid.UUID  `gorm:"type:uuid;not null" json:"evidence_id"`
	ReducerVersion string     `gorm:"type:varchar(64);not null" json:"reducer_version"`
	State          string     `gorm:"type:varchar(24);not null;default:'queued'" json:"state"`
	ClaimOwner     string     `gorm:"type:varchar(128)" json:"claim_owner,omitempty"`
	ClaimToken     *uuid.UUID `gorm:"type:uuid" json:"-"`
	ClaimExpiresAt *time.Time `gorm:"type:timestamptz;index" json:"claim_expires_at,omitempty"`
	AttemptCount   int        `gorm:"not null;default:0" json:"attempt_count"`
	ProjectedAt    *time.Time `gorm:"type:timestamptz" json:"projected_at,omitempty"`
	ErrorSummary   string     `gorm:"type:varchar(1000)" json:"error_summary,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
}

func (SourceRunProjectionWork) TableName() string { return "source_run_projection_work" }

type SourceRunReceiptQuarantine struct {
	ID               uint           `gorm:"primaryKey" json:"-"`
	PublicID         uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID         string         `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	Producer         string         `gorm:"type:varchar(32);not null" json:"producer"`
	ProducerEventKey string         `gorm:"type:varchar(255)" json:"producer_event_key,omitempty"`
	Reason           string         `gorm:"type:varchar(100);not null" json:"reason"`
	Payload          datatypes.JSON `gorm:"type:jsonb;not null" json:"payload"`
	PayloadDigest    string         `gorm:"type:varchar(128);not null" json:"payload_digest"`
	ObservedAt       time.Time      `gorm:"type:timestamptz;not null" json:"observed_at"`
	ExpiresAt        time.Time      `gorm:"type:timestamptz;not null;index" json:"expires_at"`
	CreatedAt        time.Time      `json:"created_at"`
}

func (SourceRunReceiptQuarantine) TableName() string { return "source_run_receipt_quarantine" }

type SourceRunReceiptRejectionAudit struct {
	ID           uint      `gorm:"primaryKey" json:"-"`
	PublicID     uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	Reason       string    `gorm:"type:varchar(100);not null" json:"reason"`
	RemoteClass  string    `gorm:"type:varchar(64)" json:"remote_class,omitempty"`
	PayloadBytes int64     `gorm:"not null;default:0" json:"payload_bytes"`
	ObservedAt   time.Time `gorm:"type:timestamptz;not null" json:"observed_at"`
	CreatedAt    time.Time `json:"created_at"`
}

func (SourceRunReceiptRejectionAudit) TableName() string {
	return "source_run_receipt_rejection_audits"
}

// SourceRunAdmissionProtocol is the one global, compare-and-set cutover
// record for source-work admission.  It deliberately has no tenant fallback:
// compatibility keeps legacy readers available, while durable_required makes a
// missing tenant/lane cutover record a hard admission failure.
type SourceRunAdmissionProtocol struct {
	ID          uint       `gorm:"primaryKey" json:"-"`
	ProtocolKey string     `gorm:"type:varchar(64);not null;uniqueIndex" json:"protocol_key"`
	Epoch       string     `gorm:"type:varchar(32);not null" json:"epoch"`
	Version     int64      `gorm:"not null;default:0" json:"version"`
	ActivatedAt *time.Time `gorm:"type:timestamptz" json:"activated_at,omitempty"`
	ActivatedBy string     `gorm:"type:varchar(128)" json:"activated_by,omitempty"`
	Build       string     `gorm:"type:varchar(128)" json:"build,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
}

func (SourceRunAdmissionProtocol) TableName() string { return "source_run_admission_protocol" }

// SourceRunAdmissionCutover binds durable admission to one explicit tenant
// and lane.  It is intentionally separate from policy: a policy change cannot
// accidentally promote a legacy producer.
type SourceRunAdmissionCutover struct {
	ID          uint       `gorm:"primaryKey" json:"-"`
	TenantID    string     `gorm:"type:varchar(64);not null;uniqueIndex:uq_source_run_admission_cutover" json:"tenant_id"`
	Lane        string     `gorm:"type:varchar(32);not null;uniqueIndex:uq_source_run_admission_cutover" json:"lane"`
	Mode        string     `gorm:"type:varchar(32);not null" json:"mode"`
	Protocol    string     `gorm:"type:varchar(64);not null" json:"protocol"`
	Version     int64      `gorm:"not null;default:0" json:"version"`
	ActivatedAt *time.Time `gorm:"type:timestamptz" json:"activated_at,omitempty"`
	ActivatedBy string     `gorm:"type:varchar(128)" json:"activated_by,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
}

func (SourceRunAdmissionCutover) TableName() string { return "source_run_admission_cutovers" }

type SourceUpstreamObservation struct {
	ID                  uint           `gorm:"primaryKey" json:"-"`
	PublicID            uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID            string         `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	ContentSourceID     uuid.UUID      `gorm:"type:uuid;not null;index" json:"content_source_id"`
	SourceRunRequestID  *uuid.UUID     `gorm:"type:uuid;index" json:"source_run_request_id,omitempty"`
	ProviderCapability  string         `gorm:"type:varchar(64);not null" json:"provider_capability"`
	ProviderVersion     string         `gorm:"type:varchar(64);not null" json:"provider_version"`
	UpstreamItemID      string         `gorm:"type:varchar(255);not null" json:"upstream_item_id"`
	UpstreamFingerprint string         `gorm:"type:varchar(128);not null" json:"upstream_fingerprint"`
	ReplayLocator       datatypes.JSON `gorm:"type:jsonb;not null" json:"replay_locator"`
	ReplayUntil         *time.Time     `gorm:"type:timestamptz;index" json:"replay_until,omitempty"`
	ProviderCursor      string         `gorm:"type:varchar(255)" json:"provider_cursor,omitempty"`
	ProviderPageID      string         `gorm:"type:varchar(128)" json:"provider_page_id,omitempty"`
	ObservedAt          time.Time      `gorm:"type:timestamptz;not null" json:"observed_at"`
	CreatedAt           time.Time      `json:"created_at"`
}

func (SourceUpstreamObservation) TableName() string { return "source_upstream_observations" }

type SourceUpstreamObservationEvent struct {
	ID            uint           `gorm:"primaryKey" json:"-"`
	PublicID      uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID      string         `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	EventKey      string         `gorm:"type:varchar(255);not null" json:"event_key"`
	ObservationID uuid.UUID      `gorm:"type:uuid;not null;index" json:"observation_id"`
	EventType     string         `gorm:"type:varchar(64);not null" json:"event_type"`
	CausationID   string         `gorm:"type:varchar(255)" json:"causation_id,omitempty"`
	Payload       datatypes.JSON `gorm:"type:jsonb;not null" json:"payload"`
	OccurredAt    time.Time      `gorm:"type:timestamptz;not null" json:"occurred_at"`
	CreatedAt     time.Time      `json:"created_at"`
}

func (SourceUpstreamObservationEvent) TableName() string { return "source_upstream_observation_events" }

type SourceUpstreamObservationDisposition struct {
	ID            uint       `gorm:"primaryKey" json:"-"`
	PublicID      uuid.UUID  `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID      string     `gorm:"type:varchar(64);not null;uniqueIndex:uq_source_upstream_disposition" json:"tenant_id"`
	ObservationID uuid.UUID  `gorm:"type:uuid;not null;uniqueIndex:uq_source_upstream_disposition" json:"observation_id"`
	Disposition   string     `gorm:"type:varchar(64);not null;index" json:"disposition"`
	LatestEventID uuid.UUID  `gorm:"type:uuid;not null" json:"latest_event_id"`
	LatestEventAt time.Time  `gorm:"type:timestamptz;not null" json:"latest_event_at"`
	ReplayUntil   *time.Time `gorm:"type:timestamptz;index" json:"replay_until,omitempty"`
	UpdatedAt     time.Time  `json:"updated_at"`
}

func (SourceUpstreamObservationDisposition) TableName() string {
	return "source_upstream_observation_dispositions"
}
