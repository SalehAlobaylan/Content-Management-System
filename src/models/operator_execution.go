package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

// OperatorActionPlan contains the canonical CMS-built snapshot. Neither
// browser state nor model output may be used as a replacement for this row.
type OperatorActionPlan struct {
	ID                  uint           `gorm:"primaryKey" json:"-"`
	PublicID            uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID            string         `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	ActorID             string         `json:"actor_id"`
	InvestigationID     *uint          `json:"-"`
	ToolKey             string         `json:"tool_key"`
	ToolVersion         string         `json:"tool_version"`
	State               string         `json:"state"`
	RiskTier            string         `json:"risk_tier"`
	CanonicalPlan       datatypes.JSON `gorm:"type:jsonb" json:"canonical_plan"`
	EvidenceFingerprint string         `json:"evidence_fingerprint"`
	AccessVersion       string         `json:"access_version"`
	Digest              string         `json:"digest"`
	Signature           string         `json:"-"`
	IdempotencyKey      string         `json:"idempotency_key"`
	ExpiresAt           time.Time      `json:"expires_at"`
	ClaimToken          *uuid.UUID     `gorm:"type:uuid" json:"-"`
	ClaimExpiresAt      *time.Time     `json:"claim_expires_at,omitempty"`
	ApprovedAt          *time.Time     `json:"approved_at,omitempty"`
	CompletedAt         *time.Time     `json:"completed_at,omitempty"`
	CreatedAt           time.Time      `json:"created_at"`
	UpdatedAt           time.Time      `json:"updated_at"`
}

func (OperatorActionPlan) TableName() string { return "operator_action_plans" }

type OperatorActionStep struct {
	ID             uint           `gorm:"primaryKey" json:"-"`
	PublicID       uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	PlanID         uint           `json:"-"`
	TenantID       string         `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	Ordinal        int            `json:"ordinal"`
	State          string         `json:"state"`
	ToolKey        string         `json:"tool_key"`
	Targets        datatypes.JSON `gorm:"type:jsonb" json:"targets"`
	Arguments      datatypes.JSON `gorm:"type:jsonb" json:"arguments"`
	Branch         datatypes.JSON `gorm:"type:jsonb" json:"branch"`
	BeforeState    datatypes.JSON `gorm:"type:jsonb" json:"before_state,omitempty"`
	AfterState     datatypes.JSON `gorm:"type:jsonb" json:"after_state,omitempty"`
	VerifiedState  datatypes.JSON `gorm:"type:jsonb" json:"verified_state,omitempty"`
	ClaimToken     *uuid.UUID     `gorm:"type:uuid" json:"-"`
	ClaimExpiresAt *time.Time     `json:"claim_expires_at,omitempty"`
	StartedAt      *time.Time     `json:"started_at,omitempty"`
	FinishedAt     *time.Time     `json:"finished_at,omitempty"`
	CreatedAt      time.Time      `json:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at"`
}

func (OperatorActionStep) TableName() string { return "operator_action_steps" }

type OperatorPlanEvent struct {
	ID        uint           `gorm:"primaryKey" json:"-"`
	PublicID  uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	PlanID    uint           `json:"-"`
	StepID    *uint          `json:"-"`
	TenantID  string         `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	Sequence  int64          `json:"sequence"`
	EventType string         `json:"event_type"`
	ActorType string         `json:"actor_type"`
	ActorID   string         `json:"actor_id,omitempty"`
	Payload   datatypes.JSON `gorm:"type:jsonb" json:"payload"`
	CreatedAt time.Time      `json:"created_at"`
}

func (OperatorPlanEvent) TableName() string { return "operator_plan_events" }

type OperatorPlanApproval struct {
	ID                    uint       `gorm:"primaryKey" json:"-"`
	PublicID              uuid.UUID  `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	PlanID                uint       `json:"-"`
	TenantID              string     `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	ActorID               string     `json:"actor_id"`
	AccessVersion         string     `json:"access_version"`
	PlanDigest            string     `json:"plan_digest"`
	ConfirmationTier      string     `json:"confirmation_tier"`
	ConfirmationProofHash string     `json:"-"`
	ExpiresAt             time.Time  `json:"expires_at"`
	ConsumedAt            *time.Time `json:"consumed_at,omitempty"`
	CreatedAt             time.Time  `json:"created_at"`
}

func (OperatorPlanApproval) TableName() string { return "operator_plan_approvals" }
