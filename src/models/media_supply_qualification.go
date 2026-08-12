package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

type MediaSupplyQualificationCase struct {
	ID                  uint           `gorm:"primaryKey" json:"-"`
	PublicID            uuid.UUID      `json:"id"`
	CaseKey             string         `json:"case_key"`
	TenantID            string         `json:"tenant_id"`
	ActionKey           string         `json:"action_key"`
	ActionVersion       string         `json:"action_version"`
	AdapterVersion      string         `json:"adapter_version"`
	VerifierVersion     string         `json:"verifier_version"`
	RubricVersion       string         `json:"rubric_version"`
	SchemaVersion       string         `json:"schema_version"`
	PolicyVersion       string         `json:"policy_version"`
	EnvironmentIdentity string         `json:"environment_identity"`
	BuildIdentity       string         `json:"build_identity"`
	Cohort              string         `json:"cohort"`
	FaultCase           *string        `json:"fault_case,omitempty"`
	Origin              string         `json:"origin"`
	OriginPrincipal     string         `json:"origin_principal"`
	Recommendation      string         `json:"recommendation"`
	VerifiedSuccess     bool           `json:"verified_success"`
	IndependentVerifier bool           `json:"independent_verifier"`
	EffectVerdict       string         `json:"effect_verdict"`
	ReversalOrConflict  bool           `json:"reversal_or_conflict"`
	Violations          datatypes.JSON `json:"violations"`
	CorrelationDigest   string         `json:"correlation_digest"`
	PayloadDigest       string         `json:"payload_digest"`
	CreatedAt           time.Time      `json:"created_at"`
}

func (MediaSupplyQualificationCase) TableName() string { return "media_supply_qualification_cases" }

type MediaSupplyQualificationHumanDecision struct {
	ID            uint      `gorm:"primaryKey" json:"-"`
	PublicID      uuid.UUID `json:"id"`
	CaseID        uint      `json:"-"`
	Decision      string    `json:"decision"`
	ActorID       string    `json:"actor_id"`
	AccessVersion string    `json:"access_version"`
	DecidedAt     time.Time `json:"decided_at"`
	CreatedAt     time.Time `json:"created_at"`
}

func (MediaSupplyQualificationHumanDecision) TableName() string {
	return "media_supply_qualification_human_decisions"
}

type MediaSupplyQualificationReport struct {
	ID                  uint           `gorm:"primaryKey" json:"-"`
	PublicID            uuid.UUID      `json:"id"`
	TenantID            string         `json:"tenant_id"`
	RubricVersion       string         `json:"rubric_version"`
	ActionKey           string         `json:"action_key"`
	ActionVersion       string         `json:"action_version"`
	AdapterVersion      string         `json:"adapter_version"`
	VerifierVersion     string         `json:"verifier_version"`
	SchemaVersion       string         `json:"schema_version"`
	PolicyVersion       string         `json:"policy_version"`
	EnvironmentIdentity string         `json:"environment_identity"`
	BuildIdentity       string         `json:"build_identity"`
	State               string         `json:"state"`
	Payload             datatypes.JSON `json:"-"`
	ReportDigest        string         `json:"report_digest"`
	Seal                string         `json:"seal,omitempty"`
	CreatedAt           time.Time      `json:"created_at"`
	SealedAt            *time.Time     `json:"sealed_at,omitempty"`
}

func (MediaSupplyQualificationReport) TableName() string { return "media_supply_qualification_reports" }

type MediaSupplyQualificationReportCase struct {
	ReportID uint `gorm:"primaryKey"`
	CaseID   uint `gorm:"primaryKey"`
}

func (MediaSupplyQualificationReportCase) TableName() string {
	return "media_supply_qualification_report_cases"
}

type MediaSupplyQualificationSignoff struct {
	ID            uint      `gorm:"primaryKey" json:"-"`
	PublicID      uuid.UUID `json:"id"`
	ReportID      uint      `json:"-"`
	Role          string    `json:"role"`
	ActorID       string    `json:"actor_id"`
	AccessVersion string    `json:"access_version"`
	ReportDigest  string    `json:"report_digest"`
	SignedAt      time.Time `json:"signed_at"`
}

func (MediaSupplyQualificationSignoff) TableName() string {
	return "media_supply_qualification_signoffs"
}

type MediaSupplyActionPromotion struct {
	ID                    uint       `gorm:"primaryKey" json:"-"`
	PublicID              uuid.UUID  `json:"id"`
	TenantID              string     `json:"tenant_id"`
	ActionKey             string     `json:"action_key"`
	ActionVersion         string     `json:"action_version"`
	AdapterVersion        string     `json:"adapter_version"`
	VerifierVersion       string     `json:"verifier_version"`
	SchemaVersion         string     `json:"schema_version"`
	PolicyVersion         string     `json:"policy_version"`
	EnvironmentIdentity   string     `json:"environment_identity"`
	BuildIdentity         string     `json:"build_identity"`
	ReportID              uint       `json:"report_id"`
	ReportDigest          string     `json:"report_digest"`
	State                 string     `json:"state"`
	PromotionEpoch        int64      `json:"promotion_epoch"`
	PromotedBy            string     `json:"promoted_by"`
	PromotedAccessVersion string     `json:"promoted_access_version"`
	PromotedAt            time.Time  `json:"promoted_at"`
	DemotedAt             *time.Time `json:"demoted_at,omitempty"`
	DemotionReason        string     `json:"demotion_reason,omitempty"`
	CreatedAt             time.Time  `json:"created_at"`
	UpdatedAt             time.Time  `json:"updated_at"`
}

func (MediaSupplyActionPromotion) TableName() string { return "media_supply_action_promotions" }

type MediaSupplyPromotionEvent struct {
	ID          uint           `gorm:"primaryKey" json:"-"`
	PublicID    uuid.UUID      `json:"id"`
	TenantID    string         `json:"tenant_id"`
	ActionKey   string         `json:"action_key"`
	PromotionID uuid.UUID      `json:"promotion_id"`
	EventType   string         `json:"event_type"`
	Payload     datatypes.JSON `json:"payload"`
	OccurredAt  time.Time      `json:"occurred_at"`
}

func (MediaSupplyPromotionEvent) TableName() string { return "media_supply_promotion_events" }
