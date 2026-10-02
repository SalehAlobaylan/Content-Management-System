package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

// ContentResetExecution separates immutable preview intent from durable run
// control. Version changes on operator decisions; worker receipts do not spend
// the operator's compare-and-swap version.
type ContentResetExecution struct {
	ID                     uint           `gorm:"primaryKey" json:"-"`
	PublicID               uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid()" json:"id"`
	TenantID               string         `json:"tenant_id"`
	CampaignID             uint           `json:"-"`
	RevisionID             uint           `json:"-"`
	ManifestHash           string         `json:"manifest_hash"`
	Version                int64          `json:"version"`
	Phase                  string         `json:"phase"`
	PauseRequested         bool           `json:"pause_requested"`
	ApprovedBy             string         `json:"approved_by"`
	ApprovedAt             time.Time      `json:"approved_at"`
	ApprovalExpiresAt      time.Time      `json:"approval_expires_at"`
	StartedAt              *time.Time     `json:"started_at,omitempty"`
	IrreversibleAt         *time.Time     `json:"irreversible_at,omitempty"`
	PublishedAt            *time.Time     `json:"published_at,omitempty"`
	CleanupNotBefore       *time.Time     `json:"cleanup_not_before,omitempty"`
	CleanupAuthorizedUntil *time.Time     `json:"cleanup_authorized_until,omitempty"`
	RolledBackAt           *time.Time     `json:"rolled_back_at,omitempty"`
	CompletedAt            *time.Time     `json:"completed_at,omitempty"`
	ContractHash           string         `json:"contract_hash"`
	Contract               datatypes.JSON `gorm:"type:jsonb" json:"contract"`
	CreatedAt              time.Time      `json:"created_at"`
	UpdatedAt              time.Time      `json:"updated_at"`
}

func (ContentResetExecution) TableName() string { return "content_reset_executions" }

// ContentResetDecision stores the exact response to an accepted mutation.
// Replaying a request never repeats effects or spends another reauth proof.
type ContentResetDecision struct {
	ID             uint           `gorm:"primaryKey" json:"-"`
	PublicID       uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid()" json:"-"`
	TenantID       string         `json:"-"`
	CampaignID     uint           `json:"-"`
	IdempotencyKey string         `json:"-"`
	Action         string         `json:"action"`
	ActorID        string         `json:"actor_id"`
	RequestHash    string         `json:"request_hash"`
	ProofHash      *string        `json:"-"`
	CommandID      *uuid.UUID     `gorm:"type:uuid" json:"-"`
	Response       datatypes.JSON `gorm:"type:jsonb" json:"response"`
	CreatedAt      time.Time      `json:"created_at"`
}

func (ContentResetDecision) TableName() string { return "content_reset_decisions" }

// A dependency is local to one campaign revision. Unknown and failed effects
// never satisfy a dependency; only a verified succeeded receipt does.
type ContentResetStepDependency struct {
	TenantID   string `gorm:"primaryKey"`
	CampaignID uint   `gorm:"primaryKey"`
	RevisionID uint   `gorm:"primaryKey"`
	StepID     uint   `gorm:"primaryKey"`
	RequiresID uint   `gorm:"primaryKey"`
}

func (ContentResetStepDependency) TableName() string { return "content_reset_step_dependencies" }

// ContentResetOwnerQualification records one reviewed owner-contract release
// qualification. Application code still requires its own expected qualification
// version; a row alone does not enable an effect.
type ContentResetOwnerQualification struct {
	ID                   uint       `gorm:"primaryKey" json:"-"`
	PublicID             uuid.UUID  `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	ContractOwner        string     `gorm:"type:varchar(64);not null" json:"contract_owner"`
	ContractEffect       string     `gorm:"type:varchar(64);not null" json:"contract_effect"`
	ContractTargetType   string     `gorm:"type:varchar(32);not null" json:"contract_target_type"`
	ContractVersion      string     `gorm:"type:varchar(32);not null" json:"contract_version"`
	QualificationVersion string     `gorm:"type:varchar(96);not null" json:"qualification_version"`
	EnvironmentHash      string     `gorm:"type:char(64);not null" json:"environment_hash"`
	EvidenceHash         string     `gorm:"type:char(64);not null" json:"evidence_hash"`
	QualifiedBy          string     `gorm:"type:varchar(255);not null" json:"qualified_by"`
	QualifiedAt          time.Time  `json:"qualified_at"`
	RevokedAt            *time.Time `json:"revoked_at,omitempty"`
	CreatedAt            time.Time  `json:"created_at"`
}

func (ContentResetOwnerQualification) TableName() string {
	return "content_reset_owner_qualifications"
}
