package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

// ContentResetCampaign is the durable parent for a clear, fresh-start, or
// empty-workspace operation. Planning records intent; it does not authorize an
// owner effect.
type ContentResetCampaign struct {
	ID              uint       `gorm:"primaryKey" json:"-"`
	PublicID        uuid.UUID  `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID        string     `gorm:"type:varchar(64);not null" json:"tenant_id"`
	Operation       string     `gorm:"type:varchar(24);not null" json:"operation"`
	Lane            string     `gorm:"type:varchar(16);not null" json:"lane"`
	State           string     `gorm:"type:varchar(24);not null" json:"state"`
	CurrentRevision int        `gorm:"not null" json:"current_revision"`
	IdempotencyKey  string     `gorm:"type:varchar(128);not null" json:"-"`
	RequestHash     string     `gorm:"type:char(64);not null" json:"request_hash"`
	CreatedBy       string     `gorm:"type:varchar(255);not null" json:"created_by"`
	CancelledAt     *time.Time `json:"cancelled_at,omitempty"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
}

func (ContentResetCampaign) TableName() string { return "content_reset_campaigns" }

// ContentResetRevision freezes operator intent and incrementally materializes
// the exact content set beneath the parent campaign.
type ContentResetRevision struct {
	ID                   uint           `gorm:"primaryKey" json:"-"`
	PublicID             uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	CampaignID           uint           `gorm:"not null" json:"-"`
	TenantID             string         `gorm:"type:varchar(64);not null" json:"tenant_id"`
	Revision             int            `gorm:"not null" json:"revision"`
	Request              datatypes.JSON `gorm:"type:jsonb;not null" json:"request"`
	State                string         `gorm:"type:varchar(24);not null" json:"state"`
	SelectionHighwater   int64          `gorm:"not null" json:"selection_highwater"`
	InventoryHighwater   int64          `gorm:"not null" json:"inventory_highwater"`
	ScanCursor           int64          `gorm:"not null" json:"scan_cursor"`
	TargetCount          int64          `gorm:"not null" json:"target_count"`
	ProtectedCount       int64          `gorm:"not null" json:"protected_count"`
	UnknownDateCount     int64          `gorm:"not null" json:"unknown_date_count"`
	UnattributedCount    int64          `gorm:"not null" json:"unattributed_count"`
	ReplaySourceSnapshot datatypes.JSON `gorm:"type:jsonb;not null" json:"replay_source_snapshot"`
	ReplaySourceHash     string         `gorm:"type:char(64);not null" json:"replay_source_hash"`
	MissingExplicitCount int            `gorm:"not null" json:"missing_explicit_count"`
	ManifestChainHash    string         `gorm:"type:char(64);not null" json:"-"`
	ManifestHash         *string        `gorm:"type:char(64)" json:"manifest_hash,omitempty"`
	Blockers             datatypes.JSON `gorm:"type:jsonb;not null" json:"blockers"`
	CreatedBy            string         `gorm:"type:varchar(255);not null" json:"created_by"`
	PlanningStartedAt    time.Time      `json:"planning_started_at"`
	PlanningCompletedAt  *time.Time     `json:"planning_completed_at,omitempty"`
	ExpiresAt            *time.Time     `json:"expires_at,omitempty"`
	CreatedAt            time.Time      `json:"created_at"`
}

func (ContentResetRevision) TableName() string { return "content_reset_revisions" }

type ContentResetTarget struct {
	ID                 uint           `gorm:"primaryKey" json:"-"`
	RevisionID         uint           `gorm:"not null" json:"-"`
	TenantID           string         `gorm:"type:varchar(64);not null" json:"tenant_id"`
	ContentItemID      uuid.UUID      `gorm:"type:uuid;not null" json:"content_item_id"`
	ItemOrdinal        int64          `gorm:"not null" json:"ordinal"`
	Lane               string         `gorm:"type:varchar(8);not null" json:"lane"`
	Disposition        string         `gorm:"type:varchar(16);not null" json:"disposition"`
	Protected          bool           `gorm:"not null" json:"protected"`
	ProtectionReason   string         `gorm:"type:varchar(64)" json:"protection_reason,omitempty"`
	ProtectionEvidence datatypes.JSON `gorm:"type:jsonb;not null" json:"protection_evidence"`
	SnapshotHash       string         `gorm:"type:char(64);not null" json:"snapshot_hash"`
	Snapshot           datatypes.JSON `gorm:"type:jsonb;not null" json:"snapshot"`
	CreatedAt          time.Time      `json:"created_at"`
}

func (ContentResetTarget) TableName() string { return "content_reset_targets" }

type ContentResetStep struct {
	ID              uint           `gorm:"primaryKey" json:"-"`
	PublicID        uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	CampaignID      uint           `gorm:"not null" json:"-"`
	RevisionID      uint           `gorm:"not null" json:"-"`
	TenantID        string         `gorm:"type:varchar(64);not null" json:"tenant_id"`
	StepKey         string         `gorm:"type:varchar(255);not null" json:"step_key"`
	Owner           string         `gorm:"type:varchar(32);not null" json:"owner"`
	TargetType      string         `gorm:"type:varchar(32);not null" json:"target_type"`
	TargetID        string         `gorm:"type:varchar(255);not null" json:"target_id"`
	EffectType      string         `gorm:"type:varchar(48);not null" json:"effect_type"`
	State           string         `gorm:"type:varchar(24);not null" json:"state"`
	Command         datatypes.JSON `gorm:"type:jsonb;not null" json:"command"`
	Receipt         datatypes.JSON `gorm:"type:jsonb" json:"receipt,omitempty"`
	AttemptCount    int            `gorm:"not null" json:"attempt_count"`
	LeaseToken      *uuid.UUID     `gorm:"type:uuid" json:"-"`
	LeaseUntil      *time.Time     `json:"lease_until,omitempty"`
	LastError       string         `gorm:"type:text" json:"last_error,omitempty"`
	RetryDecisionID *uuid.UUID     `gorm:"type:uuid" json:"-"`
	CreatedAt       time.Time      `json:"created_at"`
	UpdatedAt       time.Time      `json:"updated_at"`
}

func (ContentResetStep) TableName() string { return "content_reset_steps" }

type ContentResetEvidence struct {
	ID           uint           `gorm:"primaryKey" json:"-"`
	PublicID     uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	CampaignID   uint           `gorm:"not null" json:"-"`
	RevisionID   *uint          `json:"-"`
	TenantID     string         `gorm:"type:varchar(64);not null" json:"tenant_id"`
	EvidenceKey  string         `gorm:"type:varchar(255);not null" json:"evidence_key"`
	EvidenceType string         `gorm:"type:varchar(48);not null" json:"evidence_type"`
	Owner        string         `gorm:"type:varchar(32);not null" json:"owner"`
	Payload      datatypes.JSON `gorm:"type:jsonb;not null" json:"payload"`
	PayloadHash  string         `gorm:"type:char(64);not null" json:"payload_hash"`
	ObservedAt   time.Time      `json:"observed_at"`
}

func (ContentResetEvidence) TableName() string { return "content_reset_evidence" }

// ContentResetInventoryDeletion preserves the identity and scope facts of a
// row deleted after a preview's inventory boundary. It has no foreign key to
// content_items because its purpose is to survive that deletion.
type ContentResetInventoryDeletion struct {
	ID                   uint          `gorm:"primaryKey" json:"-"`
	TenantID             string        `gorm:"type:varchar(64);not null" json:"tenant_id"`
	InventorySequence    int64         `gorm:"not null" json:"inventory_sequence"`
	ContentItemID        uuid.UUID     `gorm:"type:uuid;not null" json:"content_item_id"`
	Type                 ContentType   `gorm:"type:varchar(20);not null" json:"type"`
	Status               ContentStatus `gorm:"type:varchar(20);not null" json:"status"`
	ContentSourceID      *uuid.UUID    `gorm:"type:uuid" json:"content_source_id,omitempty"`
	ProcessingGeneration int64         `gorm:"not null" json:"processing_generation"`
	CreatedAt            time.Time     `json:"created_at"`
	PublishedAt          *time.Time    `json:"published_at,omitempty"`
	DeletedAt            time.Time     `json:"deleted_at"`
}

func (ContentResetInventoryDeletion) TableName() string {
	return "content_reset_inventory_deletions"
}

// LifecycleOperationClaim represents a durable conflict boundary. Integrating
// a claim requires the affected owner to check it both at admission and at its
// mutation boundary; the model intentionally grants no implicit authority.
type LifecycleOperationClaim struct {
	ID           uint       `gorm:"primaryKey" json:"-"`
	PublicID     uuid.UUID  `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID     string     `gorm:"type:varchar(64);not null" json:"tenant_id"`
	CampaignID   *uint      `json:"-"`
	Owner        string     `gorm:"type:varchar(32);not null" json:"owner"`
	ResourceType string     `gorm:"type:varchar(32);not null" json:"resource_type"`
	ResourceKey  string     `gorm:"type:varchar(255);not null" json:"resource_key"`
	Phase        string     `gorm:"type:varchar(32);not null" json:"phase"`
	State        string     `gorm:"type:varchar(16);not null" json:"state"`
	FencingToken uuid.UUID  `gorm:"type:uuid;not null" json:"fencing_token"`
	Generation   int64      `gorm:"not null" json:"generation"`
	LeaseUntil   *time.Time `json:"lease_until,omitempty"`
	ReleasedAt   *time.Time `json:"released_at,omitempty"`
	CreatedAt    time.Time  `json:"created_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
}

func (LifecycleOperationClaim) TableName() string { return "lifecycle_operation_claims" }

// ContentResetIntakePause is an owner-scoped, durable pause receipt. A pause
// survives campaign workers and restarts; only the campaign that owns its
// fencing token may release it.
type ContentResetIntakePause struct {
	ID              uint       `gorm:"primaryKey" json:"-"`
	PublicID        uuid.UUID  `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID        string     `gorm:"type:varchar(64);not null" json:"tenant_id"`
	CampaignID      uint       `gorm:"not null" json:"-"`
	Lane            string     `gorm:"type:varchar(8);not null" json:"lane"`
	ContentSourceID *uuid.UUID `gorm:"type:uuid" json:"content_source_id,omitempty"`
	Owner           string     `gorm:"type:varchar(32);not null" json:"owner"`
	Reason          string     `gorm:"type:varchar(64);not null" json:"reason"`
	State           string     `gorm:"type:varchar(16);not null" json:"state"`
	FencingToken    uuid.UUID  `gorm:"type:uuid;not null" json:"-"`
	Generation      int64      `gorm:"not null" json:"generation"`
	CreatedAt       time.Time  `json:"created_at"`
	ReleasedAt      *time.Time `json:"released_at,omitempty"`
	UpdatedAt       time.Time  `json:"updated_at"`
}

func (ContentResetIntakePause) TableName() string { return "content_reset_intake_pauses" }

// ContentResetMilestone is an immutable operator approval for a lifecycle
// boundary (publication or rollback). It records the exact fences and readiness
// evidence observed at approval; the owning worker revalidates them at effect.
type ContentResetMilestone struct {
	ID           uint           `gorm:"primaryKey" json:"-"`
	PublicID     uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID     string         `gorm:"type:varchar(64);not null" json:"tenant_id"`
	CampaignID   uint           `gorm:"not null" json:"-"`
	RevisionID   uint           `gorm:"not null" json:"-"`
	Kind         string         `gorm:"type:varchar(24);not null" json:"kind"`
	State        string         `gorm:"type:varchar(16);not null" json:"state"`
	ManifestHash string         `gorm:"type:char(64);not null" json:"manifest_hash"`
	Payload      datatypes.JSON `gorm:"type:jsonb;not null" json:"payload"`
	PayloadHash  string         `gorm:"type:char(64);not null" json:"payload_hash"`
	ApprovedBy   string         `gorm:"type:varchar(255);not null" json:"approved_by"`
	ApprovedAt   time.Time      `json:"approved_at"`
	ExpiresAt    time.Time      `json:"expires_at"`
	ConsumedAt   *time.Time     `json:"consumed_at,omitempty"`
	RevokedAt    *time.Time     `json:"revoked_at,omitempty"`
	CreatedAt    time.Time      `json:"created_at"`
}

func (ContentResetMilestone) TableName() string { return "content_reset_milestones" }
