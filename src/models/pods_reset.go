package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

type PodsResetRun struct {
	PublicID            uuid.UUID      `gorm:"type:uuid;primaryKey;default:gen_random_uuid()" json:"id"`
	TenantID            string         `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	State               string         `gorm:"type:varchar(24);not null;index" json:"state"`
	Phase               string         `gorm:"type:varchar(32);not null" json:"phase"`
	ManifestHash        string         `gorm:"type:char(64);not null" json:"manifest_hash"`
	SchemaFingerprint   string         `gorm:"type:char(64);not null" json:"schema_fingerprint"`
	PolicyVersion       int            `gorm:"not null;default:1" json:"policy_version"`
	Manifest            datatypes.JSON `gorm:"type:jsonb;not null" json:"manifest"`
	CreatedBy           string         `gorm:"type:varchar(255);not null" json:"created_by"`
	ApprovedBy          string         `gorm:"type:varchar(255)" json:"approved_by,omitempty"`
	ApprovalPhraseHash  string         `gorm:"type:char(64)" json:"-"`
	ApprovedAt          *time.Time     `json:"approved_at,omitempty"`
	ExpiresAt           time.Time      `json:"expires_at"`
	FencingToken        *uuid.UUID     `gorm:"type:uuid" json:"-"`
	ExecutionToken      *uuid.UUID     `gorm:"type:uuid" json:"-"`
	ExecutionLeaseUntil *time.Time     `json:"-"`
	ExecutionEpoch      int64          `gorm:"not null;default:0" json:"execution_epoch"`
	PauseRequested      bool           `gorm:"not null;default:false" json:"pause_requested"`
	Error               string         `gorm:"type:text" json:"error,omitempty"`
	CreatedAt           time.Time      `json:"created_at"`
	UpdatedAt           time.Time      `json:"updated_at"`
}

func (PodsResetRun) TableName() string { return "pods_reset_runs" }

type PodsResetItem struct {
	ID                     uint           `gorm:"primaryKey" json:"-"`
	RunID                  uuid.UUID      `gorm:"type:uuid;not null;uniqueIndex:uq_pods_reset_item,priority:1;index" json:"-"`
	TenantID               string         `gorm:"type:varchar(64);not null" json:"tenant_id"`
	ContentItemID          uuid.UUID      `gorm:"type:uuid;not null;uniqueIndex:uq_pods_reset_item,priority:2" json:"content_item_id"`
	Ordinal                int            `gorm:"not null" json:"ordinal"`
	SnapshotHash           string         `gorm:"type:char(64);not null" json:"snapshot_hash"`
	Decision               datatypes.JSON `gorm:"type:jsonb;not null" json:"decision"`
	State                  string         `gorm:"type:varchar(24);not null;index" json:"state"`
	BlockedReasons         datatypes.JSON `gorm:"type:jsonb;not null" json:"blocked_reasons"`
	LastError              string         `gorm:"type:text" json:"last_error,omitempty"`
	VerificationProbeCount int            `gorm:"not null;default:0" json:"verification_probe_count"`
	VerificationNotBefore  *time.Time     `json:"verification_not_before,omitempty"`
	VerificationEvidence   datatypes.JSON `gorm:"type:jsonb;not null;default:'{}'" json:"verification_evidence"`
	CompletedAt            *time.Time     `json:"completed_at,omitempty"`
	CreatedAt              time.Time      `json:"created_at"`
	UpdatedAt              time.Time      `json:"updated_at"`
}

func (PodsResetItem) TableName() string { return "pods_reset_items" }

type PodsResetObject struct {
	ID            uint       `gorm:"primaryKey" json:"-"`
	RunID         uuid.UUID  `gorm:"type:uuid;not null;uniqueIndex:uq_pods_reset_object,priority:1;index" json:"-"`
	ContentItemID uuid.UUID  `gorm:"type:uuid;not null;uniqueIndex:uq_pods_reset_object,priority:2;index" json:"content_item_id"`
	StorageTier   string     `gorm:"type:varchar(16);not null;uniqueIndex:uq_pods_reset_object,priority:3" json:"storage_tier"`
	Bucket        string     `gorm:"type:varchar(255);not null;uniqueIndex:uq_pods_reset_object,priority:4" json:"bucket"`
	ObjectKey     string     `gorm:"type:text;not null;uniqueIndex:uq_pods_reset_object,priority:5" json:"object_key"`
	ETag          string     `gorm:"type:text;not null" json:"etag"`
	SizeBytes     int64      `gorm:"not null" json:"size_bytes"`
	State         string     `gorm:"type:varchar(24);not null;index" json:"state"`
	AttemptCount  int        `gorm:"not null;default:0" json:"attempt_count"`
	Error         string     `gorm:"type:text" json:"error,omitempty"`
	DeletedAt     *time.Time `json:"deleted_at,omitempty"`
	DeletedByRun  bool       `gorm:"not null;default:false" json:"deleted_by_run"`
	FreedBytes    int64      `gorm:"not null;default:0" json:"freed_bytes"`
	CreatedAt     time.Time  `json:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at"`
}

func (PodsResetObject) TableName() string { return "pods_reset_objects" }

type PodsResetRetirement struct {
	TenantID      string     `gorm:"type:varchar(64);primaryKey" json:"tenant_id"`
	ContentItemID uuid.UUID  `gorm:"type:uuid;primaryKey" json:"content_item_id"`
	RunPublicID   uuid.UUID  `gorm:"type:uuid;not null;index" json:"run_id"`
	ManifestHash  string     `gorm:"type:char(64);not null" json:"manifest_hash"`
	FencingToken  uuid.UUID  `gorm:"type:uuid;not null" json:"-"`
	IdentityHash  string     `gorm:"type:char(64);not null;uniqueIndex:uq_pods_retirement_identity,priority:2" json:"-"`
	State         string     `gorm:"type:varchar(16);not null" json:"state"`
	CreatedAt     time.Time  `json:"created_at"`
	RetiredAt     *time.Time `json:"retired_at,omitempty"`
}

func (PodsResetRetirement) TableName() string { return "pods_reset_retirements" }

type PodsResetAction struct {
	PublicID      uuid.UUID      `gorm:"type:uuid;primaryKey;default:gen_random_uuid()" json:"id"`
	RunID         uuid.UUID      `gorm:"type:uuid;not null;uniqueIndex:uq_pods_reset_action,priority:1" json:"run_id"`
	TenantID      string         `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	ContentItemID uuid.UUID      `gorm:"type:uuid;not null;uniqueIndex:uq_pods_reset_action,priority:2" json:"content_item_id"`
	Action        string         `gorm:"type:varchar(32);not null;uniqueIndex:uq_pods_reset_action,priority:3" json:"action"`
	ManifestHash  string         `gorm:"type:char(64);not null" json:"manifest_hash"`
	Result        datatypes.JSON `gorm:"type:jsonb;not null" json:"result"`
	CreatedAt     time.Time      `json:"created_at"`
}

func (PodsResetAction) TableName() string { return "pods_reset_actions" }
