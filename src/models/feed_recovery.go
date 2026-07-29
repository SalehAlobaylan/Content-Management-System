package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

// FeedRecoveryPlan is immutable preflight evidence. Execution remains bound to
// this identity so every repair/rotate action can be audited and resumed.
type FeedRecoveryPlan struct {
	ID               uint           `gorm:"primaryKey" json:"-"`
	PublicID         uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID         string         `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	Lane             string         `gorm:"type:varchar(16);not null" json:"lane"`
	Level            string         `gorm:"type:varchar(24);not null" json:"level"`
	CapacityMode     string         `gorm:"type:varchar(24);not null" json:"capacity_mode"`
	State            string         `gorm:"type:varchar(32);not null" json:"state"`
	PlanHash         string         `gorm:"type:char(64);not null" json:"plan_hash"`
	ManifestHash     string         `gorm:"type:char(64);not null" json:"manifest_hash"`
	TargetCount      int            `json:"target_count"`
	TargetRootHash   string         `gorm:"type:char(64)" json:"target_root_hash,omitempty"`
	TargetScope      string         `gorm:"type:varchar(48)" json:"target_scope,omitempty"`
	TargetSegments   int            `json:"target_segments,omitempty"`
	TargetByteSize   int64          `json:"target_byte_size,omitempty"`
	SourceChecksum   string         `gorm:"type:char(64);not null" json:"source_checksum"`
	SourceCount      int            `json:"source_count"`
	Evidence         datatypes.JSON `gorm:"type:jsonb" json:"evidence"`
	PolicySnapshot   datatypes.JSON `gorm:"type:jsonb" json:"policy_snapshot"`
	NoFullRollback   bool           `json:"no_full_rollback"`
	PurgeManifest    datatypes.JSON `gorm:"type:jsonb" json:"purge_manifest,omitempty"`
	ManifestFrozenAt *time.Time     `json:"manifest_frozen_at,omitempty"`
	ExpiresAt        time.Time      `json:"expires_at"`
	CreatedBy        string         `json:"created_by"`
	CreatedAt        time.Time      `json:"created_at"`
	UpdatedAt        time.Time      `json:"updated_at"`
}

func (FeedRecoveryPlan) TableName() string { return "feed_recovery_plans" }

// FeedRecoveryPlanTarget is the normalized, immutable destructive scope. The
// JSON purge manifest remains a readable compatibility/proof envelope, while
// these rows are the authoritative target set for execution and retry.
type FeedRecoveryPlanTarget struct {
	ID               uint      `gorm:"primaryKey" json:"-"`
	PlanID           uint      `gorm:"not null;index;uniqueIndex:idx_feed_recovery_plan_target,priority:1" json:"-"`
	TenantID         string    `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	Lane             string    `gorm:"type:varchar(16);not null" json:"lane"`
	TargetType       string    `gorm:"type:varchar(32);not null;uniqueIndex:idx_feed_recovery_plan_target,priority:2" json:"target_type"`
	TargetID         uuid.UUID `gorm:"type:uuid;not null;uniqueIndex:idx_feed_recovery_plan_target,priority:3" json:"target_id"`
	Ordinal          int64     `gorm:"not null" json:"ordinal"`
	Protected        bool      `gorm:"not null;default:false" json:"protected"`
	ProtectionReason string    `gorm:"type:text" json:"protection_reason,omitempty"`
	EvidenceHash     string    `gorm:"type:char(64);not null" json:"evidence_hash"`
	CreatedAt        time.Time `json:"created_at"`
}

func (FeedRecoveryPlanTarget) TableName() string { return "feed_recovery_plan_targets" }

type FeedRecoveryRun struct {
	ID                    uint           `gorm:"primaryKey" json:"-"`
	PublicID              uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	PlanID                uint           `json:"-"`
	TenantID              string         `json:"tenant_id"`
	Lane                  string         `json:"lane"`
	CorrelationID         uuid.UUID      `json:"correlation_id"`
	Phase                 string         `json:"phase"`
	NotBefore             *time.Time     `json:"not_before,omitempty"`
	CancelDeadline        *time.Time     `json:"cancel_deadline,omitempty"`
	HeartbeatAt           *time.Time     `json:"heartbeat_at,omitempty"`
	ClaimToken            *uuid.UUID     `json:"-"`
	ClaimExpiresAt        *time.Time     `json:"claim_expires_at,omitempty"`
	LaneLease             string         `json:"lane_lease,omitempty"`
	RollbackDeadline      *time.Time     `json:"rollback_deadline,omitempty"`
	VerificationDueAt     *time.Time     `json:"verification_due_at,omitempty"`
	ActiveGenerationID    *uuid.UUID     `json:"active_generation_id,omitempty"`
	CandidateGenerationID *uuid.UUID     `json:"candidate_generation_id,omitempty"`
	Outcome               string         `json:"outcome,omitempty"`
	Error                 string         `json:"error,omitempty"`
	DestructiveManifest   datatypes.JSON `gorm:"type:jsonb" json:"destructive_manifest,omitempty"`
	DestructiveLane       string         `json:"destructive_lane,omitempty"`
	ExpectedEmpty         bool           `json:"expected_empty"`
	RecoveryArtifactRef   string         `json:"recovery_artifact_ref,omitempty"`
	CreatedAt             time.Time      `json:"created_at"`
	UpdatedAt             time.Time      `json:"updated_at"`
}

func (FeedRecoveryRun) TableName() string { return "feed_recovery_runs" }

type FeedRecoveryLaneLease struct {
	TenantID     string    `gorm:"primaryKey;type:varchar(64)" json:"tenant_id"`
	Lane         string    `gorm:"primaryKey;type:varchar(16)" json:"lane"`
	RunID        uint      `gorm:"not null;index" json:"-"`
	FencingToken uuid.UUID `gorm:"type:uuid;not null;uniqueIndex" json:"fencing_token"`
	AcquiredAt   time.Time `gorm:"not null" json:"acquired_at"`
	HeartbeatAt  time.Time `gorm:"not null" json:"heartbeat_at"`
	ExpiresAt    time.Time `gorm:"not null;index" json:"expires_at"`
}

func (FeedRecoveryLaneLease) TableName() string { return "feed_recovery_lane_leases" }

type FeedRecoveryApproval struct {
	ID              uint       `gorm:"primaryKey" json:"-"`
	PublicID        uuid.UUID  `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	PlanID          uint       `json:"-"`
	TenantID        string     `json:"tenant_id"`
	Actor           string     `json:"actor"`
	PlanHash        string     `json:"plan_hash"`
	ManifestHash    string     `json:"manifest_hash"`
	TargetCount     int        `json:"target_count"`
	PhraseProofHash string     `json:"-"`
	ReauthJTI       string     `json:"-"`
	NoFullRollback  bool       `json:"no_full_rollback"`
	ApprovedAt      time.Time  `json:"approved_at"`
	ConsumedAt      *time.Time `json:"consumed_at,omitempty"`
}

func (FeedRecoveryApproval) TableName() string { return "feed_recovery_approvals" }

type FeedRecoveryAction struct {
	ID             uint           `gorm:"primaryKey" json:"-"`
	PublicID       uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	RunID          uint           `json:"-"`
	ActionType     string         `json:"action_type"`
	State          string         `json:"state"`
	IdempotencyKey string         `json:"idempotency_key"`
	Evidence       datatypes.JSON `json:"evidence"`
	Error          string         `json:"error,omitempty"`
	CreatedAt      time.Time      `json:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at"`
}

func (FeedRecoveryAction) TableName() string { return "feed_recovery_actions" }

type FeedRecoveryMediaPurgeItem struct {
	ID                     uint           `gorm:"primaryKey" json:"-"`
	PublicID               uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	RunID                  uint           `gorm:"not null;index;uniqueIndex:idx_feed_recovery_media_saga,priority:1" json:"-"`
	PlanID                 uint           `gorm:"not null;index" json:"-"`
	TenantID               string         `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	ContentItemID          uuid.UUID      `gorm:"type:uuid;not null;uniqueIndex:idx_feed_recovery_media_saga,priority:2" json:"content_item_id"`
	ManifestHash           string         `gorm:"type:char(64);not null" json:"manifest_hash"`
	ItemHash               string         `gorm:"type:char(64);not null" json:"item_hash"`
	ProviderObjects        datatypes.JSON `gorm:"type:jsonb;not null" json:"provider_objects"`
	RecoveryMapPresent     bool           `gorm:"not null;default:false" json:"recovery_map_present"`
	NoFullRollback         bool           `gorm:"not null;default:false" json:"no_full_rollback"`
	State                  string         `gorm:"type:varchar(32);not null;index" json:"state"`
	ProviderRequestID      string         `gorm:"type:text" json:"provider_request_id,omitempty"`
	ProviderResultHash     string         `gorm:"type:char(64)" json:"provider_result_hash,omitempty"`
	ProviderIdempotencyKey string         `gorm:"type:text" json:"provider_idempotency_key,omitempty"`
	AttemptCount           int            `gorm:"not null;default:0" json:"attempt_count"`
	LastError              string         `gorm:"type:text" json:"last_error,omitempty"`
	ObjectDeletedAt        *time.Time     `json:"object_deleted_at,omitempty"`
	CMSDeletedAt           *time.Time     `json:"cms_deleted_at,omitempty"`
	VerifiedAt             *time.Time     `json:"verified_at,omitempty"`
	CreatedAt              time.Time      `json:"created_at"`
	UpdatedAt              time.Time      `json:"updated_at"`
}

func (FeedRecoveryMediaPurgeItem) TableName() string { return "feed_recovery_media_purge_items" }

type FeedRecoveryArtifact struct {
	ID           uint      `gorm:"primaryKey" json:"-"`
	PublicID     uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	PlanID       uint      `json:"-"`
	TenantID     string    `json:"tenant_id"`
	ArtifactType string    `json:"artifact_type"`
	ArtifactKey  string    `json:"artifact_key"`
	SHA256       string    `json:"sha256"`
	ByteSize     int64     `json:"byte_size"`
	State        string    `json:"state"`
	ExpiresAt    time.Time `json:"expires_at"`
	CreatedAt    time.Time `json:"created_at"`
}

func (FeedRecoveryArtifact) TableName() string { return "feed_recovery_artifacts" }

type FeedAvailabilityState struct {
	TenantID          string     `gorm:"primaryKey" json:"tenant_id"`
	Lane              string     `gorm:"primaryKey" json:"lane"`
	State             string     `json:"state"`
	RecoveryRunID     *uint      `json:"recovery_run_id,omitempty"`
	FencingToken      *uuid.UUID `gorm:"type:uuid" json:"-"`
	MessageKey        string     `json:"message_key,omitempty"`
	RetryAfterSeconds *int       `json:"retry_after_seconds,omitempty"`
	UpdatedAt         time.Time  `json:"updated_at"`
}

func (FeedAvailabilityState) TableName() string { return "feed_availability_states" }

type FeedGeneration struct {
	PublicID             uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();primaryKey" json:"id"`
	TenantID             string         `json:"tenant_id"`
	Lane                 string         `json:"lane"`
	State                string         `json:"state"`
	PreviousGenerationID *uuid.UUID     `json:"previous_generation_id,omitempty"`
	BuildWatermark       time.Time      `json:"build_watermark"`
	CaughtUpAt           *time.Time     `json:"caught_up_at,omitempty"`
	CutoverAt            *time.Time     `json:"cutover_at,omitempty"`
	RollbackDeadline     *time.Time     `json:"rollback_deadline,omitempty"`
	Verification         datatypes.JSON `json:"verification"`
	CreatedAt            time.Time      `json:"created_at"`
	UpdatedAt            time.Time      `json:"updated_at"`
}

func (FeedGeneration) TableName() string { return "feed_generations" }

type FeedGenerationHead struct {
	TenantID              string     `gorm:"primaryKey" json:"tenant_id"`
	Lane                  string     `gorm:"primaryKey" json:"lane"`
	ActiveGenerationID    *uuid.UUID `json:"active_generation_id,omitempty"`
	CandidateGenerationID *uuid.UUID `json:"candidate_generation_id,omitempty"`
	Generation            int64      `json:"generation"`
	UpdatedAt             time.Time  `json:"updated_at"`
}

func (FeedGenerationHead) TableName() string { return "feed_generation_heads" }

type FeedGenerationMembership struct {
	GenerationID uuid.UUID `gorm:"primaryKey" json:"generation_id"`
	MemberType   string    `gorm:"primaryKey" json:"member_type"`
	MemberID     uuid.UUID `gorm:"primaryKey" json:"member_id"`
	AttachedAt   time.Time `json:"attached_at"`
}

func (FeedGenerationMembership) TableName() string { return "feed_generation_memberships" }
