package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

const (
	RetentionModeObserve  = "observe"
	RetentionModeAssist   = "assist"
	RetentionModeSafeAuto = "safe_auto"

	RetentionVerdictHealthy             = "healthy"
	RetentionVerdictWarning             = "warning"
	RetentionVerdictActionRequired      = "action_required"
	RetentionVerdictCritical            = "critical"
	RetentionVerdictMaintenanceRequired = "maintenance_required"
	RetentionVerdictRecoveryInProgress  = "recovery_in_progress"
	RetentionVerdictInconclusive        = "inconclusive"

	RetentionRunRunning   = "running"
	RetentionRunCompleted = "completed"
	RetentionRunPartial   = "partial"
	RetentionRunFailed    = "failed"
	RetentionRunBlocked   = "blocked"

	RetentionActionWouldExecute     = "would_execute"
	RetentionActionApprovalRequired = "approval_required"
	RetentionActionApproved         = "approved"
	RetentionActionRejected         = "rejected"
	RetentionActionReady            = "ready"
	RetentionActionClaimed          = "claimed"
	RetentionActionRunning          = "running"
	RetentionActionToolSucceeded    = "tool_succeeded"
	RetentionActionToolFailed       = "tool_failed"
	RetentionActionVerifying        = "verifying"
	RetentionActionVerified         = "verification_passed"
	RetentionActionVerifyFailed     = "verification_failed"
	RetentionActionSkipped          = "skipped"
	RetentionActionExpired          = "expired"

	RetentionActionPreviewNewsCompaction = "news_database.preview_compaction"
)

const (
	retentionMiB = int64(1024 * 1024)
)

type RetentionPolicy struct {
	ID       uint      `gorm:"primaryKey" json:"-"`
	PublicID uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID string    `gorm:"type:varchar(64);not null;uniqueIndex" json:"tenant_id"`

	Enabled                 bool       `gorm:"not null;default:false" json:"enabled"`
	Mode                    string     `gorm:"type:varchar(24);not null;default:'observe'" json:"mode"`
	ScheduleIntervalMinutes int        `gorm:"not null;default:360" json:"schedule_interval_minutes"`
	PausedUntil             *time.Time `json:"paused_until,omitempty"`
	LastRunAt               *time.Time `json:"last_run_at,omitempty"`
	PolicyVersion           int        `gorm:"not null;default:1" json:"policy_version"`
	NewsPolicyVersion       *int       `json:"news_policy_version,omitempty"`
	NewsTimezone            string     `gorm:"type:varchar(64);not null;default:'Asia/Riyadh'" json:"news_timezone"`

	DatabaseTargetBytes   int64 `gorm:"not null" json:"database_target_bytes"`
	DatabaseWarningBytes  int64 `gorm:"not null" json:"database_warning_bytes"`
	DatabaseActionBytes   int64 `gorm:"not null" json:"database_action_bytes"`
	DatabaseCriticalBytes int64 `gorm:"not null" json:"database_critical_bytes"`
	WarningForecastDays   int   `gorm:"not null;default:14" json:"warning_forecast_days"`
	ActionForecastDays    int   `gorm:"not null;default:7" json:"action_forecast_days"`
	CriticalForecastHours int   `gorm:"not null;default:48" json:"critical_forecast_hours"`

	MaxRowsPerRun        int            `gorm:"not null;default:500" json:"max_rows_per_run"`
	MaxBytesPerRun       int64          `gorm:"not null;default:33554432" json:"max_bytes_per_run"`
	MaxActionsPerRun     int            `gorm:"not null;default:4" json:"max_actions_per_run"`
	ActionModes          datatypes.JSON `gorm:"type:jsonb" json:"action_modes,omitempty"`
	TrustMinDecisions    int            `gorm:"not null;default:20" json:"trust_min_decisions"`
	TrustMinAgreementPct int            `gorm:"not null;default:95" json:"trust_min_agreement_pct"`
	UpdatedBy            string         `gorm:"type:varchar(255)" json:"updated_by,omitempty"`
	CreatedAt            time.Time      `json:"created_at"`
	UpdatedAt            time.Time      `json:"updated_at"`
}

func (RetentionPolicy) TableName() string { return "retention_policies" }

func DefaultRetentionPolicy(tenantID string) RetentionPolicy {
	return RetentionPolicy{
		TenantID: tenantID, Mode: RetentionModeObserve, ScheduleIntervalMinutes: 360,
		PolicyVersion: 1, NewsTimezone: "Asia/Riyadh",
		DatabaseTargetBytes: 400 * retentionMiB, DatabaseWarningBytes: 400 * retentionMiB,
		DatabaseActionBytes: 440 * retentionMiB, DatabaseCriticalBytes: 480 * retentionMiB,
		WarningForecastDays: 14, ActionForecastDays: 7, CriticalForecastHours: 48,
		MaxRowsPerRun: 500, MaxBytesPerRun: 32 * retentionMiB, MaxActionsPerRun: 4,
		ActionModes:       datatypes.JSON([]byte(`{"news_database.compact_story":"assist","news_database.delete_rows":"assist","database.physical_rewrite":"assist"}`)),
		TrustMinDecisions: 20, TrustMinAgreementPct: 95,
	}
}

type RetentionDBSample struct {
	ID                 uint           `gorm:"primaryKey" json:"-"`
	PublicID           uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID           string         `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	DatabaseBytes      int64          `gorm:"not null" json:"database_bytes"`
	ProviderBytes      *int64         `json:"provider_bytes,omitempty"`
	ProviderSource     string         `gorm:"type:varchar(32);not null;default:'unavailable'" json:"provider_source"`
	ProviderMeasuredAt *time.Time     `json:"provider_measured_at,omitempty"`
	RelationBytes      int64          `gorm:"not null;default:0" json:"relation_bytes"`
	IndexBytes         int64          `gorm:"not null;default:0" json:"index_bytes"`
	ToastBytes         int64          `gorm:"not null;default:0" json:"toast_bytes"`
	AllocatedBytes     int64          `gorm:"not null;default:0" json:"allocated_bytes"`
	ReusableBytes      int64          `gorm:"not null;default:0" json:"reusable_bytes"`
	LiveTuples         int64          `gorm:"not null;default:0" json:"live_tuples"`
	DeadTuples         int64          `gorm:"not null;default:0" json:"dead_tuples"`
	RelationBreakdown  datatypes.JSON `gorm:"type:jsonb" json:"relation_breakdown,omitempty"`
	ForecastInputs     datatypes.JSON `gorm:"type:jsonb" json:"forecast_inputs,omitempty"`
	MeasuredAt         time.Time      `gorm:"not null;index" json:"measured_at"`
	CreatedAt          time.Time      `json:"created_at"`
}

func (RetentionDBSample) TableName() string { return "retention_db_samples" }

type RetentionMonth struct {
	ID                   uint       `gorm:"primaryKey" json:"-"`
	PublicID             uuid.UUID  `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID             string     `gorm:"type:varchar(64);not null;uniqueIndex:idx_retention_month_tenant_start,priority:1" json:"tenant_id"`
	MonthStart           time.Time  `gorm:"type:date;uniqueIndex:idx_retention_month_tenant_start,priority:2" json:"month_start"`
	State                string     `gorm:"type:varchar(32);not null;default:'open'" json:"state"`
	StateReason          string     `gorm:"type:text" json:"state_reason,omitempty"`
	CompactedStoryCount  int        `gorm:"not null;default:0" json:"compacted_story_count"`
	RetainedContentCount int        `gorm:"not null;default:0" json:"retained_content_count"`
	ReviewRevisionID     *uuid.UUID `gorm:"type:uuid" json:"review_revision_id,omitempty"`
	CreatedAt            time.Time  `json:"created_at"`
	UpdatedAt            time.Time  `json:"updated_at"`
}

func (RetentionMonth) TableName() string { return "retention_months" }

type RetentionRun struct {
	ID               uint           `gorm:"primaryKey" json:"-"`
	PublicID         uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID         string         `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	Lane             string         `gorm:"type:varchar(32);not null;default:'database'" json:"lane"`
	Trigger          string         `gorm:"type:varchar(24);not null" json:"trigger"`
	Mode             string         `gorm:"type:varchar(24);not null" json:"mode"`
	Status           string         `gorm:"type:varchar(24);not null;index" json:"status"`
	Verdict          string         `gorm:"type:varchar(40);not null" json:"verdict"`
	PolicyVersion    int            `gorm:"not null" json:"policy_version"`
	CorrelationID    uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid()" json:"correlation_id"`
	BeforeEvidence   datatypes.JSON `gorm:"type:jsonb" json:"before_evidence,omitempty"`
	ForecastEvidence datatypes.JSON `gorm:"type:jsonb" json:"forecast_evidence,omitempty"`
	AfterEvidence    datatypes.JSON `gorm:"type:jsonb" json:"after_evidence,omitempty"`
	Counts           datatypes.JSON `gorm:"type:jsonb" json:"counts,omitempty"`
	CreatedBy        string         `gorm:"type:varchar(255)" json:"created_by,omitempty"`
	StartedAt        time.Time      `gorm:"not null;index" json:"started_at"`
	HeartbeatAt      time.Time      `gorm:"not null" json:"heartbeat_at"`
	FinishedAt       *time.Time     `json:"finished_at,omitempty"`
	ErrorClass       string         `gorm:"type:varchar(64);not null;default:'none'" json:"error_class"`
	Error            string         `gorm:"type:text" json:"error,omitempty"`
	CreatedAt        time.Time      `json:"created_at"`
	UpdatedAt        time.Time      `json:"updated_at"`
}

func (RetentionRun) TableName() string { return "retention_runs" }

type RetentionAction struct {
	ID                  uint           `gorm:"primaryKey" json:"-"`
	PublicID            uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	RunID               uint           `gorm:"not null;index" json:"-"`
	TenantID            string         `gorm:"type:varchar(64);not null;index;uniqueIndex:idx_retention_action_tenant_key,priority:1" json:"tenant_id"`
	ActionClass         string         `gorm:"type:varchar(80);not null" json:"action_class"`
	OwnerSystem         string         `gorm:"type:varchar(64);not null" json:"owner_system"`
	TargetScope         string         `gorm:"type:text;not null" json:"target_scope"`
	Mode                string         `gorm:"type:varchar(24);not null" json:"mode"`
	Decision            string         `gorm:"type:varchar(32);not null" json:"decision"`
	Outcome             string         `gorm:"type:varchar(32);not null;index" json:"outcome"`
	IdempotencyKey      string         `gorm:"type:varchar(255);not null;uniqueIndex:idx_retention_action_tenant_key,priority:2" json:"idempotency_key"`
	EvidenceFingerprint string         `gorm:"type:char(64);not null" json:"evidence_fingerprint"`
	ManifestHash        *string        `gorm:"type:char(64)" json:"manifest_hash,omitempty"`
	TargetCount         int            `gorm:"not null;default:0" json:"target_count"`
	ProtectedCount      int            `gorm:"not null;default:0" json:"protected_count"`
	EstimatedBytes      int64          `gorm:"not null;default:0" json:"estimated_bytes"`
	Guardrail           string         `gorm:"type:text" json:"guardrail,omitempty"`
	Evidence            datatypes.JSON `gorm:"type:jsonb" json:"evidence,omitempty"`
	BeforeBytes         *int64         `json:"before_bytes,omitempty"`
	ForecastAfterBytes  *int64         `json:"forecast_after_bytes,omitempty"`
	AfterBytes          *int64         `json:"after_bytes,omitempty"`
	ClaimToken          *uuid.UUID     `gorm:"type:uuid" json:"-"`
	ClaimExpiresAt      *time.Time     `json:"claim_expires_at,omitempty"`
	ApprovedAt          *time.Time     `json:"approved_at,omitempty"`
	ApprovedBy          string         `json:"approved_by,omitempty"`
	RejectedAt          *time.Time     `json:"rejected_at,omitempty"`
	RejectedBy          string         `json:"rejected_by,omitempty"`
	RejectionReason     string         `json:"rejection_reason,omitempty"`
	StartedAt           *time.Time     `json:"started_at,omitempty"`
	FinishedAt          *time.Time     `json:"finished_at,omitempty"`
	Verification        datatypes.JSON `gorm:"type:jsonb" json:"verification,omitempty"`
	RecoveryRef         string         `gorm:"type:text" json:"recovery_ref,omitempty"`
	CreatedAt           time.Time      `json:"created_at"`
	UpdatedAt           time.Time      `json:"updated_at"`
}

func (RetentionAction) TableName() string { return "retention_actions" }

// RetentionCompactionManifest freezes a small, tenant-scoped set of News
// identities before a human can approve a future compaction executor. It has
// no cascading content relationship so audit evidence survives retirement.
type RetentionCompactionManifest struct {
	ID                  uint           `gorm:"primaryKey" json:"-"`
	PublicID            uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	RunID               uint           `gorm:"not null;index" json:"-"`
	ActionID            *uint          `gorm:"uniqueIndex" json:"-"`
	TenantID            string         `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	PolicyVersion       int            `gorm:"not null" json:"policy_version"`
	Timezone            string         `gorm:"type:varchar(64);not null" json:"timezone"`
	ManifestHash        string         `gorm:"type:char(64);not null;uniqueIndex" json:"manifest_hash"`
	State               string         `gorm:"type:varchar(24);not null;default:'prepared'" json:"state"`
	StoryIDs            datatypes.JSON `gorm:"type:jsonb;not null" json:"story_ids"`
	AnchorContentIDs    datatypes.JSON `gorm:"type:jsonb;not null" json:"anchor_content_ids"`
	ProtectedContentIDs datatypes.JSON `gorm:"type:jsonb;not null" json:"protected_content_ids"`
	RetireContentIDs    datatypes.JSON `gorm:"type:jsonb;not null" json:"retire_content_ids"`
	Evidence            datatypes.JSON `gorm:"type:jsonb;not null" json:"evidence"`
	StoryCount          int            `gorm:"not null" json:"story_count"`
	AnchorCount         int            `gorm:"not null" json:"anchor_count"`
	ProtectedCount      int            `gorm:"not null" json:"protected_count"`
	RetireCount         int            `gorm:"not null" json:"retire_count"`
	EstimatedBytes      int64          `gorm:"not null" json:"estimated_bytes"`
	ExpiresAt           time.Time      `gorm:"not null;index" json:"expires_at"`
	ApprovedAt          *time.Time     `json:"approved_at,omitempty"`
	ApprovedBy          string         `gorm:"type:varchar(255)" json:"approved_by,omitempty"`
	CreatedAt           time.Time      `json:"created_at"`
	UpdatedAt           time.Time      `json:"updated_at"`
}

func (RetentionCompactionManifest) TableName() string { return "retention_compaction_manifests" }

// RetentionCompactionBatch makes the exact one-batch V1 mutation durable. A
// later multi-batch executor will append indexes rather than rediscovering or
// widening the manifest.
type RetentionCompactionBatch struct {
	ID             uint           `gorm:"primaryKey" json:"-"`
	PublicID       uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	ActionID       uint           `gorm:"not null;uniqueIndex:idx_retention_compaction_batch_action,priority:1" json:"-"`
	ManifestID     uint           `gorm:"not null" json:"-"`
	TenantID       string         `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	BatchIndex     int            `gorm:"not null;uniqueIndex:idx_retention_compaction_batch_action,priority:2" json:"batch_index"`
	State          string         `gorm:"type:varchar(32);not null" json:"state"`
	TargetHash     string         `gorm:"type:char(64);not null" json:"target_hash"`
	TargetIDs      datatypes.JSON `gorm:"type:jsonb;not null" json:"target_ids"`
	TargetCount    int            `gorm:"not null" json:"target_count"`
	EstimatedBytes int64          `gorm:"not null" json:"estimated_bytes"`
	BeforeEvidence datatypes.JSON `gorm:"type:jsonb" json:"before_evidence,omitempty"`
	AfterEvidence  datatypes.JSON `gorm:"type:jsonb" json:"after_evidence,omitempty"`
	Error          string         `gorm:"type:text" json:"error,omitempty"`
	StartedAt      *time.Time     `json:"started_at,omitempty"`
	FinishedAt     *time.Time     `json:"finished_at,omitempty"`
	CreatedAt      time.Time      `json:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at"`
}

func (RetentionCompactionBatch) TableName() string { return "retention_compaction_batches" }

// NewsIngestTombstone retains only irreversibly hashed identity evidence. It
// never references sources, and its original content UUID is a value so the
// retention deletion cannot erase the reason a later crawler was rejected.
type NewsIngestTombstone struct {
	ID                 uint       `gorm:"primaryKey" json:"-"`
	PublicID           uuid.UUID  `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID           string     `gorm:"type:varchar(64);not null;uniqueIndex:idx_news_tombstone_identity,priority:1" json:"tenant_id"`
	IdentityHash       string     `gorm:"type:char(64);not null;uniqueIndex:idx_news_tombstone_identity,priority:2" json:"identity_hash"`
	SourceIdentityHash string     `gorm:"type:char(64);not null" json:"source_identity_hash"`
	OriginalURLHash    string     `gorm:"type:char(64);not null;index" json:"original_url_hash"`
	OriginalContentID  uuid.UUID  `gorm:"type:uuid;not null" json:"original_content_id"`
	ManifestHash       string     `gorm:"type:char(64);not null" json:"manifest_hash"`
	RetirementActionID uint       `gorm:"not null" json:"-"`
	Reason             string     `gorm:"type:varchar(64);not null" json:"reason"`
	ReplayConsumedAt   *time.Time `json:"replay_consumed_at,omitempty"`
	CreatedAt          time.Time  `json:"created_at"`
}

func (NewsIngestTombstone) TableName() string { return "news_ingest_tombstones" }

type RetentionHold struct {
	ID            uint       `gorm:"primaryKey" json:"-"`
	PublicID      uuid.UUID  `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID      string     `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	TargetType    string     `gorm:"type:varchar(24);not null" json:"target_type"`
	TargetID      uuid.UUID  `gorm:"type:uuid;not null;index" json:"target_id"`
	HoldClass     string     `gorm:"type:varchar(24);not null" json:"hold_class"`
	Reason        string     `gorm:"type:text;not null" json:"reason"`
	CreatedBy     string     `gorm:"type:varchar(255);not null" json:"created_by"`
	ExpiresAt     *time.Time `json:"expires_at,omitempty"`
	ReleasedAt    *time.Time `json:"released_at,omitempty"`
	ReleasedBy    string     `json:"released_by,omitempty"`
	ReleaseReason string     `json:"release_reason,omitempty"`
	CreatedAt     time.Time  `json:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at"`
}

func (RetentionHold) TableName() string { return "retention_holds" }
