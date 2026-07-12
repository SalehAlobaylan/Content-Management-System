package models

import (
	"time"

	"gorm.io/datatypes"
)

// Embedding & Model Lifecycle System (stage 10) — persistence.
//
// Slice 2 introduces the audit half: policy (singleton), runs, and findings.
// The campaign half (campaigns/actions/exceptions) lands in Slice 3. Findings
// are OBSERVATIONS; the future action ledger is EXECUTION history — the plan
// forbids them sharing a table.

// ── Per-surface verdicts (§8) ────────────────────────────────────────────────
const (
	EmbeddingVerdictCoherent      = "coherent"       // stale=0, unstamped=0, no mixed
	EmbeddingVerdictUnstampedDebt = "unstamped_debt" // unstamped>0, stale=0 (minor)
	EmbeddingVerdictDrifting      = "drifting"       // stale>0, no campaign (alarm)
	EmbeddingVerdictMigrating     = "migrating"      // stale>0, campaign running (watched)
	EmbeddingVerdictMixedSpace    = "mixed_space"    // centroid/consumer coherence violation
	EmbeddingVerdictBlocked       = "blocked"        // campaign guard failed
	EmbeddingVerdictCheckError    = "check_error"    // evaluator failed; last verdict carried
)

// ── Run headline (family vocabulary) ─────────────────────────────────────────
const (
	EmbeddingHeadlineAllClear  = "all_clear"
	EmbeddingHeadlineWatching  = "watching"
	EmbeddingHeadlineAttention = "attention"
)

// ── Run status ───────────────────────────────────────────────────────────────
const (
	EmbeddingRunRunning   = "running"
	EmbeddingRunCompleted = "completed"
	EmbeddingRunPartial   = "partial"
	EmbeddingRunFailed    = "failed"

	EmbeddingRunTriggerManual    = "manual"
	EmbeddingRunTriggerScheduled = "scheduled"
)

// ── Finding check keys + status/severity ─────────────────────────────────────
const (
	EmbeddingCheckStaleInventory    = "stale_inventory"
	EmbeddingCheckMixedSpace        = "mixed_space"
	EmbeddingCheckNumericSanity     = "numeric_sanity"
	EmbeddingCheckConsumerGuard     = "consumer_guard_coverage"
	EmbeddingCheckWriterRegression  = "writer_regression"
	EmbeddingCheckDimChangeRequired = "dim_change_required"
	EmbeddingCheckSpaceUnavailable  = "expected_space_unavailable"

	EmbeddingFindingPass       = "pass"
	EmbeddingFindingViolation  = "violation"
	EmbeddingFindingCheckError = "check_error"

	EmbeddingSevInfo     = "info"
	EmbeddingSevMinor    = "minor"
	EmbeddingSevMajor    = "major"
	EmbeddingSevCritical = "critical"
)

// EmbeddingLifecyclePolicy is the platform singleton controlling the audit
// cadence. Observation (audit) is deliberately a separate switch from campaign
// execution — `campaigns_paused_until` never blinds the audit.
type EmbeddingLifecyclePolicy struct {
	ID       uint   `gorm:"primaryKey" json:"-"`
	TenantID string `gorm:"type:varchar(64);not null;uniqueIndex:idx_embedding_lifecycle_policy_tenant" json:"tenant_id"`

	// AuditEnabled gates the SCHEDULER only. Manual runs are always allowed.
	AuditEnabled         bool `gorm:"not null;default:false" json:"audit_enabled"`
	AuditIntervalMinutes int  `gorm:"not null;default:360" json:"audit_interval_minutes"`

	// Coherence sampling / campaign caps (campaign caps read in Slice 3).
	NumericSampleSize int `gorm:"not null;default:64" json:"numeric_sample_size"`
	ItemsPerBatch     int `gorm:"not null;default:200" json:"items_per_batch"`
	BatchesPerRun     int `gorm:"not null;default:1" json:"batches_per_run"`
	DailyItemCap      int `gorm:"not null;default:5000" json:"daily_item_cap"`
	RetryCeiling      int `gorm:"not null;default:3" json:"retry_ceiling"`
	// ExpectedDescriptorOverride — crisis escape hatch. JSON per space; normally
	// null. Admin-role-only, audited, reason+expiry (enforced in the handler).
	ExpectedDescriptorOverride datatypes.JSON `gorm:"type:jsonb" json:"expected_descriptor_override,omitempty"`
	OverrideReason             string         `gorm:"type:text" json:"override_reason,omitempty"`
	OverrideExpiresAt          *time.Time     `json:"override_expires_at,omitempty"`

	// CampaignsPausedUntil stops campaign MUTATION only (Slice 3). Named
	// narrowly so observation never appears paused.
	CampaignsPausedUntil *time.Time `json:"campaigns_paused_until,omitempty"`
	LastAuditAt          *time.Time `json:"last_audit_at,omitempty"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (EmbeddingLifecyclePolicy) TableName() string { return "embedding_lifecycle_policies" }

// EmbeddingLifecycleRun is one audit pass (Lanes A+B).
type EmbeddingLifecycleRun struct {
	ID       uint   `gorm:"primaryKey" json:"id"`
	TenantID string `gorm:"type:varchar(64);not null;index:idx_embedding_lifecycle_runs_tenant" json:"tenant_id"`

	Trigger  string `gorm:"type:varchar(16);not null" json:"trigger"`
	Status   string `gorm:"type:varchar(16);not null" json:"status"`
	Headline string `gorm:"type:varchar(24)" json:"headline"`

	// PerSurface is a jsonb map surface_key -> {verdict, counts...} — the
	// cockpit's per-space breakdown, computed per run (no snapshot table).
	PerSurface datatypes.JSON `gorm:"type:jsonb" json:"per_surface,omitempty"`

	ViolationsMajor int `gorm:"not null;default:0" json:"violations_major"`
	ViolationsMinor int `gorm:"not null;default:0" json:"violations_minor"`
	CheckErrors     int `gorm:"not null;default:0" json:"check_errors"`

	StartedAt   time.Time  `gorm:"autoCreateTime" json:"started_at"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
	DurationMS  int64      `gorm:"not null;default:0" json:"duration_ms"`

	Error      string `gorm:"type:text" json:"error,omitempty"`
	ErrorClass string `gorm:"type:varchar(48)" json:"error_class,omitempty"`
}

func (EmbeddingLifecycleRun) TableName() string { return "embedding_lifecycle_runs" }

// EmbeddingLifecycleFinding is one audit observation. Aggregate pass rows (one
// per check per surface with a count); per-target violation rows up to a cap
// plus an overflow aggregate.
type EmbeddingLifecycleFinding struct {
	ID       uint   `gorm:"primaryKey" json:"id"`
	RunID    uint   `gorm:"not null;index:idx_embedding_lifecycle_findings_run" json:"run_id"`
	TenantID string `gorm:"type:varchar(64);not null" json:"tenant_id"`

	SurfaceKey string `gorm:"type:varchar(48);not null;index:idx_embedding_lifecycle_findings_surface" json:"surface_key"`
	CheckKey   string `gorm:"type:varchar(48);not null" json:"check_key"`
	Status     string `gorm:"type:varchar(16);not null" json:"status"`
	Severity   string `gorm:"type:varchar(16)" json:"severity,omitempty"`

	// TargetType/TargetID identify the offending row for violation findings;
	// empty for aggregate pass rows.
	TargetType string `gorm:"type:varchar(32)" json:"target_type,omitempty"`
	TargetID   string `gorm:"type:varchar(80)" json:"target_id,omitempty"`

	// Count carries the aggregate magnitude on pass/overflow rows.
	Count int `gorm:"not null;default:0" json:"count"`

	Evidence datatypes.JSON `gorm:"type:jsonb" json:"evidence,omitempty"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
}

func (EmbeddingLifecycleFinding) TableName() string { return "embedding_lifecycle_findings" }
