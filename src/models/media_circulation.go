package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

// Media Circulation Engine — stage 2 (advisory verdict/recommendation layer).
//
// Unlike News Circulation (freshness-driven repeated polling), Media Circulation
// is the admission controller of a bounded, quality-ranked library whose bound is
// economic (S3/R2 bill), not physical. It is a THIN AGGREGATOR: it re-derives
// nothing, it reads the already-built pillars (Storage+Quality, Ranking,
// Atomization) and composes them into verdicts. See docs/media-circulation-engine.md.
//
// This file holds the two persistence foundations for the layer: the tenant
// policy (circulation-specific tuning knobs only — cost/protection knobs stay in
// storage_policies and are reused) and the recommendation ledger the layer emits
// and, later, hands to the stage-5 Autopilot as its track record.

const (
	MediaCirculationPresetConservative = "conservative"
	MediaCirculationPresetBalanced     = "balanced"
	MediaCirculationPresetIntakeHungry = "intake_hungry"

	// Recommendation decision units (D4): admit reasons over a source pull
	// opportunity, evict reasons over a media item/family.
	MediaCirculationUnitSource     = "source"
	MediaCirculationUnitItemFamily = "item_family"

	// Recommendation lifecycle. `processing` is a short-lived claim the Autopilot
	// runner sets to take exclusive ownership of a row across its side effect, so
	// a concurrent human action cannot be clobbered and a crash mid-apply cannot
	// cause the next run to re-execute the same side effect.
	MediaCirculationRecStatusPending    = "pending"
	MediaCirculationRecStatusProcessing = "processing"
	MediaCirculationRecStatusApplied    = "applied"
	MediaCirculationRecStatusDismissed  = "dismissed"
	MediaCirculationRecStatusSuperseded = "superseded"

	MediaCirculationOverrideNeverArchive   = "never_archive"
	MediaCirculationOverrideKeepLatestNHot = "keep_latest_n_hot"
	MediaCirculationOverridePremiumSource  = "premium_source"
	MediaCirculationOverrideNoAtomize      = "no_atomize"
	MediaCirculationOverrideEditorialHold  = "editorial_hold"

	// Autopilot mode ladder (stage 5, G5/G9): Observe is a full shadow pipeline
	// (dry-run ledger, zero side effects); Safe Auto executes the earned safe
	// tier. Promotion between them is always a manual human flip.
	MediaAutopilotModeObserve  = "observe"
	MediaAutopilotModeSafeAuto = "safe_auto"

	// Time-boxed elevated modes (G6): presets that raise specific caps until
	// autopilot_elevated_until; the preset→multiplier table is a code default.
	MediaAutopilotElevatedStorageRelief      = "storage_relief"
	MediaAutopilotElevatedQualityRepair      = "quality_repair"
	MediaAutopilotElevatedAtomizationCatchup = "atomization_catchup"

	MediaAutopilotRunStatusRunning   = "running"
	MediaAutopilotRunStatusCompleted = "completed"
	MediaAutopilotRunStatusPartial   = "partial"
	MediaAutopilotRunStatusFailed    = "failed"

	// Action statuses. Observe writes would_apply/would_skip instead of
	// success/skipped — same rows, only the terminal execute flips (G9).
	// The skip/would-skip *reason* lives in the Guardrail column.
	MediaAutopilotActionStatusRunning          = "running"
	MediaAutopilotActionStatusSuccess          = "success"
	MediaAutopilotActionStatusError            = "error"
	MediaAutopilotActionStatusSkipped          = "skipped"
	MediaAutopilotActionStatusApprovalRequired = "approval_required"
	MediaAutopilotActionStatusWouldApply       = "would_apply"
	MediaAutopilotActionStatusWouldSkip        = "would_skip"

	// Guardrail taxonomy (plan §12) — the fixed vocabulary recorded on every
	// skipped/blocked action so the ledger explains itself. (Override and
	// age-floor protections are enforced upstream at generation time — the
	// runner never sees those subjects, so it emits no reason label for them.)
	MediaAutopilotGuardTrustGate        = "trust_gate"
	MediaAutopilotGuardLowConfidence    = "low_confidence"
	MediaAutopilotGuardStaleScore       = "stale_score"
	MediaAutopilotGuardBudget           = "budget"
	MediaAutopilotGuardQueueDepth       = "queue_depth"
	MediaAutopilotGuardStaleness        = "staleness"
	MediaAutopilotGuardExplorationGuard = "exploration_guard"
	MediaAutopilotGuardApprovalTier     = "approval_tier"
	MediaAutopilotGuardActionLimit      = "action_limit"
	MediaAutopilotGuardAtomizeLimit     = "atomize_limit"
	MediaAutopilotGuardElevatedMode     = "elevated_mode"
)

// MediaCirculationPolicy stores the tenant-level circulation knobs. It holds ONLY
// circulation-specific tuning (D12); storage cost/protection/op-budget knobs are
// NOT duplicated here — they stay in storage_policies and are read through the
// storage-health aggregation (D10). Disabled by default so turning the engine on
// is always a deliberate, reversible choice.
type MediaCirculationPolicy struct {
	ID       uint   `gorm:"primaryKey" json:"-"`
	TenantID string `gorm:"type:varchar(64);not null;uniqueIndex:idx_media_circulation_policy_tenant" json:"tenant_id"`

	Enabled bool   `gorm:"not null;default:false" json:"enabled"`
	Preset  string `gorm:"type:varchar(32);not null;default:'balanced'" json:"preset"`

	// Quality gate (D8): below the storage cost target the absolute value floor
	// applies; at/above target the marginal margin decides whether incoming beats
	// the eviction candidate.
	ValueFloor     float64 `gorm:"type:double precision;not null;default:0.15" json:"value_floor"`
	MarginalMargin float64 `gorm:"type:double precision;not null;default:0.10" json:"marginal_margin"`

	// Intake budget (D13).
	MaxIntakePerSourcePerCycle int `gorm:"type:integer;not null;default:5" json:"max_intake_per_source_per_cycle"`
	MaxIntakePerCycle          int `gorm:"type:integer;not null;default:25" json:"max_intake_per_cycle"`

	// Source cadence bounds — media polls far slower than news.
	SourceMinIntervalMinutes int `gorm:"type:integer;not null;default:60" json:"source_min_interval_minutes"`
	SourceMaxIntervalMinutes int `gorm:"type:integer;not null;default:10080" json:"source_max_interval_minutes"`

	// Freshness is a demand signal, never a gate override (D2a).
	FreshnessDemandWeight float64 `gorm:"type:double precision;not null;default:0.20" json:"freshness_demand_weight"`

	// ---- Autopilot (stage 5) ----
	// Disabled by default; enabling starts in Observe (shadow) mode. All knobs
	// are DB-config surfaced via Console — zero env vars (Config Discipline).
	AutopilotEnabled              bool    `gorm:"not null;default:false" json:"autopilot_enabled"`
	AutopilotMode                 string  `gorm:"type:varchar(24);not null;default:'observe'" json:"autopilot_mode"`
	AutopilotIntervalMinutes      int     `gorm:"type:integer;not null;default:360" json:"autopilot_interval_minutes"`
	AutopilotMaxActionsPerRun     int     `gorm:"type:integer;not null;default:8" json:"autopilot_max_actions_per_run"`
	AutopilotMaxAtomizePerRun     int     `gorm:"type:integer;not null;default:3" json:"autopilot_max_atomize_per_run"`
	AutopilotMaxQueueDepth        int     `gorm:"type:integer;not null;default:100" json:"autopilot_max_queue_depth"`
	AutopilotMaxBytesPerRun       int64   `gorm:"type:bigint;not null;default:1073741824" json:"autopilot_max_bytes_per_run"`
	AutopilotEvictConfidenceFloor float64 `gorm:"type:double precision;not null;default:0.5" json:"autopilot_evict_confidence_floor"`
	AutopilotTrustMinDecisions    int     `gorm:"type:integer;not null;default:20" json:"autopilot_trust_min_decisions"`
	AutopilotTrustMaxRevertPct    int     `gorm:"type:integer;not null;default:10" json:"autopilot_trust_max_revert_pct"`

	AutopilotPausedUntil   *time.Time `gorm:"type:timestamp" json:"autopilot_paused_until,omitempty"`
	AutopilotElevatedMode  string     `gorm:"type:varchar(32)" json:"autopilot_elevated_mode,omitempty"`
	AutopilotElevatedUntil *time.Time `gorm:"type:timestamp" json:"autopilot_elevated_until,omitempty"`
	AutopilotLastRunAt     *time.Time `gorm:"type:timestamp" json:"autopilot_last_run_at,omitempty"`

	LastEvaluatedAt *time.Time `gorm:"type:timestamp" json:"last_evaluated_at,omitempty"`
	LastGeneratedAt *time.Time `gorm:"type:timestamp" json:"last_generated_at,omitempty"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (MediaCirculationPolicy) TableName() string {
	return "media_circulation_policies"
}

func DefaultMediaCirculationPolicy(tenantID string) MediaCirculationPolicy {
	return MediaCirculationPolicy{
		TenantID:                   tenantID,
		Enabled:                    false,
		Preset:                     MediaCirculationPresetBalanced,
		ValueFloor:                 0.15,
		MarginalMargin:             0.10,
		MaxIntakePerSourcePerCycle: 5,
		MaxIntakePerCycle:          25,
		SourceMinIntervalMinutes:   60,
		SourceMaxIntervalMinutes:   10080,
		FreshnessDemandWeight:      0.20,

		AutopilotEnabled:              false,
		AutopilotMode:                 MediaAutopilotModeObserve,
		AutopilotIntervalMinutes:      360, // 6h — media tempo, not news tempo (G2)
		AutopilotMaxActionsPerRun:     8,
		AutopilotMaxAtomizePerRun:     3, // one atomize fans out a whole pipeline (G8)
		AutopilotMaxQueueDepth:        100,
		AutopilotMaxBytesPerRun:       1 << 30, // 1 GiB
		AutopilotEvictConfidenceFloor: 0.5,     // established + ≥0.5 to auto-evict (G7)
		AutopilotTrustMinDecisions:    20,
		AutopilotTrustMaxRevertPct:    10,
	}
}

// MediaCirculationRun records one deterministic Autopilot pass (News pattern:
// health_before/health_after snapshots are the cockpit read-model at two
// timestamps, so "calm" is provable in the numbers the admin already trusts).
type MediaCirculationRun struct {
	ID       uint      `gorm:"primaryKey" json:"-"`
	PublicID uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex:idx_media_circ_runs_public_id" json:"id"`
	TenantID string    `gorm:"type:varchar(64);not null;index:idx_media_circ_runs_tenant" json:"tenant_id"`

	Trigger      string `gorm:"type:varchar(24);not null" json:"trigger"`
	Mode         string `gorm:"type:varchar(24);not null" json:"mode"`
	ElevatedMode string `gorm:"type:varchar(32)" json:"elevated_mode,omitempty"`
	Status       string `gorm:"type:varchar(24);not null;index:idx_media_circ_runs_status" json:"status"`

	StartedAt    time.Time      `gorm:"type:timestamp;not null;index:idx_media_circ_runs_started_at" json:"started_at"`
	FinishedAt   *time.Time     `gorm:"type:timestamp" json:"finished_at,omitempty"`
	Summary      string         `gorm:"type:text" json:"summary,omitempty"`
	HealthBefore datatypes.JSON `gorm:"type:jsonb" json:"health_before,omitempty"`
	HealthAfter  datatypes.JSON `gorm:"type:jsonb" json:"health_after,omitempty"`
	CreatedBy    string         `gorm:"type:varchar(255)" json:"created_by,omitempty"`
	Error        string         `gorm:"type:text" json:"error,omitempty"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (MediaCirculationRun) TableName() string {
	return "media_circulation_runs"
}

// MediaCirculationAction is the audit-grade ledger row: News Autopilot shape
// plus the V1 §12 audit trio (guardrail that allowed/blocked, byte/queue/feed
// impact) and the link back to the D11 recommendation it acted on.
type MediaCirculationAction struct {
	ID       uint      `gorm:"primaryKey" json:"-"`
	PublicID uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex:idx_media_circ_actions_public_id" json:"id"`
	RunID    uint      `gorm:"not null;index:idx_media_circ_actions_run_id" json:"-"`
	TenantID string    `gorm:"type:varchar(64);not null;index:idx_media_circ_actions_tenant" json:"tenant_id"`

	RecommendationID *uuid.UUID `gorm:"type:uuid;index:idx_media_circ_actions_recommendation" json:"recommendation_id,omitempty"`

	ToolName  string `gorm:"type:varchar(80);not null;index:idx_media_circ_actions_tool" json:"tool_name"`
	Status    string `gorm:"type:varchar(24);not null;index:idx_media_circ_actions_status" json:"status"`
	Reason    string `gorm:"type:text" json:"reason,omitempty"`
	Guardrail string `gorm:"type:varchar(64)" json:"guardrail,omitempty"`

	Input  datatypes.JSON `gorm:"type:jsonb" json:"input,omitempty"`
	Output datatypes.JSON `gorm:"type:jsonb" json:"output,omitempty"`
	Error  string         `gorm:"type:text" json:"error,omitempty"`

	ByteImpact  int64 `gorm:"type:bigint;not null;default:0" json:"byte_impact"`
	QueueImpact int   `gorm:"type:integer;not null;default:0" json:"queue_impact"`
	FeedImpact  int   `gorm:"type:integer;not null;default:0" json:"feed_impact"`

	StartedAt  time.Time  `gorm:"type:timestamp;not null" json:"started_at"`
	FinishedAt *time.Time `gorm:"type:timestamp" json:"finished_at,omitempty"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (MediaCirculationAction) TableName() string {
	return "media_circulation_actions"
}

// MediaCirculationRecommendation is the persisted, reviewable output of the layer
// (D11). Rows are created in later slices; the table and its autopilot-ready shape
// (stable public_id, outcome, reason snapshot) exist from Slice 1 so the stage-5
// Autopilot can consume this history as its track record without a reshape.
type MediaCirculationRecommendation struct {
	ID       uint      `gorm:"primaryKey" json:"-"`
	PublicID uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex:idx_media_circ_recs_public_id" json:"id"`
	TenantID string    `gorm:"type:varchar(64);not null;index:idx_media_circ_recs_tenant_unit_status,priority:1;index:idx_media_circ_recs_tenant_subject,priority:1" json:"tenant_id"`

	// Decision unit (D4): "source" (admit) or "item_family" (evict).
	UnitType string `gorm:"type:varchar(24);not null;index:idx_media_circ_recs_tenant_unit_status,priority:2" json:"unit_type"`

	// The concrete subject: a content_source (admit) or a content_item/family
	// (evict). SubjectKind disambiguates for the reader.
	SubjectID   uuid.UUID `gorm:"type:uuid;not null;index:idx_media_circ_recs_tenant_subject,priority:2" json:"subject_id"`
	SubjectKind string    `gorm:"type:varchar(24)" json:"subject_kind,omitempty"`

	Verdict string  `gorm:"type:varchar(32);not null" json:"verdict"`
	Action  string  `gorm:"type:varchar(32);not null" json:"action"`
	Score   float64 `gorm:"type:double precision;not null;default:0" json:"score"`

	// Human-readable proof (D9) and the input snapshot behind the verdict.
	Reasons datatypes.JSON `gorm:"type:jsonb" json:"reasons,omitempty"`
	Metrics datatypes.JSON `gorm:"type:jsonb" json:"metrics,omitempty"`

	Status  string `gorm:"type:varchar(24);not null;default:'pending';index:idx_media_circ_recs_tenant_unit_status,priority:3" json:"status"`
	Outcome string `gorm:"type:varchar(32)" json:"outcome,omitempty"`

	Applied   bool       `gorm:"not null;default:false" json:"applied"`
	AppliedAt *time.Time `gorm:"type:timestamp" json:"applied_at,omitempty"`
	AppliedBy string     `gorm:"type:varchar(255)" json:"applied_by,omitempty"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (MediaCirculationRecommendation) TableName() string {
	return "media_circulation_recommendations"
}

// MediaCirculationOverride is the standing human-exception layer consulted before
// any recommendation is emitted. These are policy exceptions, not one-shot actions.
type MediaCirculationOverride struct {
	ID       uint      `gorm:"primaryKey" json:"-"`
	PublicID uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex:idx_media_circ_overrides_public_id" json:"id"`
	TenantID string    `gorm:"type:varchar(64);not null;index:idx_media_circ_overrides_subject,priority:1;index:idx_media_circ_overrides_type,priority:1" json:"tenant_id"`

	SubjectKind  string    `gorm:"type:varchar(24);not null;index:idx_media_circ_overrides_subject,priority:2" json:"subject_kind"`
	SubjectID    uuid.UUID `gorm:"type:uuid;not null;index:idx_media_circ_overrides_subject,priority:3" json:"subject_id"`
	OverrideType string    `gorm:"type:varchar(32);not null;index:idx_media_circ_overrides_type,priority:2" json:"override_type"`

	Params    datatypes.JSON `gorm:"type:jsonb" json:"params,omitempty"`
	ExpiresAt *time.Time     `gorm:"type:timestamp;index:idx_media_circ_overrides_expires" json:"expires_at,omitempty"`
	SetBy     string         `gorm:"type:varchar(255)" json:"set_by,omitempty"`
	Notes     string         `gorm:"type:text" json:"notes,omitempty"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (MediaCirculationOverride) TableName() string {
	return "media_circulation_overrides"
}
