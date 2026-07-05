package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

// Enrichment Coverage Autopilot — Slice 1 (persistence foundation).
//
// The Enrichment page's manual loop is "cadence and clicking": CMS already
// computes coverage stats + missing counts + service health, and admins click
// "Enrich all <artifact>" on a schedule. This autopilot absorbs the cadence and
// clicking through the SAME triggerItemArtifacts path humans use — nothing here
// evicts, hides, deletes, or spends past the existing STT budget guard.
// See docs/enrichment-autopilot-plan.md.

const (
	// Mode ladder (mirrors Media/News): Observe is a full shadow pipeline (dry-run
	// ledger, zero side effects); Safe Auto executes the earned safe tier.
	// Promotion between them is always a manual human flip.
	EnrichmentAutopilotModeObserve  = "observe"
	EnrichmentAutopilotModeSafeAuto = "safe_auto"

	// Only elevated preset: post-outage catch-up raises the item caps.
	EnrichmentAutopilotElevatedBackfillCatchup = "backfill_catchup"

	// Artifact classes the autopilot manages. `sparse` is deliberately absent —
	// embedding_sparse is dead post-Qwen and never chased (plan §3).
	EnrichmentArtifactTranscript = "transcript"
	EnrichmentArtifactEmbedding  = "embedding"
	EnrichmentArtifactImage      = "image"

	// Run statuses.
	EnrichmentAutopilotRunStatusRunning   = "running"
	EnrichmentAutopilotRunStatusCompleted = "completed"
	EnrichmentAutopilotRunStatusPartial   = "partial"
	EnrichmentAutopilotRunStatusFailed    = "failed"

	// Run headlines (plan §7).
	EnrichmentAutopilotHeadlineFullyEnriched = "fully_enriched"
	EnrichmentAutopilotHeadlineBacklog       = "backlog"
	EnrichmentAutopilotHeadlineBudgetCapped  = "budget_capped"
	EnrichmentAutopilotHeadlineDegraded      = "degraded"

	// Action statuses. Observe writes would_trigger/would_skip instead of
	// success/skipped — same rows, only the terminal execute flips.
	EnrichmentAutopilotActionStatusSuccess          = "success"
	EnrichmentAutopilotActionStatusError            = "error"
	EnrichmentAutopilotActionStatusSkipped          = "skipped"
	EnrichmentAutopilotActionStatusApprovalRequired = "approval_required"
	EnrichmentAutopilotActionStatusWouldTrigger     = "would_trigger"
	EnrichmentAutopilotActionStatusWouldSkip        = "would_skip"

	// Guardrail / skip-reason taxonomy (plan §12) — the fixed vocabulary recorded
	// on every skipped/blocked action so the ledger explains itself.
	EnrichmentAutopilotGuardTrustGate         = "trust_gate"
	EnrichmentAutopilotGuardServiceDown       = "service_down"
	EnrichmentAutopilotGuardBudget            = "budget"
	EnrichmentAutopilotGuardReconcileDraining = "reconcile_draining"
	EnrichmentAutopilotGuardCirculationScope  = "circulation_scope"
	EnrichmentAutopilotGuardQueueDepth        = "queue_depth"
	EnrichmentAutopilotGuardAgeFloor          = "age_floor"
	EnrichmentAutopilotGuardClassCap          = "class_cap"
	EnrichmentAutopilotGuardRunCap            = "run_cap"
	EnrichmentAutopilotGuardBreakerTripped    = "breaker_tripped"
	EnrichmentAutopilotGuardAlreadyPresent    = "already_present"

	// Trust states (plan §8, enrichment adaptation). Media seeds trust from a
	// persisted human decision history; enrichment has none (bulk runs were
	// in-memory), so a class self-seeds through a probation window: it executes
	// while building its record and is only DEMOTED (held to approval) once it
	// proves unreliable. This avoids the trust-vs-execution deadlock while still
	// parking a misbehaving class.
	EnrichmentTrustStateTrusted   = "trusted"
	EnrichmentTrustStateProbation = "probation"
	EnrichmentTrustStateDemoted   = "demoted"
)

// EnrichmentAutopilotPolicy stores the per-tenant autopilot knobs. Disabled by
// default so turning it on is always a deliberate, reversible choice; enabling
// starts in Observe (shadow) mode. All knobs are DB-config surfaced via Console —
// zero env vars (Config Discipline).
type EnrichmentAutopilotPolicy struct {
	ID       uint   `gorm:"primaryKey" json:"-"`
	TenantID string `gorm:"type:varchar(64);not null;uniqueIndex:idx_enrichment_autopilot_policy_tenant" json:"tenant_id"`

	Enabled bool   `gorm:"not null;default:false" json:"enabled"`
	Mode    string `gorm:"type:varchar(24);not null;default:'observe'" json:"mode"`

	IntervalMinutes      int `gorm:"type:integer;not null;default:360" json:"interval_minutes"`
	MaxItemsPerRun       int `gorm:"type:integer;not null;default:200" json:"max_items_per_run"`
	MaxItemsPerClass     int `gorm:"type:integer;not null;default:100" json:"max_items_per_class"`
	MaxTranscriptsPerRun int `gorm:"type:integer;not null;default:10" json:"max_transcripts_per_run"`
	MaxQueueDepth        int `gorm:"type:integer;not null;default:100" json:"max_queue_depth"`
	FailureBreakerPct    int `gorm:"type:integer;not null;default:30" json:"failure_breaker_pct"`
	StallWindowRuns      int `gorm:"type:integer;not null;default:2" json:"stall_window_runs"`
	AgeFloorMinutes      int `gorm:"type:integer;not null;default:10" json:"age_floor_minutes"`
	TrustMinAttempts     int `gorm:"type:integer;not null;default:50" json:"trust_min_attempts"`
	TrustMaxFailurePct   int `gorm:"type:integer;not null;default:15" json:"trust_max_failure_pct"`

	PausedUntil   *time.Time `gorm:"type:timestamp" json:"paused_until,omitempty"`
	ElevatedMode  string     `gorm:"type:varchar(32)" json:"elevated_mode,omitempty"`
	ElevatedUntil *time.Time `gorm:"type:timestamp" json:"elevated_until,omitempty"`
	LastRunAt     *time.Time `gorm:"type:timestamp" json:"last_run_at,omitempty"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (EnrichmentAutopilotPolicy) TableName() string {
	return "enrichment_autopilot_policies"
}

func DefaultEnrichmentAutopilotPolicy(tenantID string) EnrichmentAutopilotPolicy {
	return EnrichmentAutopilotPolicy{
		TenantID:             tenantID,
		Enabled:              false,
		Mode:                 EnrichmentAutopilotModeObserve,
		IntervalMinutes:      360, // 6h — coverage decays gradually, not news-tempo
		MaxItemsPerRun:       200,
		MaxItemsPerClass:     100,
		MaxTranscriptsPerRun: 10, // each is a billable Deepgram call, under the budget guard
		MaxQueueDepth:        100,
		FailureBreakerPct:    30,
		StallWindowRuns:      2,
		AgeFloorMinutes:      10,
		TrustMinAttempts:     50,
		TrustMaxFailurePct:   15,
	}
}

// EnrichmentAutopilotRun records one deterministic pass. stats_before/stats_after
// are the enrichment stats read-model at two timestamps, so the coverage delta is
// directly comparable to what the admin sees on the page.
type EnrichmentAutopilotRun struct {
	ID       uint      `gorm:"primaryKey" json:"-"`
	PublicID uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex:idx_enrichment_autopilot_runs_public_id" json:"id"`
	TenantID string    `gorm:"type:varchar(64);not null;index:idx_enrichment_autopilot_runs_tenant" json:"tenant_id"`

	Trigger      string `gorm:"type:varchar(24);not null" json:"trigger"`
	Mode         string `gorm:"type:varchar(24);not null" json:"mode"`
	ElevatedMode string `gorm:"type:varchar(32)" json:"elevated_mode,omitempty"`
	Status       string `gorm:"type:varchar(24);not null;index:idx_enrichment_autopilot_runs_status" json:"status"`
	Headline     string `gorm:"type:varchar(32)" json:"headline,omitempty"`

	StartedAt   time.Time      `gorm:"type:timestamp;not null;index:idx_enrichment_autopilot_runs_started_at" json:"started_at"`
	FinishedAt  *time.Time     `gorm:"type:timestamp" json:"finished_at,omitempty"`
	Summary     string         `gorm:"type:text" json:"summary,omitempty"`
	StatsBefore datatypes.JSON `gorm:"type:jsonb" json:"stats_before,omitempty"`
	StatsAfter  datatypes.JSON `gorm:"type:jsonb" json:"stats_after,omitempty"`
	CreatedBy   string         `gorm:"type:varchar(255)" json:"created_by,omitempty"`
	Error       string         `gorm:"type:text" json:"error,omitempty"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (EnrichmentAutopilotRun) TableName() string {
	return "enrichment_autopilot_runs"
}

// EnrichmentAutopilotAction is the audit-grade ledger row: one per considered
// (item × artifact). transcription_job_id cross-links autopilot STT triggers into
// the transcription-jobs / Media Studio surfaces.
type EnrichmentAutopilotAction struct {
	ID       uint      `gorm:"primaryKey" json:"-"`
	PublicID uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex:idx_enrichment_autopilot_actions_public_id" json:"id"`
	RunID    uint      `gorm:"not null;index:idx_enrichment_autopilot_actions_run_id" json:"-"`
	TenantID string    `gorm:"type:varchar(64);not null;index:idx_enrichment_autopilot_actions_tenant" json:"tenant_id"`

	ContentID *uuid.UUID `gorm:"type:uuid" json:"content_id,omitempty"`
	Artifact  string     `gorm:"type:varchar(24);not null;index:idx_enrichment_autopilot_actions_artifact" json:"artifact"`
	Status    string     `gorm:"type:varchar(24);not null;index:idx_enrichment_autopilot_actions_status" json:"status"`
	Reason    string     `gorm:"type:text" json:"reason,omitempty"`
	Guardrail string     `gorm:"type:varchar(64)" json:"guardrail,omitempty"`

	TranscriptionJobID *uuid.UUID `gorm:"type:uuid" json:"transcription_job_id,omitempty"`
	DurationMs         int        `gorm:"type:integer;not null;default:0" json:"duration_ms"`

	StartedAt  time.Time  `gorm:"type:timestamp;not null" json:"started_at"`
	FinishedAt *time.Time `gorm:"type:timestamp" json:"finished_at,omitempty"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (EnrichmentAutopilotAction) TableName() string {
	return "enrichment_autopilot_actions"
}
