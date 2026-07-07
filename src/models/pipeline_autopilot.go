package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

const (
	PipelineAutopilotModeObserve  = "observe"
	PipelineAutopilotModeSafeAuto = "safe_auto"

	PipelineAutopilotElevatedBacklogDrain = "backlog_drain"

	PipelineAutopilotRunStatusRunning   = "running"
	PipelineAutopilotRunStatusCompleted = "completed"
	PipelineAutopilotRunStatusPartial   = "partial"
	PipelineAutopilotRunStatusFailed    = "failed"

	PipelineAutopilotHeadlineFlowing    = "flowing"
	PipelineAutopilotHeadlineRepairing  = "repairing"
	PipelineAutopilotHeadlineBacklogged = "backlogged"
	PipelineAutopilotHeadlineClogged    = "clogged"
	PipelineAutopilotHeadlineDegraded   = "degraded"

	PipelineAutopilotErrorClassNone                   = "none"
	PipelineAutopilotErrorClassAggregationUnreachable = "aggregation_unreachable"
	PipelineAutopilotErrorClassCMSDB                  = "cms_db"
	PipelineAutopilotErrorClassToken                  = "token"

	PipelineLanePendingStuck     = "pending_stuck"
	PipelineLaneFailedRetryable  = "failed_retryable"
	PipelineLaneProcessingStuck  = "processing_stuck"
	PipelineLaneFailedExhausted  = "failed_exhausted"
	PipelineLaneSourceBroken     = "source_broken"
	PipelineLaneDLQReview        = "dlq_review"
	PipelineVerdictRetryPending  = "retry_pending"
	PipelineVerdictRetryFailed   = "retry_failed"
	PipelineVerdictResetStuck    = "reset_stuck"
	PipelineVerdictFailedExhaust = "failed_exhausted"
	PipelineVerdictSourceBroken  = "source_broken"
	PipelineVerdictDLQReview     = "dlq_review"

	PipelineAutopilotActionStatusSuccess      = "success"
	PipelineAutopilotActionStatusError        = "error"
	PipelineAutopilotActionStatusAttention    = "attention"
	PipelineAutopilotActionStatusSkipped      = "skipped"
	PipelineAutopilotActionStatusWouldExecute = "would_execute"
	PipelineAutopilotActionStatusWouldSkip    = "would_skip"

	PipelineAutopilotOutcomePending     = "pending"
	PipelineAutopilotOutcomeRecovered   = "recovered"
	PipelineAutopilotOutcomeFailedAgain = "failed_again"
	PipelineAutopilotOutcomeUnresolved  = "unresolved"

	PipelineAutopilotGuardTrustGate        = "trust_gate"
	PipelineAutopilotGuardAttemptCap       = "attempt_cap"
	PipelineAutopilotGuardBackoff          = "backoff"
	PipelineAutopilotGuardAgeFloor         = "age_floor"
	PipelineAutopilotGuardQueueDepth       = "queue_depth"
	PipelineAutopilotGuardSourceCeiling    = "source_ceiling"
	PipelineAutopilotGuardRecoveryCooldown = "recovery_cooldown"
	PipelineAutopilotGuardBudget           = "budget"
	PipelineAutopilotGuardServiceDown      = "service_down"
	PipelineAutopilotGuardPaused           = "paused"
	PipelineAutopilotGuardStale            = "stale"

	PipelineTrustStateTrusted   = "trusted"
	PipelineTrustStateProbation = "probation"
	PipelineTrustStateDemoted   = "demoted"
)

type PipelineAutopilotPolicy struct {
	ID       uint   `gorm:"primaryKey" json:"-"`
	TenantID string `gorm:"type:varchar(64);not null;uniqueIndex:idx_pipeline_autopilot_policy_tenant" json:"tenant_id"`

	Enabled bool   `gorm:"not null;default:false" json:"enabled"`
	Mode    string `gorm:"type:varchar(24);not null;default:'observe'" json:"mode"`

	IntervalMinutes         int `gorm:"type:integer;not null;default:180" json:"interval_minutes"`
	MaxItemsPerRun          int `gorm:"type:integer;not null;default:200" json:"max_items_per_run"`
	MaxBatchesPerRun        int `gorm:"type:integer;not null;default:4" json:"max_batches_per_run"`
	MaxAttempts             int `gorm:"type:integer;not null;default:3" json:"max_attempts"`
	RetryBackoffHours       int `gorm:"type:integer;not null;default:12" json:"retry_backoff_hours"`
	PendingAgeFloorMinutes  int `gorm:"type:integer;not null;default:30" json:"pending_age_floor_minutes"`
	ProcessingStuckHours    int `gorm:"type:integer;not null;default:4" json:"processing_stuck_hours"`
	MaxQueueDepth           int `gorm:"type:integer;not null;default:100" json:"max_queue_depth"`
	PerSourceDailyRetries   int `gorm:"type:integer;not null;default:100" json:"per_source_daily_retries"`
	RecoveryCooldownMinutes int `gorm:"type:integer;not null;default:60" json:"recovery_cooldown_minutes"`
	TrustMinOutcomes        int `gorm:"type:integer;not null;default:20" json:"trust_min_outcomes"`
	TrustMinSuccessPct      int `gorm:"type:integer;not null;default:40" json:"trust_min_success_pct"`

	PausedUntil    *time.Time `gorm:"type:timestamp" json:"paused_until,omitempty"`
	ElevatedMode   string     `gorm:"type:varchar(32)" json:"elevated_mode,omitempty"`
	ElevatedUntil  *time.Time `gorm:"type:timestamp" json:"elevated_until,omitempty"`
	LastRunAt      *time.Time `gorm:"type:timestamp" json:"last_run_at,omitempty"`
	LastHealthOKAt *time.Time `gorm:"type:timestamp" json:"last_health_ok_at,omitempty"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (PipelineAutopilotPolicy) TableName() string {
	return "pipeline_autopilot_policies"
}

func DefaultPipelineAutopilotPolicy(tenantID string) PipelineAutopilotPolicy {
	return PipelineAutopilotPolicy{
		TenantID:                tenantID,
		Enabled:                 false,
		Mode:                    PipelineAutopilotModeObserve,
		IntervalMinutes:         180,
		MaxItemsPerRun:          200,
		MaxBatchesPerRun:        4,
		MaxAttempts:             3,
		RetryBackoffHours:       12,
		PendingAgeFloorMinutes:  30,
		ProcessingStuckHours:    4,
		MaxQueueDepth:           100,
		PerSourceDailyRetries:   100,
		RecoveryCooldownMinutes: 60,
		TrustMinOutcomes:        20,
		TrustMinSuccessPct:      40,
	}
}

type PipelineAutopilotRun struct {
	ID       uint      `gorm:"primaryKey" json:"-"`
	PublicID uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex:idx_pipeline_autopilot_runs_public_id" json:"id"`
	TenantID string    `gorm:"type:varchar(64);not null;index:idx_pipeline_autopilot_runs_tenant" json:"tenant_id"`

	Trigger      string `gorm:"type:varchar(24);not null" json:"trigger"`
	Mode         string `gorm:"type:varchar(24);not null" json:"mode"`
	ElevatedMode string `gorm:"type:varchar(32)" json:"elevated_mode,omitempty"`
	Status       string `gorm:"type:varchar(24);not null;index:idx_pipeline_autopilot_runs_status" json:"status"`
	Headline     string `gorm:"type:varchar(32)" json:"headline,omitempty"`

	StartedAt    time.Time      `gorm:"type:timestamp;not null;index:idx_pipeline_autopilot_runs_started_at" json:"started_at"`
	FinishedAt   *time.Time     `gorm:"type:timestamp" json:"finished_at,omitempty"`
	Summary      string         `gorm:"type:text" json:"summary,omitempty"`
	HealthBefore datatypes.JSON `gorm:"type:jsonb" json:"health_before,omitempty"`
	HealthAfter  datatypes.JSON `gorm:"type:jsonb" json:"health_after,omitempty"`
	CreatedBy    string         `gorm:"type:varchar(255)" json:"created_by,omitempty"`
	Error        string         `gorm:"type:text" json:"error,omitempty"`
	ErrorClass   string         `gorm:"type:varchar(48);not null;default:'none';index:idx_pipeline_autopilot_runs_error_class" json:"error_class"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (PipelineAutopilotRun) TableName() string {
	return "pipeline_autopilot_runs"
}

type PipelineAutopilotAction struct {
	ID       uint      `gorm:"primaryKey" json:"-"`
	PublicID uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex:idx_pipeline_autopilot_actions_public_id" json:"id"`
	RunID    uint      `gorm:"not null;index:idx_pipeline_autopilot_actions_run_id" json:"-"`
	TenantID string    `gorm:"type:varchar(64);not null;index:idx_pipeline_autopilot_actions_tenant" json:"tenant_id"`

	Lane          string     `gorm:"type:varchar(32);not null;index:idx_pipeline_autopilot_actions_lane" json:"lane"`
	Verdict       string     `gorm:"type:varchar(32);not null" json:"verdict"`
	SourceFilter  string     `gorm:"type:varchar(255)" json:"source_filter,omitempty"`
	TargetQueue   string     `gorm:"type:varchar(32)" json:"target_queue,omitempty"`
	ContentItemID *uuid.UUID `gorm:"type:uuid;index:idx_pipeline_autopilot_actions_content" json:"content_item_id,omitempty"`
	Status        string     `gorm:"type:varchar(32);not null;index:idx_pipeline_autopilot_actions_status" json:"status"`
	Outcome       string     `gorm:"type:varchar(24);index:idx_pipeline_autopilot_actions_outcome" json:"outcome,omitempty"`
	Reason        string     `gorm:"type:text" json:"reason,omitempty"`
	Guardrail     string     `gorm:"type:varchar(64)" json:"guardrail,omitempty"`

	RequestedCount int            `gorm:"type:integer;not null;default:0" json:"requested_count"`
	EnqueuedCount  int            `gorm:"type:integer;not null;default:0" json:"enqueued_count"`
	ErrorCount     int            `gorm:"type:integer;not null;default:0" json:"error_count"`
	Output         datatypes.JSON `gorm:"type:jsonb" json:"output,omitempty"`

	StartedAt  time.Time  `gorm:"type:timestamp;not null" json:"started_at"`
	FinishedAt *time.Time `gorm:"type:timestamp" json:"finished_at,omitempty"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (PipelineAutopilotAction) TableName() string {
	return "pipeline_autopilot_actions"
}
