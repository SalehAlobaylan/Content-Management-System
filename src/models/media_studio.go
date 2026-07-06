package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

// Media Studio Clearance Autopilot (stage 6) — the editorial-clearance helper
// downstream of the Media Circulation Autopilot. It absorbs the `needs_review`
// residue the lead autopilot creates and clears only the mechanically-decidable
// subset; everything editorial stays approval-tier, at most LLM-proposed.
// See docs/media-studio-autopilot-plan.md (S1–S20 / H1–H9).

// ---- Review-reason code taxonomy (S4/S5) ----
// The trust gate keys on these codes, not the free-text needs_review_reason.
const (
	StudioReviewCodeShortUnmergeable = "short_unmergeable" // <270s, cannot merge — structurally unpublishable
	StudioReviewCodeBelowMin         = "below_min"         // below 4:30 minimum feed duration
	StudioReviewCodeAboveHardMax     = "above_hard_max"    // exceeds hard maximum duration
	StudioReviewCodePlannerFallback  = "planner_fallback"  // planner returned no usable chapters
	StudioReviewCodeSponsorIntro     = "sponsor_intro"     // contains sponsor/intro segment
	StudioReviewCodeLowConfidence    = "low_confidence"    // confidence below the high-confidence threshold
	StudioReviewCodeMergedShort      = "merged_short"      // built from a short-chapter merge
)

// studioReviewCodePrecedence orders codes most-editorial → least (S5). The
// primary code of a multi-flag chapter is the highest-precedence one present.
var studioReviewCodePrecedence = []string{
	StudioReviewCodeSponsorIntro,
	StudioReviewCodePlannerFallback,
	StudioReviewCodeLowConfidence,
	StudioReviewCodeMergedShort,
	StudioReviewCodeBelowMin,
	StudioReviewCodeAboveHardMax,
	StudioReviewCodeShortUnmergeable,
}

// StudioReviewPrimaryCode returns the most-editorial code from a set (S5).
func StudioReviewPrimaryCode(codes []string) string {
	present := make(map[string]bool, len(codes))
	for _, c := range codes {
		present[c] = true
	}
	for _, c := range studioReviewCodePrecedence {
		if present[c] {
			return c
		}
	}
	return ""
}

// ---- Autopilot mode ladder (shared with the lead; stage 5 G5/G9) ----
const (
	StudioAutopilotModeObserve  = "observe"
	StudioAutopilotModeSafeAuto = "safe_auto"

	StudioRunTriggerChained  = "chained"
	StudioRunTriggerInterval = "interval"
	StudioRunTriggerManual   = "manual"

	StudioRunStatusRunning   = "running"
	StudioRunStatusCompleted = "completed"
	StudioRunStatusPartial   = "partial"
	StudioRunStatusFailed    = "failed"

	StudioActionStatusSuccess          = "success"
	StudioActionStatusError            = "error"
	StudioActionStatusApprovalRequired = "approval_required"
	StudioActionStatusSkipped          = "skipped"
	StudioActionStatusWouldApply       = "would_apply"
	StudioActionStatusWouldSkip        = "would_skip"
	StudioActionStatusWouldPropose     = "would_propose"

	StudioUnitChapterReview  = "chapter_review"
	StudioUnitTranscriptCase = "transcript_case"

	// Verdicts (§7)
	StudioVerdictAutoRejectImpossible  = "auto_reject_impossible"
	StudioVerdictAutoPublishMechanical = "auto_publish_mechanical"
	StudioVerdictProposePublish        = "propose_publish"
	StudioVerdictProposeReject         = "propose_reject"
	StudioVerdictHoldStale             = "hold_stale"
	StudioVerdictEmitReatomize         = "emit_reatomize_recommendation"
	StudioVerdictRerunSTT              = "rerun_stt"
	StudioVerdictBlockedJobInFlight    = "blocked_job_in_flight"

	// Skip / would-skip guardrail taxonomy (§9). Fixed set.
	StudioGuardTrustGate        = "trust_gate"
	StudioGuardEditorialReason  = "editorial_reason"
	StudioGuardMultiCode        = "multi_code"
	StudioGuardInvalidDuration  = "invalid_duration"
	StudioGuardUpstreamDisabled = "upstream_disabled"
	StudioGuardRecentlyEdited   = "recently_edited"
	StudioGuardOverride         = "override"
	StudioGuardJobInFlight      = "job_in_flight"
	StudioGuardBudget           = "budget"
	StudioGuardStaleness        = "staleness"
	StudioGuardLLMInvalidOutput = "llm_invalid_output"
	StudioGuardLLMUnavailable   = "llm_unavailable"
	StudioGuardActionLimit      = "action_limit"
	StudioGuardPublishLimit     = "publish_limit"
	StudioGuardRejectLimit      = "reject_limit"
	StudioGuardSTTLimit         = "stt_limit"
	StudioGuardProposalLimit    = "proposal_limit"
)

// StudioAuditPrincipal is the audit-log user_email attributed to autopilot
// clears (H9). Trust queries distinguish autopilot from human by this value.
const StudioAuditPrincipal = "autopilot:media_studio"

// MediaStudioAutopilotPolicy holds the per-tenant automation knobs (S17, H8).
type MediaStudioAutopilotPolicy struct {
	ID       uint   `gorm:"primaryKey" json:"-"`
	TenantID string `gorm:"type:varchar(64);not null;uniqueIndex" json:"tenant_id"`

	AutopilotEnabled bool   `gorm:"not null;default:false" json:"autopilot_enabled"`
	AutopilotMode    string `gorm:"type:varchar(24);not null;default:'observe'" json:"autopilot_mode"`
	ObserveProposals bool   `gorm:"not null;default:false" json:"observe_proposals"`

	IntervalMinutes       int `gorm:"type:integer;not null;default:360" json:"interval_minutes"`
	ChainDebounceMinutes  int `gorm:"type:integer;not null;default:15" json:"chain_debounce_minutes"`
	MaxClearsPerRun       int `gorm:"type:integer;not null;default:10" json:"max_clears_per_run"`
	MaxPublishesPerRun    int `gorm:"type:integer;not null;default:5" json:"max_publishes_per_run"`
	MaxRejectsPerRun      int `gorm:"type:integer;not null;default:10" json:"max_rejects_per_run"`
	MaxSTTPerRun          int `gorm:"type:integer;not null;default:3" json:"max_stt_per_run"`
	MaxProposalsPerRun    int `gorm:"type:integer;not null;default:15" json:"max_proposals_per_run"`
	AgedThresholdDays     int `gorm:"type:integer;not null;default:7" json:"aged_threshold_days"`
	DirtyWorkbenchMinutes int `gorm:"type:integer;not null;default:30" json:"dirty_workbench_minutes"`

	TrustMinDecisions   int `gorm:"type:integer;not null;default:20" json:"trust_min_decisions"`
	TrustMinApprovePct  int `gorm:"type:integer;not null;default:90" json:"trust_min_approve_pct"`
	TrustMaxReversalPct int `gorm:"type:integer;not null;default:5" json:"trust_max_reversal_pct"`

	PausedUntil *time.Time `gorm:"type:timestamp" json:"paused_until,omitempty"`
	LastRunAt   *time.Time `gorm:"type:timestamp" json:"last_run_at,omitempty"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (MediaStudioAutopilotPolicy) TableName() string {
	return "media_studio_autopilot_policies"
}

func DefaultMediaStudioAutopilotPolicy(tenantID string) MediaStudioAutopilotPolicy {
	return MediaStudioAutopilotPolicy{
		TenantID:              tenantID,
		AutopilotEnabled:      false,
		AutopilotMode:         StudioAutopilotModeObserve,
		ObserveProposals:      false,
		IntervalMinutes:       360,
		ChainDebounceMinutes:  15,
		MaxClearsPerRun:       10,
		MaxPublishesPerRun:    5,
		MaxRejectsPerRun:      10,
		MaxSTTPerRun:          3,
		MaxProposalsPerRun:    15,
		AgedThresholdDays:     7,
		DirtyWorkbenchMinutes: 30,
		TrustMinDecisions:     20,
		TrustMinApprovePct:    90,
		TrustMaxReversalPct:   5,
	}
}

// MediaStudioRun records one deterministic clearance pass. health_before/after
// are the studio-tab read-model at two timestamps (G10 pattern).
type MediaStudioRun struct {
	ID       uint      `gorm:"primaryKey" json:"-"`
	PublicID uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex:idx_media_studio_runs_public_id" json:"id"`
	TenantID string    `gorm:"type:varchar(64);not null;index:idx_media_studio_runs_tenant" json:"tenant_id"`

	Trigger string `gorm:"type:varchar(24);not null" json:"trigger"`
	Mode    string `gorm:"type:varchar(24);not null" json:"mode"`
	Status  string `gorm:"type:varchar(24);not null;index:idx_media_studio_runs_status" json:"status"`

	StartedAt    time.Time      `gorm:"type:timestamp;not null;index:idx_media_studio_runs_started_at" json:"started_at"`
	FinishedAt   *time.Time     `gorm:"type:timestamp" json:"finished_at,omitempty"`
	Summary      string         `gorm:"type:text" json:"summary,omitempty"`
	HealthBefore datatypes.JSON `gorm:"type:jsonb" json:"health_before,omitempty"`
	HealthAfter  datatypes.JSON `gorm:"type:jsonb" json:"health_after,omitempty"`
	CreatedBy    string         `gorm:"type:varchar(255)" json:"created_by,omitempty"`
	Error        string         `gorm:"type:text" json:"error,omitempty"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (MediaStudioRun) TableName() string {
	return "media_studio_runs"
}

// MediaStudioAction is the audit-grade ledger row. Proposal rows additionally
// carry the LLM draft and its eventual human outcome (S19).
type MediaStudioAction struct {
	ID       uint      `gorm:"primaryKey" json:"-"`
	PublicID uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex:idx_media_studio_actions_public_id" json:"id"`
	RunID    uint      `gorm:"not null;index:idx_media_studio_actions_run_id" json:"-"`
	TenantID string    `gorm:"type:varchar(64);not null;index:idx_media_studio_actions_tenant" json:"tenant_id"`

	UnitType         string     `gorm:"type:varchar(24);not null" json:"unit_type"`
	ChapterID        *uuid.UUID `gorm:"type:uuid;index:idx_media_studio_actions_chapter" json:"chapter_id,omitempty"`
	ContentItemID    *uuid.UUID `gorm:"type:uuid;index:idx_media_studio_actions_content_item" json:"content_item_id,omitempty"`
	RecommendationID *uuid.UUID `gorm:"type:uuid" json:"recommendation_id,omitempty"`

	Verdict   string `gorm:"type:varchar(40);not null" json:"verdict"`
	ToolName  string `gorm:"type:varchar(80);not null" json:"tool_name"`
	Status    string `gorm:"type:varchar(24);not null;index:idx_media_studio_actions_status" json:"status"`
	Reason    string `gorm:"type:text" json:"reason,omitempty"`
	Guardrail string `gorm:"type:varchar(64)" json:"guardrail,omitempty"`

	Proposal           datatypes.JSON `gorm:"type:jsonb" json:"proposal,omitempty"`
	ProposalModel      string         `gorm:"type:varchar(80)" json:"proposal_model,omitempty"`
	ProposalConfidence *float64       `gorm:"type:double precision" json:"proposal_confidence,omitempty"`
	HumanOutcome       string         `gorm:"type:varchar(24)" json:"human_outcome,omitempty"`
	HumanOutcomeBy     string         `gorm:"type:varchar(255)" json:"human_outcome_by,omitempty"`
	HumanOutcomeAt     *time.Time     `gorm:"type:timestamp" json:"human_outcome_at,omitempty"`

	Input      datatypes.JSON `gorm:"type:jsonb" json:"input,omitempty"`
	Output     datatypes.JSON `gorm:"type:jsonb" json:"output,omitempty"`
	Error      string         `gorm:"type:text" json:"error,omitempty"`
	FeedImpact int            `gorm:"type:integer;not null;default:0" json:"feed_impact"`
	STTImpact  int            `gorm:"type:integer;not null;default:0" json:"stt_impact"`

	StartedAt  time.Time  `gorm:"type:timestamp;not null" json:"started_at"`
	FinishedAt *time.Time `gorm:"type:timestamp" json:"finished_at,omitempty"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (MediaStudioAction) TableName() string {
	return "media_studio_actions"
}
