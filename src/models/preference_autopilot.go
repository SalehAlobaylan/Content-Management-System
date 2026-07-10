package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

// Preferences Autopilot — Stage 7 of the autopilot family.
//
// Wraps the deterministic catalog-maintenance loop (mapping sweeps, mining,
// affinity recompute, centroid/member-count refresh) and the proposal advisor.
// The bare StartTopicsHeartbeat cron becomes a bounded, ledgered runner with a
// health verdict. Authority: Safe Auto for derived-data maintenance + proposal
// enrichment/scoring, plus the EARNED auto-approve tier (human-flipped,
// trust-gated, quarantined, revertible — the only catalog mutation it may ever
// perform). It never auto-rejects/merges proposals, features topics, flips feed
// switches, or edits preference weights. See plans/user-preferences-autopilot.md.

const (
	// Mode ladder. disabled + observe preserve the incumbent baseline heartbeat
	// (§9); observe additionally records shadow decisions. safe_auto replaces the
	// baseline with the bounded runner. Promotion is always a human flip.
	PreferenceAutopilotModeObserve  = "observe"
	PreferenceAutopilotModeSafeAuto = "safe_auto"

	// Run statuses.
	PreferenceAutopilotRunStatusRunning   = "running"
	PreferenceAutopilotRunStatusCompleted = "completed"
	PreferenceAutopilotRunStatusPartial   = "partial"
	PreferenceAutopilotRunStatusFailed    = "failed"

	// Run headlines (plan §7) — fixed vocabulary, headline primary.
	PreferenceAutopilotHeadlineCurationCurrent = "curation_current"
	PreferenceAutopilotHeadlineReviewReady     = "review_ready"
	PreferenceAutopilotHeadlineBacklog         = "backlog"
	PreferenceAutopilotHeadlineCoverageGap     = "coverage_gap"
	PreferenceAutopilotHeadlineFlipEligible    = "flip_eligible"
	PreferenceAutopilotHeadlineIntegrityAlert  = "integrity_alert"
	PreferenceAutopilotHeadlineDegraded        = "degraded"
	PreferenceAutopilotHeadlineNotObserved     = "not_observed"

	// Action classes (§8 safe actions).
	PreferenceActionMapSweep        = "map_sweep"
	PreferenceActionDirtySweep      = "dirty_sweep"
	PreferenceActionCentroidRefresh = "centroid_refresh"
	PreferenceActionMemberRefresh   = "member_refresh"
	PreferenceActionRecompute       = "recompute"
	PreferenceActionMine            = "mine"
	PreferenceActionProposalEnrich  = "proposal_enrich"
	PreferenceActionMergeSuggest    = "merge_suggest"
	PreferenceActionSnapshot        = "snapshot"

	// Action statuses. Observe writes would_* for the NEW-action logic and
	// baseline_* for the incumbent maintenance it still runs once per due tick.
	PreferenceActionStatusSuccess         = "success"
	PreferenceActionStatusError           = "error"
	PreferenceActionStatusSkipped         = "skipped"
	PreferenceActionStatusWouldTrigger    = "would_trigger"
	PreferenceActionStatusWouldSkip       = "would_skip"
	PreferenceActionStatusBaselineSuccess = "baseline_success"
	PreferenceActionStatusBaselineError   = "baseline_error"

	// Subject-type taxonomy — proposals use numeric ids, topics/users UUIDs, and
	// some actions are tenant-wide aggregates (§0.1.6). subject_ref is textual.
	PreferenceSubjectTopic     = "topic"
	PreferenceSubjectProposal  = "proposal"
	PreferenceSubjectUser      = "user"
	PreferenceSubjectAggregate = "aggregate"

	// Guardrail / skip-reason taxonomy (§9, §12).
	PreferenceGuardRunCap         = "run_cap"
	PreferenceGuardPendingCeiling = "pending_ceiling"
	PreferenceGuardBreakerTripped = "breaker_tripped"
	PreferenceGuardClassBreaker   = "class_breaker"
	PreferenceGuardEmptyCatalog   = "empty_catalog"
	PreferenceGuardEnrichmentDown = "enrichment_down"
	PreferenceGuardAlreadyMapped  = "already_mapped"
	PreferenceGuardStaleStats     = "stale_stats"
	PreferenceGuardPaused         = "paused"
	PreferenceGuardNotDue         = "not_due"

	// Prediction / trust (§15). The scorer freezes a verdict per proposal; on human
	// resolution we compare. The earned auto-approve tier consumes this evidence.
	PreferencePredictionVersion = "prefs-v1"
	PreferenceVerdictReview     = "review"
	PreferenceVerdictHighConf   = "high_confidence"
	PreferenceVerdictSuggestRej = "suggest_reject"

	// Earned auto-approve tier. auto_approve is the ONLY catalog mutation the
	// autopilot may ever perform, and only when the human-flipped switch is on AND
	// trust is re-verified at run time. Auto-REJECT is permanently forbidden — a
	// machine may promote a clearly-good topic into quarantine, but burying a
	// possibly-good one is always a human call. Resolutions by the autopilot are
	// attributed to PreferenceAutopilotResolver and EXCLUDED from trust evidence so
	// the tier can never grade its own homework.
	PreferenceActionAutoApprove    = "auto_approve"
	PreferenceGuardTrustGate       = "trust_gate"
	PreferenceGuardBlockerFlag     = "blocker_flag"
	PreferenceGuardSlugExists      = "slug_exists"
	PreferenceAutopilotResolver    = "autopilot:preferences"
	PreferenceCreatedFromAutopilot = "autopilot"

	// Recompute-queue reasons (§10).
	PreferenceRecomputeReasonMerge  = "merge"
	PreferenceRecomputeReasonFailed = "failed_sync"
	PreferenceRecomputeReasonManual = "manual"
	PreferenceRecomputeReasonRevert = "revert"
)

// PreferenceAutopilotPolicy stores the per-tenant knobs. Disabled by default so
// enabling is always deliberate; enabling starts in observe. Every tunable is a
// code-default persisted here and surfaced through Console — zero env vars
// (Config Discipline). There is deliberately NO elevated mode (§9): raising caps
// or running a full remap are explicit human actions because this surface feeds
// user-facing ranking.
type PreferenceAutopilotPolicy struct {
	ID       uint   `gorm:"primaryKey" json:"-"`
	TenantID string `gorm:"type:varchar(64);not null;uniqueIndex:idx_preference_autopilot_policy_tenant" json:"tenant_id"`

	Enabled bool   `gorm:"not null;default:false" json:"enabled"`
	Mode    string `gorm:"type:varchar(24);not null;default:'observe'" json:"mode"`

	IntervalMinutes int `gorm:"type:integer;not null;default:15" json:"interval_minutes"`

	// Per-run caps (§9 V1 envelope).
	MaxItemCandidates    int `gorm:"type:integer;not null;default:250" json:"max_item_candidates"`
	MaxStoryCandidates   int `gorm:"type:integer;not null;default:100" json:"max_story_candidates"`
	MaxDirtyTopics       int `gorm:"type:integer;not null;default:3" json:"max_dirty_topics"`
	MaxUsersRecompute    int `gorm:"type:integer;not null;default:100" json:"max_users_recompute"`
	MaxProposalsEnriched int `gorm:"type:integer;not null;default:20" json:"max_proposals_enriched"`
	MaxEmbeddingCalls    int `gorm:"type:integer;not null;default:20" json:"max_embedding_calls"`
	MaxTranslationCalls  int `gorm:"type:integer;not null;default:10" json:"max_translation_calls"`
	MaxMinedProposals    int `gorm:"type:integer;not null;default:25" json:"max_mined_proposals"`
	MaxCentroidRefresh   int `gorm:"type:integer;not null;default:3" json:"max_centroid_refresh"`
	MaxPendingProposals  int `gorm:"type:integer;not null;default:100" json:"max_pending_proposals"`

	// Coverage floors (percent) — advisory flip-gate defaults, not flip authority.
	CoverageFloorForyouPct int `gorm:"type:integer;not null;default:70" json:"coverage_floor_foryou_pct"`
	CoverageFloorNewsPct   int `gorm:"type:integer;not null;default:60" json:"coverage_floor_news_pct"`
	CoverageFloorStoryPct  int `gorm:"type:integer;not null;default:50" json:"coverage_floor_story_pct"`

	// Scoring thresholds (§6 V1 defaults).
	HighConfidence      float64 `gorm:"type:double precision;not null;default:0.80" json:"high_confidence"`
	AdvisoryRejectFloor float64 `gorm:"type:double precision;not null;default:0.35" json:"advisory_reject_floor"`
	DuplicateCosine     float64 `gorm:"type:double precision;not null;default:0.90" json:"duplicate_cosine"`

	FailureBreakerPct int `gorm:"type:integer;not null;default:25" json:"failure_breaker_pct"`
	DeadTopicDays     int `gorm:"type:integer;not null;default:14" json:"dead_topic_days"`

	// Trust ladder evidence gate (§15) + the earned auto-approve tier. The switch
	// is human-flipped and server-gated: enabling requires the trust banner to be
	// eligible at flip time, and the runner re-verifies eligibility on every run.
	// AutoApproveMinConfidence is the tier's OWN stricter threshold (0.85 floor) —
	// it deliberately cannot inherit the 0.80 high-confidence default.
	TrustMinDecisions        int     `gorm:"type:integer;not null;default:30" json:"trust_min_decisions"`
	TrustMinAgreementPct     int     `gorm:"type:integer;not null;default:90" json:"trust_min_agreement_pct"`
	AutoApproveEnabled       bool    `gorm:"not null;default:false" json:"auto_approve_enabled"`
	AutoApproveMinConfidence float64 `gorm:"type:double precision;not null;default:0.92" json:"auto_approve_min_confidence"`
	MaxAutoApprovals         int     `gorm:"type:integer;not null;default:3" json:"max_auto_approvals"`

	// Durable cursors for the limited mapping sweep (§0.1.1). Advance only on a
	// successful page; wrap to 0 when the tail is reached so holes get re-swept.
	ItemMapCursor    uint `gorm:"type:bigint;not null;default:0" json:"item_map_cursor"`
	StoryMapCursor   uint `gorm:"type:bigint;not null;default:0" json:"story_map_cursor"`
	DirtyItemCursor  uint `gorm:"type:bigint;not null;default:0" json:"dirty_item_cursor"`
	DirtyStoryCursor uint `gorm:"type:bigint;not null;default:0" json:"dirty_story_cursor"`

	PausedUntil *time.Time `gorm:"type:timestamp" json:"paused_until,omitempty"`
	LastRunAt   *time.Time `gorm:"type:timestamp" json:"last_run_at,omitempty"`
	LastMineAt  *time.Time `gorm:"type:timestamp" json:"last_mine_at,omitempty"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (PreferenceAutopilotPolicy) TableName() string {
	return "preference_autopilot_policies"
}

func DefaultPreferenceAutopilotPolicy(tenantID string) PreferenceAutopilotPolicy {
	return PreferenceAutopilotPolicy{
		TenantID:                 tenantID,
		Enabled:                  false,
		Mode:                     PreferenceAutopilotModeObserve,
		IntervalMinutes:          15,
		MaxItemCandidates:        250,
		MaxStoryCandidates:       100,
		MaxDirtyTopics:           3,
		MaxUsersRecompute:        100,
		MaxProposalsEnriched:     20,
		MaxEmbeddingCalls:        20,
		MaxTranslationCalls:      10,
		MaxMinedProposals:        25,
		MaxCentroidRefresh:       3,
		MaxPendingProposals:      100,
		CoverageFloorForyouPct:   70,
		CoverageFloorNewsPct:     60,
		CoverageFloorStoryPct:    50,
		HighConfidence:           0.80,
		AdvisoryRejectFloor:      0.35,
		DuplicateCosine:          0.90,
		FailureBreakerPct:        25,
		DeadTopicDays:            14,
		TrustMinDecisions:        30,
		TrustMinAgreementPct:     90,
		AutoApproveEnabled:       false,
		AutoApproveMinConfidence: 0.92,
		MaxAutoApprovals:         3,
	}
}

// PreferenceAutopilotRun records one bounded pass. stats_before/after hold the §6
// snapshot (coverage, integrity, boost analysis) — this is where coverage history
// lives; there is no separate snapshot table (§11).
type PreferenceAutopilotRun struct {
	ID       uint      `gorm:"primaryKey" json:"-"`
	PublicID uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex:idx_preference_autopilot_runs_public_id" json:"id"`
	TenantID string    `gorm:"type:varchar(64);not null;index:idx_preference_autopilot_runs_tenant" json:"tenant_id"`

	Trigger  string `gorm:"type:varchar(24);not null" json:"trigger"`
	Mode     string `gorm:"type:varchar(24);not null" json:"mode"`
	Status   string `gorm:"type:varchar(24);not null;index:idx_preference_autopilot_runs_status" json:"status"`
	Headline string `gorm:"type:varchar(32)" json:"headline,omitempty"`

	StartedAt         time.Time      `gorm:"type:timestamp;not null;index:idx_preference_autopilot_runs_started_at" json:"started_at"`
	FinishedAt        *time.Time     `gorm:"type:timestamp" json:"finished_at,omitempty"`
	Summary           string         `gorm:"type:text" json:"summary,omitempty"`
	RecommendedAction string         `gorm:"type:text" json:"recommended_action,omitempty"`
	StatsBefore       datatypes.JSON `gorm:"type:jsonb" json:"stats_before,omitempty"`
	StatsAfter        datatypes.JSON `gorm:"type:jsonb" json:"stats_after,omitempty"`
	CreatedBy         string         `gorm:"type:varchar(255)" json:"created_by,omitempty"`
	Error             string         `gorm:"type:text" json:"error,omitempty"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (PreferenceAutopilotRun) TableName() string {
	return "preference_autopilot_runs"
}

// PreferenceAutopilotAction is one row per considered unit (§11). subject_ref is
// textual to hold a topic UUID, a proposal integer id, a user UUID, or an
// aggregate label under one column.
type PreferenceAutopilotAction struct {
	ID       uint      `gorm:"primaryKey" json:"-"`
	PublicID uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex:idx_preference_autopilot_actions_public_id" json:"id"`
	RunID    uint      `gorm:"not null;index:idx_preference_autopilot_actions_run_id" json:"-"`
	TenantID string    `gorm:"type:varchar(64);not null;index:idx_preference_autopilot_actions_tenant" json:"tenant_id"`

	ActionClass string `gorm:"type:varchar(32);not null;index:idx_preference_autopilot_actions_class" json:"action_class"`
	SubjectType string `gorm:"type:varchar(16);not null" json:"subject_type"`
	SubjectRef  string `gorm:"type:text" json:"subject_ref,omitempty"`
	Status      string `gorm:"type:varchar(24);not null;index:idx_preference_autopilot_actions_status" json:"status"`
	Guardrail   string `gorm:"type:varchar(48)" json:"guardrail,omitempty"`
	Reason      string `gorm:"type:text" json:"reason,omitempty"`
	DurationMs  int    `gorm:"type:integer;not null;default:0" json:"duration_ms"`

	StartedAt  time.Time  `gorm:"type:timestamp;not null" json:"started_at"`
	FinishedAt *time.Time `gorm:"type:timestamp" json:"finished_at,omitempty"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
}

func (PreferenceAutopilotAction) TableName() string {
	return "preference_autopilot_actions"
}

// PreferenceAffinityRecomputeQueue is durable CORRECTNESS state (not an audit
// ledger): a tenant/user deduplicated work queue. A catalog merge can affect users
// who are not recently active, and a failed synchronous recompute must be retried;
// both enqueue here and the bounded runner drains queued users before
// recently-active users (§10).
type PreferenceAffinityRecomputeQueue struct {
	ID        uint      `gorm:"primaryKey" json:"-"`
	TenantID  string    `gorm:"type:varchar(64);not null;uniqueIndex:idx_pref_recompute_queue_tenant_user,priority:1" json:"tenant_id"`
	UserID    uuid.UUID `gorm:"type:uuid;not null;uniqueIndex:idx_pref_recompute_queue_tenant_user,priority:2" json:"user_id"`
	Reason    string    `gorm:"type:varchar(24);not null" json:"reason"`
	Attempts  int       `gorm:"type:integer;not null;default:0" json:"attempts"`
	LastError string    `gorm:"type:text" json:"last_error,omitempty"`
	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (PreferenceAffinityRecomputeQueue) TableName() string {
	return "preference_affinity_recompute_queue"
}
