package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

const (
	FeedIntegrityTierLight = "light"
	FeedIntegrityTierDeep  = "deep"

	FeedIntegrityRunRunning   = "running"
	FeedIntegrityRunCompleted = "completed"
	FeedIntegrityRunPartial   = "partial"
	FeedIntegrityRunFailed    = "failed"

	FeedIntegrityEpisodeOpen       = "open"
	FeedIntegrityEpisodeRecovering = "recovering"
	FeedIntegrityEpisodeResolved   = "resolved"
	FeedIntegrityEpisodeClosed     = "closed_by_human"

	FeedIntegrityAxisConsumer  = "consumer"
	FeedIntegrityAxisReadiness = "readiness"

	FeedIntegrityVerdictHealthy       = "healthy"
	FeedIntegrityVerdictDegradedMinor = "degraded_minor"
	FeedIntegrityVerdictDegradedMajor = "degraded_major"
	FeedIntegrityVerdictBroken        = "broken"
	FeedIntegrityVerdictNotReady      = "not_ready"
	FeedIntegrityVerdictInconclusive  = "inconclusive"

	FeedIntegrityAutopilotModeObserve  = "observe"
	FeedIntegrityAutopilotModeAssist   = "assist"
	FeedIntegrityAutopilotModeSafeAuto = "safe_auto"

	FeedIntegrityDecisionNoAction         = "no_action"
	FeedIntegrityDecisionConfirming       = "confirming"
	FeedIntegrityDecisionAttention        = "attention"
	FeedIntegrityDecisionApprovalRequired = "approval_required"
	FeedIntegrityDecisionReady            = "ready"
	FeedIntegrityDecisionExecuted         = "executed"
	FeedIntegrityDecisionRecovering       = "recovering"
	FeedIntegrityDecisionBlocked          = "blocked"
	FeedIntegrityDecisionActionFailed     = "action_failed"

	FeedIntegrityActionWouldExecute       = "would_execute"
	FeedIntegrityActionApprovalRequired   = "approval_required"
	FeedIntegrityActionApproved           = "approved"
	FeedIntegrityActionRejected           = "rejected"
	FeedIntegrityActionReady              = "ready"
	FeedIntegrityActionClaimed            = "claimed"
	FeedIntegrityActionRunning            = "running"
	FeedIntegrityActionToolSucceeded      = "tool_succeeded"
	FeedIntegrityActionToolFailed         = "tool_failed"
	FeedIntegrityActionVerifying          = "verifying"
	FeedIntegrityActionVerificationPassed = "verification_passed"
	FeedIntegrityActionVerificationFailed = "verification_failed"
	FeedIntegrityActionSkipped            = "skipped"
	FeedIntegrityActionExpired            = "expired"

	FeedIntegrityActionConfirm       = "integrity.targeted_confirm"
	FeedIntegrityActionRefreshWindow = "news_snapshot.refresh_window"
)

// FeedIntegrityPolicy controls deterministic, read-only CMS feed checks. It is
// deliberately separate from the later Feed Integrity Autopilot policy.
type FeedIntegrityPolicy struct {
	ID       uint   `gorm:"primaryKey" json:"-"`
	TenantID string `gorm:"type:varchar(64);not null;uniqueIndex:idx_feed_integrity_policy_tenant" json:"tenant_id"`

	ScheduledEnabled     bool `gorm:"not null;default:false" json:"scheduled_enabled"`
	LightIntervalMinutes int  `gorm:"not null;default:15" json:"light_interval_minutes"`
	DeepIntervalHours    int  `gorm:"not null;default:24" json:"deep_interval_hours"`
	ConfirmRuns          int  `gorm:"not null;default:2" json:"confirm_runs"`
	ResolveRuns          int  `gorm:"not null;default:3" json:"resolve_runs"`
	// Column overrides: GORM's naming strategy derives "flap_cycles24h",
	// "for_you_latency_budget_ms", and "expected_min_for_you_units" from these
	// field names, none of which match the migration's columns. Pin them
	// explicitly so reads/writes round-trip instead of silently returning 0.
	FlapCycles24h          int        `gorm:"column:flap_cycles_24h;not null;default:3" json:"flap_cycles_24h"`
	EdgePagesPerFeed       int        `gorm:"not null;default:3" json:"edge_pages_per_feed"`
	ProbeURLBudget         int        `gorm:"not null;default:40" json:"probe_url_budget"`
	ProbeConcurrency       int        `gorm:"not null;default:2" json:"probe_concurrency"`
	ProbeTimeoutMS         int        `gorm:"not null;default:5000" json:"probe_timeout_ms"`
	ForYouLatencyBudgetMS  int        `gorm:"column:foryou_latency_budget_ms;not null;default:1500" json:"foryou_latency_budget_ms"`
	NewsLatencyBudgetMS    int        `gorm:"not null;default:2000" json:"news_latency_budget_ms"`
	ThinSlideFloor         float64    `gorm:"not null;default:0.80" json:"thin_slide_floor"`
	ExpectedMinForYouUnits int        `gorm:"column:expected_min_foryou_units;not null;default:1" json:"expected_min_foryou_units"`
	ExpectedMinNewsSlides  int        `gorm:"not null;default:1" json:"expected_min_news_slides"`
	PausedUntil            *time.Time `gorm:"type:timestamp" json:"paused_until,omitempty"`
	LastLightRunAt         *time.Time `gorm:"type:timestamp" json:"last_light_run_at,omitempty"`
	LastDeepRunAt          *time.Time `gorm:"type:timestamp" json:"last_deep_run_at,omitempty"`

	AutopilotEnabled               bool           `gorm:"column:autopilot_enabled;not null;default:false" json:"autopilot_enabled"`
	AutopilotMode                  string         `gorm:"column:autopilot_mode;type:varchar(24);not null;default:'observe'" json:"autopilot_mode"`
	AutopilotPausedUntil           *time.Time     `gorm:"column:autopilot_paused_until;type:timestamp" json:"autopilot_paused_until,omitempty"`
	AutopilotActionModes           datatypes.JSON `gorm:"column:autopilot_action_modes;type:jsonb" json:"autopilot_action_modes,omitempty"`
	AutopilotActionHourlyCap       int            `gorm:"column:autopilot_action_hourly_cap;not null;default:2" json:"autopilot_action_hourly_cap"`
	AutopilotDiagnosticHourlyCap   int            `gorm:"column:autopilot_diagnostic_hourly_cap;not null;default:4" json:"autopilot_diagnostic_hourly_cap"`
	AutopilotCooldownMinutes       int            `gorm:"column:autopilot_cooldown_minutes;not null;default:60" json:"autopilot_cooldown_minutes"`
	AutopilotEvidenceMaxAgeMinutes int            `gorm:"column:autopilot_evidence_max_age_minutes;not null;default:10" json:"autopilot_evidence_max_age_minutes"`
	AutopilotRetryLimit            int            `gorm:"column:autopilot_retry_limit;not null;default:1" json:"autopilot_retry_limit"`
	AutopilotTrustMinDecisions     int            `gorm:"column:autopilot_trust_min_decisions;not null;default:20" json:"autopilot_trust_min_decisions"`
	AutopilotTrustMinAgreementPct  int            `gorm:"column:autopilot_trust_min_agreement_pct;not null;default:95" json:"autopilot_trust_min_agreement_pct"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (FeedIntegrityPolicy) TableName() string { return "feed_integrity_policies" }

func DefaultFeedIntegrityPolicy(tenantID string) FeedIntegrityPolicy {
	return FeedIntegrityPolicy{TenantID: tenantID, LightIntervalMinutes: 15, DeepIntervalHours: 24, ConfirmRuns: 2, ResolveRuns: 3, FlapCycles24h: 3, EdgePagesPerFeed: 3, ProbeURLBudget: 40, ProbeConcurrency: 2, ProbeTimeoutMS: 5000, ForYouLatencyBudgetMS: 1500, NewsLatencyBudgetMS: 2000, ThinSlideFloor: 0.80, ExpectedMinForYouUnits: 1, ExpectedMinNewsSlides: 1, AutopilotMode: FeedIntegrityAutopilotModeObserve, AutopilotActionHourlyCap: 2, AutopilotDiagnosticHourlyCap: 4, AutopilotCooldownMinutes: 60, AutopilotEvidenceMaxAgeMinutes: 10, AutopilotRetryLimit: 1, AutopilotTrustMinDecisions: 20, AutopilotTrustMinAgreementPct: 95}
}

type FeedIntegrityRun struct {
	ID                   uint           `gorm:"primaryKey" json:"-"`
	PublicID             uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex:idx_feed_integrity_runs_public_id" json:"id"`
	TenantID             string         `gorm:"type:varchar(64);not null;index:idx_feed_integrity_runs_tenant" json:"tenant_id"`
	Trigger              string         `gorm:"type:varchar(24);not null" json:"trigger"`
	Tier                 string         `gorm:"type:varchar(16);not null" json:"tier"`
	Status               string         `gorm:"type:varchar(24);not null;index:idx_feed_integrity_runs_status" json:"status"`
	Headline             string         `gorm:"type:varchar(32);not null" json:"headline"`
	StartedAt            time.Time      `gorm:"type:timestamp;not null;index:idx_feed_integrity_runs_started_at" json:"started_at"`
	FinishedAt           *time.Time     `gorm:"type:timestamp" json:"finished_at,omitempty"`
	Summary              string         `gorm:"type:text" json:"summary,omitempty"`
	FeedResults          datatypes.JSON `gorm:"type:jsonb" json:"feed_results,omitempty"`
	Counts               datatypes.JSON `gorm:"type:jsonb" json:"counts,omitempty"`
	CreatedBy            string         `gorm:"type:varchar(255)" json:"created_by,omitempty"`
	Error                string         `gorm:"type:text" json:"error,omitempty"`
	ErrorClass           string         `gorm:"type:varchar(48);not null;default:'none'" json:"error_class"`
	LaneResults          datatypes.JSON `gorm:"column:lane_results;type:jsonb" json:"lane_results,omitempty"`
	AutopilotEvaluatedAt *time.Time     `gorm:"column:autopilot_evaluated_at;type:timestamp" json:"autopilot_evaluated_at,omitempty"`
	AutopilotDecision    string         `gorm:"column:autopilot_decision;type:varchar(32)" json:"autopilot_decision,omitempty"`
	AutopilotCounts      datatypes.JSON `gorm:"column:autopilot_counts;type:jsonb" json:"autopilot_counts,omitempty"`
	AutopilotErrorClass  string         `gorm:"column:autopilot_error_class;type:varchar(48);not null;default:'none'" json:"autopilot_error_class"`
	CreatedAt            time.Time      `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt            time.Time      `gorm:"autoUpdateTime" json:"updated_at"`
}

func (FeedIntegrityRun) TableName() string { return "feed_integrity_runs" }

type FeedIntegrityFinding struct {
	ID             uint           `gorm:"primaryKey" json:"-"`
	PublicID       uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex:idx_feed_integrity_findings_public_id" json:"id"`
	RunID          uint           `gorm:"not null;index:idx_feed_integrity_findings_run" json:"-"`
	TenantID       string         `gorm:"type:varchar(64);not null;index:idx_feed_integrity_findings_tenant" json:"tenant_id"`
	Lane           string         `gorm:"type:varchar(16);not null" json:"lane"`
	CheckKey       string         `gorm:"type:varchar(80);not null;index:idx_feed_integrity_findings_check" json:"check_key"`
	Axis           string         `gorm:"type:varchar(16);not null" json:"axis"`
	Feed           string         `gorm:"type:varchar(16);not null" json:"feed"`
	Variant        string         `gorm:"type:varchar(32);not null;default:'default'" json:"variant"`
	TargetType     string         `gorm:"type:varchar(24)" json:"target_type,omitempty"`
	TargetRef      string         `gorm:"type:text" json:"target_ref,omitempty"`
	CandidateCount int            `gorm:"not null;default:0" json:"candidate_count"`
	AffectedCount  int            `gorm:"column:affected_count;not null;default:1" json:"affected_count"`
	SampleCount    int            `gorm:"column:sample_count;not null;default:0" json:"sample_count"`
	Status         string         `gorm:"type:varchar(32);not null;index:idx_feed_integrity_findings_status" json:"status"`
	Severity       string         `gorm:"type:varchar(16)" json:"severity,omitempty"`
	Evidence       datatypes.JSON `gorm:"type:jsonb" json:"evidence,omitempty"`
	CreatedAt      time.Time      `gorm:"autoCreateTime;index:idx_feed_integrity_findings_created_at" json:"created_at"`
}

func (FeedIntegrityFinding) TableName() string { return "feed_integrity_findings" }

type FeedIntegrityEpisode struct {
	ID                 uint           `gorm:"primaryKey" json:"-"`
	PublicID           uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex:idx_feed_integrity_episodes_public_id" json:"id"`
	TenantID           string         `gorm:"type:varchar(64);not null;index:idx_feed_integrity_episodes_tenant" json:"tenant_id"`
	CheckKey           string         `gorm:"type:varchar(80);not null" json:"check_key"`
	Axis               string         `gorm:"type:varchar(16);not null" json:"axis"`
	Feed               string         `gorm:"type:varchar(16);not null" json:"feed"`
	Variant            string         `gorm:"type:varchar(32);not null;default:'default'" json:"variant"`
	Scope              string         `gorm:"type:text;not null;default:'feed'" json:"scope"`
	Status             string         `gorm:"type:varchar(32);not null;index:idx_feed_integrity_episodes_status" json:"status"`
	Severity           string         `gorm:"type:varchar(16);not null" json:"severity"`
	Summary            string         `gorm:"type:text" json:"summary"`
	AffectedTrend      datatypes.JSON `gorm:"type:jsonb" json:"affected_trend,omitempty"`
	Evidence           datatypes.JSON `gorm:"type:jsonb" json:"evidence,omitempty"`
	Attribution        datatypes.JSON `gorm:"type:jsonb" json:"attribution,omitempty"`
	FirstDetectedAt    time.Time      `gorm:"type:timestamp;not null" json:"first_detected_at"`
	LastSeenAt         time.Time      `gorm:"type:timestamp;not null" json:"last_seen_at"`
	RecoveringSince    *time.Time     `gorm:"type:timestamp" json:"recovering_since,omitempty"`
	ResolvedAt         *time.Time     `gorm:"type:timestamp" json:"resolved_at,omitempty"`
	ClosedBy           string         `gorm:"type:varchar(255)" json:"closed_by,omitempty"`
	CloseReasonClass   string         `gorm:"type:varchar(32)" json:"close_reason_class,omitempty"`
	CloseNotes         string         `gorm:"type:text" json:"close_notes,omitempty"`
	ViolationStreak    int            `gorm:"column:violation_streak;not null;default:0" json:"violation_streak"`
	CleanStreak        int            `gorm:"column:clean_streak;not null;default:0" json:"clean_streak"`
	FlapCount24h       int            `gorm:"column:flap_count_24h;not null;default:0" json:"flap_count_24h"`
	AttributionVersion string         `gorm:"column:attribution_version;type:varchar(32)" json:"attribution_version,omitempty"`
	RecommendedAction  string         `gorm:"column:recommended_action;type:varchar(80)" json:"recommended_action,omitempty"`
	LatestActionID     *uint          `gorm:"column:latest_action_id" json:"-"`
	CreatedAt          time.Time      `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt          time.Time      `gorm:"autoUpdateTime" json:"updated_at"`
}

func (FeedIntegrityEpisode) TableName() string { return "feed_integrity_episodes" }

type FeedIntegritySuppression struct {
	ID        uint       `gorm:"primaryKey" json:"-"`
	PublicID  uuid.UUID  `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex:idx_feed_integrity_suppressions_public_id" json:"id"`
	TenantID  string     `gorm:"type:varchar(64);not null;index:idx_feed_integrity_suppressions_tenant" json:"tenant_id"`
	CheckKey  string     `gorm:"type:varchar(80);not null" json:"check_key"`
	Feed      string     `gorm:"type:varchar(16)" json:"feed,omitempty"`
	Variant   string     `gorm:"type:varchar(32)" json:"variant,omitempty"`
	Scope     string     `gorm:"type:text" json:"scope,omitempty"`
	Reason    string     `gorm:"type:text;not null" json:"reason"`
	StartsAt  time.Time  `gorm:"type:timestamp;not null" json:"starts_at"`
	ExpiresAt time.Time  `gorm:"type:timestamp;not null;index:idx_feed_integrity_suppressions_expires" json:"expires_at"`
	CreatedBy string     `gorm:"type:varchar(255)" json:"created_by,omitempty"`
	RevokedAt *time.Time `gorm:"type:timestamp" json:"revoked_at,omitempty"`
	RevokedBy string     `gorm:"type:varchar(255)" json:"revoked_by,omitempty"`
	CreatedAt time.Time  `gorm:"autoCreateTime" json:"created_at"`
}

func (FeedIntegritySuppression) TableName() string { return "feed_integrity_suppressions" }

type FeedIntegrityAction struct {
	ID                          uint           `gorm:"primaryKey" json:"-"`
	PublicID                    uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex:idx_feed_integrity_actions_public_id" json:"id"`
	TenantID                    string         `gorm:"type:varchar(64);not null;index:idx_feed_integrity_actions_tenant" json:"tenant_id"`
	RunID                       uint           `gorm:"not null;index:idx_feed_integrity_actions_run" json:"-"`
	EpisodeID                   uint           `gorm:"not null;index:idx_feed_integrity_actions_episode" json:"-"`
	RetryOfActionID             *uint          `gorm:"column:retry_of_action_id" json:"-"`
	ActionClass                 string         `gorm:"column:action_class;type:varchar(80);not null;index:idx_feed_integrity_actions_class" json:"action_class"`
	OwnerSystem                 string         `gorm:"column:owner_system;type:varchar(64);not null" json:"owner_system"`
	TargetScope                 string         `gorm:"column:target_scope;type:text;not null" json:"target_scope"`
	Mode                        string         `gorm:"type:varchar(24);not null" json:"mode"`
	Outcome                     string         `gorm:"type:varchar(32);not null;index:idx_feed_integrity_actions_outcome" json:"outcome"`
	Decision                    string         `gorm:"type:varchar(32);not null" json:"decision"`
	Guardrail                   string         `gorm:"type:varchar(64)" json:"guardrail,omitempty"`
	Reason                      string         `gorm:"type:text" json:"reason,omitempty"`
	IdempotencyKey              string         `gorm:"column:idempotency_key;type:varchar(160);not null;uniqueIndex:idx_feed_integrity_actions_idempotency" json:"idempotency_key"`
	EvidenceFingerprint         string         `gorm:"column:evidence_fingerprint;type:varchar(64);not null" json:"evidence_fingerprint"`
	RegistryVersion             string         `gorm:"column:registry_version;type:varchar(32);not null" json:"registry_version"`
	ToolVersion                 string         `gorm:"column:tool_version;type:varchar(32)" json:"tool_version,omitempty"`
	VerificationContractVersion string         `gorm:"column:verification_contract_version;type:varchar(32)" json:"verification_contract_version,omitempty"`
	Input                       datatypes.JSON `gorm:"type:jsonb" json:"input,omitempty"`
	Output                      datatypes.JSON `gorm:"type:jsonb" json:"output,omitempty"`
	Verification                datatypes.JSON `gorm:"type:jsonb" json:"verification,omitempty"`
	Actor                       string         `gorm:"type:varchar(255)" json:"actor,omitempty"`
	CorrelationID               string         `gorm:"column:correlation_id;type:varchar(80)" json:"correlation_id,omitempty"`
	ClaimToken                  string         `gorm:"column:claim_token;type:varchar(80)" json:"-"`
	ClaimedAt                   *time.Time     `gorm:"column:claimed_at;type:timestamp" json:"claimed_at,omitempty"`
	ClaimExpiresAt              *time.Time     `gorm:"column:claim_expires_at;type:timestamp" json:"claim_expires_at,omitempty"`
	ApprovedAt                  *time.Time     `gorm:"column:approved_at;type:timestamp" json:"approved_at,omitempty"`
	ExecutedAt                  *time.Time     `gorm:"column:executed_at;type:timestamp" json:"executed_at,omitempty"`
	VerificationDueAt           *time.Time     `gorm:"column:verification_due_at;type:timestamp" json:"verification_due_at,omitempty"`
	FinishedAt                  *time.Time     `gorm:"column:finished_at;type:timestamp" json:"finished_at,omitempty"`
	DurationMS                  int64          `gorm:"column:duration_ms;not null;default:0" json:"duration_ms"`
	ErrorClass                  string         `gorm:"column:error_class;type:varchar(48);not null;default:'none'" json:"error_class"`
	CreatedAt                   time.Time      `gorm:"autoCreateTime;index:idx_feed_integrity_actions_created" json:"created_at"`
	UpdatedAt                   time.Time      `gorm:"autoUpdateTime" json:"updated_at"`
}

func (FeedIntegrityAction) TableName() string { return "feed_integrity_actions" }
