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
)

// FeedIntegrityPolicy controls deterministic, read-only CMS feed checks. It is
// deliberately separate from the later Feed Integrity Autopilot policy.
type FeedIntegrityPolicy struct {
	ID       uint   `gorm:"primaryKey" json:"-"`
	TenantID string `gorm:"type:varchar(64);not null;uniqueIndex:idx_feed_integrity_policy_tenant" json:"tenant_id"`

	ScheduledEnabled       bool       `gorm:"not null;default:false" json:"scheduled_enabled"`
	LightIntervalMinutes   int        `gorm:"not null;default:15" json:"light_interval_minutes"`
	DeepIntervalHours      int        `gorm:"not null;default:24" json:"deep_interval_hours"`
	ConfirmRuns            int        `gorm:"not null;default:2" json:"confirm_runs"`
	ResolveRuns            int        `gorm:"not null;default:3" json:"resolve_runs"`
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

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (FeedIntegrityPolicy) TableName() string { return "feed_integrity_policies" }

func DefaultFeedIntegrityPolicy(tenantID string) FeedIntegrityPolicy {
	return FeedIntegrityPolicy{TenantID: tenantID, LightIntervalMinutes: 15, DeepIntervalHours: 24, ConfirmRuns: 2, ResolveRuns: 3, FlapCycles24h: 3, EdgePagesPerFeed: 3, ProbeURLBudget: 40, ProbeConcurrency: 2, ProbeTimeoutMS: 5000, ForYouLatencyBudgetMS: 1500, NewsLatencyBudgetMS: 2000, ThinSlideFloor: 0.80, ExpectedMinForYouUnits: 1, ExpectedMinNewsSlides: 1}
}

type FeedIntegrityRun struct {
	ID          uint           `gorm:"primaryKey" json:"-"`
	PublicID    uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex:idx_feed_integrity_runs_public_id" json:"id"`
	TenantID    string         `gorm:"type:varchar(64);not null;index:idx_feed_integrity_runs_tenant" json:"tenant_id"`
	Trigger     string         `gorm:"type:varchar(24);not null" json:"trigger"`
	Tier        string         `gorm:"type:varchar(16);not null" json:"tier"`
	Status      string         `gorm:"type:varchar(24);not null;index:idx_feed_integrity_runs_status" json:"status"`
	Headline    string         `gorm:"type:varchar(32);not null" json:"headline"`
	StartedAt   time.Time      `gorm:"type:timestamp;not null;index:idx_feed_integrity_runs_started_at" json:"started_at"`
	FinishedAt  *time.Time     `gorm:"type:timestamp" json:"finished_at,omitempty"`
	Summary     string         `gorm:"type:text" json:"summary,omitempty"`
	FeedResults datatypes.JSON `gorm:"type:jsonb" json:"feed_results,omitempty"`
	Counts      datatypes.JSON `gorm:"type:jsonb" json:"counts,omitempty"`
	CreatedBy   string         `gorm:"type:varchar(255)" json:"created_by,omitempty"`
	Error       string         `gorm:"type:text" json:"error,omitempty"`
	ErrorClass  string         `gorm:"type:varchar(48);not null;default:'none'" json:"error_class"`
	CreatedAt   time.Time      `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt   time.Time      `gorm:"autoUpdateTime" json:"updated_at"`
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
	Status         string         `gorm:"type:varchar(32);not null;index:idx_feed_integrity_findings_status" json:"status"`
	Severity       string         `gorm:"type:varchar(16)" json:"severity,omitempty"`
	Evidence       datatypes.JSON `gorm:"type:jsonb" json:"evidence,omitempty"`
	CreatedAt      time.Time      `gorm:"autoCreateTime;index:idx_feed_integrity_findings_created_at" json:"created_at"`
}

func (FeedIntegrityFinding) TableName() string { return "feed_integrity_findings" }

type FeedIntegrityEpisode struct {
	ID               uint           `gorm:"primaryKey" json:"-"`
	PublicID         uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex:idx_feed_integrity_episodes_public_id" json:"id"`
	TenantID         string         `gorm:"type:varchar(64);not null;index:idx_feed_integrity_episodes_tenant" json:"tenant_id"`
	CheckKey         string         `gorm:"type:varchar(80);not null" json:"check_key"`
	Axis             string         `gorm:"type:varchar(16);not null" json:"axis"`
	Feed             string         `gorm:"type:varchar(16);not null" json:"feed"`
	Variant          string         `gorm:"type:varchar(32);not null;default:'default'" json:"variant"`
	Scope            string         `gorm:"type:text;not null;default:'feed'" json:"scope"`
	Status           string         `gorm:"type:varchar(32);not null;index:idx_feed_integrity_episodes_status" json:"status"`
	Severity         string         `gorm:"type:varchar(16);not null" json:"severity"`
	Summary          string         `gorm:"type:text" json:"summary"`
	AffectedTrend    datatypes.JSON `gorm:"type:jsonb" json:"affected_trend,omitempty"`
	Evidence         datatypes.JSON `gorm:"type:jsonb" json:"evidence,omitempty"`
	Attribution      datatypes.JSON `gorm:"type:jsonb" json:"attribution,omitempty"`
	FirstDetectedAt  time.Time      `gorm:"type:timestamp;not null" json:"first_detected_at"`
	LastSeenAt       time.Time      `gorm:"type:timestamp;not null" json:"last_seen_at"`
	RecoveringSince  *time.Time     `gorm:"type:timestamp" json:"recovering_since,omitempty"`
	ResolvedAt       *time.Time     `gorm:"type:timestamp" json:"resolved_at,omitempty"`
	ClosedBy         string         `gorm:"type:varchar(255)" json:"closed_by,omitempty"`
	CloseReasonClass string         `gorm:"type:varchar(32)" json:"close_reason_class,omitempty"`
	CloseNotes       string         `gorm:"type:text" json:"close_notes,omitempty"`
	CreatedAt        time.Time      `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt        time.Time      `gorm:"autoUpdateTime" json:"updated_at"`
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
