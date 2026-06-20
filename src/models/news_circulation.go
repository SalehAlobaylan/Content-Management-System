package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

const (
	NewsWindowToday = "today"
	NewsWindowWeek  = "week"
	NewsWindowMonth = "month"

	NewsLifecycleBreaking   = "breaking"
	NewsLifecycleActive     = "active"
	NewsLifecycleCooling    = "cooling"
	NewsLifecycleHistorical = "historical"

	NewsCirculationPresetLatestPlus = "latest_plus"

	SourceCadenceModeSuggest   = "suggest"
	SourceCadenceModeAutoApply = "auto_apply"
	SourceCadenceModeManual    = "manual"
)

// NewsCirculationPolicy stores the tenant-level story circulation knobs. It is
// intentionally separate from RankingConfig: RankingConfig is the generic
// content-ranking engine, while this model governs story windows, carryover,
// story overrides, and adaptive source cadence.
type NewsCirculationPolicy struct {
	ID       uint   `gorm:"primaryKey" json:"-"`
	TenantID string `gorm:"type:varchar(64);not null;uniqueIndex:idx_news_circulation_policy_tenant" json:"tenant_id"`

	Preset   string `gorm:"type:varchar(32);not null;default:'latest_plus'" json:"preset"`
	Timezone string `gorm:"type:varchar(64);not null;default:'Asia/Riyadh'" json:"timezone"`

	MinTodayStories       int     `gorm:"type:integer;not null;default:8" json:"min_today_stories"`
	CarryoverHours        int     `gorm:"type:integer;not null;default:72" json:"carryover_hours"`
	CarryoverMinScore     float64 `gorm:"type:double precision;not null;default:0.25" json:"carryover_min_score"`
	BreakingMaxAgeMinutes int     `gorm:"type:integer;not null;default:180" json:"breaking_max_age_minutes"`
	BreakingMinMembers    int     `gorm:"type:integer;not null;default:3" json:"breaking_min_members"`

	RecencyWeight       float64 `gorm:"type:double precision;not null;default:0.55" json:"recency_weight"`
	ImportanceWeight    float64 `gorm:"type:double precision;not null;default:0.15" json:"importance_weight"`
	MomentumWeight      float64 `gorm:"type:double precision;not null;default:0.10" json:"momentum_weight"`
	CoverageWeight      float64 `gorm:"type:double precision;not null;default:0.30" json:"coverage_weight"`
	SourceQualityWeight float64 `gorm:"type:double precision;not null;default:0.10" json:"source_quality_weight"`
	DiversityWeight     float64 `gorm:"type:double precision;not null;default:0.05" json:"diversity_weight"`
	TrendingWeight      float64 `gorm:"type:double precision;not null;default:0.05" json:"trending_weight"`

	SourceCadenceMode        string `gorm:"type:varchar(20);not null;default:'suggest'" json:"source_cadence_mode"`
	SourceClaimIntervalMins  int    `gorm:"type:integer;not null;default:15" json:"source_claim_interval_minutes"`
	SourceClaimBatchSize     int    `gorm:"type:integer;not null;default:20" json:"source_claim_batch_size"`
	SourceMinIntervalMinutes int    `gorm:"type:integer;not null;default:10" json:"source_min_interval_minutes"`
	SourceMaxIntervalMinutes int    `gorm:"type:integer;not null;default:360" json:"source_max_interval_minutes"`
	SourceMaxChangePercent   int    `gorm:"type:integer;not null;default:50" json:"source_max_change_percent"`

	// Automation heartbeat — the self-running recommendation loop. Off by default
	// so turning the system "automatic" is always a deliberate, reversible choice.
	// The cadence mode (suggest/auto_apply/manual) decides what the heartbeat DOES;
	// these knobs decide whether it runs and how aggressively it may act.
	AutomationEnabled         bool       `gorm:"not null;default:false" json:"automation_enabled"`
	AutomationIntervalMinutes int        `gorm:"type:integer;not null;default:60" json:"automation_interval_minutes"`
	AutoApplySpeedups         bool       `gorm:"not null;default:false" json:"auto_apply_speedups"`
	MaxAutoAppliesPerRun      int        `gorm:"type:integer;not null;default:5" json:"max_auto_applies_per_run"`
	MinRunsForAuto            int        `gorm:"type:integer;not null;default:4" json:"min_runs_for_auto"`
	LastAutomationRunAt       *time.Time `gorm:"type:timestamp" json:"last_automation_run_at,omitempty"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (NewsCirculationPolicy) TableName() string {
	return "news_circulation_policies"
}

func DefaultNewsCirculationPolicy(tenantID string) NewsCirculationPolicy {
	return NewsCirculationPolicy{
		TenantID:                  tenantID,
		Preset:                    NewsCirculationPresetLatestPlus,
		Timezone:                  "Asia/Riyadh",
		MinTodayStories:           8,
		CarryoverHours:            72,
		CarryoverMinScore:         0.25,
		BreakingMaxAgeMinutes:     180,
		BreakingMinMembers:        3,
		RecencyWeight:             0.55,
		ImportanceWeight:          0.15,
		MomentumWeight:            0.10,
		CoverageWeight:            0.30,
		SourceQualityWeight:       0.10,
		DiversityWeight:           0.05,
		TrendingWeight:            0.05,
		SourceCadenceMode:         SourceCadenceModeSuggest,
		SourceClaimIntervalMins:   15,
		SourceClaimBatchSize:      20,
		SourceMinIntervalMinutes:  10,
		SourceMaxIntervalMinutes:  360,
		SourceMaxChangePercent:    50,
		AutomationEnabled:         false,
		AutomationIntervalMinutes: 60,
		AutoApplySpeedups:         false,
		MaxAutoAppliesPerRun:      5,
		MinRunsForAuto:            4,
	}
}

// NewsStoryOverride is the story-level exception layer. It targets topics,
// not content items, so admins can pin/suppress/exclude a whole story without
// changing member status or abusing lead-item flags.
type NewsStoryOverride struct {
	ID       uint      `gorm:"primaryKey" json:"-"`
	PublicID uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex:idx_news_story_overrides_public_id" json:"id"`
	TenantID string    `gorm:"type:varchar(64);not null;index:idx_news_story_overrides_tenant;uniqueIndex:idx_news_story_overrides_topic_tenant,priority:2" json:"tenant_id"`
	StoryID  uuid.UUID `gorm:"type:uuid;not null;index:idx_news_story_overrides_story;uniqueIndex:idx_news_story_overrides_topic_tenant,priority:1" json:"story_id"`

	PinToTop        bool    `gorm:"default:false" json:"pin_to_top"`
	Suppress        bool    `gorm:"default:false" json:"suppress"`
	ExcludeFromFeed bool    `gorm:"default:false" json:"exclude_from_feed"`
	ImportanceBoost float64 `gorm:"type:double precision;not null;default:1.0" json:"importance_boost"`

	Notes     string     `gorm:"type:text" json:"notes,omitempty"`
	SetBy     string     `gorm:"type:varchar(255)" json:"set_by,omitempty"`
	ExpiresAt *time.Time `gorm:"type:timestamp;index:idx_news_story_overrides_expires_at" json:"expires_at,omitempty"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (NewsStoryOverride) TableName() string {
	return "news_story_overrides"
}

// SourceRunTelemetry stores one source circulation run. Fetch and normalize
// stages can report to the same JobID; CMS merges the latest counters.
type SourceRunTelemetry struct {
	ID       uint      `gorm:"primaryKey" json:"-"`
	PublicID uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID string    `gorm:"type:varchar(64);not null;index:idx_source_run_telemetry_tenant" json:"tenant_id"`
	SourceID uuid.UUID `gorm:"type:uuid;not null;index:idx_source_run_telemetry_source" json:"source_id"`

	JobID       string `gorm:"type:varchar(128);not null;uniqueIndex:idx_source_run_telemetry_job" json:"job_id"`
	TriggeredBy string `gorm:"type:varchar(20);not null;default:'schedule'" json:"triggered_by"`

	Fetched    int `gorm:"type:integer;not null;default:0" json:"fetched"`
	Accepted   int `gorm:"type:integer;not null;default:0" json:"accepted"`
	Duplicates int `gorm:"type:integer;not null;default:0" json:"duplicates"`
	Filtered   int `gorm:"type:integer;not null;default:0" json:"filtered"`
	Failed     int `gorm:"type:integer;not null;default:0" json:"failed"`

	StartedAt  *time.Time     `gorm:"type:timestamp" json:"started_at,omitempty"`
	FinishedAt *time.Time     `gorm:"type:timestamp;index:idx_source_run_telemetry_finished_at" json:"finished_at,omitempty"`
	DurationMs int            `gorm:"type:integer;not null;default:0" json:"duration_ms"`
	Metadata   datatypes.JSON `gorm:"type:jsonb" json:"metadata,omitempty"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (SourceRunTelemetry) TableName() string {
	return "source_run_telemetry"
}

// SourceCirculationRecommendation is a generated, reviewable source cadence
// change. It starts as a recommendation; policy can later auto-apply it inside
// guardrails.
type SourceCirculationRecommendation struct {
	ID       uint      `gorm:"primaryKey" json:"-"`
	PublicID uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID string    `gorm:"type:varchar(64);not null;index:idx_source_circ_recs_tenant" json:"tenant_id"`
	SourceID uuid.UUID `gorm:"type:uuid;not null;index:idx_source_circ_recs_source" json:"source_id"`

	SourceName string `gorm:"type:varchar(255)" json:"source_name"`
	SourceType string `gorm:"type:varchar(20)" json:"source_type"`

	CurrentIntervalMinutes     int     `gorm:"type:integer;not null" json:"current_interval_minutes"`
	RecommendedIntervalMinutes int     `gorm:"type:integer;not null" json:"recommended_interval_minutes"`
	Score                      float64 `gorm:"type:double precision;not null;default:0" json:"score"`
	Reason                     string  `gorm:"type:text" json:"reason"`
	Mode                       string  `gorm:"type:varchar(20);not null;default:'suggest'" json:"mode"`

	Metrics   datatypes.JSON `gorm:"type:jsonb" json:"metrics,omitempty"`
	Applied   bool           `gorm:"default:false;index:idx_source_circ_recs_applied" json:"applied"`
	AppliedAt *time.Time     `gorm:"type:timestamp" json:"applied_at,omitempty"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (SourceCirculationRecommendation) TableName() string {
	return "source_circulation_recommendations"
}
