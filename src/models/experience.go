package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

// Real User Experience (RUX) System models. Browser-observed reliability
// telemetry, Observe-only. These tables are deliberately separate from
// user_interactions: RUX events are operational, high-volume, and short-lived,
// and must never feed ranking or user-affinity calculations.

// RuxSchemaVersion is the current event envelope version. The Wahb-Platform
// TypeScript contract (src/lib/experience/types.ts) mirrors this value.
const RuxSchemaVersion = 1

// ── Allowlisted enums (mirror src/lib/experience/types.ts) ───────────────────

var RuxSurfaces = map[string]bool{"foryou": true, "news": true}

var RuxEventTypes = map[string]bool{
	"session_started":       true,
	"feed_requested":        true,
	"feed_rendered":         true,
	"feed_failed":           true,
	"feed_empty":            true,
	"pagination_requested":  true,
	"pagination_received":   true,
	"pagination_starved":    true,
	"playback_attempted":    true,
	"playback_started":      true,
	"playback_waiting":      true,
	"playback_resumed":      true,
	"playback_failed":       true,
	"playback_backgrounded": true,
	"playback_fallback":     true,
	"handoff_started":       true,
	"handoff_completed":     true,
	"handoff_failed":        true,
	"article_opened":        true,
	"article_ready":         true,
	"article_failed":        true,
	"client_failure":        true,
}

var RuxPlaybackTypes = map[string]bool{"hls": true, "mp4": true, "audio": true, "unknown": true}

var RuxFailureClasses = map[string]bool{
	"media_error": true, "network": true, "autoplay_blocked": true, "timeout": true,
	"decode": true, "not_supported": true, "parse": true, "empty": true, "unknown": true,
}

var RuxBrowserFamilies = map[string]bool{
	"safari": true, "chrome": true, "firefox": true, "edge": true, "samsung": true, "other": true,
}

var RuxDeviceClasses = map[string]bool{"mobile": true, "tablet": true, "desktop": true}

var RuxNetworkClasses = map[string]bool{
	"slow-2g": true, "2g": true, "3g": true, "4g": true, "unknown": true,
}

// Measurement bounds enforced server-side. duration/stall in ms.
const (
	RuxMaxDurationMS = 600000 // 10 min — anything larger is a clock/measurement bug
	RuxMaxStallMS    = 600000
	RuxMinErrorCode  = 1
	RuxMaxErrorCode  = 4 // HTMLMediaElement.error.code range
)

// Incident / verdict vocabulary (shared family language).
const (
	RuxVerdictHealthy          = "healthy"
	RuxVerdictWatching         = "watching"
	RuxVerdictDegraded         = "degraded"
	RuxVerdictCritical         = "critical"
	RuxVerdictInsufficient     = "insufficient_data"
	RuxVerdictTelemetryDegrade = "telemetry_degraded"

	RuxIncidentOpen       = "open"
	RuxIncidentRecovering = "recovering"
	RuxIncidentResolved   = "resolved"
	RuxIncidentClosed     = "closed"

	RuxSeverityWatching = "watching"
	RuxSeverityDegraded = "degraded"
	RuxSeverityCritical = "critical"

	RuxRunRunning   = "running"
	RuxRunCompleted = "completed"
	RuxRunPartial   = "partial"
	RuxRunFailed    = "failed"
)

// ── experience_events (raw, short-lived) ─────────────────────────────────────

type ExperienceEvent struct {
	ID            uint       `gorm:"primaryKey" json:"-"`
	EventID       uuid.UUID  `gorm:"type:uuid;not null;uniqueIndex:idx_experience_events_event_id" json:"event_id"`
	TenantID      string     `gorm:"type:varchar(64);not null;default:default" json:"tenant_id"`
	SchemaVersion int        `gorm:"not null;default:1" json:"schema_version"`
	EventType     string     `gorm:"type:varchar(40);not null" json:"event_type"`
	Surface       string     `gorm:"type:varchar(16);not null" json:"surface"`
	OccurredAt    time.Time  `gorm:"not null" json:"occurred_at"`
	ReceivedAt    time.Time  `gorm:"not null;autoCreateTime" json:"received_at"`
	SessionID     string     `gorm:"type:varchar(64);not null" json:"session_id"`
	PageLoadID    uuid.UUID  `gorm:"type:uuid;not null" json:"page_load_id"`
	Sequence      int        `gorm:"not null;default:0" json:"sequence"`
	JourneyID     *uuid.UUID `gorm:"type:uuid" json:"journey_id,omitempty"`
	Release       string     `gorm:"type:varchar(80);not null" json:"release"`
	ContentID     *uuid.UUID `gorm:"type:uuid" json:"content_id,omitempty"`
	StoryID       *uuid.UUID `gorm:"type:uuid" json:"story_id,omitempty"`
	PlaybackType  *string    `gorm:"type:varchar(16)" json:"playback_type,omitempty"`
	Locale        *string    `gorm:"type:varchar(16)" json:"locale,omitempty"`

	BrowserFamily string `gorm:"type:varchar(16);not null;default:other" json:"browser_family"`
	BrowserMajor  int    `gorm:"not null;default:0" json:"browser_major"`
	DeviceClass   string `gorm:"type:varchar(16);not null;default:mobile" json:"device_class"`
	NetworkClass  string `gorm:"type:varchar(16);not null;default:unknown" json:"network_class"`
	InstalledPWA  bool   `gorm:"column:installed_pwa;not null;default:false" json:"installed_pwa"`

	DurationMS      *int    `gorm:"column:duration_ms" json:"duration_ms,omitempty"`
	MediaErrorCode  *int    `gorm:"column:media_error_code" json:"media_error_code,omitempty"`
	StallDurationMS *int    `gorm:"column:stall_duration_ms" json:"stall_duration_ms,omitempty"`
	FailureClass    *string `gorm:"type:varchar(24)" json:"failure_class,omitempty"`
	Visible         *bool   `gorm:"column:visible" json:"visible,omitempty"`

	Measurements datatypes.JSON `gorm:"type:jsonb" json:"measurements,omitempty"`
	CreatedAt    time.Time      `gorm:"autoCreateTime" json:"-"`
}

func (ExperienceEvent) TableName() string { return "experience_events" }

// ── experience_metric_rollups ────────────────────────────────────────────────

type ExperienceMetricRollup struct {
	ID             uint           `gorm:"primaryKey" json:"-"`
	TenantID       string         `gorm:"type:varchar(64);not null;default:default;uniqueIndex:idx_experience_rollups_identity,priority:1" json:"tenant_id"`
	BucketStart    time.Time      `gorm:"not null;uniqueIndex:idx_experience_rollups_identity,priority:2" json:"bucket_start"`
	Resolution     string         `gorm:"type:varchar(8);not null;default:hour;uniqueIndex:idx_experience_rollups_identity,priority:3" json:"resolution"`
	MetricKey      string         `gorm:"type:varchar(64);not null;uniqueIndex:idx_experience_rollups_identity,priority:4" json:"metric_key"`
	Surface        string         `gorm:"type:varchar(16);not null;uniqueIndex:idx_experience_rollups_identity,priority:5" json:"surface"`
	CohortDim      string         `gorm:"type:varchar(24);not null;default:global;uniqueIndex:idx_experience_rollups_identity,priority:6" json:"cohort_dim"`
	CohortVal      string         `gorm:"type:varchar(80);not null;default:all;uniqueIndex:idx_experience_rollups_identity,priority:7" json:"cohort_val"`
	Numerator      int64          `gorm:"not null;default:0" json:"numerator"`
	Denominator    int64          `gorm:"not null;default:0" json:"denominator"`
	SampleCount    int64          `gorm:"not null;default:0" json:"sample_count"`
	LatencySum     int64          `gorm:"not null;default:0" json:"latency_sum"`
	LatencyBuckets datatypes.JSON `gorm:"type:jsonb" json:"latency_buckets,omitempty"`
	CreatedAt      time.Time      `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt      time.Time      `gorm:"autoUpdateTime" json:"updated_at"`
}

func (ExperienceMetricRollup) TableName() string { return "experience_metric_rollups" }

// ── experience_policies (singleton per tenant) ───────────────────────────────

type ExperiencePolicy struct {
	ID                         uint           `gorm:"primaryKey" json:"-"`
	TenantID                   string         `gorm:"type:varchar(64);not null;uniqueIndex:idx_experience_policies_tenant" json:"tenant_id"`
	IngestEnabled              bool           `gorm:"not null;default:true" json:"ingest_enabled"`
	EvaluationEnabled          bool           `gorm:"not null;default:false" json:"evaluation_enabled"`
	EnabledSurfaces            string         `gorm:"type:varchar(64);not null;default:foryou,news" json:"enabled_surfaces"`
	MinSampleFloor             int            `gorm:"not null;default:50" json:"min_sample_floor"`
	ConfirmWindows             int            `gorm:"not null;default:2" json:"confirm_windows"`
	ResolveWindows             int            `gorm:"not null;default:3" json:"resolve_windows"`
	TelemetryFreshnessMinutes  int            `gorm:"not null;default:15" json:"telemetry_freshness_minutes"`
	RollupMaxBucketsPerPass    int            `gorm:"not null;default:180" json:"rollup_max_buckets_per_pass"`
	RawRetentionDays           int            `gorm:"not null;default:7" json:"raw_retention_days"`
	MinuteRollupRetentionHours int            `gorm:"not null;default:48" json:"minute_rollup_retention_hours"`
	HourRollupRetentionDays    int            `gorm:"not null;default:400" json:"hour_rollup_retention_days"`
	MaxReleaseCohorts          int            `gorm:"not null;default:4" json:"max_release_cohorts"`
	Thresholds                 datatypes.JSON `gorm:"type:jsonb" json:"thresholds,omitempty"`
	PausedUntil                *time.Time     `gorm:"type:timestamp" json:"paused_until,omitempty"`
	LastEvaluatedBucket        *time.Time     `gorm:"type:timestamp" json:"last_evaluated_bucket,omitempty"`
	CreatedAt                  time.Time      `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt                  time.Time      `gorm:"autoUpdateTime" json:"updated_at"`
}

func (ExperiencePolicy) TableName() string { return "experience_policies" }

func DefaultExperiencePolicy(tenantID string) ExperiencePolicy {
	return ExperiencePolicy{
		TenantID: tenantID, IngestEnabled: true, EvaluationEnabled: false,
		EnabledSurfaces: "foryou,news", MinSampleFloor: 50, ConfirmWindows: 2, ResolveWindows: 3,
		TelemetryFreshnessMinutes: 15, RollupMaxBucketsPerPass: 180, RawRetentionDays: 7,
		MinuteRollupRetentionHours: 48, HourRollupRetentionDays: 400, MaxReleaseCohorts: 4,
	}
}

// ── experience_evaluation_runs ───────────────────────────────────────────────

type ExperienceEvaluationRun struct {
	ID               uint           `gorm:"primaryKey" json:"-"`
	PublicID         uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID         string         `gorm:"type:varchar(64);not null;default:default" json:"tenant_id"`
	Trigger          string         `gorm:"type:varchar(24);not null" json:"trigger"`
	Status           string         `gorm:"type:varchar(24);not null" json:"status"`
	WindowStart      *time.Time     `gorm:"type:timestamp" json:"window_start,omitempty"`
	WindowEnd        *time.Time     `gorm:"type:timestamp" json:"window_end,omitempty"`
	TelemetryFresh   bool           `gorm:"not null;default:true" json:"telemetry_fresh"`
	SurfaceVerdicts  datatypes.JSON `gorm:"type:jsonb" json:"surface_verdicts,omitempty"`
	Summary          string         `gorm:"type:text" json:"summary,omitempty"`
	BucketsProcessed int            `gorm:"not null;default:0" json:"buckets_processed"`
	StartedAt        time.Time      `gorm:"not null" json:"started_at"`
	FinishedAt       *time.Time     `gorm:"type:timestamp" json:"finished_at,omitempty"`
	Error            string         `gorm:"type:text" json:"error,omitempty"`
	ErrorClass       string         `gorm:"type:varchar(48);not null;default:none" json:"error_class"`
	CreatedAt        time.Time      `gorm:"autoCreateTime" json:"created_at"`
}

func (ExperienceEvaluationRun) TableName() string { return "experience_evaluation_runs" }

// ── experience_incidents ─────────────────────────────────────────────────────

type ExperienceIncident struct {
	ID               uint           `gorm:"primaryKey" json:"-"`
	PublicID         uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID         string         `gorm:"type:varchar(64);not null;default:default" json:"tenant_id"`
	Fingerprint      string         `gorm:"type:varchar(200);not null" json:"fingerprint"`
	MetricKey        string         `gorm:"type:varchar(64);not null" json:"metric_key"`
	Surface          string         `gorm:"type:varchar(16);not null" json:"surface"`
	CohortDim        string         `gorm:"type:varchar(24);not null;default:global" json:"cohort_dim"`
	CohortVal        string         `gorm:"type:varchar(80);not null;default:all" json:"cohort_val"`
	Severity         string         `gorm:"type:varchar(16);not null" json:"severity"`
	Status           string         `gorm:"type:varchar(24);not null" json:"status"`
	Summary          string         `gorm:"type:text" json:"summary,omitempty"`
	Evidence         datatypes.JSON `gorm:"type:jsonb" json:"evidence,omitempty"`
	Recommendation   string         `gorm:"type:text" json:"recommendation,omitempty"`
	LikelyOwner      string         `gorm:"type:varchar(48)" json:"likely_owner,omitempty"`
	ViolationStreak  int            `gorm:"not null;default:0" json:"violation_streak"`
	CleanStreak      int            `gorm:"not null;default:0" json:"clean_streak"`
	FirstSeenAt      time.Time      `gorm:"not null" json:"first_seen_at"`
	LastSeenAt       time.Time      `gorm:"not null" json:"last_seen_at"`
	RecoveringSince  *time.Time     `gorm:"type:timestamp" json:"recovering_since,omitempty"`
	ResolvedAt       *time.Time     `gorm:"type:timestamp" json:"resolved_at,omitempty"`
	ClosedBy         string         `gorm:"type:varchar(255)" json:"closed_by,omitempty"`
	CloseReasonClass string         `gorm:"type:varchar(32)" json:"close_reason_class,omitempty"`
	CloseNotes       string         `gorm:"type:text" json:"close_notes,omitempty"`
	CreatedAt        time.Time      `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt        time.Time      `gorm:"autoUpdateTime" json:"updated_at"`
}

func (ExperienceIncident) TableName() string { return "experience_incidents" }

// ── experience_actions (ledger) ──────────────────────────────────────────────

type ExperienceAction struct {
	ID          uint           `gorm:"primaryKey" json:"-"`
	PublicID    uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID    string         `gorm:"type:varchar(64);not null;default:default" json:"tenant_id"`
	RunID       *uint          `json:"run_id,omitempty"`
	IncidentID  *uint          `json:"incident_id,omitempty"`
	ActionClass string         `gorm:"type:varchar(48);not null" json:"action_class"`
	Label       string         `gorm:"type:text;not null" json:"label"`
	MetricKey   string         `gorm:"type:varchar(64)" json:"metric_key,omitempty"`
	Surface     string         `gorm:"type:varchar(16)" json:"surface,omitempty"`
	CohortDim   string         `gorm:"type:varchar(24)" json:"cohort_dim,omitempty"`
	CohortVal   string         `gorm:"type:varchar(80)" json:"cohort_val,omitempty"`
	Guardrail   string         `gorm:"type:varchar(64)" json:"guardrail,omitempty"`
	Evidence    datatypes.JSON `gorm:"type:jsonb" json:"evidence,omitempty"`
	CreatedAt   time.Time      `gorm:"autoCreateTime" json:"created_at"`
}

func (ExperienceAction) TableName() string { return "experience_actions" }

// ── experience_suppressions ──────────────────────────────────────────────────

type ExperienceSuppression struct {
	ID        uint       `gorm:"primaryKey" json:"-"`
	PublicID  uuid.UUID  `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID  string     `gorm:"type:varchar(64);not null;default:default" json:"tenant_id"`
	MetricKey string     `gorm:"type:varchar(64)" json:"metric_key,omitempty"`
	Surface   string     `gorm:"type:varchar(16)" json:"surface,omitempty"`
	CohortDim string     `gorm:"type:varchar(24)" json:"cohort_dim,omitempty"`
	CohortVal string     `gorm:"type:varchar(80)" json:"cohort_val,omitempty"`
	Reason    string     `gorm:"type:text;not null" json:"reason"`
	StartsAt  time.Time  `gorm:"not null" json:"starts_at"`
	ExpiresAt time.Time  `gorm:"not null" json:"expires_at"`
	CreatedBy string     `gorm:"type:varchar(255)" json:"created_by,omitempty"`
	RevokedAt *time.Time `gorm:"type:timestamp" json:"revoked_at,omitempty"`
	RevokedBy string     `gorm:"type:varchar(255)" json:"revoked_by,omitempty"`
	CreatedAt time.Time  `gorm:"autoCreateTime" json:"created_at"`
}

func (ExperienceSuppression) TableName() string { return "experience_suppressions" }
