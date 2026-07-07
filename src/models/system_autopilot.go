package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

const (
	SystemAutopilotModeObserve  = "observe"
	SystemAutopilotModeSafeAuto = "safe_auto"

	SystemAutopilotRunStatusRunning   = "running"
	SystemAutopilotRunStatusCompleted = "completed"
	SystemAutopilotRunStatusPartial   = "partial"
	SystemAutopilotRunStatusFailed    = "failed"

	SystemAutopilotErrorClassNone               = "none"
	SystemAutopilotErrorClassEpisodePersistence = "episode_persistence"

	SystemAutopilotHeadlineAllClear     = "all_clear"
	SystemAutopilotHeadlineWatching     = "watching"
	SystemAutopilotHeadlineIncidentOpen = "incident_open"
	SystemAutopilotHeadlineContained    = "contained"
	SystemAutopilotHeadlineRecovering   = "recovering"

	SystemIncidentStatusOpen          = "open"
	SystemIncidentStatusRecovering    = "recovering"
	SystemIncidentStatusResolved      = "resolved"
	SystemIncidentStatusClosedByHuman = "closed_by_human"

	SystemVerdictHealthy               = "healthy"
	SystemVerdictTransientProbeFailure = "transient_probe_failure"
	SystemVerdictServiceDown           = "service_down"
	SystemVerdictDependencyDown        = "dependency_down"
	SystemVerdictQueueBacklog          = "queue_backlog"
	SystemVerdictModelUnloaded         = "model_unloaded"
	SystemVerdictWorkerStalled         = "worker_stalled"
	SystemVerdictMultiServiceIncident  = "multi_service_incident"

	SystemAutopilotActionOpenEpisode    = "open_episode"
	SystemAutopilotActionUpdateEpisode  = "update_episode"
	SystemAutopilotActionResolveEpisode = "resolve_episode"
	SystemAutopilotActionCloseEpisode   = "close_episode"
	SystemAutopilotActionPauseSibling   = "pause_autopilot"
	SystemAutopilotActionResumeSibling  = "resume_autopilot"
	SystemAutopilotActionWouldPause     = "would_pause"
	SystemAutopilotActionWouldResume    = "would_resume"
	SystemAutopilotActionSkipped        = "skipped"

	SystemAutopilotGuardUnconfirmed            = "unconfirmed"
	SystemAutopilotGuardFlapping               = "flapping"
	SystemAutopilotGuardDegradedNoContainment  = "degraded_no_containment"
	SystemAutopilotGuardContainmentTTL         = "containment_ttl"
	SystemAutopilotGuardScopeCap               = "scope_cap"
	SystemAutopilotGuardOptedOut               = "opted_out"
	SystemAutopilotGuardHumanPause             = "human_pause"
	SystemAutopilotGuardObserveMode            = "observe_mode"
	SystemAutopilotGuardPaused                 = "paused"
	SystemAutopilotGuardQueueBacklogNoIncident = "queue_backlog_no_incident"
)

type SystemAutopilotPolicy struct {
	ID    uint   `gorm:"primaryKey" json:"-"`
	Scope string `gorm:"type:varchar(32);not null;uniqueIndex:idx_system_autopilot_policy_scope;default:'platform'" json:"scope"`

	Enabled bool   `gorm:"not null;default:false" json:"enabled"`
	Mode    string `gorm:"type:varchar(24);not null;default:'observe'" json:"mode"`

	IntervalMinutes       int `gorm:"type:integer;not null;default:10" json:"interval_minutes"`
	ConfirmProbes         int `gorm:"type:integer;not null;default:2" json:"confirm_probes"`
	ResolveProbes         int `gorm:"type:integer;not null;default:3" json:"resolve_probes"`
	FlapCycles24h         int `gorm:"type:integer;not null;default:3" json:"flap_cycles_24h"`
	ContainmentTTLMinutes int `gorm:"type:integer;not null;default:60" json:"containment_ttl_minutes"`

	ContainmentDisabledFor datatypes.JSON `gorm:"type:jsonb" json:"containment_disabled_for,omitempty"`
	ContainmentPausedUntil *time.Time     `gorm:"type:timestamp" json:"containment_paused_until,omitempty"`
	LastRunAt              *time.Time     `gorm:"type:timestamp" json:"last_run_at,omitempty"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (SystemAutopilotPolicy) TableName() string {
	return "system_autopilot_policies"
}

func DefaultSystemAutopilotPolicy() SystemAutopilotPolicy {
	return SystemAutopilotPolicy{
		Scope:                  "platform",
		Enabled:                false,
		Mode:                   SystemAutopilotModeObserve,
		IntervalMinutes:        10,
		ConfirmProbes:          2,
		ResolveProbes:          3,
		FlapCycles24h:          3,
		ContainmentTTLMinutes:  60,
		ContainmentDisabledFor: datatypes.JSON([]byte(`["news_circulation","media_circulation","media_studio"]`)),
	}
}

type SystemIncidentEpisode struct {
	ID       uint      `gorm:"primaryKey" json:"-"`
	PublicID uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex:idx_system_incident_episodes_public_id" json:"id"`

	RootService     string         `gorm:"type:varchar(48);not null;index:idx_system_incident_episodes_root_service" json:"root_service"`
	Verdict         string         `gorm:"type:varchar(48);not null;index:idx_system_incident_episodes_verdict" json:"verdict"`
	Status          string         `gorm:"type:varchar(32);not null;index:idx_system_incident_episodes_status" json:"status"`
	Severity        string         `gorm:"type:varchar(24);not null;default:'warning'" json:"severity"`
	Shadow          bool           `gorm:"not null;default:false;index:idx_system_incident_episodes_shadow" json:"shadow"`
	Summary         string         `gorm:"type:text" json:"summary"`
	RootCauseHint   string         `gorm:"type:text" json:"root_cause_hint,omitempty"`
	Evidence        datatypes.JSON `gorm:"type:jsonb" json:"evidence,omitempty"`
	Timeline        datatypes.JSON `gorm:"type:jsonb" json:"timeline,omitempty"`
	Containment     datatypes.JSON `gorm:"type:jsonb" json:"containment,omitempty"`
	FirstDetectedAt time.Time      `gorm:"type:timestamp;not null;index:idx_system_incident_episodes_first_detected" json:"first_detected_at"`
	LastSeenAt      time.Time      `gorm:"type:timestamp;not null;index:idx_system_incident_episodes_last_seen" json:"last_seen_at"`
	RecoveringSince *time.Time     `gorm:"type:timestamp" json:"recovering_since,omitempty"`
	ResolvedAt      *time.Time     `gorm:"type:timestamp" json:"resolved_at,omitempty"`
	ClosedBy        string         `gorm:"type:varchar(255)" json:"closed_by,omitempty"`
	CloseReason     string         `gorm:"type:text" json:"close_reason,omitempty"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (SystemIncidentEpisode) TableName() string {
	return "system_incident_episodes"
}

type SystemAutopilotRun struct {
	ID       uint      `gorm:"primaryKey" json:"-"`
	PublicID uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex:idx_system_autopilot_runs_public_id" json:"id"`

	Trigger  string `gorm:"type:varchar(24);not null" json:"trigger"`
	Mode     string `gorm:"type:varchar(24);not null" json:"mode"`
	Status   string `gorm:"type:varchar(24);not null;index:idx_system_autopilot_runs_status" json:"status"`
	Headline string `gorm:"type:varchar(32);not null" json:"headline"`

	StartedAt    time.Time      `gorm:"type:timestamp;not null;index:idx_system_autopilot_runs_started_at" json:"started_at"`
	FinishedAt   *time.Time     `gorm:"type:timestamp" json:"finished_at,omitempty"`
	Summary      string         `gorm:"type:text" json:"summary,omitempty"`
	ProbeResults datatypes.JSON `gorm:"type:jsonb" json:"probe_results,omitempty"`
	CreatedBy    string         `gorm:"type:varchar(255)" json:"created_by,omitempty"`
	Error        string         `gorm:"type:text" json:"error,omitempty"`
	ErrorClass   string         `gorm:"type:varchar(48);not null;default:'none';index:idx_system_autopilot_runs_error_class" json:"error_class"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (SystemAutopilotRun) TableName() string {
	return "system_autopilot_runs"
}

type SystemAutopilotAction struct {
	ID        uint           `gorm:"primaryKey" json:"-"`
	PublicID  uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex:idx_system_autopilot_actions_public_id" json:"id"`
	RunID     uint           `gorm:"not null;index:idx_system_autopilot_actions_run_id" json:"-"`
	EpisodeID *uint          `gorm:"index:idx_system_autopilot_actions_episode_id" json:"-"`
	Target    string         `gorm:"type:varchar(64);not null;index:idx_system_autopilot_actions_target" json:"target"`
	Action    string         `gorm:"type:varchar(48);not null;index:idx_system_autopilot_actions_action" json:"action"`
	Verdict   string         `gorm:"type:varchar(48)" json:"verdict,omitempty"`
	Status    string         `gorm:"type:varchar(32);not null;index:idx_system_autopilot_actions_status" json:"status"`
	Guardrail string         `gorm:"type:varchar(64)" json:"guardrail,omitempty"`
	Reason    string         `gorm:"type:text" json:"reason,omitempty"`
	Output    datatypes.JSON `gorm:"type:jsonb" json:"output,omitempty"`

	StartedAt  time.Time  `gorm:"type:timestamp;not null" json:"started_at"`
	FinishedAt *time.Time `gorm:"type:timestamp" json:"finished_at,omitempty"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (SystemAutopilotAction) TableName() string {
	return "system_autopilot_actions"
}
