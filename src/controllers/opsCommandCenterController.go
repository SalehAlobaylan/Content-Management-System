package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"net/http"
	"time"
)

const (
	opsCommandMaxTTLMinutes = 24 * 60
	opsCommandMaxTargets    = 200
	opsAttentionRetention   = 90 * 24 * time.Hour
)

type opsFleetLane struct {
	Key           string
	Label         string
	Table         string
	TenantColumn  string
	PauseColumn   string
	StatusSQL     string
	Scheduled     bool
	ManualOnly    bool
	CockpitPath   string
	RunStart      string
	RunTable      string
	RunFinished   string
	RunState      string
	RunHeadline   string
	RunWhere      string
	ActionTable   string
	ActionOutcome string
	ActionKind    string
	Grace         time.Duration
	MaxRun        time.Duration
}

type opsFleetMember struct {
	Key          string
	Label        string
	Family       string
	Kind         string
	TrustCapable bool
	CockpitPath  string
	Lanes        []opsFleetLane
}

// All identifiers below are static code constants. Never derive a table or
// column name from a request parameter.
var opsFleetRegistry = []opsFleetMember{
	{Key: "pods_pipeline", Label: "Pods pipeline", Family: "media", Kind: "pipeline", CockpitPath: "/platform/media/sources#pods-pipeline", Lanes: []opsFleetLane{{Key: "processing", Label: "Processing", CockpitPath: "/platform/media/sources#pods-pipeline", StatusSQL: opsPodsPipelineSQL}}},
	// The governor is visible here but deliberately not pausable through the
	// fleet switchboard: pausing it bypasses protection rather than stopping work.
	{Key: "workload_governor", Label: "Workload & Capacity", Family: "platform", Kind: "governor", CockpitPath: "/platform/workload", Lanes: []opsFleetLane{{Key: "capacity", Label: "Capacity observation", Table: "workload_governor_policies", TenantColumn: "scope", Scheduled: true, CockpitPath: "/platform/workload", StatusSQL: "SELECT scope AS tenant_id, (mode <> 'off') AS enabled, mode, paused_until, (SELECT max(created_at) FROM workload_capacity_snapshots) AS last_run_at, 1 AS interval_minutes, updated_at AS created_at FROM workload_governor_policies WHERE scope='platform'"}}},
	{Key: "system_health", Label: "System Health", Family: "platform", Kind: "autopilot", CockpitPath: "/platform/system-health", Lanes: []opsFleetLane{{RunTable: "system_autopilot_runs", RunFinished: "finished_at", RunState: "status", RunHeadline: "headline", ActionTable: "system_autopilot_actions", ActionOutcome: "status", ActionKind: "action", Key: "containment", Label: "Containment", Table: "system_autopilot_policies", TenantColumn: "scope", PauseColumn: "containment_paused_until", Scheduled: true, CockpitPath: "/platform/system-health", StatusSQL: "SELECT scope AS tenant_id, enabled, mode, containment_paused_until AS paused_until, last_run_at, interval_minutes, created_at FROM system_autopilot_policies"}}},
	{Key: "feed_integrity", TrustCapable: true, Label: "Feed Integrity", Family: "edge", Kind: "engine", CockpitPath: "/platform/feed-integrity", Lanes: []opsFleetLane{
		{RunTable: "feed_integrity_runs", RunFinished: "finished_at", RunState: "status", RunHeadline: "headline", ActionTable: "feed_integrity_actions", ActionOutcome: "outcome", ActionKind: "action_class", Key: "checks", Label: "Checks", Table: "feed_integrity_policies", TenantColumn: "tenant_id", PauseColumn: "paused_until", Scheduled: true, CockpitPath: "/platform/feed-integrity", StatusSQL: "SELECT tenant_id, scheduled_enabled AS enabled, 'observe' AS mode, paused_until, last_light_run_at AS last_run_at, last_deep_run_at AS deep_last_run_at, deep_interval_hours, light_interval_minutes AS interval_minutes, created_at FROM feed_integrity_policies"},
		{RunStart: "COALESCE(claimed_at,created_at)", RunTable: "feed_integrity_actions", RunFinished: "finished_at", RunState: "outcome", RunHeadline: "reason", RunWhere: "outcome IN ('claimed','running','verifying') OR finished_at IS NOT NULL", Key: "autopilot", Label: "Autopilot", Table: "feed_integrity_policies", TenantColumn: "tenant_id", PauseColumn: "autopilot_paused_until", ManualOnly: true, CockpitPath: "/platform/feed-integrity", StatusSQL: "SELECT tenant_id, autopilot_enabled AS enabled, CASE WHEN autopilot_mode='assist' THEN 'assist' ELSE 'observe' END AS mode, autopilot_paused_until AS paused_until, COALESCE(last_light_run_at, last_deep_run_at) AS last_run_at, 0 AS interval_minutes, created_at FROM feed_integrity_policies"},
	}},
	{Key: "retention", Label: "Retention", Family: "platform", Kind: "observatory", CockpitPath: "/platform/retention", Lanes: []opsFleetLane{{RunTable: "retention_runs", RunFinished: "finished_at", RunState: "status", RunHeadline: "verdict", ActionTable: "retention_actions", ActionOutcome: "outcome", ActionKind: "action_class", Key: "database", Label: "Database custody", Table: "retention_policies", TenantColumn: "tenant_id", PauseColumn: "paused_until", Scheduled: true, CockpitPath: "/platform/retention", StatusSQL: "SELECT tenant_id, enabled, CASE WHEN mode = 'assist' THEN 'assist' ELSE 'observe' END AS mode, paused_until, last_run_at, schedule_interval_minutes AS interval_minutes, created_at FROM retention_policies"}}},
	// Experience evaluation has no interval column: its sweep is a fixed hourly
	// tick in experienceScheduler.go, so 60 below mirrors that code constant.
	{Key: "experience", Label: "Real Experience", Family: "edge", Kind: "observatory", CockpitPath: "/platform/real-experience", Lanes: []opsFleetLane{{RunTable: "experience_evaluation_runs", RunFinished: "finished_at", RunState: "status", RunHeadline: "summary", ActionTable: "experience_actions", ActionOutcome: "action_class", ActionKind: "action_class", Key: "evaluation", Label: "Evaluation", Table: "experience_policies", TenantColumn: "tenant_id", PauseColumn: "paused_until", Scheduled: true, CockpitPath: "/platform/real-experience", StatusSQL: "SELECT p.tenant_id, p.evaluation_enabled AS enabled, 'observe' AS mode, p.paused_until, p.last_evaluated_bucket AS last_run_at, 60 AS interval_minutes, p.created_at FROM experience_policies p"}}},
	{Key: "pipeline", TrustCapable: true, Label: "Pipeline Repair", Family: "content", Kind: "autopilot", CockpitPath: "/platform/pipeline", Lanes: []opsFleetLane{{RunTable: "pipeline_autopilot_runs", RunFinished: "finished_at", RunState: "status", RunHeadline: "headline", ActionTable: "pipeline_autopilot_actions", ActionOutcome: "status", ActionKind: "lane", Key: "autopilot", Label: "Autopilot", Table: "pipeline_autopilot_policies", TenantColumn: "tenant_id", PauseColumn: "paused_until", Scheduled: true, CockpitPath: "/platform/pipeline", StatusSQL: "SELECT tenant_id, enabled, 'observe' AS mode, paused_until, last_run_at, interval_minutes, created_at FROM pipeline_autopilot_policies"}}},
	{Key: "enrichment", TrustCapable: true, Label: "Enrichment", Family: "content", Kind: "autopilot", CockpitPath: "/platform/enrichment", Lanes: []opsFleetLane{{RunTable: "enrichment_autopilot_runs", RunFinished: "finished_at", RunState: "status", RunHeadline: "headline", ActionTable: "enrichment_autopilot_actions", ActionOutcome: "status", ActionKind: "artifact", Key: "autopilot", Label: "Autopilot", Table: "enrichment_autopilot_policies", TenantColumn: "tenant_id", PauseColumn: "paused_until", Scheduled: true, CockpitPath: "/platform/enrichment", StatusSQL: "SELECT tenant_id, enabled, mode, paused_until, last_run_at, interval_minutes, created_at FROM enrichment_autopilot_policies"}}},
	{Key: "media_circulation", TrustCapable: true, Label: "Media Circulation", Family: "media", Kind: "autopilot", CockpitPath: "/platform/media/circulation", Lanes: []opsFleetLane{{RunTable: "media_circulation_runs", RunFinished: "finished_at", RunState: "status", RunHeadline: "summary", ActionTable: "media_circulation_actions", ActionOutcome: "status", ActionKind: "tool_name", Key: "autopilot", Label: "Autopilot", Table: "media_circulation_policies", TenantColumn: "tenant_id", PauseColumn: "autopilot_paused_until", Scheduled: true, CockpitPath: "/platform/media/circulation", StatusSQL: "SELECT tenant_id, autopilot_enabled AS enabled, autopilot_mode AS mode, autopilot_paused_until AS paused_until, autopilot_last_run_at AS last_run_at, autopilot_interval_minutes AS interval_minutes, created_at FROM media_circulation_policies"}}},
	{Key: "media_studio", TrustCapable: true, Label: "Media Studio", Family: "media", Kind: "autopilot", CockpitPath: "/platform/media/atomization?tab=autopilot", Lanes: []opsFleetLane{{RunTable: "media_studio_runs", RunFinished: "finished_at", RunState: "status", RunHeadline: "summary", ActionTable: "media_studio_actions", ActionOutcome: "status", ActionKind: "tool_name", Key: "autopilot", Label: "Autopilot", Table: "media_studio_autopilot_policies", TenantColumn: "tenant_id", PauseColumn: "paused_until", Scheduled: true, CockpitPath: "/platform/media/atomization?tab=autopilot", StatusSQL: "SELECT tenant_id, autopilot_enabled AS enabled, autopilot_mode AS mode, paused_until, last_run_at, interval_minutes, created_at FROM media_studio_autopilot_policies"}}},
	{Key: "redundancy", Label: "Redundancy Hygiene", Family: "media", Kind: "autopilot", CockpitPath: "/platform/media/redundancy", Lanes: []opsFleetLane{{RunTable: "redundancy_runs", RunFinished: "finished_at", RunState: "status", RunHeadline: "summary", ActionTable: "redundancy_actions", ActionOutcome: "outcome", ActionKind: "action_kind", Key: "autopilot", Label: "Autopilot", Table: "redundancy_policies", TenantColumn: "tenant_id", PauseColumn: "paused_until", Scheduled: true, CockpitPath: "/platform/media/redundancy", StatusSQL: "SELECT tenant_id, enabled, 'observe' AS mode, paused_until, last_swept_at AS last_run_at, sweep_interval_minutes AS interval_minutes, created_at FROM redundancy_policies"}}},
	{Key: "news_circulation", Label: "News Circulation", Family: "news", Kind: "autopilot", CockpitPath: "/platform/news/circulation", Lanes: []opsFleetLane{{RunTable: "news_autopilot_runs", RunFinished: "finished_at", RunState: "status", RunHeadline: "summary", ActionTable: "news_autopilot_actions", ActionOutcome: "status", ActionKind: "tool_name", Key: "autopilot", Label: "Autopilot", Table: "news_circulation_policies", TenantColumn: "tenant_id", PauseColumn: "autopilot_paused_until", Scheduled: true, CockpitPath: "/platform/news/circulation", StatusSQL: "SELECT tenant_id, autopilot_enabled AS enabled, autopilot_mode AS mode, autopilot_paused_until AS paused_until, autopilot_last_run_at AS last_run_at, autopilot_interval_minutes AS interval_minutes, created_at FROM news_circulation_policies"}}},
	{Key: "preferences", TrustCapable: true, Label: "Preferences", Family: "content", Kind: "autopilot", CockpitPath: "/platform/topics?tab=autopilot", Lanes: []opsFleetLane{{RunTable: "preference_autopilot_runs", RunFinished: "finished_at", RunState: "status", RunHeadline: "headline", ActionTable: "preference_autopilot_actions", ActionOutcome: "status", ActionKind: "action_class", Key: "autopilot", Label: "Autopilot", Table: "preference_autopilot_policies", TenantColumn: "tenant_id", PauseColumn: "paused_until", Scheduled: true, CockpitPath: "/platform/topics?tab=autopilot", StatusSQL: "SELECT tenant_id, enabled, mode, paused_until, last_run_at, interval_minutes, created_at FROM preference_autopilot_policies"}}},
	{Key: "embedding_lifecycle", Label: "Embedding Lifecycle", Family: "platform", Kind: "engine", CockpitPath: "/platform/intelligence/embeddings", Lanes: []opsFleetLane{
		{RunTable: "embedding_lifecycle_runs", RunFinished: "completed_at", RunState: "status", RunHeadline: "headline", ActionTable: "embedding_campaign_actions", ActionOutcome: "status", ActionKind: "tool", Key: "audit", Label: "Audit", Table: "embedding_lifecycle_policies", TenantColumn: "tenant_id", Scheduled: true, CockpitPath: "/platform/intelligence/embeddings", StatusSQL: "SELECT tenant_id, audit_enabled AS enabled, 'observe' AS mode, NULL::timestamp AS paused_until, last_audit_at AS last_run_at, audit_interval_minutes AS interval_minutes, created_at FROM embedding_lifecycle_policies"},
		{RunTable: "embedding_campaigns", RunFinished: "completed_at", RunState: "state", RunHeadline: "blocked_reason", MaxRun: 0, Key: "campaigns", Label: "Campaigns", Table: "embedding_lifecycle_policies", TenantColumn: "tenant_id", PauseColumn: "campaigns_paused_until", ManualOnly: true, CockpitPath: "/platform/intelligence/embeddings", StatusSQL: "SELECT tenant_id, true AS enabled, 'manual' AS mode, campaigns_paused_until AS paused_until, (SELECT max(updated_at) FROM embedding_campaigns c WHERE c.tenant_id=embedding_lifecycle_policies.tenant_id) AS last_run_at, 0 AS interval_minutes, created_at FROM embedding_lifecycle_policies"},
	}},
	{Key: "ai_spend", Label: "AI Spend", Family: "economics", Kind: "observatory", CockpitPath: "/platform/economics", Lanes: []opsFleetLane{{RunTable: "ai_spend_runs", RunFinished: "completed_at", RunState: "status", RunHeadline: "headline", Key: "governor", Label: "Governor", Table: "ai_spend_policies", TenantColumn: "tenant_id", PauseColumn: "paused_until", Scheduled: true, CockpitPath: "/platform/economics", StatusSQL: "SELECT tenant_id, enabled, 'observe' AS mode, paused_until, last_run_at, aggregation_interval_minutes AS interval_minutes, created_at FROM ai_spend_policies"}}},
	{Key: "ranking_intelligence", Label: "Ranking Intelligence", Family: "media", Kind: "engine", CockpitPath: "/platform/intelligence", Lanes: []opsFleetLane{{Key: "refresh", Label: "Refresh", ManualOnly: true, CockpitPath: "/platform/intelligence"}}},
	{Key: "storage", Label: "Storage", Family: "media", Kind: "observatory", CockpitPath: "/platform/storage", Lanes: []opsFleetLane{{Key: "health", Label: "Health", ManualOnly: true, CockpitPath: "/platform/storage"}}},
	{Key: "atomization", Label: "Atomization", Family: "media", Kind: "engine", CockpitPath: "/platform/media/atomization", Lanes: []opsFleetLane{{RunTable: "media_atomization_runs", RunFinished: "completed_at", RunState: "status", RunHeadline: "error_message", Key: "engine", Label: "Engine", ManualOnly: true, CockpitPath: "/platform/media/atomization", StatusSQL: "SELECT tenant_id,true AS enabled, 'manual' AS mode,NULL::timestamp AS paused_until,max(started_at) AS last_run_at,0 AS interval_minutes,min(created_at) AS created_at FROM media_atomization_runs GROUP BY tenant_id"}}},
	{Key: "supply_continuity", Label: "Supply Continuity", Family: "media", Kind: "observatory", CockpitPath: "/platform/media/circulation", Lanes: []opsFleetLane{{Key: "evaluation", Label: "Supply evaluation", Scheduled: true, CockpitPath: "/platform/media/circulation", StatusSQL: "SELECT c.tenant_id, NOT EXISTS(SELECT 1 FROM media_supply_controls s WHERE s.tenant_id=c.tenant_id AND s.control_key='supply_read_evaluation' AND s.scope_type='tenant' AND s.scope_id='all') AS enabled, 'observe' AS mode, NULL::timestamp AS paused_until, last_observed_at AS last_run_at, 5 AS interval_minutes,last_outcome AS owner_outcome, created_at FROM media_supply_evaluation_checkpoints c"}}},
}

type opsStatusRow struct {
	PipelineReady      bool
	PipelineRunning    int64
	PipelineAdmitted   int64
	PipelineVerifying  int64
	PipelineUnresolved int64
	PipelineQueued     int64
	TenantID           string     `gorm:"column:tenant_id" json:"tenant_id"`
	Enabled            bool       `gorm:"column:enabled" json:"enabled"`
	Mode               string     `gorm:"column:mode" json:"mode"`
	PausedUntil        *time.Time `gorm:"column:paused_until" json:"paused_until,omitempty"`
	LastRunAt          *time.Time `gorm:"column:last_run_at" json:"last_run_at,omitempty"`
	IntervalMinutes    int        `gorm:"column:interval_minutes" json:"interval_minutes"`
	CreatedAt          time.Time  `gorm:"column:created_at" json:"created_at"`
	DeepLastRunAt      *time.Time
	DeepIntervalHours  int
	OwnerOutcome       string
	LatestRunID        *string
	LatestRunStatus    string
	LatestRunHeadline  string
	LatestStartedAt    *time.Time
	LatestFinishedAt   *time.Time
}

type opsMemberStatus struct {
	MemberKey   string     `json:"member_key"`
	MemberLabel string     `json:"member_label"`
	Family      string     `json:"family"`
	Kind        string     `json:"kind"`
	LaneKey     string     `json:"lane_key"`
	LaneLabel   string     `json:"lane_label"`
	TenantID    string     `json:"tenant_id,omitempty"`
	State       string     `json:"state"`
	Enabled     bool       `json:"enabled"`
	Pausable    bool       `json:"pausable"`
	Mode        string     `json:"mode,omitempty"`
	PausedUntil *time.Time `json:"paused_until,omitempty"`
	LastRunAt   *time.Time `json:"last_run_at,omitempty"`
	NextDueAt   *time.Time `json:"next_due_at,omitempty"`
	// StalledSince is the deterministic moment the lane crossed its stall
	// threshold (due + grace), so attention age is stable across requests.
	StalledSince       *time.Time      `json:"stalled_since,omitempty"`
	Liveness           string          `json:"liveness"`
	CockpitPath        string          `json:"cockpit_path"`
	Error              string          `json:"error,omitempty"`
	LastRunStatus      string          `json:"last_run_status,omitempty"`
	LastRunHeadline    string          `json:"last_run_headline,omitempty"`
	DeepDueAt          *time.Time      `json:"deep_due_at,omitempty"`
	OpenAttentionCount int             `json:"open_attention_count"`
	Trust              opsTrustSummary `json:"trust"`
	Containment        opsContainment  `json:"containment"`
	Evidence           string          `json:"evidence,omitempty"`
}

type opsAttentionItem struct {
	Key            string     `json:"key"`
	Fingerprint    string     `json:"fingerprint"`
	System         string     `json:"system"`
	TenantID       string     `json:"tenant_id,omitempty"`
	Kind           string     `json:"kind"`
	Severity       string     `json:"severity"`
	Title          string     `json:"title"`
	Detail         string     `json:"detail"`
	Count          int        `json:"count"`
	FirstSeen      time.Time  `json:"first_seen"`
	Href           string     `json:"href"`
	IssueHref      string     `json:"issue_href,omitempty"`
	NativeID       string     `json:"-"`
	State          string     `json:"state,omitempty"`
	Snoozed        bool       `json:"snoozed"`
	SnoozedUntil   *time.Time `json:"snoozed_until,omitempty"`
	ClientPlatform string     `json:"client_platform,omitempty"`
}

func opsFindLane(memberKey, laneKey string) (opsFleetMember, opsFleetLane, bool) {
	for _, member := range opsFleetRegistry {
		if member.Key != memberKey {
			continue
		}
		for _, lane := range member.Lanes {
			if lane.Key == laneKey {
				return member, lane, true
			}
		}
	}
	return opsFleetMember{}, opsFleetLane{}, false
}

func ListOpsCommands(c *gin.Context) {
	principal, ok := utils.GetAdminPrincipal(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return
	}
	db, cancel, _ := opsRequestDB(c)
	defer cancel()
	limit := opsPageInt(c.Query("limit"), 50, 1, 200)
	offset := opsPageInt(c.Query("offset"), 0, 0, 100000)
	var total int64
	var rows []models.OpsFleetCommand
	if err := db.Model(&models.OpsFleetCommand{}).Where("tenant_id=?", principal.TenantID).Count(&total).Error; err != nil {
		c.JSON(503, gin.H{"error": "command history unavailable"})
		return
	}
	if err := db.Where("tenant_id=?", principal.TenantID).Order("created_at DESC,id DESC").Limit(limit).Offset(offset).Find(&rows).Error; err != nil {
		c.JSON(503, gin.H{"error": "command history unavailable"})
		return
	}
	if rows == nil {
		rows = []models.OpsFleetCommand{}
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"items": rows, "total": total, "offset": offset, "limit": limit, "has_more": int64(offset+len(rows)) < total}})
}
func GetOpsCommand(c *gin.Context) {
	principal, ok := utils.GetAdminPrincipal(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid command id"})
		return
	}
	db, cancel, _ := opsRequestDB(c)
	defer cancel()
	var row models.OpsFleetCommand
	if err := db.Where("tenant_id=? AND public_id=?", principal.TenantID, id).First(&row).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			c.JSON(http.StatusNotFound, gin.H{"error": "command not found"})
		} else {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "command evidence unavailable"})
		}
		return
	}
	var actions []models.OpsFleetCommandAction
	if err := db.Where("command_id=?", row.ID).Order("id ASC").Find(&actions).Error; err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "command action evidence unavailable"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"command": row, "actions": actions}})
}
