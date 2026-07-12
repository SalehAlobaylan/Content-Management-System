package controllers

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/utils"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	opsCommandMaxTTLMinutes = 24 * 60
	opsCommandMaxTargets    = 200
	opsAttentionRetention   = 90 * 24 * time.Hour
)

type opsFleetLane struct {
	Key          string
	Label        string
	Table        string
	TenantColumn string
	PauseColumn  string
	StatusSQL    string
	Scheduled    bool
	ManualOnly   bool
	CockpitPath  string
}

type opsFleetMember struct {
	Key         string
	Label       string
	Family      string
	Kind        string
	CockpitPath string
	Lanes       []opsFleetLane
}

// All identifiers below are static code constants. Never derive a table or
// column name from a request parameter.
var opsFleetRegistry = []opsFleetMember{
	{Key: "system_health", Label: "System Health", Family: "platform", Kind: "autopilot", CockpitPath: "/platform/system-health", Lanes: []opsFleetLane{{Key: "containment", Label: "Containment", Table: "system_autopilot_policies", TenantColumn: "scope", PauseColumn: "containment_paused_until", Scheduled: true, CockpitPath: "/platform/system-health", StatusSQL: "SELECT scope AS tenant_id, enabled, mode, containment_paused_until AS paused_until, last_run_at, interval_minutes, created_at FROM system_autopilot_policies"}}},
	{Key: "feed_integrity", Label: "Feed Integrity", Family: "edge", Kind: "engine", CockpitPath: "/platform/feed-integrity", Lanes: []opsFleetLane{
		{Key: "checks", Label: "Checks", Table: "feed_integrity_policies", TenantColumn: "tenant_id", PauseColumn: "paused_until", Scheduled: true, CockpitPath: "/platform/feed-integrity", StatusSQL: "SELECT tenant_id, scheduled_enabled AS enabled, 'observe' AS mode, paused_until, COALESCE(last_light_run_at, last_deep_run_at) AS last_run_at, light_interval_minutes AS interval_minutes, created_at FROM feed_integrity_policies"},
		{Key: "autopilot", Label: "Autopilot", Table: "feed_integrity_policies", TenantColumn: "tenant_id", PauseColumn: "autopilot_paused_until", ManualOnly: true, CockpitPath: "/platform/feed-integrity", StatusSQL: "SELECT tenant_id, autopilot_enabled AS enabled, autopilot_mode AS mode, autopilot_paused_until AS paused_until, COALESCE(last_light_run_at, last_deep_run_at) AS last_run_at, 0 AS interval_minutes, created_at FROM feed_integrity_policies"},
	}},
	// Experience evaluation has no interval column: its sweep is a fixed hourly
	// tick in experienceScheduler.go, so 60 below mirrors that code constant.
	{Key: "experience", Label: "Real Experience", Family: "edge", Kind: "observatory", CockpitPath: "/platform/real-experience", Lanes: []opsFleetLane{{Key: "evaluation", Label: "Evaluation", Table: "experience_policies", TenantColumn: "tenant_id", PauseColumn: "paused_until", Scheduled: true, CockpitPath: "/platform/real-experience", StatusSQL: "SELECT p.tenant_id, p.evaluation_enabled AS enabled, 'observe' AS mode, p.paused_until, (SELECT max(started_at) FROM experience_evaluation_runs r WHERE r.tenant_id=p.tenant_id) AS last_run_at, 60 AS interval_minutes, p.created_at FROM experience_policies p"}}},
	{Key: "pipeline", Label: "Pipeline Repair", Family: "content", Kind: "autopilot", CockpitPath: "/platform/pipeline", Lanes: []opsFleetLane{{Key: "autopilot", Label: "Autopilot", Table: "pipeline_autopilot_policies", TenantColumn: "tenant_id", PauseColumn: "paused_until", Scheduled: true, CockpitPath: "/platform/pipeline", StatusSQL: "SELECT tenant_id, enabled, mode, paused_until, last_run_at, interval_minutes, created_at FROM pipeline_autopilot_policies"}}},
	{Key: "enrichment", Label: "Enrichment", Family: "content", Kind: "autopilot", CockpitPath: "/platform/enrichment", Lanes: []opsFleetLane{{Key: "autopilot", Label: "Autopilot", Table: "enrichment_autopilot_policies", TenantColumn: "tenant_id", PauseColumn: "paused_until", Scheduled: true, CockpitPath: "/platform/enrichment", StatusSQL: "SELECT tenant_id, enabled, mode, paused_until, last_run_at, interval_minutes, created_at FROM enrichment_autopilot_policies"}}},
	{Key: "media_circulation", Label: "Media Circulation", Family: "media", Kind: "autopilot", CockpitPath: "/platform/media/circulation", Lanes: []opsFleetLane{{Key: "autopilot", Label: "Autopilot", Table: "media_circulation_policies", TenantColumn: "tenant_id", PauseColumn: "autopilot_paused_until", Scheduled: true, CockpitPath: "/platform/media/circulation", StatusSQL: "SELECT tenant_id, autopilot_enabled AS enabled, autopilot_mode AS mode, autopilot_paused_until AS paused_until, autopilot_last_run_at AS last_run_at, autopilot_interval_minutes AS interval_minutes, created_at FROM media_circulation_policies"}}},
	{Key: "media_studio", Label: "Media Studio", Family: "media", Kind: "autopilot", CockpitPath: "/platform/media/atomization?tab=autopilot", Lanes: []opsFleetLane{{Key: "autopilot", Label: "Autopilot", Table: "media_studio_autopilot_policies", TenantColumn: "tenant_id", PauseColumn: "paused_until", Scheduled: true, CockpitPath: "/platform/media/atomization?tab=autopilot", StatusSQL: "SELECT tenant_id, autopilot_enabled AS enabled, autopilot_mode AS mode, paused_until, last_run_at, interval_minutes, created_at FROM media_studio_autopilot_policies"}}},
	{Key: "redundancy", Label: "Redundancy Hygiene", Family: "media", Kind: "autopilot", CockpitPath: "/platform/media/circulation", Lanes: []opsFleetLane{{Key: "autopilot", Label: "Autopilot", Table: "redundancy_policies", TenantColumn: "tenant_id", PauseColumn: "paused_until", Scheduled: true, CockpitPath: "/platform/media/circulation", StatusSQL: "SELECT tenant_id, enabled, 'observe' AS mode, paused_until, last_swept_at AS last_run_at, sweep_interval_minutes AS interval_minutes, created_at FROM redundancy_policies"}}},
	{Key: "news_circulation", Label: "News Circulation", Family: "news", Kind: "autopilot", CockpitPath: "/platform/news/circulation", Lanes: []opsFleetLane{{Key: "autopilot", Label: "Autopilot", Table: "news_circulation_policies", TenantColumn: "tenant_id", PauseColumn: "autopilot_paused_until", Scheduled: true, CockpitPath: "/platform/news/circulation", StatusSQL: "SELECT tenant_id, autopilot_enabled AS enabled, autopilot_mode AS mode, autopilot_paused_until AS paused_until, autopilot_last_run_at AS last_run_at, autopilot_interval_minutes AS interval_minutes, created_at FROM news_circulation_policies"}}},
	{Key: "preferences", Label: "Preferences", Family: "content", Kind: "autopilot", CockpitPath: "/platform/topics?tab=autopilot", Lanes: []opsFleetLane{{Key: "autopilot", Label: "Autopilot", Table: "preference_autopilot_policies", TenantColumn: "tenant_id", PauseColumn: "paused_until", Scheduled: true, CockpitPath: "/platform/topics?tab=autopilot", StatusSQL: "SELECT tenant_id, enabled, mode, paused_until, last_run_at, interval_minutes, created_at FROM preference_autopilot_policies"}}},
	{Key: "embedding_lifecycle", Label: "Embedding Lifecycle", Family: "platform", Kind: "engine", CockpitPath: "/platform/intelligence/embeddings", Lanes: []opsFleetLane{
		{Key: "audit", Label: "Audit", Table: "embedding_lifecycle_policies", TenantColumn: "tenant_id", Scheduled: true, CockpitPath: "/platform/intelligence/embeddings", StatusSQL: "SELECT tenant_id, audit_enabled AS enabled, 'observe' AS mode, NULL::timestamp AS paused_until, last_audit_at AS last_run_at, audit_interval_minutes AS interval_minutes, created_at FROM embedding_lifecycle_policies"},
		{Key: "campaigns", Label: "Campaigns", Table: "embedding_lifecycle_policies", TenantColumn: "tenant_id", PauseColumn: "campaigns_paused_until", ManualOnly: true, CockpitPath: "/platform/intelligence/embeddings", StatusSQL: "SELECT tenant_id, true AS enabled, 'manual' AS mode, campaigns_paused_until AS paused_until, (SELECT max(updated_at) FROM embedding_campaigns) AS last_run_at, 0 AS interval_minutes, created_at FROM embedding_lifecycle_policies"},
	}},
	{Key: "ai_spend", Label: "AI Spend", Family: "economics", Kind: "observatory", CockpitPath: "/platform/economics", Lanes: []opsFleetLane{{Key: "governor", Label: "Governor", Table: "ai_spend_policies", TenantColumn: "tenant_id", PauseColumn: "paused_until", Scheduled: true, CockpitPath: "/platform/economics", StatusSQL: "SELECT tenant_id, enabled, 'observe' AS mode, paused_until, last_run_at, aggregation_interval_minutes AS interval_minutes, created_at FROM ai_spend_policies"}}},
	{Key: "ranking_intelligence", Label: "Ranking Intelligence", Family: "media", Kind: "engine", CockpitPath: "/platform/intelligence", Lanes: []opsFleetLane{{Key: "refresh", Label: "Refresh", ManualOnly: true, CockpitPath: "/platform/intelligence"}}},
	{Key: "storage", Label: "Storage", Family: "media", Kind: "observatory", CockpitPath: "/platform/storage", Lanes: []opsFleetLane{{Key: "health", Label: "Health", ManualOnly: true, CockpitPath: "/platform/storage"}}},
	{Key: "atomization", Label: "Atomization", Family: "media", Kind: "engine", CockpitPath: "/platform/media/atomization", Lanes: []opsFleetLane{{Key: "engine", Label: "Engine", ManualOnly: true, CockpitPath: "/platform/media/atomization"}}},
}

type opsStatusRow struct {
	TenantID        string     `gorm:"column:tenant_id" json:"tenant_id"`
	Enabled         bool       `gorm:"column:enabled" json:"enabled"`
	Mode            string     `gorm:"column:mode" json:"mode"`
	PausedUntil     *time.Time `gorm:"column:paused_until" json:"paused_until,omitempty"`
	LastRunAt       *time.Time `gorm:"column:last_run_at" json:"last_run_at,omitempty"`
	IntervalMinutes int        `gorm:"column:interval_minutes" json:"interval_minutes"`
	CreatedAt       time.Time  `gorm:"column:created_at" json:"created_at"`
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
	StalledSince *time.Time `json:"stalled_since,omitempty"`
	Liveness     string     `json:"liveness"`
	CockpitPath  string     `json:"cockpit_path"`
	Error        string     `json:"error,omitempty"`
}

type opsAttentionItem struct {
	Key         string    `json:"key"`
	Fingerprint string    `json:"fingerprint"`
	System      string    `json:"system"`
	TenantID    string    `json:"tenant_id,omitempty"`
	Kind        string    `json:"kind"`
	Severity    string    `json:"severity"`
	Title       string    `json:"title"`
	Detail      string    `json:"detail"`
	Count       int       `json:"count"`
	FirstSeen   time.Time `json:"first_seen"`
	Href        string    `json:"href"`
	State       string    `json:"state,omitempty"`
	Snoozed     bool      `json:"snoozed"`
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

func opsStatusForLane(db *gorm.DB, member opsFleetMember, lane opsFleetLane, now time.Time) []opsMemberStatus {
	if lane.StatusSQL == "" {
		return []opsMemberStatus{{MemberKey: member.Key, MemberLabel: member.Label, Family: member.Family, Kind: member.Kind, LaneKey: lane.Key, LaneLabel: lane.Label, State: "unknown", Liveness: "unknown", CockpitPath: lane.CockpitPath}}
	}
	var rows []opsStatusRow
	if err := db.Raw(lane.StatusSQL).Scan(&rows).Error; err != nil {
		return []opsMemberStatus{{MemberKey: member.Key, MemberLabel: member.Label, Family: member.Family, Kind: member.Kind, LaneKey: lane.Key, LaneLabel: lane.Label, State: "errored", Liveness: "unknown", CockpitPath: lane.CockpitPath, Error: err.Error()}}
	}
	if len(rows) == 0 {
		return []opsMemberStatus{{MemberKey: member.Key, MemberLabel: member.Label, Family: member.Family, Kind: member.Kind, LaneKey: lane.Key, LaneLabel: lane.Label, State: "unconfigured", Liveness: "unknown", CockpitPath: lane.CockpitPath}}
	}
	out := make([]opsMemberStatus, 0, len(rows))
	for _, row := range rows {
		status := opsMemberStatus{MemberKey: member.Key, MemberLabel: member.Label, Family: member.Family, Kind: member.Kind, LaneKey: lane.Key, LaneLabel: lane.Label, TenantID: row.TenantID, Enabled: row.Enabled, Pausable: lane.PauseColumn != "", Mode: row.Mode, PausedUntil: row.PausedUntil, LastRunAt: row.LastRunAt, CockpitPath: lane.CockpitPath, Liveness: "unknown"}
		if row.PausedUntil != nil && row.PausedUntil.After(now) {
			status.State, status.Liveness = "paused", "not_applicable"
		} else if !row.Enabled {
			status.State, status.Liveness = "disabled", "not_applicable"
		} else if lane.ManualOnly || !lane.Scheduled || row.IntervalMinutes <= 0 {
			status.State = "idle"
		} else if row.LastRunAt == nil {
			firstRunDeadline := row.CreatedAt.Add(time.Duration(row.IntervalMinutes+30) * time.Minute)
			if now.Before(firstRunDeadline) {
				status.State, status.Liveness = "idle", "observable"
			} else {
				status.State, status.Liveness = "stalled", "observable"
				status.StalledSince = &firstRunDeadline
			}
		} else {
			due := row.LastRunAt.Add(time.Duration(row.IntervalMinutes) * time.Minute)
			status.NextDueAt, status.Liveness = &due, "observable"
			grace := time.Duration(row.IntervalMinutes) * time.Minute
			if grace < 30*time.Minute {
				grace = 30 * time.Minute
			}
			if threshold := due.Add(grace); now.After(threshold) {
				status.State = "stalled"
				status.StalledSince = &threshold
			} else {
				status.State = "idle"
			}
		}
		out = append(out, status)
	}
	return out
}

func collectOpsStatus(db *gorm.DB, now time.Time) []opsMemberStatus {
	all := make([]opsMemberStatus, 0, 20)
	for _, member := range opsFleetRegistry {
		for _, lane := range member.Lanes {
			all = append(all, opsStatusForLane(db, member, lane, now)...)
		}
	}
	return all
}

func opsSeverityRank(value string) int {
	switch value {
	case "critical":
		return 4
	case "major":
		return 3
	case "minor", "warning":
		return 2
	default:
		return 1
	}
}

func opsFingerprint(parts ...string) string {
	h := sha256.Sum256([]byte(strings.Join(parts, "|")))
	return hex.EncodeToString(h[:12])
}

func opsTimeFingerprint(value *time.Time) string {
	if value == nil {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

// opsPauseValuesMatch decides whether the pause currently on a policy row is
// still the one a Command Center pause wrote. The policy columns are
// `timestamp` while the command ledger stores `timestamptz`, so the two values
// round-trip through different precisions; exact Equal would misclassify our
// own pause as foreign. One second of tolerance absorbs that while still
// rejecting any real containment/human pause, which is always minutes away.
func opsPauseValuesMatch(current, written *time.Time) bool {
	if current == nil || written == nil {
		return false
	}
	diff := current.Sub(*written)
	if diff < 0 {
		diff = -diff
	}
	return diff <= time.Second
}

func opsOpenAttention(db *gorm.DB, now time.Time) ([]opsAttentionItem, []string) {
	items, errors := []opsAttentionItem{}, []string{}
	appendRows := func(system, kind, href, sql string) {
		var rows []struct {
			TenantID, NativeID, Severity, Title, Detail string
			Count                                       int
			FirstSeen                                   time.Time
		}
		if err := db.Raw(sql).Scan(&rows).Error; err != nil {
			errors = append(errors, system+": "+err.Error())
			return
		}
		for _, row := range rows {
			severity := strings.ToLower(row.Severity)
			if severity == "" {
				severity = "major"
			}
			items = append(items, opsAttentionItem{Key: system + ":" + kind + ":" + row.NativeID, Fingerprint: opsFingerprint(system, kind, row.NativeID, severity, fmt.Sprint(row.Count)), System: system, TenantID: row.TenantID, Kind: kind, Severity: severity, Title: row.Title, Detail: row.Detail, Count: row.Count, FirstSeen: row.FirstSeen, Href: href})
		}
	}
	appendRows("system_health", "episode", "/platform/system-health", "SELECT 'platform' AS tenant_id, public_id::text AS native_id, severity, COALESCE(summary, verdict) AS title, COALESCE(root_service, 'platform') AS detail, 1 AS count, first_detected_at AS first_seen FROM system_incident_episodes WHERE status IN ('open','recovering')")
	appendRows("feed_integrity", "episode", "/platform/feed-integrity", "SELECT tenant_id, public_id::text AS native_id, severity, COALESCE(summary, check_key) AS title, COALESCE(feed, 'feed') AS detail, 1 AS count, first_seen_at AS first_seen FROM feed_integrity_episodes WHERE status IN ('open','recovering')")
	appendRows("experience", "episode", "/platform/real-experience", "SELECT tenant_id, public_id::text AS native_id, severity, COALESCE(summary, 'Experience incident') AS title, COALESCE(surface, 'experience') AS detail, 1 AS count, first_seen_at AS first_seen FROM experience_incidents WHERE status IN ('open','recovering')")
	appendRows("ai_spend", "budget", "/platform/economics", "SELECT tenant_id, id::text AS native_id, 'major' AS severity, kind AS title, COALESCE(scope, 'platform') AS detail, 1 AS count, first_seen_at AS first_seen FROM ai_spend_episodes WHERE status='open'")
	// 'pending' alone is every generated recommendation (Safe Auto applies most
	// on its next pass; Observe leaves all of them pending). The approval queue
	// is only the subset the autopilot explicitly ledgered as approval_required.
	appendRows("media_circulation", "approval", "/platform/media/circulation", "SELECT r.tenant_id, r.public_id::text AS native_id, 'major' AS severity, r.action AS title, r.verdict AS detail, 1 AS count, r.created_at AS first_seen FROM media_circulation_recommendations r WHERE r.status='pending' AND EXISTS (SELECT 1 FROM media_circulation_actions a WHERE a.recommendation_id=r.public_id AND a.status='approval_required')")
	appendRows("media_studio", "approval", "/platform/media/atomization?tab=autopilot", "SELECT tenant_id, native_id, severity, title, detail, count, first_seen FROM (SELECT DISTINCT ON (COALESCE(chapter_id::text, content_item_id::text, public_id::text)) tenant_id, public_id::text AS native_id, 'major' AS severity, verdict AS title, COALESCE(NULLIF(reason,''), 'Studio case awaits review') AS detail, 1 AS count, created_at AS first_seen FROM media_studio_actions WHERE status='approval_required' AND COALESCE(human_outcome,'')='' ORDER BY COALESCE(chapter_id::text, content_item_id::text, public_id::text), created_at DESC) studio_cases")
	appendRows("feed_integrity", "approval", "/platform/feed-integrity", "SELECT tenant_id, public_id::text AS native_id, 'major' AS severity, action_class AS title, COALESCE(reason, 'Autopilot action requires approval') AS detail, 1 AS count, created_at AS first_seen FROM feed_integrity_actions WHERE outcome='approval_required'")
	appendRows("preferences", "attention", "/platform/topics?tab=autopilot", "SELECT tenant_id, id::text AS native_id, 'minor' AS severity, COALESCE(suggested_label_en, suggested_slug) AS title, 'Topic proposal pending review' AS detail, 1 AS count, created_at AS first_seen FROM topic_proposals WHERE status='pending'")
	appendRows("embedding_lifecycle", "exception", "/platform/intelligence/embeddings", "SELECT tenant_id, id::text AS native_id, 'major' AS severity, COALESCE(failure_class, 'Campaign exception') AS title, surface_key AS detail, 1 AS count, created_at AS first_seen FROM embedding_campaign_exceptions WHERE status='open'")
	for _, status := range collectOpsStatus(db, now) {
		if status.State != "stalled" && status.State != "errored" {
			continue
		}
		firstSeen := now
		if status.StalledSince != nil {
			firstSeen = *status.StalledSince
		}
		items = append(items, opsAttentionItem{Key: status.MemberKey + ":stalled:" + status.LaneKey + ":" + status.TenantID, Fingerprint: opsFingerprint(status.MemberKey, status.LaneKey, status.State, opsTimeFingerprint(status.LastRunAt)), System: status.MemberKey, TenantID: status.TenantID, Kind: "stalled", Severity: "major", Title: status.MemberLabel + " " + status.LaneLabel + " is " + status.State, Detail: status.Error, Count: 1, FirstSeen: firstSeen, Href: status.CockpitPath})
	}
	return items, errors
}

func applyOpsAttentionState(db *gorm.DB, tenant string, items []opsAttentionItem, now time.Time) []opsAttentionItem {
	var states []models.OpsAttentionState
	_ = db.Where("tenant_id = ?", tenant).Find(&states).Error
	byKey := map[string]models.OpsAttentionState{}
	for _, state := range states {
		byKey[state.AttentionKey] = state
	}
	for i := range items {
		state, ok := byKey[items[i].Key]
		if !ok || now.Sub(state.UpdatedAt) > opsAttentionRetention {
			continue
		}
		escalated := state.BaselineFingerprint != items[i].Fingerprint || opsSeverityRank(items[i].Severity) > opsSeverityRank(state.BaselineSeverity) || items[i].Count > state.BaselineCount
		if escalated {
			continue
		}
		items[i].State = state.State
		items[i].Snoozed = state.State == "snoozed" && state.SnoozedUntil != nil && state.SnoozedUntil.After(now)
	}
	sort.SliceStable(items, func(i, j int) bool {
		if opsSeverityRank(items[i].Severity) != opsSeverityRank(items[j].Severity) {
			return opsSeverityRank(items[i].Severity) > opsSeverityRank(items[j].Severity)
		}
		return items[i].FirstSeen.Before(items[j].FirstSeen)
	})
	return items
}

func opsHeadline(items []opsAttentionItem, statuses []opsMemberStatus) (string, string) {
	stalledMembers := map[string]bool{}
	major := false
	visible := 0
	for _, item := range items {
		if item.Snoozed {
			continue
		}
		visible++
		if opsSeverityRank(item.Severity) >= 3 {
			major = true
		}
		if item.Kind == "stalled" {
			stalledMembers[item.System] = true
		}
		if item.System == "system_health" && item.Kind == "episode" {
			return "incident", "System Health has an open incident."
		}
	}
	if len(stalledMembers) >= 3 {
		return "incident", "Multiple CMS-owned lanes are stalled."
	}
	if major {
		return "attention", fmt.Sprintf("%d fleet items need attention.", visible)
	}
	for _, status := range statuses {
		if status.State == "errored" {
			return "attention", "A fleet adapter could not read its status."
		}
	}
	if visible > 0 {
		return "watching", fmt.Sprintf("%d minor or acknowledged fleet items are being watched.", visible)
	}
	return "all_clear", "No open fleet attention."
}

func GetOpsStatus(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	now := time.Now().UTC()
	statuses := collectOpsStatus(db, now)
	items, errors := opsOpenAttention(db, now)
	principal, _ := utils.GetAdminPrincipal(c)
	items = applyOpsAttentionState(db, principal.TenantID, items, now)
	headline, summary := opsHeadline(items, statuses)
	counts := map[string]int{}
	for _, item := range items {
		if !item.Snoozed {
			counts[item.Severity]++
		}
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"as_of": now, "headline": headline, "summary": summary, "fleet": statuses, "attention_counts": counts, "adapter_errors": errors}})
}

func GetOpsFleet(c *gin.Context) {
	now := time.Now().UTC()
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"as_of": now, "items": collectOpsStatus(c.MustGet("db").(*gorm.DB), now)}})
}

func ListOpsAttention(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	now := time.Now().UTC()
	principal, _ := utils.GetAdminPrincipal(c)
	items, errors := opsOpenAttention(db, now)
	items = applyOpsAttentionState(db, principal.TenantID, items, now)
	includeSnoozed := c.Query("include_snoozed") == "true"
	visible := make([]opsAttentionItem, 0, len(items))
	for _, item := range items {
		if !includeSnoozed && item.Snoozed {
			continue
		}
		if system := c.Query("system"); system != "" && item.System != system {
			continue
		}
		if severity := c.Query("severity"); severity != "" && item.Severity != severity {
			continue
		}
		visible = append(visible, item)
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"as_of": now, "items": visible, "adapter_errors": errors}})
}

type opsAttentionMutationRequest struct {
	Key        string `json:"key"`
	TTLMinutes int    `json:"ttl_minutes"`
}

func mutateOpsAttentionState(c *gin.Context, state string) {
	principal, ok := utils.GetAdminPrincipal(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return
	}
	var req opsAttentionMutationRequest
	if err := c.ShouldBindJSON(&req); err != nil || strings.TrimSpace(req.Key) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "attention key is required"})
		return
	}
	now := time.Now().UTC()
	db := c.MustGet("db").(*gorm.DB)
	items, _ := opsOpenAttention(db, now)
	var item *opsAttentionItem
	for i := range items {
		if items[i].Key == req.Key {
			item = &items[i]
			break
		}
	}
	if item == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "attention item is not open"})
		return
	}
	row := models.OpsAttentionState{TenantID: principal.TenantID, AttentionKey: item.Key, State: state, BaselineFingerprint: item.Fingerprint, BaselineSeverity: item.Severity, BaselineCount: item.Count, ActorID: principal.UserID, ActorEmail: principal.Email}
	if state == "snoozed" {
		if req.TTLMinutes < 1 || req.TTLMinutes > opsCommandMaxTTLMinutes {
			c.JSON(http.StatusBadRequest, gin.H{"error": "ttl_minutes must be between 1 and 1440"})
			return
		}
		until := now.Add(time.Duration(req.TTLMinutes) * time.Minute)
		row.SnoozedUntil = &until
	}
	if err := db.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "tenant_id"}, {Name: "attention_key"}}, DoUpdates: clause.AssignmentColumns([]string{"state", "snoozed_until", "baseline_fingerprint", "baseline_severity", "baseline_count", "actor_id", "actor_email", "updated_at"})}).Create(&row).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	_ = db.Where("updated_at < ?", now.Add(-opsAttentionRetention)).Delete(&models.OpsAttentionState{}).Error
	c.JSON(http.StatusOK, gin.H{"data": row})
}

func AckOpsAttention(c *gin.Context)    { mutateOpsAttentionState(c, "acked") }
func SnoozeOpsAttention(c *gin.Context) { mutateOpsAttentionState(c, "snoozed") }
func ClearOpsAttentionState(c *gin.Context) {
	principal, ok := utils.GetAdminPrincipal(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return
	}
	var req opsAttentionMutationRequest
	if err := c.ShouldBindJSON(&req); err != nil || req.Key == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "attention key is required"})
		return
	}
	if err := c.MustGet("db").(*gorm.DB).Where("tenant_id=? AND attention_key=?", principal.TenantID, req.Key).Delete(&models.OpsAttentionState{}).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.Status(http.StatusNoContent)
}

type opsPauseRequest struct {
	MemberKey      string `json:"member_key"`
	LaneKey        string `json:"lane_key"`
	Reason         string `json:"reason"`
	TTLMinutes     int    `json:"ttl_minutes"`
	IdempotencyKey string `json:"idempotency_key"`
}
type opsResumeRequest struct {
	SourceCommandID string `json:"source_command_id"`
	IdempotencyKey  string `json:"idempotency_key"`
}

type opsPauseTarget struct {
	Member   opsFleetMember
	Lane     opsFleetLane
	TenantID string
	Prior    *time.Time
}

func opsPauseTargets(tx *gorm.DB, memberKey, laneKey string, all bool) ([]opsPauseTarget, error) {
	lanes := []struct {
		Member opsFleetMember
		Lane   opsFleetLane
	}{}
	if all {
		for _, m := range opsFleetRegistry {
			for _, l := range m.Lanes {
				if l.PauseColumn != "" {
					lanes = append(lanes, struct {
						Member opsFleetMember
						Lane   opsFleetLane
					}{m, l})
				}
			}
		}
	} else {
		m, l, ok := opsFindLane(memberKey, laneKey)
		if !ok || l.PauseColumn == "" {
			return nil, fmt.Errorf("member lane is not pausable")
		}
		lanes = append(lanes, struct {
			Member opsFleetMember
			Lane   opsFleetLane
		}{m, l})
	}
	allTargets := []opsPauseTarget{}
	for _, entry := range lanes {
		query := fmt.Sprintf("SELECT %s AS tenant_id, %s AS paused_until FROM %s FOR UPDATE", entry.Lane.TenantColumn, entry.Lane.PauseColumn, entry.Lane.Table)
		var rows []struct {
			TenantID    string
			PausedUntil *time.Time `gorm:"column:paused_until"`
		}
		if err := tx.Raw(query).Scan(&rows).Error; err != nil {
			return nil, err
		}
		for _, row := range rows {
			allTargets = append(allTargets, opsPauseTarget{Member: entry.Member, Lane: entry.Lane, TenantID: row.TenantID, Prior: row.PausedUntil})
		}
	}
	sort.Slice(allTargets, func(i, j int) bool {
		return allTargets[i].Member.Key+allTargets[i].Lane.Key+allTargets[i].TenantID < allTargets[j].Member.Key+allTargets[j].Lane.Key+allTargets[j].TenantID
	})
	if len(allTargets) > opsCommandMaxTargets {
		return nil, fmt.Errorf("fleet command exceeds the %d-target safety cap", opsCommandMaxTargets)
	}
	return allTargets, nil
}

func opsWriteAudit(tx *gorm.DB, principal utils.AdminPrincipal, action, resource, status string, payload any) {
	raw, _ := json.Marshal(payload)
	_ = tx.Create(&models.AuditLog{TenantID: principal.TenantID, UserID: principal.UserID, UserEmail: principal.Email, Action: action, TargetService: "cms", TargetResource: resource, Status: status, Payload: datatypes.JSON(raw)}).Error
}

func runOpsPause(c *gin.Context, all bool) {
	principal, ok := utils.GetAdminPrincipal(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return
	}
	var req opsPauseRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
		return
	}
	if strings.TrimSpace(req.Reason) == "" || len(req.Reason) > 1000 || req.TTLMinutes < 1 || req.TTLMinutes > opsCommandMaxTTLMinutes || strings.TrimSpace(req.IdempotencyKey) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "reason, ttl_minutes (1..1440), and idempotency_key are required"})
		return
	}
	now := time.Now().UTC()
	until := now.Add(time.Duration(req.TTLMinutes) * time.Minute)
	db := c.MustGet("db").(*gorm.DB)
	var response models.OpsFleetCommand
	err := db.Transaction(func(tx *gorm.DB) error {
		var existing models.OpsFleetCommand
		if err := tx.Where("tenant_id=? AND idempotency_key=?", principal.TenantID, req.IdempotencyKey).First(&existing).Error; err == nil {
			response = existing
			return nil
		} else if err != gorm.ErrRecordNotFound {
			return err
		}
		command := "pause_member"
		scope := req.MemberKey + ":" + req.LaneKey
		if all {
			command = "pause_all"
			scope = "all"
		}
		ttl := req.TTLMinutes
		row := models.OpsFleetCommand{TenantID: principal.TenantID, Command: command, Scope: scope, Reason: req.Reason, TTLMinutes: &ttl, IdempotencyKey: req.IdempotencyKey, ActorID: principal.UserID, ActorEmail: principal.Email, Status: "succeeded", Counts: datatypes.JSON([]byte(`{}`))}
		if err := tx.Create(&row).Error; err != nil {
			return err
		}
		targets, err := opsPauseTargets(tx, req.MemberKey, req.LaneKey, all)
		if err != nil {
			return err
		}
		if len(targets) == 0 {
			return fmt.Errorf("no configured pause targets")
		}
		counts := map[string]int{"paused": 0, "skipped": 0}
		for _, target := range targets {
			action := models.OpsFleetCommandAction{CommandID: row.ID, MemberKey: target.Member.Key, LaneKey: target.Lane.Key, TenantID: target.TenantID, PriorPausedUntil: target.Prior, WrittenPausedUntil: &until}
			if target.Prior != nil && target.Prior.After(until) {
				action.Outcome = "skipped"
				action.Guardrail = "longer_existing_pause"
				action.Reason = "Existing pause is longer"
				counts["skipped"]++
			} else {
				update := fmt.Sprintf("UPDATE %s SET %s=?, updated_at=? WHERE %s=?", target.Lane.Table, target.Lane.PauseColumn, target.Lane.TenantColumn)
				if err := tx.Exec(update, until, now, target.TenantID).Error; err != nil {
					return err
				}
				action.Outcome = "paused"
				counts["paused"]++
			}
			if err := tx.Create(&action).Error; err != nil {
				return err
			}
		}
		raw, _ := json.Marshal(counts)
		row.Counts = datatypes.JSON(raw)
		if counts["skipped"] > 0 {
			row.Status = "partial"
		}
		if err := tx.Save(&row).Error; err != nil {
			return err
		}
		opsWriteAudit(tx, principal, "ops."+command, scope, "success", gin.H{"reason": req.Reason, "ttl_minutes": req.TTLMinutes, "counts": counts})
		response = row
		return nil
	})
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": response})
}

func PauseOpsMember(c *gin.Context) { runOpsPause(c, false) }
func PauseOpsFleet(c *gin.Context)  { runOpsPause(c, true) }

func ResumeOpsCommand(c *gin.Context) {
	principal, ok := utils.GetAdminPrincipal(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return
	}
	var req opsResumeRequest
	if err := c.ShouldBindJSON(&req); err != nil || req.SourceCommandID == "" || req.IdempotencyKey == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "source_command_id and idempotency_key are required"})
		return
	}
	sourceID, err := uuid.Parse(req.SourceCommandID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid source_command_id"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var response models.OpsFleetCommand
	err = db.Transaction(func(tx *gorm.DB) error {
		var existing models.OpsFleetCommand
		if err := tx.Where("tenant_id=? AND idempotency_key=?", principal.TenantID, req.IdempotencyKey).First(&existing).Error; err == nil {
			response = existing
			return nil
		} else if err != gorm.ErrRecordNotFound {
			return err
		}
		var source models.OpsFleetCommand
		if err := tx.Where("tenant_id=? AND public_id=?", principal.TenantID, sourceID).First(&source).Error; err != nil {
			return err
		}
		if source.Command != "pause_member" && source.Command != "pause_all" {
			return fmt.Errorf("source command is not a pause")
		}
		row := models.OpsFleetCommand{TenantID: principal.TenantID, Command: "resume", Scope: source.Scope, Reason: "resume command", IdempotencyKey: req.IdempotencyKey, SourceCommandID: &sourceID, ActorID: principal.UserID, ActorEmail: principal.Email, Status: "succeeded", Counts: datatypes.JSON([]byte(`{}`))}
		if err := tx.Create(&row).Error; err != nil {
			return err
		}
		var actions []models.OpsFleetCommandAction
		if err := tx.Where("command_id=? AND outcome='paused'", source.ID).Order("member_key,lane_key,tenant_id").Find(&actions).Error; err != nil {
			return err
		}
		counts := map[string]int{"restored": 0, "cleared": 0, "skipped": 0}
		now := time.Now().UTC()
		for _, sourceAction := range actions {
			_, lane, ok := opsFindLane(sourceAction.MemberKey, sourceAction.LaneKey)
			if !ok {
				return fmt.Errorf("source action references unknown lane")
			}
			query := fmt.Sprintf("SELECT %s AS paused_until FROM %s WHERE %s=? FOR UPDATE", lane.PauseColumn, lane.Table, lane.TenantColumn)
			var current struct {
				PausedUntil *time.Time `gorm:"column:paused_until"`
			}
			if err := tx.Raw(query, sourceAction.TenantID).Scan(&current).Error; err != nil {
				return err
			}
			action := models.OpsFleetCommandAction{CommandID: row.ID, SourceActionID: &sourceAction.ID, MemberKey: sourceAction.MemberKey, LaneKey: sourceAction.LaneKey, TenantID: sourceAction.TenantID, PriorPausedUntil: current.PausedUntil, WrittenPausedUntil: sourceAction.PriorPausedUntil}
			if !opsPauseValuesMatch(current.PausedUntil, sourceAction.WrittenPausedUntil) {
				action.Outcome = "skipped"
				action.Guardrail = "foreign_pause"
				action.Reason = "Pause changed after source command"
				counts["skipped"]++
			} else {
				restore := sourceAction.PriorPausedUntil
				if restore != nil && !restore.After(now) {
					restore = nil
				}
				update := fmt.Sprintf("UPDATE %s SET %s=?, updated_at=? WHERE %s=?", lane.Table, lane.PauseColumn, lane.TenantColumn)
				if err := tx.Exec(update, restore, now, sourceAction.TenantID).Error; err != nil {
					return err
				}
				if restore == nil {
					action.Outcome = "cleared"
					counts["cleared"]++
				} else {
					action.Outcome = "restored"
					counts["restored"]++
				}
			}
			if err := tx.Create(&action).Error; err != nil {
				return err
			}
		}
		raw, _ := json.Marshal(counts)
		row.Counts = datatypes.JSON(raw)
		if counts["skipped"] > 0 {
			row.Status = "partial"
		}
		if err := tx.Save(&row).Error; err != nil {
			return err
		}
		opsWriteAudit(tx, principal, "ops.resume", source.Scope, "success", counts)
		response = row
		return nil
	})
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": response})
}

func ListOpsCommands(c *gin.Context) {
	principal, ok := utils.GetAdminPrincipal(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return
	}
	var rows []models.OpsFleetCommand
	db := c.MustGet("db").(*gorm.DB)
	if err := db.Where("tenant_id=?", principal.TenantID).Order("created_at DESC").Limit(100).Find(&rows).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"items": rows}})
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
	db := c.MustGet("db").(*gorm.DB)
	var row models.OpsFleetCommand
	if err := db.Where("tenant_id=? AND public_id=?", principal.TenantID, id).First(&row).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "command not found"})
		return
	}
	var actions []models.OpsFleetCommandAction
	_ = db.Where("command_id=?", row.ID).Order("id ASC").Find(&actions).Error
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"command": row, "actions": actions}})
}

func GetOpsBriefing(c *gin.Context) {
	principal, ok := utils.GetAdminPrincipal(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var cursor models.OpsBriefingCursor
	_ = db.Where("tenant_id=? AND admin_id=?", principal.TenantID, principal.UserID).First(&cursor).Error
	through := time.Now().UTC()
	since := cursor.LastSeenAt
	if since.IsZero() {
		since = through.Add(-24 * time.Hour)
	}
	var commands []models.OpsFleetCommand
	_ = db.Where("created_at > ? AND created_at <= ?", since.Add(-5*time.Minute), through).Order("created_at DESC").Limit(50).Find(&commands)
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"since": since, "through": through, "commands": commands}})
}
func MarkOpsBriefingSeen(c *gin.Context) {
	principal, ok := utils.GetAdminPrincipal(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return
	}
	var req struct {
		Through time.Time `json:"through"`
	}
	if err := c.ShouldBindJSON(&req); err != nil || req.Through.IsZero() {
		c.JSON(http.StatusBadRequest, gin.H{"error": "through is required"})
		return
	}
	if req.Through.After(time.Now().UTC()) {
		req.Through = time.Now().UTC()
	}
	db := c.MustGet("db").(*gorm.DB)
	row := models.OpsBriefingCursor{TenantID: principal.TenantID, AdminID: principal.UserID, LastSeenAt: req.Through}
	err := db.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "tenant_id"}, {Name: "admin_id"}}, DoUpdates: clause.Assignments(map[string]interface{}{"last_seen_at": gorm.Expr("GREATEST(ops_briefing_cursors.last_seen_at, EXCLUDED.last_seen_at)"), "updated_at": time.Now().UTC()})}).Create(&row).Error
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"through": req.Through}})
}

func GetOpsCalendar(c *gin.Context) {
	now := time.Now().UTC()
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"as_of": now, "fleet": collectOpsStatus(c.MustGet("db").(*gorm.DB), now)}})
}
