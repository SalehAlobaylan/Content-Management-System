package controllers

import (
	"content-management-system/src/models"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// Preferences Autopilot — cockpit deep-logging read models. Mirrors the Media
// Studio insights pattern (GetMediaStudioAutopilotInsights): last N runs + ONE
// grouped SQL over the action ledger, folded in memory. Pure reads; the heavy
// per-run snapshot work already happened inside the runs — these endpoints only
// aggregate what is persisted.

// ----------------------------------------------------------------
// Insights (GET /admin/preferences/autopilot/insights)
// ----------------------------------------------------------------

const preferenceInsightsRunWindow = 20

// prefRunBuckets groups a run's ledger rows into cockpit-meaningful outcome
// buckets. Observe's would_* rows fold into their safe-auto equivalents so the
// flow shape is identical across modes (the studio convention).
type prefRunBuckets struct {
	MapSweep      int `json:"map_sweep"`
	DirtySweep    int `json:"dirty_sweep"`
	Centroid      int `json:"centroid_refresh"`
	MemberRefresh int `json:"member_refresh"`
	Recompute     int `json:"recompute"`
	Mine          int `json:"mine"`
	Enrich        int `json:"proposal_enrich"`
	AutoApprove   int `json:"auto_approve"`
	MergeSuggest  int `json:"merge_suggest"`
	Baseline      int `json:"baseline"`
	Skipped       int `json:"skipped"`
	Errored       int `json:"errored"`
}

// bucketPreferenceAction folds one grouped ledger row into the buckets. Pure —
// unit-tested. Skips/would_skips land in Skipped regardless of class (the class
// is visible in the guardrail totals); errors land in Errored.
func bucketPreferenceAction(b *prefRunBuckets, class, status string, n int) {
	switch status {
	case models.PreferenceActionStatusError, models.PreferenceActionStatusBaselineError:
		b.Errored += n
		return
	case models.PreferenceActionStatusSkipped, models.PreferenceActionStatusWouldSkip:
		b.Skipped += n
		return
	}
	// success / baseline_success / would_trigger → the class's executed bucket.
	switch class {
	case models.PreferenceActionMapSweep:
		b.MapSweep += n
	case models.PreferenceActionDirtySweep:
		b.DirtySweep += n
	case models.PreferenceActionCentroidRefresh:
		b.Centroid += n
	case models.PreferenceActionMemberRefresh:
		b.MemberRefresh += n
	case models.PreferenceActionRecompute:
		b.Recompute += n
	case models.PreferenceActionMine:
		b.Mine += n
	case models.PreferenceActionProposalEnrich:
		b.Enrich += n
	case models.PreferenceActionAutoApprove:
		b.AutoApprove += n
	case models.PreferenceActionMergeSuggest:
		b.MergeSuggest += n
	case models.PreferenceActionSnapshot:
		b.Baseline += n
	}
}

type prefRunHistoryEntry struct {
	ID             uuid.UUID      `json:"id"`
	StartedAt      time.Time      `json:"started_at"`
	Trigger        string         `json:"trigger"`
	Mode           string         `json:"mode"`
	Status         string         `json:"status"`
	Headline       string         `json:"headline"`
	Buckets        prefRunBuckets `json:"buckets"`
	CoverageBefore float64        `json:"coverage_before"`
	CoverageAfter  float64        `json:"coverage_after"`
}

type prefCoveragePoint struct {
	StartedAt       time.Time `json:"started_at"`
	ForyouPct       float64   `json:"foryou_pct"`
	NewsPct         float64   `json:"news_pct"`
	StoryPct        float64   `json:"story_pct"`
	UnmappedBacklog int64     `json:"unmapped_backlog"`
	Pending         int64     `json:"pending"`
	QueueDepth      int64     `json:"queue_depth"`
}

type prefClassBreaker struct {
	Class      string    `json:"class"`
	Tripped    bool      `json:"tripped"`
	LastStatus string    `json:"last_status"`
	At         time.Time `json:"at"`
}

type prefTrustPoint struct {
	Week       time.Time `json:"week"`
	Decisions  int64     `json:"decisions"`
	Agreements int64     `json:"agreements"`
}

type prefAutoApprovedTopic struct {
	ID          string    `json:"id"`
	Slug        string    `json:"slug"`
	LabelEN     string    `json:"label_en"`
	Active      bool      `json:"active"`
	MemberCount int       `json:"member_count"`
	CreatedAt   time.Time `json:"created_at"`
}

type prefLatestFlow struct {
	Buckets prefRunBuckets `json:"buckets"`
	Observe bool           `json:"observe"`
	RunID   uuid.UUID      `json:"run_id"`
}

// GetPreferenceAutopilotInsights handles GET /admin/preferences/autopilot/insights.
func GetPreferenceAutopilotInsights(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	tenant := principal.TenantID
	policy := loadPreferenceAutopilotPolicy(db, tenant)

	var runs []models.PreferenceAutopilotRun
	if err := db.Where("tenant_id = ?", tenant).Order("started_at DESC").Limit(preferenceInsightsRunWindow).Find(&runs).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to load autopilot runs", Code: "QUERY_FAILED"})
		return
	}

	// ONE grouped aggregate over the ledger for the whole window.
	runIDs := make([]uint, 0, len(runs))
	for _, r := range runs {
		runIDs = append(runIDs, r.ID)
	}
	type groupedRow struct {
		RunID       uint
		ActionClass string
		Status      string
		Guardrail   string
		N           int
	}
	var grouped []groupedRow
	if len(runIDs) > 0 {
		if err := db.Model(&models.PreferenceAutopilotAction{}).
			Select("run_id, action_class, status, guardrail, COUNT(*) AS n").
			Where("tenant_id = ? AND run_id IN ?", tenant, runIDs).
			Group("run_id, action_class, status, guardrail").
			Scan(&grouped).Error; err != nil {
			c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to load autopilot outcomes", Code: "QUERY_FAILED"})
			return
		}
	}
	bucketsByRun := map[uint]*prefRunBuckets{}
	guardrailTotals := map[string]int{}
	outcomeTotals := map[string]int{}
	for _, g := range grouped {
		b := bucketsByRun[g.RunID]
		if b == nil {
			b = &prefRunBuckets{}
			bucketsByRun[g.RunID] = b
		}
		bucketPreferenceAction(b, g.ActionClass, g.Status, g.N)
		outcomeTotals[g.Status] += g.N
		if g.Guardrail != "" {
			guardrailTotals[g.Guardrail] += g.N
		}
	}

	// run_history (newest-first as stored; Console reverses for the timeline) +
	// coverage_series (oldest-first) from persisted snapshots. Unparseable/legacy
	// snapshots are SKIPPED — zeros would fake a coverage crash on the chart.
	history := make([]prefRunHistoryEntry, 0, len(runs))
	series := make([]prefCoveragePoint, 0, len(runs))
	var latest *prefLatestFlow
	for _, r := range runs {
		entry := prefRunHistoryEntry{
			ID: r.PublicID, StartedAt: r.StartedAt, Trigger: r.Trigger,
			Mode: r.Mode, Status: r.Status, Headline: r.Headline,
		}
		if b := bucketsByRun[r.ID]; b != nil {
			entry.Buckets = *b
		}
		if before, ok := parsePreferenceSnapshotJSON(r.StatsBefore); ok {
			entry.CoverageBefore = before.ForyouCoveragePct
		}
		if after, ok := parsePreferenceSnapshotJSON(r.StatsAfter); ok {
			entry.CoverageAfter = after.ForyouCoveragePct
			series = append(series, prefCoveragePoint{
				StartedAt: r.StartedAt,
				ForyouPct: after.ForyouCoveragePct, NewsPct: after.NewsCoveragePct, StoryPct: after.StoryCoveragePct,
				UnmappedBacklog: after.UnmappedBacklog, Pending: after.PendingProposals, QueueDepth: after.RecomputeQueueDepth,
			})
		}
		if latest == nil && (r.Status == models.PreferenceAutopilotRunStatusCompleted || r.Status == models.PreferenceAutopilotRunStatusPartial) {
			flow := prefLatestFlow{Observe: r.Mode != models.PreferenceAutopilotModeSafeAuto, RunID: r.PublicID}
			if b := bucketsByRun[r.ID]; b != nil {
				flow.Buckets = *b
			}
			latest = &flow
		}
		history = append(history, entry)
	}
	// series was appended newest-first; reverse to oldest-first for charting.
	for i, j := 0, len(series)-1; i < j; i, j = i+1, j-1 {
		series[i], series[j] = series[j], series[i]
	}

	// Class breakers: last recorded action per class (mirrors classBreakerTripped).
	var lastPerClass []models.PreferenceAutopilotAction
	if err := db.Raw(`
		SELECT DISTINCT ON (action_class) *
		FROM preference_autopilot_actions
		WHERE tenant_id = ?
		ORDER BY action_class, id DESC
	`, tenant).Scan(&lastPerClass).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to load autopilot guardrails", Code: "QUERY_FAILED"})
		return
	}
	breakers := make([]prefClassBreaker, 0, len(lastPerClass))
	for _, a := range lastPerClass {
		breakers = append(breakers, prefClassBreaker{
			Class:      a.ActionClass,
			Tripped:    a.Status == models.PreferenceActionStatusError || a.Status == models.PreferenceActionStatusBaselineError,
			LastStatus: a.Status,
			At:         a.StartedAt,
		})
	}

	// Trust series: weekly human decisions vs agreements (same FILTERs as
	// computePreferenceTrust, incl. the autopilot-resolver exclusion).
	var trustSeries []prefTrustPoint
	if err := db.Raw(`
		SELECT date_trunc('week', resolved_at) AS week,
		       COUNT(*) FILTER (WHERE predicted_verdict IN ('high_confidence','suggest_reject') AND status IN ('approved','rejected')) AS decisions,
		       COUNT(*) FILTER (WHERE (predicted_verdict = 'high_confidence' AND status = 'approved') OR (predicted_verdict = 'suggest_reject' AND status = 'rejected')) AS agreements
		FROM topic_proposals
		WHERE tenant_id = ? AND prediction_version <> '' AND resolved_at IS NOT NULL
		  AND COALESCE(resolved_by, '') <> ?
		GROUP BY 1 ORDER BY 1 ASC
	`, tenant, models.PreferenceAutopilotResolver).Scan(&trustSeries).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to load autopilot trust history", Code: "QUERY_FAILED"})
		return
	}

	// Auto-approved quarantine list (incl. inactive so reverted rows show as such).
	var autoTopics []models.Topic
	if err := db.Where("tenant_id = ? AND created_from = ?", tenant, models.PreferenceCreatedFromAutopilot).
		Order("created_at DESC").Limit(20).Find(&autoTopics).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to load auto-approved topics", Code: "QUERY_FAILED"})
		return
	}
	autoApproved := make([]prefAutoApprovedTopic, 0, len(autoTopics))
	for _, t := range autoTopics {
		autoApproved = append(autoApproved, prefAutoApprovedTopic{
			ID: t.PublicID.String(), Slug: t.Slug, LabelEN: t.LabelEN,
			Active: t.Active, MemberCount: t.MemberCount, CreatedAt: t.CreatedAt,
		})
	}

	c.JSON(http.StatusOK, gin.H{"data": gin.H{
		"run_history":      history,
		"coverage_series":  series,
		"coverage_floors":  gin.H{"foryou": policy.CoverageFloorForyouPct, "news": policy.CoverageFloorNewsPct, "story": policy.CoverageFloorStoryPct},
		"guardrail_totals": guardrailTotals,
		"outcome_totals":   outcomeTotals,
		"latest_flow":      latest,
		"class_breakers":   breakers,
		"trust_series":     trustSeries,
		"auto_approved":    autoApproved,
	}})
}

func parsePreferenceSnapshotJSON(raw []byte) (preferenceSnapshot, bool) {
	var snap preferenceSnapshot
	if len(raw) == 0 {
		return snap, false
	}
	if err := json.Unmarshal(raw, &snap); err != nil {
		return snap, false
	}
	// A legacy/foreign payload that unmarshals but carries no coverage keys shows
	// as all-zero with no flip gates — treat as unparseable.
	if snap.FlipGates == nil && snap.ForyouCoveragePct == 0 && snap.ActiveTopics == 0 {
		return snap, false
	}
	return snap, true
}

// ----------------------------------------------------------------
// Cross-run filterable ledger (GET /admin/preferences/autopilot/actions)
// ----------------------------------------------------------------

// ListPreferenceAutopilotActions handles the deep ledger explorer. Offset paging:
// the table is per-tenant, indexed started_at DESC, and operators page shallowly.
func ListPreferenceAutopilotActions(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	q := db.Model(&models.PreferenceAutopilotAction{}).Where("tenant_id = ?", principal.TenantID)
	if v := strings.TrimSpace(c.Query("action_class")); v != "" {
		q = q.Where("action_class = ?", v)
	}
	if v := strings.TrimSpace(c.Query("status")); v != "" {
		q = q.Where("status = ?", v)
	}
	if v := strings.TrimSpace(c.Query("guardrail")); v != "" {
		q = q.Where("guardrail = ?", v)
	}
	if v := strings.TrimSpace(c.Query("subject_type")); v != "" {
		q = q.Where("subject_type = ?", v)
	}
	if v := strings.TrimSpace(c.Query("subject_ref")); v != "" {
		q = q.Where("subject_ref = ?", v)
	}
	if v := strings.TrimSpace(c.Query("since")); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			q = q.Where("started_at >= ?", t)
		}
	}
	if v := strings.TrimSpace(c.Query("until")); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			q = q.Where("started_at <= ?", t)
		}
	}
	limit := boundedLimit(c.Query("limit"), 50, 200)
	offset, _ := strconv.Atoi(c.DefaultQuery("offset", "0"))
	if offset < 0 {
		offset = 0
	}

	var actions []models.PreferenceAutopilotAction
	// Fetch limit+1 to compute has_more without a COUNT.
	if err := q.Order("started_at DESC, id DESC").Offset(offset).Limit(limit + 1).Find(&actions).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to query ledger", Code: "QUERY_FAILED"})
		return
	}
	hasMore := len(actions) > limit
	if hasMore {
		actions = actions[:limit]
	}

	// Attach run public ids so the explorer can deep-link into a run.
	runIDSet := map[uint]bool{}
	for _, a := range actions {
		runIDSet[a.RunID] = true
	}
	runPublic := map[uint]uuid.UUID{}
	if len(runIDSet) > 0 {
		ids := make([]uint, 0, len(runIDSet))
		for id := range runIDSet {
			ids = append(ids, id)
		}
		var runRows []models.PreferenceAutopilotRun
		_ = db.Select("id, public_id").Where("id IN ?", ids).Find(&runRows).Error
		for _, r := range runRows {
			runPublic[r.ID] = r.PublicID
		}
	}
	type ledgerItem struct {
		models.PreferenceAutopilotAction
		RunPublicID uuid.UUID `json:"run_id"`
	}
	items := make([]ledgerItem, 0, len(actions))
	for _, a := range actions {
		items = append(items, ledgerItem{PreferenceAutopilotAction: a, RunPublicID: runPublic[a.RunID]})
	}

	c.JSON(http.StatusOK, gin.H{"data": gin.H{
		"items": items, "limit": limit, "offset": offset, "has_more": hasMore,
	}})
}

// ----------------------------------------------------------------
// Recompute-queue ops + cursor reset
// ----------------------------------------------------------------

// ListPreferenceRecomputeQueue handles GET /admin/preferences/autopilot/recompute-queue.
func ListPreferenceRecomputeQueue(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	limit := boundedLimit(c.Query("limit"), 50, 200)
	var total int64
	db.Model(&models.PreferenceAffinityRecomputeQueue{}).Where("tenant_id = ?", principal.TenantID).Count(&total)
	var items []models.PreferenceAffinityRecomputeQueue
	// updated_at ASC = the runner's drain order, so the list reads as "next up".
	if err := db.Where("tenant_id = ?", principal.TenantID).Order("updated_at ASC").Limit(limit).Find(&items).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to list queue", Code: "QUERY_FAILED"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"items": items, "total": total}})
}

// RequeuePreferenceRecompute handles POST /admin/preferences/autopilot/recompute-queue/requeue.
func RequeuePreferenceRecompute(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var req struct {
		UserID string `json:"user_id"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid request", Code: "INVALID_REQUEST"})
		return
	}
	uid, err := uuid.Parse(strings.TrimSpace(req.UserID))
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid user id", Code: "INVALID_ID"})
		return
	}
	if err := enqueueAffinityRecompute(db, principal.TenantID, uid, models.PreferenceRecomputeReasonManual); err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Requeue failed", Code: "REQUEUE_FAILED"})
		return
	}
	// A manual requeue is a fresh instruction — clear the retry/error residue.
	_ = db.Model(&models.PreferenceAffinityRecomputeQueue{}).
		Where("tenant_id = ? AND user_id = ?", principal.TenantID, uid).
		Updates(map[string]interface{}{"attempts": 0, "last_error": "", "updated_at": time.Now()}).Error
	writeCirculationAudit(db, principal, "preferences.autopilot.recompute_requeue", principal.TenantID, map[string]interface{}{"user_id": uid.String()})
	c.JSON(http.StatusOK, gin.H{"message": "User queued for recompute"})
}

// DeletePreferenceRecomputeRow handles DELETE /admin/preferences/autopilot/recompute-queue/:user_id.
func DeletePreferenceRecomputeRow(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	uid, err := uuid.Parse(c.Param("user_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid user id", Code: "INVALID_ID"})
		return
	}
	if err := db.Where("tenant_id = ? AND user_id = ?", principal.TenantID, uid).
		Delete(&models.PreferenceAffinityRecomputeQueue{}).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Delete failed", Code: "DELETE_FAILED"})
		return
	}
	writeCirculationAudit(db, principal, "preferences.autopilot.recompute_clear", principal.TenantID, map[string]interface{}{"user_id": uid.String()})
	c.JSON(http.StatusOK, gin.H{"message": "Queue row cleared"})
}

// preferenceCursorColumns whitelists resettable checkpoint columns.
var preferenceCursorColumns = map[string]string{
	"item_map":    "item_map_cursor",
	"story_map":   "story_map_cursor",
	"dirty_item":  "dirty_item_cursor",
	"dirty_story": "dirty_story_cursor",
}

// ResetPreferenceCursors handles POST /admin/preferences/autopilot/cursors/reset.
// Safe by design: cursors only control where the bounded sweeps RESUME; zeroing
// them means "re-sweep from the head", never data loss.
func ResetPreferenceCursors(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var req struct {
		Cursors []string `json:"cursors"` // empty = all
	}
	_ = c.ShouldBindJSON(&req)
	targets := req.Cursors
	if len(targets) == 0 {
		targets = []string{"item_map", "story_map", "dirty_item", "dirty_story"}
	}
	updates := map[string]interface{}{"updated_at": time.Now()}
	reset := make([]string, 0, len(targets))
	for _, t := range targets {
		col, ok := preferenceCursorColumns[strings.TrimSpace(t)]
		if !ok {
			c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Unknown cursor: " + t, Code: "INVALID_CURSOR"})
			return
		}
		updates[col] = 0
		reset = append(reset, t)
	}
	if err := db.Model(&models.PreferenceAutopilotPolicy{}).Where("tenant_id = ?", principal.TenantID).
		Updates(updates).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Cursor reset failed", Code: "RESET_FAILED"})
		return
	}
	writeCirculationAudit(db, principal, "preferences.autopilot.cursors_reset", principal.TenantID, map[string]interface{}{"cursors": reset})
	c.JSON(http.StatusOK, gin.H{"message": "Cursors reset", "data": gin.H{"cursors": reset}})
}
