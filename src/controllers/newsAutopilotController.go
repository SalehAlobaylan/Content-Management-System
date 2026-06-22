package controllers

import (
	"bytes"
	"content-management-system/src/models"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	autopilotRunStatusRunning   = "running"
	autopilotRunStatusCompleted = "completed"
	autopilotRunStatusPartial   = "partial"
	autopilotRunStatusFailed    = "failed"

	autopilotActionStatusRunning = "running"
	autopilotActionStatusSuccess = "success"
	autopilotActionStatusSkipped = "skipped"
	autopilotActionStatusError   = "error"
)

type autopilotQueueStat struct {
	Queue     string `json:"queue"`
	Waiting   int    `json:"waiting"`
	Active    int    `json:"active"`
	Completed int    `json:"completed"`
	Failed    int    `json:"failed"`
	Delayed   int    `json:"delayed"`
}

type autopilotSnapshotSignal struct {
	Window     string `json:"window"`
	BuiltAt    string `json:"built_at,omitempty"`
	Dirty      bool   `json:"dirty"`
	AgeSeconds int64  `json:"age_seconds,omitempty"`
}

type autopilotHealthSignal struct {
	State                  string                    `json:"state"`
	AggregationReachable   bool                      `json:"aggregation_reachable"`
	AggregationError       string                    `json:"aggregation_error,omitempty"`
	QueueDepth             int                       `json:"queue_depth"`
	MaxQueueDepth          int                       `json:"max_queue_depth"`
	TodayStoryCount        int64                     `json:"today_story_count"`
	TodayCarryoverCount    int64                     `json:"today_carryover_count"`
	TodayCarryoverRatio    float64                   `json:"today_carryover_ratio"`
	ActiveSources          int64                     `json:"active_sources"`
	DueSources             int64                     `json:"due_sources"`
	PendingRecommendations int64                     `json:"pending_recommendations"`
	SourceErrorRate        float64                   `json:"source_error_rate"`
	Snapshots              []autopilotSnapshotSignal `json:"snapshots"`
	Queues                 []autopilotQueueStat      `json:"queues"`
	GeneratedAt            string                    `json:"generated_at"`
}

type autopilotBlockedTool struct {
	Name   string `json:"name"`
	Reason string `json:"reason"`
}

type autopilotRunOptions struct {
	Trigger   string
	ToolScope string
	CreatedBy string
}

type autopilotRunner struct {
	db      *gorm.DB
	run     *models.NewsAutopilotRun
	policy  models.NewsCirculationPolicy
	used    int
	success int
	skipped int
	errors  int
}

type autopilotSettingsRequest struct {
	Enabled          *bool   `json:"autopilot_enabled"`
	Mode             *string `json:"autopilot_mode"`
	IntervalMinutes  *int    `json:"autopilot_interval_minutes"`
	MaxQueueDepth    *int    `json:"autopilot_max_queue_depth"`
	MaxActionsPerRun *int    `json:"autopilot_max_actions_per_run"`
}

type autopilotBoostRequest struct {
	DurationMinutes *int `json:"duration_minutes"`
}

type autopilotRunResponse struct {
	ID           string                       `json:"id"`
	TenantID     string                       `json:"tenant_id"`
	Trigger      string                       `json:"trigger"`
	Mode         string                       `json:"mode"`
	ToolScope    string                       `json:"tool_scope"`
	Status       string                       `json:"status"`
	StartedAt    string                       `json:"started_at"`
	FinishedAt   *string                      `json:"finished_at,omitempty"`
	Summary      string                       `json:"summary,omitempty"`
	HealthBefore json.RawMessage              `json:"health_before,omitempty"`
	HealthAfter  json.RawMessage              `json:"health_after,omitempty"`
	CreatedBy    string                       `json:"created_by,omitempty"`
	Error        string                       `json:"error,omitempty"`
	CreatedAt    string                       `json:"created_at"`
	UpdatedAt    string                       `json:"updated_at"`
	ActionCount  int64                        `json:"action_count,omitempty"`
	Actions      []models.NewsAutopilotAction `json:"actions,omitempty"`
}

func GetCirculationAutopilotStatus(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	policy := loadCirculationPolicy(db, principal.TenantID)
	health := collectAutopilotHealth(db, principal.TenantID, policy)
	health.State = autopilotStateForPolicy(policy, health, time.Now())

	latest, latestActions := latestAutopilotRunSummary(db, principal.TenantID)
	if latestActions == nil {
		latestActions = []models.NewsAutopilotAction{}
	}
	latestRun := mapAutopilotRunResponse(db, latest, nil)
	if latestRun != nil {
		latestRun.HealthBefore = nil
		latestRun.HealthAfter = nil
	}
	allowed, blocked := autopilotToolAccess(policy, health, time.Now())
	c.JSON(http.StatusOK, gin.H{
		"state":          health.State,
		"policy":         policy,
		"health":         health,
		"next_run_at":    autopilotNextRunAt(policy, time.Now()),
		"boost_until":    policy.AutopilotBoostUntil,
		"paused_until":   policy.AutopilotPausedUntil,
		"allowed_tools":  allowed,
		"blocked_tools":  blocked,
		"latest_run":     latestRun,
		"latest_actions": latestActions,
	})
}

func UpdateCirculationAutopilotSettings(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var req autopilotSettingsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid request: " + err.Error(), Code: "INVALID_REQUEST"})
		return
	}

	policy := loadCirculationPolicy(db, principal.TenantID)
	if req.Enabled != nil {
		policy.AutopilotEnabled = *req.Enabled
		if *req.Enabled {
			policy.AutopilotPausedUntil = nil
		} else {
			now := time.Now()
			policy.AutopilotBoostUntil = nil
			policy.AutopilotPausedUntil = &now
		}
	}
	if req.Mode != nil {
		policy.AutopilotMode = strings.TrimSpace(*req.Mode)
	}
	if req.IntervalMinutes != nil {
		policy.AutopilotIntervalMinutes = *req.IntervalMinutes
	}
	if req.MaxQueueDepth != nil {
		policy.AutopilotMaxQueueDepth = *req.MaxQueueDepth
	}
	if req.MaxActionsPerRun != nil {
		policy.AutopilotMaxActionsPerRun = *req.MaxActionsPerRun
	}
	policy.TenantID = principal.TenantID
	policy = sanitizeCirculationPolicy(policy)

	if err := db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "tenant_id"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"autopilot_enabled", "autopilot_mode", "autopilot_interval_minutes",
			"autopilot_boost_until", "autopilot_paused_until", "autopilot_max_queue_depth",
			"autopilot_max_actions_per_run", "updated_at",
		}),
	}).Create(&policy).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to save Autopilot settings", Code: "SAVE_FAILED"})
		return
	}
	writeCirculationAudit(db, principal, "circulation.autopilot.settings", principal.TenantID, map[string]interface{}{
		"enabled": policy.AutopilotEnabled,
		"mode":    policy.AutopilotMode,
	})
	c.JSON(http.StatusOK, policy)
}

func RunCirculationAutopilotNow(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	run, actions, err := runCirculationAutopilot(db, principal.TenantID, autopilotRunOptions{
		Trigger:   "manual",
		ToolScope: models.NewsAutopilotToolScopeCore,
		CreatedBy: principal.Email,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: err.Error(), Code: "AUTOPILOT_FAILED"})
		return
	}
	writeCirculationAudit(db, principal, "circulation.autopilot.run", run.PublicID.String(), map[string]interface{}{
		"status": run.Status,
		"scope":  run.ToolScope,
	})
	c.JSON(http.StatusOK, mapAutopilotRunResponse(db, run, actions))
}

func BoostCirculationAutopilot(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var req autopilotBoostRequest
	_ = c.ShouldBindJSON(&req)
	duration := 120
	if req.DurationMinutes != nil {
		duration = *req.DurationMinutes
	}
	if duration < 15 {
		duration = 15
	}
	if duration > 360 {
		duration = 360
	}
	boostUntil := time.Now().Add(time.Duration(duration) * time.Minute)
	policy := loadCirculationPolicy(db, principal.TenantID)
	policy.AutopilotEnabled = true
	policy.AutopilotMode = models.NewsAutopilotModeSafeAuto
	policy.AutopilotBoostUntil = &boostUntil
	policy.AutopilotPausedUntil = nil
	policy = sanitizeCirculationPolicy(policy)
	if err := saveAutopilotPolicyFields(db, policy); err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to start boost", Code: "SAVE_FAILED"})
		return
	}

	run, actions, err := runCirculationAutopilot(db, principal.TenantID, autopilotRunOptions{
		Trigger:   "boost",
		ToolScope: models.NewsAutopilotToolScopeBoosted,
		CreatedBy: principal.Email,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: err.Error(), Code: "AUTOPILOT_FAILED"})
		return
	}
	writeCirculationAudit(db, principal, "circulation.autopilot.boost", run.PublicID.String(), map[string]interface{}{
		"duration_minutes": duration,
		"boost_until":      boostUntil.Format(time.RFC3339),
		"status":           run.Status,
	})
	c.JSON(http.StatusOK, mapAutopilotRunResponse(db, run, actions))
}

func PauseCirculationAutopilot(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	now := time.Now()
	policy := loadCirculationPolicy(db, principal.TenantID)
	policy.AutopilotEnabled = false
	policy.AutopilotBoostUntil = nil
	policy.AutopilotPausedUntil = &now
	policy = sanitizeCirculationPolicy(policy)
	if err := saveAutopilotPolicyFields(db, policy); err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to pause Autopilot", Code: "SAVE_FAILED"})
		return
	}
	writeCirculationAudit(db, principal, "circulation.autopilot.pause", principal.TenantID, nil)
	status := collectAutopilotHealth(db, principal.TenantID, policy)
	status.State = autopilotStateForPolicy(policy, status, now)
	c.JSON(http.StatusOK, gin.H{"policy": policy, "state": status.State, "health": status})
}

func ListCirculationAutopilotRuns(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "20"))
	if limit < 1 || limit > 100 {
		limit = 20
	}
	var runs []models.NewsAutopilotRun
	db.Where("tenant_id = ?", principal.TenantID).
		Order("started_at DESC").
		Limit(limit).
		Find(&runs)
	out := make([]*autopilotRunResponse, 0, len(runs))
	for _, run := range runs {
		out = append(out, mapAutopilotRunResponse(db, run, nil))
	}
	c.JSON(http.StatusOK, gin.H{"data": out})
}

func GetCirculationAutopilotRun(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	runID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid run ID", Code: "INVALID_ID"})
		return
	}
	var run models.NewsAutopilotRun
	if err := db.Where("tenant_id = ? AND public_id = ?", principal.TenantID, runID).First(&run).Error; err != nil {
		c.JSON(http.StatusNotFound, authErrorResponse{Message: "Autopilot run not found", Code: "NOT_FOUND"})
		return
	}
	var actions []models.NewsAutopilotAction
	db.Where("tenant_id = ? AND run_id = ?", principal.TenantID, run.ID).
		Order("started_at ASC, id ASC").
		Find(&actions)
	c.JSON(http.StatusOK, mapAutopilotRunResponse(db, run, actions))
}

func saveAutopilotPolicyFields(db *gorm.DB, policy models.NewsCirculationPolicy) error {
	return db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "tenant_id"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"autopilot_enabled", "autopilot_mode", "autopilot_interval_minutes",
			"autopilot_boost_until", "autopilot_paused_until", "autopilot_last_run_at",
			"autopilot_max_queue_depth", "autopilot_max_actions_per_run", "updated_at",
		}),
	}).Create(&policy).Error
}

func runCirculationAutopilot(db *gorm.DB, tenantID string, opts autopilotRunOptions) (models.NewsAutopilotRun, []models.NewsAutopilotAction, error) {
	if strings.TrimSpace(tenantID) == "" {
		tenantID = defaultCirculationTenant
	}
	now := time.Now()
	policy := loadCirculationPolicy(db, tenantID)
	scope := opts.ToolScope
	if scope == "" {
		scope = models.NewsAutopilotToolScopeCore
	}
	if policy.AutopilotBoostUntil != nil && policy.AutopilotBoostUntil.After(now) {
		scope = models.NewsAutopilotToolScopeBoosted
	}
	if scope != models.NewsAutopilotToolScopeBoosted {
		scope = models.NewsAutopilotToolScopeCore
	}
	trigger := strings.TrimSpace(opts.Trigger)
	if trigger == "" {
		trigger = "scheduled"
	}
	healthBefore := collectAutopilotHealth(db, tenantID, policy)
	healthBefore.State = autopilotStateForPolicy(policy, healthBefore, now)
	run := models.NewsAutopilotRun{
		TenantID:     tenantID,
		Trigger:      trigger,
		Mode:         policy.AutopilotMode,
		ToolScope:    scope,
		Status:       autopilotRunStatusRunning,
		StartedAt:    now,
		HealthBefore: marshalAutopilotJSON(healthBefore),
		CreatedBy:    opts.CreatedBy,
	}
	if run.CreatedBy == "" {
		run.CreatedBy = "automation"
	}
	if err := db.Create(&run).Error; err != nil {
		return run, nil, err
	}

	runner := &autopilotRunner{db: db, run: &run, policy: policy}
	runner.execute("health.evaluate", "Read feed, source, snapshot, and queue health before acting.", gin.H{
		"scope": scope,
	}, func() (interface{}, error) {
		return healthBefore, nil
	})

	canPull := healthBefore.AggregationReachable && healthBefore.QueueDepth <= policy.AutopilotMaxQueueDepth
	if scope == models.NewsAutopilotToolScopeBoosted {
		if canPull {
			runner.execute("circulation.sweep", "Boosted freshness can force-claim due news sources through Aggregation.", gin.H{
				"tenant_id": tenantID,
			}, func() (interface{}, error) {
				return callAggregationInternalJSON(http.MethodPost, "/internal/circulation/sweep-now", gin.H{"tenant_id": tenantID})
			})
			runner.execute("discovery.sweep", "Boosted freshness can refresh source-finding evidence without approving sources.", nil, func() (interface{}, error) {
				return callAggregationInternalJSON(http.MethodPost, "/internal/discovery/sweep-now", gin.H{})
			})
			runner.execute("source_graph.build", "Boosted freshness can rebuild the source graph for better future suggestions.", nil, func() (interface{}, error) {
				return callAggregationInternalJSON(http.MethodPost, "/internal/discovery/build-graph-now", gin.H{})
			})
		} else {
			reason := "Aggregation is unreachable or queue depth is above the safety limit."
			runner.skip("circulation.sweep", reason, gin.H{"queue_depth": healthBefore.QueueDepth, "max_queue_depth": policy.AutopilotMaxQueueDepth})
			runner.skip("discovery.sweep", reason, nil)
			runner.skip("source_graph.build", reason, nil)
		}
	}

	runner.execute("source_recommendations.generate", "Generate source cadence recommendations and apply only changes allowed by existing guardrails.", nil, func() (interface{}, error) {
		rows, applied := generateSourceRecommendations(db, policy)
		return gin.H{"recommendations": len(rows), "auto_applied": applied}, nil
	})

	runner.execute("snapshots.refresh", "Refresh stale or dirty News snapshots for all circulation windows.", gin.H{
		"force": scope == models.NewsAutopilotToolScopeBoosted,
	}, func() (interface{}, error) {
		return refreshAutopilotSnapshots(db, tenantID, scope == models.NewsAutopilotToolScopeBoosted)
	})

	healthAfter := collectAutopilotHealth(db, tenantID, policy)
	healthAfter.State = autopilotStateForPolicy(policy, healthAfter, time.Now())
	finishedAt := time.Now()
	status := autopilotRunStatusCompleted
	if runner.errors > 0 && runner.success == 0 {
		status = autopilotRunStatusFailed
	} else if runner.errors > 0 || runner.skipped > 0 {
		status = autopilotRunStatusPartial
	}
	summary := fmt.Sprintf("%d tools completed, %d skipped, %d failed", runner.success, runner.skipped, runner.errors)
	errText := ""
	if status == autopilotRunStatusFailed {
		errText = "all executable Autopilot tools failed"
	}
	db.Model(&models.NewsAutopilotRun{}).
		Where("id = ?", run.ID).
		Updates(map[string]interface{}{
			"status":       status,
			"finished_at":  finishedAt,
			"summary":      summary,
			"health_after": marshalAutopilotJSON(healthAfter),
			"error":        errText,
			"updated_at":   finishedAt,
		})
	db.Model(&models.NewsCirculationPolicy{}).
		Where("tenant_id = ?", tenantID).
		Updates(map[string]interface{}{
			"autopilot_last_run_at": finishedAt,
			"updated_at":            finishedAt,
		})
	run.Status = status
	run.FinishedAt = &finishedAt
	run.Summary = summary
	run.HealthAfter = marshalAutopilotJSON(healthAfter)
	run.Error = errText

	var actions []models.NewsAutopilotAction
	db.Where("tenant_id = ? AND run_id = ?", tenantID, run.ID).Order("started_at ASC, id ASC").Find(&actions)
	return run, actions, nil
}

func (r *autopilotRunner) execute(toolName, reason string, input interface{}, fn func() (interface{}, error)) {
	if r.used >= r.policy.AutopilotMaxActionsPerRun {
		r.skip("autopilot.action_limit", "Autopilot action limit reached for this run.", gin.H{
			"blocked_tool": toolName,
			"limit":        r.policy.AutopilotMaxActionsPerRun,
		})
		return
	}
	r.used++
	startedAt := time.Now()
	action := models.NewsAutopilotAction{
		RunID:     r.run.ID,
		TenantID:  r.run.TenantID,
		ToolName:  toolName,
		Status:    autopilotActionStatusRunning,
		Reason:    reason,
		Input:     marshalAutopilotJSON(input),
		StartedAt: startedAt,
	}
	_ = r.db.Create(&action).Error
	output, err := fn()
	finishedAt := time.Now()
	updates := map[string]interface{}{
		"status":      autopilotActionStatusSuccess,
		"output":      marshalAutopilotJSON(output),
		"finished_at": finishedAt,
		"updated_at":  finishedAt,
	}
	if err != nil {
		updates["status"] = autopilotActionStatusError
		updates["error"] = err.Error()
		r.errors++
	} else {
		r.success++
	}
	_ = r.db.Model(&models.NewsAutopilotAction{}).Where("id = ?", action.ID).Updates(updates).Error
}

func (r *autopilotRunner) skip(toolName, reason string, input interface{}) {
	if r.used >= r.policy.AutopilotMaxActionsPerRun {
		return
	}
	r.used++
	now := time.Now()
	action := models.NewsAutopilotAction{
		RunID:      r.run.ID,
		TenantID:   r.run.TenantID,
		ToolName:   toolName,
		Status:     autopilotActionStatusSkipped,
		Reason:     reason,
		Input:      marshalAutopilotJSON(input),
		StartedAt:  now,
		FinishedAt: &now,
	}
	_ = r.db.Create(&action).Error
	r.skipped++
}

func collectAutopilotHealth(db *gorm.DB, tenantID string, policy models.NewsCirculationPolicy) autopilotHealthSignal {
	now := time.Now()
	health := autopilotHealthSignal{
		AggregationReachable: true,
		MaxQueueDepth:        policy.AutopilotMaxQueueDepth,
		GeneratedAt:          now.Format(time.RFC3339),
	}
	stats, err := fetchAggregationQueueStats()
	if err != nil {
		health.AggregationReachable = false
		health.AggregationError = err.Error()
	} else {
		health.Queues = stats
		health.QueueDepth = relevantQueueDepth(stats)
	}

	var metrics struct {
		Stories   int64
		Carryover int64
	}
	circ := circulationContextFromPolicy(policy, models.NewsWindowToday, now)
	metrics.Stories, metrics.Carryover = cheapTodayStoryMetrics(db, tenantID, circ)
	health.TodayStoryCount = metrics.Stories
	health.TodayCarryoverCount = metrics.Carryover
	if metrics.Stories > 0 {
		health.TodayCarryoverRatio = float64(metrics.Carryover) / float64(metrics.Stories)
	}

	sourceBase := db.Model(&models.ContentSource{}).
		Where("tenant_id = ? AND category = ? AND is_active = ?", tenantID, models.SourceCategoryNews, true)
	sourceBase.Count(&health.ActiveSources)
	sourceBase.
		Where("feed_url IS NOT NULL AND feed_url <> ''").
		Where("last_fetched_at IS NULL OR last_fetched_at < (?::timestamp - (GREATEST(fetch_interval_minutes, ?)::integer * interval '1 minute'))",
			now, policy.SourceMinIntervalMinutes).
		Count(&health.DueSources)

	db.Model(&models.SourceCirculationRecommendation{}).
		Where("tenant_id = ? AND applied = false", tenantID).
		Count(&health.PendingRecommendations)

	var totalRuns int64
	var failedRuns int64
	runBase := db.Model(&models.SourceRunTelemetry{}).
		Where("tenant_id = ? AND finished_at > ?", tenantID, now.Add(-24*time.Hour)).
		Where("fetched > 0 OR accepted > 0 OR failed > 0")
	runBase.Count(&totalRuns)
	runBase.Where("failed > 0 AND accepted = 0").Count(&failedRuns)
	if totalRuns > 0 {
		health.SourceErrorRate = float64(failedRuns) / float64(totalRuns)
	}

	windows := []string{models.NewsWindowToday, models.NewsWindowWeek, models.NewsWindowMonth}
	var snaps []models.NewsSnapshot
	db.Select("window", "built_at", "dirty").Where("tenant_id = ? AND \"window\" IN ?", tenantID, windows).Find(&snaps)
	snapshotByWindow := make(map[string]models.NewsSnapshot, len(snaps))
	for _, snap := range snaps {
		snapshotByWindow[snap.Window] = snap
	}
	for _, window := range windows {
		signal := autopilotSnapshotSignal{Window: window}
		if snap, ok := snapshotByWindow[window]; ok {
			signal.BuiltAt = snap.BuiltAt.Format(time.RFC3339)
			signal.Dirty = snap.Dirty
			if !snap.BuiltAt.IsZero() {
				signal.AgeSeconds = int64(now.Sub(snap.BuiltAt).Seconds())
			}
		} else {
			signal.Dirty = true
		}
		health.Snapshots = append(health.Snapshots, signal)
	}
	return health
}

func cheapTodayStoryMetrics(db *gorm.DB, tenantID string, circ circulationContext) (int64, int64) {
	var snap models.NewsSnapshot
	if err := db.Select("slide_count").
		Where("tenant_id = ? AND \"window\" = ?", tenantID, models.NewsWindowToday).
		First(&snap).Error; err == nil && snap.SlideCount > 0 {
		if snap.SlideCount >= circ.Policy.MinTodayStories {
			return int64(snap.SlideCount), 0
		}
	}

	var primaryStories int64
	db.Model(&models.Topic{}).
		Where("tenant_id = ? AND last_member_at >= ?", tenantID, circ.Window.PrimaryStart).
		Count(&primaryStories)

	carryover := int64(0)
	if primaryStories < int64(circ.Policy.MinTodayStories) {
		db.Model(&models.Topic{}).
			Where("tenant_id = ? AND last_member_at >= ? AND last_member_at < ?", tenantID, circ.Window.QueryStart, circ.Window.PrimaryStart).
			Count(&carryover)
		needed := int64(circ.Policy.MinTodayStories) - primaryStories
		if carryover > needed {
			carryover = needed
		}
	}
	return primaryStories + carryover, carryover
}

func refreshAutopilotSnapshots(db *gorm.DB, tenantID string, force bool) (gin.H, error) {
	queued := []string{}
	skipped := []string{}
	for _, window := range []string{models.NewsWindowToday, models.NewsWindowWeek, models.NewsWindowMonth} {
		needsRefresh := force
		var snap models.NewsSnapshot
		if err := db.Where("tenant_id = ? AND \"window\" = ?", tenantID, window).First(&snap).Error; err != nil {
			needsRefresh = true
		} else if snap.Dirty || time.Since(snap.BuiltAt) > newsSnapshotTTL {
			needsRefresh = true
		}
		if !needsRefresh {
			skipped = append(skipped, window)
			continue
		}
		startSnapshotRebuild(db, tenantID, window)
		queued = append(queued, window)
	}
	return gin.H{"queued": queued, "skipped": skipped}, nil
}

func autopilotStateForPolicy(policy models.NewsCirculationPolicy, health autopilotHealthSignal, now time.Time) string {
	if !policy.AutopilotEnabled {
		return models.NewsAutopilotStatePaused
	}
	if !health.AggregationReachable {
		return models.NewsAutopilotStateDegraded
	}
	if health.QueueDepth > policy.AutopilotMaxQueueDepth {
		return models.NewsAutopilotStateSafety
	}
	if policy.AutopilotBoostUntil != nil && policy.AutopilotBoostUntil.After(now) {
		return models.NewsAutopilotStateBoosting
	}
	return models.NewsAutopilotStateWatching
}

func autopilotNextRunAt(policy models.NewsCirculationPolicy, now time.Time) *time.Time {
	if !policy.AutopilotEnabled {
		return nil
	}
	if policy.AutopilotLastRunAt == nil {
		return &now
	}
	next := policy.AutopilotLastRunAt.Add(time.Duration(policy.AutopilotIntervalMinutes) * time.Minute)
	return &next
}

func autopilotToolAccess(policy models.NewsCirculationPolicy, health autopilotHealthSignal, now time.Time) ([]string, []autopilotBlockedTool) {
	allowed := []string{
		"health.evaluate",
		"source_recommendations.generate",
		"snapshots.refresh",
	}
	blocked := []autopilotBlockedTool{
		{Name: "story_overrides.write", Reason: "Autopilot V1 does not create editorial exceptions."},
		{Name: "sources.approve", Reason: "Source approvals stay human-gated."},
		{Name: "topics.recluster", Reason: "Structural story correction remains in topic tools."},
		{Name: "ranking_weights.write", Reason: "Autopilot can inspect ranking outcomes but cannot mutate ranking weights."},
	}
	boosting := policy.AutopilotBoostUntil != nil && policy.AutopilotBoostUntil.After(now)
	if boosting {
		if health.AggregationReachable && health.QueueDepth <= policy.AutopilotMaxQueueDepth {
			allowed = append(allowed, "circulation.sweep", "discovery.sweep", "source_graph.build")
		} else {
			reason := "Blocked by safety: Aggregation is unreachable or queue depth is above the configured limit."
			blocked = append(blocked,
				autopilotBlockedTool{Name: "circulation.sweep", Reason: reason},
				autopilotBlockedTool{Name: "discovery.sweep", Reason: reason},
				autopilotBlockedTool{Name: "source_graph.build", Reason: reason},
			)
		}
	}
	return allowed, blocked
}

func latestAutopilotRunSummary(db *gorm.DB, tenantID string) (models.NewsAutopilotRun, []models.NewsAutopilotAction) {
	var run models.NewsAutopilotRun
	if err := db.Select("id", "public_id", "tenant_id", "trigger", "mode", "tool_scope", "status", "started_at", "finished_at", "summary", "created_by", "error", "created_at", "updated_at").
		Where("tenant_id = ?", tenantID).Order("started_at DESC").First(&run).Error; err != nil {
		return models.NewsAutopilotRun{}, nil
	}
	var actions []models.NewsAutopilotAction
	db.Select("id", "public_id", "run_id", "tenant_id", "tool_name", "status", "reason", "error", "started_at", "finished_at", "created_at", "updated_at").
		Where("tenant_id = ? AND run_id = ?", tenantID, run.ID).
		Order("started_at ASC, id ASC").
		Limit(20).
		Find(&actions)
	return run, actions
}

func mapAutopilotRunResponse(db *gorm.DB, run models.NewsAutopilotRun, actions []models.NewsAutopilotAction) *autopilotRunResponse {
	if run.ID == 0 {
		return nil
	}
	var actionCount int64
	if actions == nil {
		db.Model(&models.NewsAutopilotAction{}).Where("run_id = ?", run.ID).Count(&actionCount)
	} else {
		actionCount = int64(len(actions))
	}
	var finished *string
	if run.FinishedAt != nil {
		value := run.FinishedAt.Format(time.RFC3339)
		finished = &value
	}
	return &autopilotRunResponse{
		ID:           run.PublicID.String(),
		TenantID:     run.TenantID,
		Trigger:      run.Trigger,
		Mode:         run.Mode,
		ToolScope:    run.ToolScope,
		Status:       run.Status,
		StartedAt:    run.StartedAt.Format(time.RFC3339),
		FinishedAt:   finished,
		Summary:      run.Summary,
		HealthBefore: json.RawMessage(run.HealthBefore),
		HealthAfter:  json.RawMessage(run.HealthAfter),
		CreatedBy:    run.CreatedBy,
		Error:        run.Error,
		CreatedAt:    run.CreatedAt.Format(time.RFC3339),
		UpdatedAt:    run.UpdatedAt.Format(time.RFC3339),
		ActionCount:  actionCount,
		Actions:      actions,
	}
}

func marshalAutopilotJSON(value interface{}) datatypes.JSON {
	if value == nil {
		return nil
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	return datatypes.JSON(raw)
}

func relevantQueueDepth(stats []autopilotQueueStat) int {
	relevant := map[string]bool{
		"fetch-queue":            true,
		"normalize-queue":        true,
		"news-circulation-queue": true,
		"discovery-queue":        true,
		"discovery-sweep-queue":  true,
		"source-graph-queue":     true,
	}
	total := 0
	for _, stat := range stats {
		if !relevant[stat.Queue] {
			continue
		}
		total += stat.Waiting + stat.Active + stat.Delayed
	}
	return total
}

func fetchAggregationQueueStats() ([]autopilotQueueStat, error) {
	body, status, err := callAggregationInternal(http.MethodGet, "/internal/queues", nil)
	if err != nil {
		return nil, err
	}
	if status < http.StatusOK || status >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("aggregation queues responded with status %d", status)
	}
	var wrapped struct {
		Data []autopilotQueueStat `json:"data"`
	}
	if err := json.Unmarshal(body, &wrapped); err != nil {
		return nil, err
	}
	return wrapped.Data, nil
}

func callAggregationInternalJSON(method, path string, payload interface{}) (interface{}, error) {
	body, status, err := callAggregationInternal(method, path, payload)
	if err != nil {
		return nil, err
	}
	var decoded interface{}
	if len(body) > 0 {
		_ = json.Unmarshal(body, &decoded)
	}
	if status < http.StatusOK || status >= http.StatusMultipleChoices {
		return decoded, fmt.Errorf("aggregation internal %s failed with status %d", path, status)
	}
	if decoded == nil {
		return gin.H{"status": status}, nil
	}
	return decoded, nil
}

func callAggregationInternal(method, path string, payload interface{}) ([]byte, int, error) {
	base := strings.TrimRight(strings.TrimSpace(os.Getenv("AGGREGATION_BASE_URL")), "/")
	if base == "" {
		return nil, 0, fmt.Errorf("aggregation service URL is not configured")
	}
	token := aggregationInternalServiceToken()
	if token == "" {
		return nil, 0, fmt.Errorf("aggregation service token is not configured")
	}
	var body io.Reader
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			return nil, 0, err
		}
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, base+path, body)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, err
	}
	return respBody, resp.StatusCode, nil
}

func aggregationInternalServiceToken() string {
	if token := strings.TrimSpace(os.Getenv("AGGREGATION_SERVICE_TOKEN")); token != "" {
		return token
	}
	if token := strings.TrimSpace(os.Getenv("INTERNAL_SERVICE_TOKEN")); token != "" {
		return token
	}
	if token := strings.TrimSpace(os.Getenv("SERVICE_AUTH_TOKEN")); token != "" {
		return token
	}
	return strings.TrimSpace(os.Getenv("CMS_SERVICE_TOKEN"))
}
