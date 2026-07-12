package controllers

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"content-management-system/src/models"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// Real User Experience — admin read/mutation cockpit surface. Registered on the
// admin group under the existing `feed` permission resource (plan §13).

// GET /admin/experience/status — single cockpit bootstrap call.
func GetExperienceStatus(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	tenantID := principal.TenantID
	policy := getOrCreateExperiencePolicy(db, tenantID)

	var latestRun models.ExperienceEvaluationRun
	db.Where("tenant_id = ?", tenantID).Order("started_at DESC").First(&latestRun)

	var openIncidents int64
	db.Model(&models.ExperienceIncident{}).
		Where("tenant_id = ? AND status IN ?", tenantID, []string{models.RuxIncidentOpen, models.RuxIncidentRecovering}).
		Count(&openIncidents)

	now := time.Now()
	fresh := isTelemetryFresh(db, tenantID, policy, now)

	c.JSON(http.StatusOK, gin.H{"data": gin.H{
		"policy":          policy,
		"latest_run":      latestRun,
		"telemetry_fresh": fresh,
		"open_incidents":  openIncidents,
		"surface_verdicts": latestRunVerdicts(&latestRun),
	}})
}

func latestRunVerdicts(run *models.ExperienceEvaluationRun) any {
	if run == nil || len(run.SurfaceVerdicts) == 0 {
		return map[string]any{}
	}
	return run.SurfaceVerdicts
}

// GET /admin/experience/policy
func GetExperiencePolicy(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	c.JSON(http.StatusOK, gin.H{"data": getOrCreateExperiencePolicy(db, principal.TenantID)})
}

// PUT /admin/experience/policy — Observe policy tuning.
func UpdateExperiencePolicy(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	policy := getOrCreateExperiencePolicy(db, principal.TenantID)

	var body struct {
		IngestEnabled             *bool `json:"ingest_enabled"`
		EvaluationEnabled         *bool `json:"evaluation_enabled"`
		MinSampleFloor            *int  `json:"min_sample_floor"`
		ConfirmWindows            *int  `json:"confirm_windows"`
		ResolveWindows            *int  `json:"resolve_windows"`
		TelemetryFreshnessMinutes *int  `json:"telemetry_freshness_minutes"`
		RawRetentionDays          *int  `json:"raw_retention_days"`
		MaxReleaseCohorts         *int  `json:"max_release_cohorts"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}
	if body.IngestEnabled != nil {
		policy.IngestEnabled = *body.IngestEnabled
	}
	if body.EvaluationEnabled != nil {
		policy.EvaluationEnabled = *body.EvaluationEnabled
	}
	if body.MinSampleFloor != nil && *body.MinSampleFloor >= 0 {
		policy.MinSampleFloor = *body.MinSampleFloor
	}
	if body.ConfirmWindows != nil && *body.ConfirmWindows >= 1 {
		policy.ConfirmWindows = *body.ConfirmWindows
	}
	if body.ResolveWindows != nil && *body.ResolveWindows >= 1 {
		policy.ResolveWindows = *body.ResolveWindows
	}
	if body.TelemetryFreshnessMinutes != nil && *body.TelemetryFreshnessMinutes >= 1 {
		policy.TelemetryFreshnessMinutes = *body.TelemetryFreshnessMinutes
	}
	if body.RawRetentionDays != nil && *body.RawRetentionDays >= 1 {
		policy.RawRetentionDays = *body.RawRetentionDays
	}
	if body.MaxReleaseCohorts != nil && *body.MaxReleaseCohorts >= 1 {
		policy.MaxReleaseCohorts = *body.MaxReleaseCohorts
	}
	db.Save(&policy)
	c.JSON(http.StatusOK, gin.H{"data": policy})
}

// POST /admin/experience/run — evaluate already-ingested data now. This never
// generates synthetic user events.
func RunExperienceNow(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	if !tryStartExperienceRun() {
		c.JSON(http.StatusConflict, gin.H{"error": "an evaluation run is already in progress"})
		return
	}
	defer finishExperienceRunLock()
	run, err := RunExperienceEvaluation(db, principal.TenantID, "manual")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error(), "run": run})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": run})
}

// POST /admin/experience/pause — set/clear paused_until (stops the scheduler,
// keeps ingestion + policy).
func PauseExperienceSchedule(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var body struct {
		Minutes *int `json:"minutes"` // nil or 0 clears the pause
	}
	_ = c.ShouldBindJSON(&body)
	policy := getOrCreateExperiencePolicy(db, principal.TenantID)
	if body.Minutes == nil || *body.Minutes <= 0 {
		policy.PausedUntil = nil
	} else {
		until := time.Now().Add(time.Duration(*body.Minutes) * time.Minute)
		policy.PausedUntil = &until
	}
	db.Save(&policy)
	c.JSON(http.StatusOK, gin.H{"data": policy})
}

// GET /admin/experience/runs (+ /:id)
func ListExperienceRuns(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	limit := parseLimit(c.Query("limit"), 25, 100)
	var runs []models.ExperienceEvaluationRun
	db.Where("tenant_id = ?", principal.TenantID).Order("started_at DESC").Limit(limit).Find(&runs)
	c.JSON(http.StatusOK, gin.H{"data": runs})
}

// GET /admin/experience/metrics?surface=foryou — latest per-SLI values + verdict
// for the last closed bucket (drives the cockpit's "is it healthy" section).
func GetExperienceMetrics(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	tenantID := principal.TenantID
	policy := getOrCreateExperiencePolicy(db, tenantID)
	now := time.Now().UTC()
	bucket := floorBucket(now).Add(-rollupBucketDuration)
	fresh := isTelemetryFresh(db, tenantID, policy, now)

	out := map[string]any{}
	for _, surface := range enabledSurfaces(policy) {
		statuses := globalSLIStatuses(db, tenantID, policy, surface, bucket)
		out[surface] = gin.H{"verdict": surfaceVerdict(statuses, fresh), "slis": statuses}
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"bucket": bucketLabel(bucket), "telemetry_fresh": fresh, "surfaces": out}})
}

// GET /admin/experience/incidents (+ /:id)
func ListExperienceIncidents(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	q := db.Where("tenant_id = ?", principal.TenantID)
	if status := c.Query("status"); status != "" {
		q = q.Where("status = ?", status)
	} else {
		q = q.Where("status IN ?", []string{models.RuxIncidentOpen, models.RuxIncidentRecovering})
	}
	var incidents []models.ExperienceIncident
	q.Order("last_seen_at DESC").Limit(parseLimit(c.Query("limit"), 50, 200)).Find(&incidents)
	c.JSON(http.StatusOK, gin.H{"data": incidents})
}

// POST /admin/experience/incidents/:id/close
func CloseExperienceIncident(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var body struct {
		ReasonClass string `json:"reason_class"`
		Notes       string `json:"notes"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}
	valid := map[string]bool{"resolved_externally": true, "expected_behavior": true, "false_positive": true, "accepted_risk": true}
	if !valid[body.ReasonClass] {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid reason_class"})
		return
	}
	var inc models.ExperienceIncident
	if err := db.Where("public_id = ? AND tenant_id = ?", c.Param("id"), principal.TenantID).First(&inc).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "incident not found"})
		return
	}
	now := time.Now()
	inc.Status = models.RuxIncidentClosed
	inc.ResolvedAt = &now
	inc.ClosedBy = principal.Email
	inc.CloseReasonClass = body.ReasonClass
	inc.CloseNotes = body.Notes
	db.Save(&inc)
	writeExperienceAction(db, principal.TenantID, nil, &inc.ID, "incident_closed",
		"Closed by "+principal.Email+" as "+body.ReasonClass, sliStatus{Metric: inc.MetricKey}, "", now)
	c.JSON(http.StatusOK, gin.H{"data": inc})
}

// GET /admin/experience/actions — the ledger.
func ListExperienceActions(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var actions []models.ExperienceAction
	db.Where("tenant_id = ?", principal.TenantID).Order("created_at DESC").
		Limit(parseLimit(c.Query("limit"), 50, 200)).Find(&actions)
	c.JSON(http.StatusOK, gin.H{"data": actions})
}

// POST /admin/experience/suppressions — create a bounded TTL mute.
func CreateExperienceSuppression(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var body struct {
		MetricKey  string `json:"metric_key"`
		Surface    string `json:"surface"`
		Reason     string `json:"reason"`
		TTLMinutes int    `json:"ttl_minutes"`
	}
	if err := c.ShouldBindJSON(&body); err != nil || strings.TrimSpace(body.Reason) == "" || body.TTLMinutes <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "reason and positive ttl_minutes required"})
		return
	}
	now := time.Now()
	sup := models.ExperienceSuppression{
		TenantID: principal.TenantID, MetricKey: body.MetricKey, Surface: body.Surface,
		Reason: body.Reason, StartsAt: now, ExpiresAt: now.Add(time.Duration(body.TTLMinutes) * time.Minute),
		CreatedBy: principal.Email,
	}
	db.Create(&sup)
	c.JSON(http.StatusOK, gin.H{"data": sup})
}

// DELETE /admin/experience/suppressions/:id — revoke early.
func RevokeExperienceSuppression(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var sup models.ExperienceSuppression
	if err := db.Where("public_id = ? AND tenant_id = ?", c.Param("id"), principal.TenantID).First(&sup).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "suppression not found"})
		return
	}
	now := time.Now()
	sup.RevokedAt = &now
	sup.RevokedBy = principal.Email
	db.Save(&sup)
	c.JSON(http.StatusOK, gin.H{"data": sup})
}

// GET /admin/experience/suppressions — active list.
func ListExperienceSuppressions(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var sups []models.ExperienceSuppression
	db.Where("tenant_id = ? AND revoked_at IS NULL AND expires_at > ?", principal.TenantID, time.Now()).
		Order("created_at DESC").Find(&sups)
	c.JSON(http.StatusOK, gin.H{"data": sups})
}

func parseLimit(raw string, def, max int) int {
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return def
	}
	if n > max {
		return max
	}
	return n
}
