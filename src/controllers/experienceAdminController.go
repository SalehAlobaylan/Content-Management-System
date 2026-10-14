package controllers

import (
	"content-management-system/src/models"
	"encoding/json"
	"errors"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"
)

func experienceReadError(c *gin.Context, err error) bool {
	if err == nil {
		return false
	}
	c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Real Experience evidence unavailable"})
	return true
}
func experiencePolicyFor(c *gin.Context, db *gorm.DB, tenant string) (models.ExperiencePolicy, bool) {
	p, e := loadExperiencePolicy(db, tenant)
	return p, !experienceReadError(c, e)
}
func GetExperienceStatus(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	policy, ok := experiencePolicyFor(c, db, principal.TenantID)
	if !ok {
		return
	}
	var run models.ExperienceEvaluationRun
	err := db.Where("tenant_id=?", principal.TenantID).Order("started_at DESC,id DESC").First(&run).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		experienceReadError(c, err)
		return
	}
	var count int64
	if experienceReadError(c, db.Model(&models.ExperienceIncident{}).Where("tenant_id=? AND status IN ('open','recovering','confirming')", principal.TenantID).Count(&count).Error) {
		return
	}
	verdicts, fresh, freshness, err := allClientExperienceSnapshot(db, principal.TenantID, policy, time.Now().UTC())
	if experienceReadError(c, err) {
		return
	}
	c.JSON(200, gin.H{"data": gin.H{"policy": policy, "latest_run": func() any {
		if run.ID == 0 {
			return nil
		}
		return run
	}(), "telemetry_fresh": fresh, "open_incidents": count, "surface_verdicts": verdicts, "observed_scope": "enabled_clients", "enabled_clients": enabledExperienceClients(policy), "client_freshness": freshness, "decision_resolution": "hour"}})
}
func GetExperiencePolicy(c *gin.Context) {
	p, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	policy, ok := experiencePolicyFor(c, c.MustGet("db").(*gorm.DB), p.TenantID)
	if ok {
		c.JSON(200, gin.H{"data": policy})
	}
}
func knownExperienceMetric(key string) bool {
	for _, s := range experienceSLIs {
		if s.metric == key {
			return true
		}
	}
	return key == mAutoplayBlockedRate
}
func validExperienceThresholds(raw []byte) bool {
	var v map[string]struct {
		Target     *float64 `json:"target"`
		LatencyP75 *int     `json:"latency_p75_ms"`
	}
	if json.Unmarshal(raw, &v) != nil || len(v) > len(experienceSLIs) {
		return false
	}
	for key, t := range v {
		if !knownExperienceMetric(key) || t.Target != nil && (*t.Target < 0 || *t.Target > 1) || t.LatencyP75 != nil && (*t.LatencyP75 < 1 || *t.LatencyP75 > 600000) {
			return false
		}
	}
	return true
}
func UpdateExperiencePolicy(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var patch struct {
		IngestEnabled              *bool           `json:"ingest_enabled"`
		EvaluationEnabled          *bool           `json:"evaluation_enabled"`
		EnabledClients             *string         `json:"enabled_clients"`
		EnabledSurfaces            *string         `json:"enabled_surfaces"`
		MinSampleFloor             *int            `json:"min_sample_floor"`
		LatencySampleFloor         *int            `json:"latency_sample_floor"`
		ConfirmWindows             *int            `json:"confirm_windows"`
		ResolveWindows             *int            `json:"resolve_windows"`
		TelemetryFreshnessMinutes  *int            `json:"telemetry_freshness_minutes"`
		RawRetentionDays           *int            `json:"raw_retention_days"`
		MaxReleaseCohorts          *int            `json:"max_release_cohorts"`
		MinuteRollupRetentionHours *int            `json:"minute_rollup_retention_hours"`
		HourRollupRetentionDays    *int            `json:"hour_rollup_retention_days"`
		Thresholds                 json.RawMessage `json:"thresholds"`
	}
	if c.ShouldBindJSON(&patch) != nil {
		c.JSON(400, gin.H{"error": "Invalid policy"})
		return
	}
	for _, b := range []struct {
		v   *int
		max int
	}{{patch.MinSampleFloor, 100000}, {patch.LatencySampleFloor, 100000}, {patch.ConfirmWindows, 24}, {patch.ResolveWindows, 24}, {patch.TelemetryFreshnessMinutes, 1440}, {patch.RawRetentionDays, 30}, {patch.MaxReleaseCohorts, 8}, {patch.MinuteRollupRetentionHours, 168}, {patch.HourRollupRetentionDays, 730}} {
		if b.v != nil && (*b.v < 1 || *b.v > b.max) {
			c.JSON(400, gin.H{"error": "Policy values outside supported bounds"})
			return
		}
	}
	if patch.EnabledClients != nil {
		seen := map[string]bool{}
		for _, v := range strings.Split(*patch.EnabledClients, ",") {
			if !models.RuxClientPlatforms[v] || seen[v] {
				c.JSON(400, gin.H{"error": "invalid enabled clients"})
				return
			}
			seen[v] = true
		}
	}

	if patch.EnabledSurfaces != nil && *patch.EnabledSurfaces != "pods" && *patch.EnabledSurfaces != "news" && *patch.EnabledSurfaces != "pods,news" {
		c.JSON(400, gin.H{"error": "Select Pods, News, or both"})
		return
	}
	if len(patch.Thresholds) > 0 && !validExperienceThresholds(patch.Thresholds) {
		c.JSON(400, gin.H{"error": "Unsupported thresholds"})
		return
	}
	policy, ok := experiencePolicyFor(c, db, principal.TenantID)
	if !ok {
		return
	}
	before := policy
	if patch.IngestEnabled != nil {
		policy.IngestEnabled = *patch.IngestEnabled
	}
	if patch.EvaluationEnabled != nil {
		policy.EvaluationEnabled = *patch.EvaluationEnabled
	}
	if patch.EnabledClients != nil {
		policy.EnabledClients = *patch.EnabledClients
	}
	if patch.EnabledSurfaces != nil {
		policy.EnabledSurfaces = *patch.EnabledSurfaces
	}
	for _, v := range []struct {
		from *int
		to   *int
	}{{patch.MinSampleFloor, &policy.MinSampleFloor}, {patch.LatencySampleFloor, &policy.LatencySampleFloor}, {patch.ConfirmWindows, &policy.ConfirmWindows}, {patch.ResolveWindows, &policy.ResolveWindows}, {patch.TelemetryFreshnessMinutes, &policy.TelemetryFreshnessMinutes}, {patch.RawRetentionDays, &policy.RawRetentionDays}, {patch.MaxReleaseCohorts, &policy.MaxReleaseCohorts}, {patch.MinuteRollupRetentionHours, &policy.MinuteRollupRetentionHours}, {patch.HourRollupRetentionDays, &policy.HourRollupRetentionDays}} {
		if v.from != nil {
			*v.to = *v.from
		}
	}
	if len(patch.Thresholds) > 0 {
		policy.Thresholds = datatypes.JSON(patch.Thresholds)
	}
	err := db.Transaction(func(tx *gorm.DB) error {
		var current models.ExperiencePolicy
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=?", principal.TenantID).First(&current).Error; err != nil {
			return err
		}
		before = current
		updates := map[string]any{}
		for _, change := range []struct {
			name      string
			requested any
			value     any
		}{
			{"ingest_enabled", patch.IngestEnabled, policy.IngestEnabled}, {"evaluation_enabled", patch.EvaluationEnabled, policy.EvaluationEnabled},
			{"enabled_clients", patch.EnabledClients, policy.EnabledClients}, {"enabled_surfaces", patch.EnabledSurfaces, policy.EnabledSurfaces}, {"min_sample_floor", patch.MinSampleFloor, policy.MinSampleFloor},
			{"latency_sample_floor", patch.LatencySampleFloor, policy.LatencySampleFloor}, {"confirm_windows", patch.ConfirmWindows, policy.ConfirmWindows},
			{"resolve_windows", patch.ResolveWindows, policy.ResolveWindows}, {"telemetry_freshness_minutes", patch.TelemetryFreshnessMinutes, policy.TelemetryFreshnessMinutes},
			{"raw_retention_days", patch.RawRetentionDays, policy.RawRetentionDays}, {"max_release_cohorts", patch.MaxReleaseCohorts, policy.MaxReleaseCohorts},
			{"minute_rollup_retention_hours", patch.MinuteRollupRetentionHours, policy.MinuteRollupRetentionHours}, {"hour_rollup_retention_days", patch.HourRollupRetentionDays, policy.HourRollupRetentionDays},
		} {
			if change.requested != nil && !reflect.ValueOf(change.requested).IsNil() {
				updates[change.name] = change.value
			}
		}
		if len(patch.Thresholds) > 0 {
			updates["thresholds"] = policy.Thresholds
		}
		if err := tx.Model(&current).Updates(updates).Error; err != nil {
			return err
		}
		if err := tx.Where("tenant_id=?", principal.TenantID).First(&policy).Error; err != nil {
			return err
		}
		return tx.Create(&models.ExperienceAction{TenantID: principal.TenantID, ActionClass: "policy_updated", Label: "Observe policy updated by " + principal.Email, Evidence: jsonBytes(map[string]any{"before": before, "after": policy})}).Error
	})
	if !experienceReadError(c, err) {
		c.JSON(200, gin.H{"data": policy})
	}
}
func RunExperienceNow(c *gin.Context) {
	p, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	if !tryStartExperienceRun() {
		c.JSON(409, gin.H{"error": "Evaluation already running"})
		return
	}
	defer finishExperienceRunLock()
	run, err := RunExperienceEvaluation(c.MustGet("db").(*gorm.DB), p.TenantID, "manual")
	if experienceReadError(c, err) {
		return
	}
	c.JSON(200, gin.H{"data": run})
}
func PauseExperienceSchedule(c *gin.Context) {
	p, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var body struct {
		Minutes int `json:"minutes"`
	}
	if c.ShouldBindJSON(&body) != nil || body.Minutes < 0 || body.Minutes > 10080 {
		c.JSON(400, gin.H{"error": "Invalid pause duration"})
		return
	}
	policy, ok := experiencePolicyFor(c, db, p.TenantID)
	if !ok {
		return
	}
	policy.PausedUntil = nil
	if body.Minutes > 0 {
		until := time.Now().UTC().Add(time.Duration(body.Minutes) * time.Minute)
		policy.PausedUntil = &until
	}
	err := db.Transaction(func(tx *gorm.DB) error {
		var current models.ExperiencePolicy
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=?", p.TenantID).First(&current).Error; err != nil {
			return err
		}
		if err := tx.Model(&current).Update("paused_until", policy.PausedUntil).Error; err != nil {
			return err
		}
		return tx.Create(&models.ExperienceAction{TenantID: p.TenantID, ActionClass: "schedule_pause_changed", Label: "Evaluation schedule updated by " + p.Email, Evidence: jsonBytes(map[string]any{"minutes": body.Minutes})}).Error
	})
	if !experienceReadError(c, err) {
		c.JSON(200, gin.H{"data": policy})
	}
}
func GetExperienceMetrics(c *gin.Context) {
	client, validClient := requestedExperienceClient(c)
	if !validClient {
		return
	}

	p, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	policy, ok := experiencePolicyFor(c, db, p.TenantID)
	if !ok {
		return
	}
	bucket := floorBucket(time.Now().UTC()).Add(-time.Hour)
	complete := policy.LastEvaluatedBucket != nil && !policy.LastEvaluatedBucket.Before(bucket)
	out, err := computeDisplayVerdicts(db, p.TenantID, policy, bucket, complete, client)
	if experienceReadError(c, err) {
		return
	}
	fresh, err := isTelemetryFresh(db, p.TenantID, policy, time.Now().UTC(), client)
	if experienceReadError(c, err) {
		return
	}
	c.JSON(200, gin.H{"data": gin.H{"bucket": bucket, "window_end": bucket.Add(time.Hour), "telemetry_fresh": fresh, "surfaces": out, "observed_scope": client, "client_enabled": experienceClientEnabled(policy, client), "rollups_complete": complete, "last_minute_bucket": policy.LastMinuteBucket}})
}
func experienceListQuery(c *gin.Context, db *gorm.DB, tenant string) *gorm.DB {
	q := db.Where("tenant_id=?", tenant)
	if raw := c.Query("before"); raw != "" {
		if t, e := time.Parse(time.RFC3339Nano, raw); e == nil {
			q = q.Where("created_at<?", t.UTC())
		}
	}
	return q.Order("created_at DESC,id DESC").Limit(parseLimit(c.Query("limit"), 50, 200))
}
func ListExperienceRuns(c *gin.Context) {
	p, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	rows := make([]models.ExperienceEvaluationRun, 0)
	err := experienceListQuery(c, c.MustGet("db").(*gorm.DB), p.TenantID).Find(&rows).Error
	if !experienceReadError(c, err) {
		c.JSON(200, gin.H{"data": rows})
	}
}
func ListExperienceIncidents(c *gin.Context) {
	client, validClient := requestedExperienceClient(c)
	if !validClient {
		return
	}

	p, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	q := db.Where("tenant_id=?", p.TenantID).Where("client_platform=?", client)
	status := c.Query("status")
	if status != "" && status != "all" {
		if status != "open" && status != "recovering" && status != "confirming" && status != "resolved" && status != "closed" {
			c.JSON(400, gin.H{"error": "Invalid incident status"})
			return
		}
		q = q.Where("status=?", status)
	} else if status == "" {
		q = q.Where("status IN ('open','recovering','confirming')")
	}
	rows := make([]models.ExperienceIncident, 0)
	err := q.Order("CASE severity WHEN 'critical' THEN 0 WHEN 'degraded' THEN 1 ELSE 2 END, COALESCE((evidence->>'affected_sessions')::bigint,0) DESC, first_seen_at ASC, id ASC").Limit(parseLimit(c.Query("limit"), 50, 200)).Find(&rows).Error
	if !experienceReadError(c, err) {
		c.JSON(200, gin.H{"data": rows})
	}
}
func CloseExperienceIncident(c *gin.Context) {
	p, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var body struct {
		ReasonClass string `json:"reason_class"`
		Notes       string `json:"notes"`
	}
	if c.ShouldBindJSON(&body) != nil || !map[string]bool{"resolved_externally": true, "expected_behavior": true, "false_positive": true, "accepted_risk": true}[body.ReasonClass] || len(strings.TrimSpace(body.Notes)) < 1 || len(body.Notes) > 2000 {
		c.JSON(400, gin.H{"error": "Valid reason and 1–2000 character notes required"})
		return
	}
	var inc models.ExperienceIncident
	err := db.Transaction(func(tx *gorm.DB) error {
		var policy models.ExperiencePolicy
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=?", p.TenantID).First(&policy).Error; err != nil {
			return err
		}
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id=? AND tenant_id=?", c.Param("id"), p.TenantID).First(&inc).Error; err != nil {
			return err
		}
		inc.Status = "closed"
		inc.ClosedBy = p.Email
		inc.CloseReasonClass = body.ReasonClass
		inc.CloseNotes = strings.TrimSpace(body.Notes)
		if err := tx.Save(&inc).Error; err != nil {
			return err
		}
		return writeExperienceAction(tx, p.TenantID, nil, &inc.ID, "incident_closed", "Human closure as "+body.ReasonClass, sliStatus{Metric: inc.MetricKey, ClientPlatform: inc.ClientPlatform, Surface: inc.Surface, CohortDim: inc.CohortDim, CohortVal: inc.CohortVal}, "human_close_is_not_verified_recovery", time.Now().UTC())
	})
	if !experienceReadError(c, err) {
		c.JSON(200, gin.H{"data": inc})
	}
}
func ListExperienceActions(c *gin.Context) {
	client, validClient := requestedExperienceClient(c)
	if !validClient {
		return
	}

	p, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	rows := make([]models.ExperienceAction, 0)
	err := experienceListQuery(c, c.MustGet("db").(*gorm.DB), p.TenantID).Where("evidence->>'client_platform'=? OR evidence->>'client_platform' IS NULL", client).Find(&rows).Error
	if !experienceReadError(c, err) {
		c.JSON(200, gin.H{"data": rows})
	}
}
func validExperienceCohort(dim, val string) bool {
	if len(val) > 80 || val == "" {
		return false
	}
	switch dim {
	case "global":
		return val == "all"
	case "release":
		return true
	case "browser":
		for family := range models.RuxBrowserFamilies {
			if strings.HasPrefix(val, family+"-") {
				tail := strings.TrimPrefix(val, family+"-")
				n, e := strconv.Atoi(tail)
				return tail == "other" || e == nil && n >= 0 && n <= 999
			}
		}
	case "os":
		n, err := strconv.Atoi(val)
		return err == nil && n >= 0 && n <= 999 && strconv.Itoa(n) == val
	case "device":
		return models.RuxDeviceClasses[val]
	case "network":
		return models.RuxNetworkClasses[val]
	case "playback_type":
		return models.RuxPlaybackTypes[val]
	case "locale":
		return val == "ar" || val == "en" || val == "unknown"
	}
	return false
}
func CreateExperienceSuppression(c *gin.Context) {
	p, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	var body struct {
		ClientPlatform string `json:"client_platform"`
		MetricKey      string `json:"metric_key"`
		Surface        string `json:"surface"`
		CohortDim      string `json:"cohort_dim"`
		CohortVal      string `json:"cohort_val"`
		Reason         string `json:"reason"`
		TTLMinutes     int    `json:"ttl_minutes"`
	}
	if c.ShouldBindJSON(&body) != nil || !knownExperienceMetric(body.MetricKey) || !models.RuxSurfaces[body.Surface] || !validExperienceCohort(body.CohortDim, body.CohortVal) || strings.TrimSpace(body.Reason) == "" || len(body.Reason) > 1000 || body.TTLMinutes < 1 || body.TTLMinutes > 10080 {
		c.JSON(400, gin.H{"error": "Exact metric, surface, cohort, reason and bounded TTL required"})
		return
	}
	if body.ClientPlatform == "" {
		body.ClientPlatform = "web"
	}
	if !models.RuxClientPlatforms[body.ClientPlatform] || body.ClientPlatform == "web" && body.CohortDim == "os" || body.ClientPlatform != "web" && body.CohortDim == "browser" {
		c.JSON(400, gin.H{"error": "invalid client platform"})
		return
	}
	now := time.Now().UTC()
	sup := models.ExperienceSuppression{TenantID: p.TenantID, ClientPlatform: body.ClientPlatform, MetricKey: body.MetricKey, Surface: body.Surface, CohortDim: body.CohortDim, CohortVal: body.CohortVal, Reason: strings.TrimSpace(body.Reason), StartsAt: now, ExpiresAt: now.Add(time.Duration(body.TTLMinutes) * time.Minute), CreatedBy: p.Email}
	db := c.MustGet("db").(*gorm.DB)
	err := db.Transaction(func(tx *gorm.DB) error {
		var policy models.ExperiencePolicy
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=?", p.TenantID).First(&policy).Error; err != nil {
			return err
		}
		if err := tx.Create(&sup).Error; err != nil {
			return err
		}
		return tx.Create(&models.ExperienceAction{TenantID: p.TenantID, ActionClass: "suppression_created", Label: "Scoped suppression created by " + p.Email, MetricKey: sup.MetricKey, Surface: sup.Surface, CohortDim: sup.CohortDim, CohortVal: sup.CohortVal, Evidence: jsonBytes(sup)}).Error
	})
	if !experienceReadError(c, err) {
		c.JSON(200, gin.H{"data": sup})
	}
}
func RevokeExperienceSuppression(c *gin.Context) {
	p, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var sup models.ExperienceSuppression
	err := db.Transaction(func(tx *gorm.DB) error {
		var policy models.ExperiencePolicy
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=?", p.TenantID).First(&policy).Error; err != nil {
			return err
		}
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=?", p.TenantID, c.Param("id")).First(&sup).Error; err != nil {
			return err
		}
		now := time.Now().UTC()
		sup.RevokedAt = &now
		sup.RevokedBy = p.Email
		if err := tx.Save(&sup).Error; err != nil {
			return err
		}
		return tx.Create(&models.ExperienceAction{TenantID: p.TenantID, ActionClass: "suppression_revoked", Label: "Suppression revoked by " + p.Email, Evidence: jsonBytes(sup)}).Error
	})
	if !experienceReadError(c, err) {
		c.JSON(200, gin.H{"data": sup})
	}
}
func ListExperienceSuppressions(c *gin.Context) {
	client, validClient := requestedExperienceClient(c)
	if !validClient {
		return
	}

	p, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	rows := make([]models.ExperienceSuppression, 0)
	err := c.MustGet("db").(*gorm.DB).Where("tenant_id=? AND revoked_at IS NULL AND expires_at>?", p.TenantID, time.Now().UTC()).Where("client_platform=?", client).Order("created_at DESC,id DESC").Limit(200).Find(&rows).Error
	if !experienceReadError(c, err) {
		c.JSON(200, gin.H{"data": rows})
	}
}
func parseLimit(raw string, def, max int) int {
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return def
	}
	if n > max {
		return max
	}
	return n
}

// Investigation reads are tenant scoped, bounded, and allow only fixed filters.
func GetExperienceEvidence(c *gin.Context) {
	client, validClient := requestedExperienceClient(c)
	if !validClient {
		return
	}

	p, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	kind := c.Param("kind")
	if kind == "incident" || kind == "run" {
		if _, err := uuid.Parse(c.Param("id")); err != nil {
			c.JSON(400, gin.H{"error": "Invalid evidence identity"})
			return
		}
		if kind == "incident" {
			var row models.ExperienceIncident
			err := db.Where("tenant_id=? AND public_id=? AND client_platform=?", p.TenantID, c.Param("id"), client).First(&row).Error
			if !experienceReadError(c, err) {
				actions := make([]models.ExperienceAction, 0)
				if experienceReadError(c, db.Where("tenant_id=? AND incident_id=?", p.TenantID, row.ID).Order("id DESC").Limit(100).Find(&actions).Error) {
					return
				}
				c.JSON(200, gin.H{"data": gin.H{"incident": row, "actions": actions}})
			}
		} else {
			var row models.ExperienceEvaluationRun
			err := db.Where("tenant_id=? AND public_id=?", p.TenantID, c.Param("id")).First(&row).Error
			if !experienceReadError(c, err) {
				actions := make([]models.ExperienceAction, 0)
				if experienceReadError(c, db.Where("tenant_id=? AND run_id=?", p.TenantID, row.ID).Where("evidence->>'client_platform'=? OR evidence->>'client_platform' IS NULL", client).Order("id DESC").Limit(200).Find(&actions).Error) {
					return
				}
				c.JSON(200, gin.H{"data": gin.H{"run": row, "actions": actions}})
			}
		}
		return
	}
	start := time.Now().UTC().Add(-24 * time.Hour)
	end := time.Now().UTC()
	if raw := c.Query("start"); raw != "" {
		t, e := time.Parse(time.RFC3339, raw)
		if e != nil {
			c.JSON(400, gin.H{"error": "Invalid start"})
			return
		}
		start = t.UTC()
	}
	if raw := c.Query("end"); raw != "" {
		t, e := time.Parse(time.RFC3339, raw)
		if e != nil {
			c.JSON(400, gin.H{"error": "Invalid end"})
			return
		}
		end = t.UTC()
	}
	maxRange := 7 * 24 * time.Hour
	if kind == "history" || kind == "cohorts" || kind == "releases" {
		maxRange = 400 * 24 * time.Hour
	}
	if !end.After(start) || end.Sub(start) > maxRange || end.After(time.Now().UTC().Add(time.Minute)) {
		c.JSON(400, gin.H{"error": "Evidence range exceeds the supported retention horizon"})
		return
	}
	if surface := c.Query("surface"); surface != "" && !models.RuxSurfaces[surface] {
		c.JSON(400, gin.H{"error": "Invalid surface"})
		return
	}
	q := db.Where("tenant_id=?", p.TenantID).Where("client_platform=?", client)
	if surface := c.Query("surface"); surface != "" {
		q = q.Where("surface=?", surface)
	}
	if kind == "history" || kind == "cohorts" || kind == "releases" {
		resolution := c.DefaultQuery("resolution", "hour")
		if resolution != "hour" && resolution != "minute" {
			c.JSON(400, gin.H{"error": "Unsupported resolution"})
			return
		}
		q = q.Where("bucket_start>=? AND bucket_start<? AND resolution=? AND completed=true", start, end, resolution)
		policy, ok := experiencePolicyFor(c, db, p.TenantID)
		if !ok {
			return
		}
		checkpoint := policy.LastEvaluatedBucket
		if resolution == "minute" {
			checkpoint = policy.LastMinuteBucket
		}
		if checkpoint == nil {
			c.JSON(200, gin.H{"data": gin.H{"rows": []models.ExperienceMetricRollup{}}})
			return
		}
		q = q.Where("bucket_start<=?", checkpoint)
		if raw := c.Query("before_id"); raw != "" {
			id, e := strconv.ParseUint(raw, 10, 64)
			if e != nil {
				c.JSON(400, gin.H{"error": "Invalid aggregate cursor"})
				return
			}
			q = q.Where("id<?", id)
		}
		switch kind {
		case "history":
			q = q.Where("cohort_dim='global'")
		case "cohorts":
			q = q.Where("cohort_dim NOT IN ('global','release')")
		case "releases":
			q = q.Where("cohort_dim='release' AND cohort_val<>'other'")
		}
		rows := make([]models.ExperienceMetricRollup, 0)
		err := q.Order("id DESC").Limit(parseLimit(c.Query("limit"), 200, 1000)).Find(&rows).Error
		if !experienceReadError(c, err) {
			var comparison *experienceReleaseComparison
			if kind == "releases" && resolution == "hour" {
				value, err := compareExperienceReleases(db, p.TenantID, policy, start, end, client)
				if experienceReadError(c, err) {
					return
				}
				comparison = &value
			}
			var next uint
			if len(rows) > 0 {
				next = rows[len(rows)-1].ID
			}
			c.JSON(200, gin.H{"data": gin.H{"rows": rows, "next_before_id": next, "minimum_denominator": policy.MinSampleFloor, "resolution": resolution, "window_start": start, "window_end": end, "comparison_is_evidence_only": true, "release_comparison": comparison}})
		}
		return
	}
	if kind != "events" && kind != "content-failures" {
		c.JSON(404, gin.H{"error": "Unknown evidence view"})
		return
	}
	q = q.Where("received_at>=? AND received_at<?", start, end)
	for key, column := range map[string]string{"event_type": "event_type", "release": "release", "content_id": "content_id", "journey_id": "journey_id"} {
		if raw := c.Query(key); raw != "" {
			if key == "event_type" && !models.RuxEventTypes[raw] || key == "release" && len(raw) > 80 {
				c.JSON(400, gin.H{"error": "Unsupported event filter"})
				return
			}
			if key == "content_id" || key == "journey_id" {
				if _, err := uuid.Parse(raw); err != nil {
					c.JSON(400, gin.H{"error": "Invalid diagnostic UUID"})
					return
				}
			}
			q = q.Where(column+"=?", raw)
		}
	}
	if kind == "content-failures" {
		var rows []struct {
			ContentID uuid.UUID `json:"content_id"`
			Failures  int64     `json:"failures"`
			Sessions  int64     `json:"affected_sessions"`
		}
		err := q.Model(&models.ExperienceEvent{}).Select("content_id,COUNT(*) AS failures,COUNT(DISTINCT session_id) AS sessions").Where("content_id IS NOT NULL AND schema_version IN (2,3) AND terminal_status='accepted' AND outcome IN ('failed','timeout') AND visible=true").Group("content_id").Order("COUNT(*) DESC,content_id ASC").Limit(20).Scan(&rows).Error
		if !experienceReadError(c, err) {
			c.JSON(200, gin.H{"data": rows})
		}
		return
	}
	if raw := c.Query("before_id"); raw != "" {
		id, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			c.JSON(400, gin.H{"error": "Invalid cursor"})
			return
		}
		q = q.Where("id<?", id)
	}
	rows := make([]models.ExperienceEvent, 0)
	err := q.Order("id DESC").Limit(parseLimit(c.Query("limit"), 50, 200)).Find(&rows).Error
	if !experienceReadError(c, err) {
		var next uint
		if len(rows) > 0 {
			next = rows[len(rows)-1].ID
		}
		c.JSON(200, gin.H{"data": rows, "next_before_id": next})
	}
}
