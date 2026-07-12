package controllers

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"sync"
	"time"

	"content-management-system/src/models"

	"github.com/gin-gonic/gin"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// Embedding & Model Lifecycle System (stage 10) — audit runner + HTTP surface
// (Slice 2). Single-flight via a package mutex PLUS a PostgreSQL advisory lock
// so two CMS instances never run the audit concurrently.

const embeddingLifecycleTenant = "default" // platform singleton scope

var embeddingLifecycleMu sync.Mutex

// jsonEvidence marshals an evidence map to datatypes.JSON (nil on error).
func jsonEvidence(m map[string]any) datatypes.JSON {
	b, err := json.Marshal(m)
	if err != nil {
		return nil
	}
	return datatypes.JSON(b)
}

// checkErrorFinding builds a check_error finding — an evaluator failure is never
// a violation (family rule).
func checkErrorFinding(runID uint, tenant, surfaceKey, checkKey, msg string) models.EmbeddingLifecycleFinding {
	return models.EmbeddingLifecycleFinding{
		RunID: runID, TenantID: tenant, SurfaceKey: surfaceKey,
		CheckKey: checkKey, Status: models.EmbeddingFindingCheckError,
		Severity: models.EmbeddingSevMajor,
		Evidence: jsonEvidence(map[string]any{"error": msg}),
	}
}

// getOrCreateEmbeddingPolicy returns the singleton policy, creating defaults.
func getOrCreateEmbeddingPolicy(db *gorm.DB) (*models.EmbeddingLifecyclePolicy, error) {
	var p models.EmbeddingLifecyclePolicy
	err := db.Where("tenant_id = ?", embeddingLifecycleTenant).First(&p).Error
	if err == gorm.ErrRecordNotFound {
		p = models.EmbeddingLifecyclePolicy{
			TenantID: embeddingLifecycleTenant, AuditEnabled: false,
			AuditIntervalMinutes: 360, NumericSampleSize: 64, ItemsPerBatch: 200,
			BatchesPerRun: 1, DailyItemCap: 5000, RetryCeiling: 3,
		}
		if err := db.Create(&p).Error; err != nil {
			return nil, err
		}
		return &p, nil
	}
	if err != nil {
		return nil, err
	}
	return &p, nil
}

// tryEmbeddingAdvisoryLock grabs a cross-instance advisory lock; the returned
// release func is always safe to call.
func tryEmbeddingAdvisoryLock(db *gorm.DB) (func(), bool) {
	ctx := context.Background()
	sqlDB, err := db.DB()
	if err != nil {
		return func() {}, false
	}
	conn, err := sqlDB.Conn(ctx)
	if err != nil {
		return func() {}, false
	}
	key := int64(0)
	for _, b := range []byte("wahb:embedding-lifecycle:" + embeddingLifecycleTenant) {
		key = key*31 + int64(b)
	}
	var acquired bool
	if err := conn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", key).Scan(&acquired); err != nil || !acquired {
		conn.Close()
		return func() {}, false
	}
	return func() {
		_, _ = conn.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1)", key)
		_ = conn.Close()
	}, true
}

// runEmbeddingAudit executes one audit pass (Lanes A+B) over every registered
// surface, persisting a run + findings. Single-flight; returns the run or nil if
// another run holds the lock.
func runEmbeddingAudit(db *gorm.DB, trigger string) (*models.EmbeddingLifecycleRun, bool, error) {
	if !embeddingLifecycleMu.TryLock() {
		return nil, false, nil
	}
	defer embeddingLifecycleMu.Unlock()

	release, ok := tryEmbeddingAdvisoryLock(db)
	if !ok {
		return nil, false, nil
	}
	defer release()

	start := time.Now()
	run := models.EmbeddingLifecycleRun{
		TenantID: embeddingLifecycleTenant, Trigger: trigger, Status: models.EmbeddingRunRunning,
	}
	if err := db.Create(&run).Error; err != nil {
		return nil, true, err
	}

	perSurface := map[string]surfaceAudit{}
	var allFindings []models.EmbeddingLifecycleFinding
	var major, minor, checkErrs int

	for _, s := range EmbeddingSurfaces() {
		res := safeAuditSurface(db, run.ID, embeddingLifecycleTenant, s)
		perSurface[s.Key] = res
		for _, f := range res.findings {
			allFindings = append(allFindings, f)
			if f.Status == models.EmbeddingFindingViolation {
				switch f.Severity {
				case models.EmbeddingSevMajor, models.EmbeddingSevCritical:
					major++
				case models.EmbeddingSevMinor, models.EmbeddingSevInfo:
					minor++
				}
			}
			if f.Status == models.EmbeddingFindingCheckError {
				checkErrs++
			}
		}
	}

	if len(allFindings) > 0 {
		if err := db.CreateInBatches(&allFindings, 200).Error; err != nil {
			// Persisting findings failed — mark the run failed but don't panic.
			finishEmbeddingRun(db, &run, start, models.EmbeddingRunFailed, models.EmbeddingHeadlineAttention, perSurface, major, minor, checkErrs, err.Error(), "persist_findings")
			return &run, true, err
		}
	}

	// Roll surface verdicts into the headline.
	surfacesList := make([]surfaceAudit, 0, len(perSurface))
	for _, s := range EmbeddingSurfaces() {
		surfacesList = append(surfacesList, perSurface[s.Key])
	}
	headline := runHeadline(surfacesList)
	status := models.EmbeddingRunCompleted
	if checkErrs > 0 {
		status = models.EmbeddingRunPartial
	}
	finishEmbeddingRun(db, &run, start, status, headline, perSurface, major, minor, checkErrs, "", "")

	// Stamp last-audit on policy.
	db.Model(&models.EmbeddingLifecyclePolicy{}).
		Where("tenant_id = ?", embeddingLifecycleTenant).
		Update("last_audit_at", time.Now())

	return &run, true, nil
}

// safeAuditSurface wraps auditSurface with per-surface panic isolation (G11).
func safeAuditSurface(db *gorm.DB, runID uint, tenant string, s EmbeddingSurface) (res surfaceAudit) {
	defer func() {
		if r := recover(); r != nil {
			res = surfaceAudit{
				Surface: s, Verdict: models.EmbeddingVerdictCheckError,
				findings: []models.EmbeddingLifecycleFinding{
					checkErrorFinding(runID, tenant, s.Key, models.EmbeddingCheckStaleInventory, "panic in evaluator"),
				},
			}
		}
	}()
	return auditSurface(db, runID, tenant, s)
}

func finishEmbeddingRun(db *gorm.DB, run *models.EmbeddingLifecycleRun, start time.Time,
	status, headline string, perSurface map[string]surfaceAudit, major, minor, checkErrs int,
	errMsg, errClass string) {
	now := time.Now()
	run.Status = status
	run.Headline = headline
	run.ViolationsMajor = major
	run.ViolationsMinor = minor
	run.CheckErrors = checkErrs
	run.CompletedAt = &now
	run.DurationMS = time.Since(start).Milliseconds()
	run.Error = errMsg
	run.ErrorClass = errClass

	// Compact per-surface summary for the cockpit (no snapshot table).
	summary := map[string]any{}
	for k, v := range perSurface {
		summary[k] = map[string]any{
			"verdict": v.Verdict, "with_vec": v.WithVec, "current": v.Current,
			"stale": v.Stale, "unstamped": v.Unstamped, "missing": v.Missing,
			"mixed_space": v.MixedSpace, "numeric_invalid": v.NumericInvalid,
			"space": v.Surface.Space, "note": v.Note,
			"expected_space_id": v.ExpectedSpace.SpaceID,
		}
	}
	run.PerSurface = jsonEvidence(summary)
	db.Save(run)
}

// ─── HTTP handlers ───────────────────────────────────────────────────────────

// GetEmbeddingLifecycleStatus — cockpit bootstrap.
func GetEmbeddingLifecycleStatus(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	policy, err := getOrCreateEmbeddingPolicy(db)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	var latest models.EmbeddingLifecycleRun
	db.Where("tenant_id = ?", embeddingLifecycleTenant).Order("started_at DESC").First(&latest)

	c.JSON(http.StatusOK, gin.H{
		"policy":     policy,
		"latest_run": latest,
		"surfaces":   EmbeddingSurfaces(),
		"spaces": gin.H{
			"text":  currentExpectedSpace(EmbeddingSpaceText),
			"image": currentExpectedSpace(EmbeddingSpaceImage),
		},
	})
}

// GetEmbeddingLifecyclePolicy / UpdateEmbeddingLifecyclePolicy.
func GetEmbeddingLifecyclePolicy(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	policy, err := getOrCreateEmbeddingPolicy(db)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, policy)
}

func UpdateEmbeddingLifecyclePolicy(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	policy, err := getOrCreateEmbeddingPolicy(db)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	var body struct {
		AuditEnabled         *bool `json:"audit_enabled"`
		AuditIntervalMinutes *int  `json:"audit_interval_minutes"`
		NumericSampleSize    *int  `json:"numeric_sample_size"`
		ItemsPerBatch        *int  `json:"items_per_batch"`
		BatchesPerRun        *int  `json:"batches_per_run"`
		DailyItemCap         *int  `json:"daily_item_cap"`
		RetryCeiling         *int  `json:"retry_ceiling"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}
	if body.AuditEnabled != nil {
		policy.AuditEnabled = *body.AuditEnabled
	}
	if body.AuditIntervalMinutes != nil && *body.AuditIntervalMinutes > 0 {
		policy.AuditIntervalMinutes = clampEmbeddingInt(*body.AuditIntervalMinutes, 5, 10080)
	}
	if body.NumericSampleSize != nil && *body.NumericSampleSize >= 0 {
		policy.NumericSampleSize = clampEmbeddingInt(*body.NumericSampleSize, 0, 500)
	}
	if body.ItemsPerBatch != nil && *body.ItemsPerBatch > 0 {
		policy.ItemsPerBatch = clampEmbeddingInt(*body.ItemsPerBatch, 1, 500)
	}
	if body.BatchesPerRun != nil && *body.BatchesPerRun > 0 {
		policy.BatchesPerRun = clampEmbeddingInt(*body.BatchesPerRun, 1, 5)
	}
	if body.DailyItemCap != nil && *body.DailyItemCap >= 0 {
		policy.DailyItemCap = clampEmbeddingInt(*body.DailyItemCap, 1, 50000)
	}
	if body.RetryCeiling != nil && *body.RetryCeiling >= 0 {
		policy.RetryCeiling = clampEmbeddingInt(*body.RetryCeiling, 1, 10)
	}
	if err := db.Save(policy).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, policy)
}

func clampEmbeddingInt(v, minV, maxV int) int {
	if v < minV {
		return minV
	}
	if v > maxV {
		return maxV
	}
	return v
}

// RunEmbeddingLifecycleNow — manual audit (409 if already running).
func RunEmbeddingLifecycleNow(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	run, started, err := runEmbeddingAudit(db, models.EmbeddingRunTriggerManual)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if !started {
		c.JSON(http.StatusConflict, gin.H{"error": "an audit is already running"})
		return
	}
	c.JSON(http.StatusOK, run)
}

// ListEmbeddingLifecycleRuns / GetEmbeddingLifecycleRun.
func ListEmbeddingLifecycleRuns(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	limit := 50
	if l := c.Query("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 && n <= 200 {
			limit = n
		}
	}
	var runs []models.EmbeddingLifecycleRun
	db.Where("tenant_id = ?", embeddingLifecycleTenant).Order("started_at DESC").Limit(limit).Find(&runs)
	c.JSON(http.StatusOK, gin.H{"runs": runs})
}

func GetEmbeddingLifecycleRun(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	id, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid run id"})
		return
	}
	var run models.EmbeddingLifecycleRun
	if err := db.First(&run, id).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "run not found"})
		return
	}
	var findings []models.EmbeddingLifecycleFinding
	db.Where("run_id = ?", id).Order("severity DESC, surface_key").Find(&findings)
	c.JSON(http.StatusOK, gin.H{"run": run, "findings": findings})
}

// ListEmbeddingLifecycleFindings — filterable ledger.
func ListEmbeddingLifecycleFindings(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	q := db.Model(&models.EmbeddingLifecycleFinding{}).Where("tenant_id = ?", embeddingLifecycleTenant)
	if v := c.Query("run_id"); v != "" {
		q = q.Where("run_id = ?", v)
	}
	if v := c.Query("surface"); v != "" {
		q = q.Where("surface_key = ?", v)
	}
	if v := c.Query("check"); v != "" {
		q = q.Where("check_key = ?", v)
	}
	if v := c.Query("status"); v != "" {
		q = q.Where("status = ?", v)
	}
	limit := 200
	if l := c.Query("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 && n <= 1000 {
			limit = n
		}
	}
	var findings []models.EmbeddingLifecycleFinding
	q.Order("id DESC").Limit(limit).Find(&findings)
	c.JSON(http.StatusOK, gin.H{"findings": findings})
}

// GetEmbeddingLifecycleSurfaces — the registry (UI renders from this).
func GetEmbeddingLifecycleSurfaces(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"surfaces": EmbeddingSurfaces()})
}
