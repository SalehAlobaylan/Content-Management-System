package controllers

import (
	"content-management-system/src/intelligence"
	"content-management-system/src/models"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Media Circulation Engine — stage 2, Slice 1 (persistence + read model).
//
// This is a THIN AGGREGATOR (D10): it re-derives nothing. The storage/cost side
// of the health headline reuses the existing storage-health path wholesale
// (buildStorageHealthResponse); the only circulation-owned computation here is the
// per-duration-bucket feed inventory (intake demand) and the composition of the two
// into a single library headline. Intake scoring, evict aggregation, recommendation
// generation, and execution belong to later slices. See docs/media-circulation-engine.md.

// Duration buckets served by the For You feed, stored canonically with the "m"
// suffix to match atomization child writes.
var mediaCirculationBuckets = []string{"5m", "10m", "15m", "20m", "30m", "40m"}

// Code-default bucket thresholds (Config Discipline: capacity constants, not env,
// not policy yet). A bucket below the thin floor is under-supplied; above the
// saturated ceiling it is over-supplied. Promote to policy knobs if admins need to
// tune them at runtime.
const (
	mediaCirculationBucketThinFloor     = 20
	mediaCirculationBucketSaturatedCeil = 200
	mediaCircHealthCacheTTL             = 10 * time.Second
)

type libraryBucketHealth struct {
	Bucket       string `json:"bucket"`
	VisibleUnits int64  `json:"visible_units"`
	State        string `json:"state"` // thin | ok | saturated

	// Demand surface (stage 4, measured serve-side telemetry). When Measured
	// is true, State is derived from Gap (demand − value-weighted coverage)
	// instead of raw supply counts, and the D13 intake matcher weights buckets
	// by Gap.
	DemandScore   float64 `json:"demand_score"`
	CoverageScore float64 `json:"coverage_score"`
	Gap           float64 `json:"gap"`
	Measured      bool    `json:"measured"`
}

type mediaCirculationProof struct {
	Storage              storageProofMetrics             `json:"storage"`
	OpBudget             opBudgetStatus                  `json:"op_budget"`
	AtomizationBacklog   mediaCircAtomizationBacklog     `json:"atomization_backlog"`
	Buckets              []libraryBucketHealth           `json:"buckets"`
	ThinBuckets          []string                        `json:"thin_buckets"`
	EvictByVerdict       map[string]int64                `json:"evict_by_verdict"`
	AppliedYieldByBucket map[string]mediaCircBucketYield `json:"applied_yield_by_bucket,omitempty"`
}

type mediaCirculationHealthResponse struct {
	Headline     string                        `json:"headline"`
	Score        int                           `json:"score"`
	Summary      string                        `json:"summary"`
	StorageState string                        `json:"storage_state"`
	Reasons      []string                      `json:"reasons"`
	GeneratedAt  string                        `json:"generated_at"`
	Proof        mediaCirculationProof         `json:"proof"`
	Policy       models.MediaCirculationPolicy `json:"policy"`
}

// ----------------------------------------------------------------
// Endpoints
// ----------------------------------------------------------------

func GetMediaCirculationHealth(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	if cached, ok := getCachedMediaCircHealth(principal.TenantID); ok {
		c.JSON(http.StatusOK, cached)
		return
	}
	resp := buildMediaCirculationHealth(db, principal.TenantID, c.GetHeader("Authorization"))
	storeMediaCircHealth(principal.TenantID, resp)
	c.JSON(http.StatusOK, resp)
}

func GetMediaCirculationPolicy(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	c.JSON(http.StatusOK, loadEffectiveMediaCirculationPolicy(db, principal.TenantID))
}

func UpdateMediaCirculationPolicy(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	var req models.MediaCirculationPolicy
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid request: " + err.Error(), Code: "INVALID_REQUEST"})
		return
	}
	req.TenantID = principal.TenantID
	req = sanitizeMediaCirculationPolicy(req)
	if err := db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "tenant_id"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"enabled", "preset", "value_floor", "marginal_margin",
			"max_intake_per_source_per_cycle", "max_intake_per_cycle",
			"source_min_interval_minutes", "source_max_interval_minutes",
			"freshness_demand_weight", "last_evaluated_at", "updated_at",
		}),
	}).Create(&req).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to save policy", Code: "SAVE_FAILED"})
		return
	}
	invalidateMediaCircHealth(principal.TenantID)
	c.JSON(http.StatusOK, req)
}

// ----------------------------------------------------------------
// Recommendations (Slice 2: evict side; intake side added in Slice 3)
// ----------------------------------------------------------------

func GenerateMediaCirculationRecommendations(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	circPolicy := loadEffectiveMediaCirculationPolicy(db, principal.TenantID)
	if !circPolicy.Enabled {
		c.JSON(http.StatusConflict, authErrorResponse{Message: "Media circulation is disabled for this tenant", Code: "MEDIA_CIRCULATION_DISABLED"})
		return
	}
	storagePolicy := loadEffectiveStoragePolicy(db, principal.TenantID)
	// Cold availability + cost headroom come from live storage stats — reuse the path.
	storage := buildStorageHealthResponse(db, principal.TenantID, c.GetHeader("Authorization"))
	overrides := loadActiveMediaCircOverrides(db, principal.TenantID)
	enrichAppliedIntakeYields(db, principal.TenantID)

	evict := computeEvictRecommendations(db, principal.TenantID, storagePolicy, circPolicy, storage.Proof.ColdEnabled)
	evict = append(evict, computeAtomizationRecommendations(db, principal.TenantID, overrides)...)
	intake := computeIntakeRecommendations(db, principal.TenantID, circPolicy, storagePolicy, storage)

	if err := persistRecommendationsForUnit(db, principal.TenantID, models.MediaCirculationUnitItemFamily, evict); err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to persist evict recommendations", Code: "SAVE_FAILED"})
		return
	}
	if err := persistRecommendationsForUnit(db, principal.TenantID, models.MediaCirculationUnitSource, intake); err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to persist intake recommendations", Code: "SAVE_FAILED"})
		return
	}
	now := time.Now().UTC()
	_ = db.Model(&models.MediaCirculationPolicy{}).
		Where("tenant_id = ?", principal.TenantID).
		Updates(map[string]interface{}{"last_generated_at": now, "last_evaluated_at": now}).Error
	invalidateMediaCircHealth(principal.TenantID)

	evictCounts := evictVerdictCounts(evict)
	intakeCounts := evictVerdictCounts(intake)
	writeCirculationAudit(db, principal, "media_circulation.recommendations.generate", principal.TenantID, map[string]interface{}{
		"item_family_count": len(evict),
		"source_count":      len(intake),
	})
	c.JSON(http.StatusOK, gin.H{"data": gin.H{
		"item_family": gin.H{"count": len(evict), "by_verdict": evictCounts},
		"source":      gin.H{"count": len(intake), "by_verdict": intakeCounts},
	}})
}

func ListMediaCirculationRecommendations(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	q := db.Where("tenant_id = ?", principal.TenantID)
	if ut := strings.TrimSpace(c.Query("unit_type")); ut != "" {
		q = q.Where("unit_type = ?", ut)
	}
	status := strings.TrimSpace(c.Query("status"))
	switch {
	case status == "":
		q = q.Where("status = ?", models.MediaCirculationRecStatusPending)
	case status != "all":
		q = q.Where("status = ?", status)
	}
	var rows []models.MediaCirculationRecommendation
	q.Order("applied ASC, score DESC, updated_at DESC").Limit(200).Find(&rows)
	c.JSON(http.StatusOK, gin.H{"data": rows})
}

func ApplyMediaCirculationRecommendation(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	circPolicy := loadEffectiveMediaCirculationPolicy(db, principal.TenantID)
	if !circPolicy.Enabled {
		c.JSON(http.StatusConflict, authErrorResponse{Message: "Media circulation is disabled for this tenant", Code: "MEDIA_CIRCULATION_DISABLED"})
		return
	}
	recID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid recommendation ID", Code: "INVALID_ID"})
		return
	}
	var rec models.MediaCirculationRecommendation
	if err := db.Where("tenant_id = ? AND public_id = ?", principal.TenantID, recID).First(&rec).Error; err != nil {
		c.JSON(http.StatusNotFound, authErrorResponse{Message: "Recommendation not found", Code: "NOT_FOUND"})
		return
	}
	if rec.Status != models.MediaCirculationRecStatusPending {
		c.JSON(http.StatusConflict, authErrorResponse{Message: "Recommendation is not pending", Code: "NOT_PENDING"})
		return
	}

	outcome, applyErr := applyRecommendation(db, principal.TenantID, principal.Email, c.GetHeader("Authorization"), rec)
	if applyErr != nil {
		// Leave the recommendation pending/retryable; record the failed attempt.
		writeCirculationAudit(db, principal, "media_circulation.recommendation.apply", rec.PublicID.String(), map[string]interface{}{
			"unit_type": rec.UnitType, "verdict": rec.Verdict, "outcome": mediaCircOutcomeFailed, "error": applyErr.Error(),
		})
		if errors.Is(applyErr, errMediaCircIntakeBudgetExhausted) {
			c.JSON(http.StatusConflict, authErrorResponse{Message: applyErr.Error(), Code: "INTAKE_BUDGET_EXHAUSTED"})
			return
		}
		c.JSON(http.StatusBadGateway, authErrorResponse{Message: "Execution failed: " + applyErr.Error(), Code: "APPLY_EXEC_FAILED"})
		return
	}

	now := time.Now().UTC()
	rec.Status = models.MediaCirculationRecStatusApplied
	rec.Applied = true
	rec.AppliedAt = &now
	rec.AppliedBy = principal.Email
	rec.Outcome = outcome
	if err := db.Save(&rec).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to record apply", Code: "SAVE_FAILED"})
		return
	}
	invalidateMediaCircHealth(principal.TenantID)
	writeCirculationAudit(db, principal, "media_circulation.recommendation.apply", rec.PublicID.String(), map[string]interface{}{
		"unit_type": rec.UnitType, "verdict": rec.Verdict, "outcome": outcome,
	})
	c.JSON(http.StatusOK, gin.H{"data": rec})
}

func RevertMediaCirculationRecommendation(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	recID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid recommendation ID", Code: "INVALID_ID"})
		return
	}
	var rec models.MediaCirculationRecommendation
	if err := db.Where("tenant_id = ? AND public_id = ?", principal.TenantID, recID).First(&rec).Error; err != nil {
		c.JSON(http.StatusNotFound, authErrorResponse{Message: "Recommendation not found", Code: "NOT_FOUND"})
		return
	}
	if rec.Status != models.MediaCirculationRecStatusApplied {
		c.JSON(http.StatusConflict, authErrorResponse{Message: "Only applied recommendations can be reverted", Code: "NOT_APPLIED"})
		return
	}
	if err := revertRecommendation(db, principal.TenantID, rec); err != nil {
		c.JSON(http.StatusConflict, authErrorResponse{Message: "Recommendation cannot be reverted: " + err.Error(), Code: "NOT_REVERTIBLE"})
		return
	}
	rec.Outcome = mediaCircOutcomeReverted
	if err := db.Save(&rec).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to record revert", Code: "SAVE_FAILED"})
		return
	}
	invalidateMediaCircHealth(principal.TenantID)
	writeCirculationAudit(db, principal, "media_circulation.recommendation.revert", rec.PublicID.String(), map[string]interface{}{
		"unit_type": rec.UnitType, "verdict": rec.Verdict,
	})
	c.JSON(http.StatusOK, gin.H{"data": rec})
}

func DismissMediaCirculationRecommendation(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	recID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid recommendation ID", Code: "INVALID_ID"})
		return
	}
	var rec models.MediaCirculationRecommendation
	if err := db.Where("tenant_id = ? AND public_id = ?", principal.TenantID, recID).First(&rec).Error; err != nil {
		c.JSON(http.StatusNotFound, authErrorResponse{Message: "Recommendation not found", Code: "NOT_FOUND"})
		return
	}
	if rec.Status != models.MediaCirculationRecStatusPending {
		c.JSON(http.StatusConflict, authErrorResponse{Message: "Recommendation is not pending", Code: "NOT_PENDING"})
		return
	}
	now := time.Now().UTC()
	rec.Status = models.MediaCirculationRecStatusDismissed
	rec.Outcome = mediaCircOutcomeDismissed
	rec.AppliedAt = &now
	rec.AppliedBy = principal.Email
	if err := db.Save(&rec).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to dismiss", Code: "SAVE_FAILED"})
		return
	}
	writeCirculationAudit(db, principal, "media_circulation.recommendation.dismiss", rec.PublicID.String(), map[string]interface{}{
		"unit_type": rec.UnitType, "verdict": rec.Verdict,
	})
	c.JSON(http.StatusOK, gin.H{"data": rec})
}

// ----------------------------------------------------------------
// Policy load / sanitize (mirrors loadCirculationPolicy in news circulation)
// ----------------------------------------------------------------

func loadEffectiveMediaCirculationPolicy(db *gorm.DB, tenantID string) models.MediaCirculationPolicy {
	var policy models.MediaCirculationPolicy
	if err := db.Where("tenant_id = ?", tenantID).First(&policy).Error; err != nil {
		return models.DefaultMediaCirculationPolicy(tenantID)
	}
	return sanitizeMediaCirculationPolicy(policy)
}

func sanitizeMediaCirculationPolicy(p models.MediaCirculationPolicy) models.MediaCirculationPolicy {
	if strings.TrimSpace(p.TenantID) == "" {
		p.TenantID = defaultCirculationTenant
	}
	switch p.Preset {
	case models.MediaCirculationPresetConservative,
		models.MediaCirculationPresetBalanced,
		models.MediaCirculationPresetIntakeHungry:
		// valid
	default:
		p.Preset = models.MediaCirculationPresetBalanced
	}
	p.ValueFloor = clampFloat(p.ValueFloor, 0, 1)
	p.MarginalMargin = clampFloat(p.MarginalMargin, 0, 1)
	p.FreshnessDemandWeight = clampFloat(p.FreshnessDemandWeight, 0, 1)
	if p.MaxIntakePerSourcePerCycle < 0 {
		p.MaxIntakePerSourcePerCycle = 0
	}
	if p.MaxIntakePerCycle < 0 {
		p.MaxIntakePerCycle = 0
	}
	if p.SourceMinIntervalMinutes < 1 {
		p.SourceMinIntervalMinutes = 60
	}
	if p.SourceMaxIntervalMinutes < p.SourceMinIntervalMinutes {
		p.SourceMaxIntervalMinutes = p.SourceMinIntervalMinutes
	}
	return p
}

// ----------------------------------------------------------------
// Health composition
// ----------------------------------------------------------------

func buildMediaCirculationHealth(db *gorm.DB, tenantID, authorization string) mediaCirculationHealthResponse {
	// Storage/cost side — reuse wholesale, do not re-derive (D10).
	storage := buildStorageHealthResponse(db, tenantID, authorization)
	opBudget := getStorageOpBudgetStatus(db, tenantID)
	backlog := computeMediaCircAtomizationBacklog(db, tenantID)
	// Intake-demand side — circulation-owned.
	buckets := computeLibraryBucketInventory(db, tenantID)
	policy := loadEffectiveMediaCirculationPolicy(db, tenantID)

	thin := make([]string, 0, len(buckets))
	for _, b := range buckets {
		if b.State == "thin" {
			thin = append(thin, b.Bucket)
		}
	}

	headline, reasons := composeMediaCirculationHeadline(storage.State, thin)
	return mediaCirculationHealthResponse{
		Headline:     headline,
		Score:        storage.Score,
		Summary:      mediaCirculationSummary(headline),
		StorageState: storage.State,
		Reasons:      reasons,
		GeneratedAt:  time.Now().UTC().Format(time.RFC3339),
		Proof: mediaCirculationProof{
			Storage:              storage.Proof,
			OpBudget:             opBudget,
			AtomizationBacklog:   backlog,
			Buckets:              buckets,
			ThinBuckets:          thin,
			EvictByVerdict:       countPendingEvictByVerdict(db, tenantID),
			AppliedYieldByBucket: computeAppliedIntakeYieldByBucket(db, tenantID),
		},
		Policy: policy,
	}
}

// computeLibraryBucketInventory counts currently-visible feed units per duration
// bucket and classifies each thin/ok/saturated. This is the "does the library need
// more?" read model that gates intake (D5).
func computeLibraryBucketInventory(db *gorm.DB, tenantID string) []libraryBucketHealth {
	type bucketRow struct {
		DurationBucket string
		Count          int64
	}
	var rows []bucketRow
	db.Model(&models.ContentItem{}).
		Select("duration_bucket, COUNT(*) as count").
		Where("tenant_id = ?", tenantID).
		Where("is_feed_unit = TRUE").
		Where("feed_visibility = ?", "visible").
		Where("status = ?", models.ContentStatusReady).
		Where("type IN ?", []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}).
		Where("duration_bucket IS NOT NULL").
		Group("duration_bucket").
		Scan(&rows)

	counts := make(map[string]int64, len(rows))
	for _, r := range rows {
		counts[r.DurationBucket] = r.Count
	}

	// Demand surface: measured serve-side demand/coverage/gap. When measured,
	// the gap replaces supply counts as the state judgment (W6 — the cache
	// finally has a miss signal); otherwise the count-based classification
	// remains the fallback guess.
	snapshots, measured := intelligence.Engine{DB: db}.DemandSnapshots(tenantID, mediaCirculationBuckets)

	inv := make([]libraryBucketHealth, 0, len(mediaCirculationBuckets))
	for _, b := range mediaCirculationBuckets {
		count := counts[b]
		snap := snapshots[b]
		state := classifyBucketHealth(count)
		if measured {
			state = intelligence.GapState(snap.Gap)
		}
		inv = append(inv, libraryBucketHealth{
			Bucket:        b,
			VisibleUnits:  count,
			State:         state,
			DemandScore:   snap.DemandScore,
			CoverageScore: snap.CoverageScore,
			Gap:           snap.Gap,
			Measured:      measured,
		})
	}
	return inv
}

func classifyBucketHealth(count int64) string {
	switch {
	case count < mediaCirculationBucketThinFloor:
		return "thin"
	case count > mediaCirculationBucketSaturatedCeil:
		return "saturated"
	default:
		return "ok"
	}
}

// composeMediaCirculationHeadline maps the storage state to a base headline and
// overlays feed_thin. Pure function (no DB) so it is unit-testable. Precedence:
// degraded / over_budget (urgent cost/health) outrank feed_thin (supply gap), but
// thin buckets always surface in reasons.
func composeMediaCirculationHeadline(storageState string, thinBuckets []string) (string, []string) {
	reasons := make([]string, 0, 3)
	base := "healthy"
	switch storageState {
	case "healthy":
		base = "healthy"
	case "watch":
		base = "watch"
		reasons = append(reasons, "Storage is approaching the cost target.")
	case "pressure", "critical":
		base = "over_budget"
		reasons = append(reasons, "Storage is over the cost target; eviction should fire before new intake.")
	case "degraded_no_cold", "degraded":
		base = "degraded"
		reasons = append(reasons, "Storage metrics are degraded; treat recommendations cautiously.")
	default:
		base = "watch"
	}

	if len(thinBuckets) > 0 {
		reasons = append(reasons, "Feed inventory is thin in duration buckets: "+strings.Join(thinBuckets, ", ")+".")
		if base == "healthy" || base == "watch" {
			base = "feed_thin"
		}
	}
	if base == "healthy" {
		reasons = append(reasons, "Library is within the storage cost target and all duration buckets are adequately supplied.")
	}
	return base, reasons
}

func mediaCirculationSummary(headline string) string {
	switch headline {
	case "healthy":
		return "Library is within the storage cost target and all duration buckets are adequately supplied."
	case "watch":
		return "Storage is approaching the cost target; prepare eviction before intake."
	case "feed_thin":
		return "Some duration buckets are under-supplied; intake is warranted where quality clears the floor."
	case "over_budget":
		return "Storage is over the cost target; prioritize eviction / cost hygiene before new intake."
	case "degraded":
		return "Storage metrics are degraded; verdicts are best-effort until live stats recover."
	default:
		return "Media circulation health computed."
	}
}

// ----------------------------------------------------------------
// Per-tenant TTL cache (mirrors storageHealthCache, minus singleflight — the heavy
// storage call underneath is already cached)
// ----------------------------------------------------------------

type cachedMediaCircHealth struct {
	response  mediaCirculationHealthResponse
	expiresAt time.Time
}

var (
	mediaCircHealthMu    sync.Mutex
	mediaCircHealthCache = map[string]cachedMediaCircHealth{}
)

func getCachedMediaCircHealth(tenantID string) (mediaCirculationHealthResponse, bool) {
	mediaCircHealthMu.Lock()
	defer mediaCircHealthMu.Unlock()
	cached, ok := mediaCircHealthCache[tenantID]
	if !ok || time.Now().After(cached.expiresAt) {
		return mediaCirculationHealthResponse{}, false
	}
	return cached.response, true
}

func storeMediaCircHealth(tenantID string, resp mediaCirculationHealthResponse) {
	mediaCircHealthMu.Lock()
	defer mediaCircHealthMu.Unlock()
	mediaCircHealthCache[tenantID] = cachedMediaCircHealth{
		response:  resp,
		expiresAt: time.Now().Add(mediaCircHealthCacheTTL),
	}
}

func invalidateMediaCircHealth(tenantID string) {
	mediaCircHealthMu.Lock()
	defer mediaCircHealthMu.Unlock()
	delete(mediaCircHealthCache, tenantID)
}
