package controllers

import (
	"net/http"

	"content-management-system/src/intelligence"
	"content-management-system/src/models"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// GetMediaIntelligenceDiagnostics handles
// GET /admin/media/circulation/intelligence — the Ranking/Intelligence
// observability read model: exploration pipeline counts, score-refresh health,
// and the per-topic demand table. Read-only.
func GetMediaIntelligenceDiagnostics(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	snapshot := intelligence.Engine{DB: db}.DiagnosticsSnapshot(principal.TenantID)
	c.JSON(http.StatusOK, snapshot)
}

// GetMediaIntelligenceObservatory handles
// GET /admin/media/intelligence/observatory — the full visualization read model
// for the Intelligence page: diagnostics counts + the value histogram, corpus
// signal averages, per-bucket demand, and the live tuning the curves need.
func GetMediaIntelligenceObservatory(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	snapshot := intelligence.Engine{DB: db}.ObservatorySnapshot(principal.TenantID, mediaCirculationBuckets)
	c.JSON(http.StatusOK, snapshot)
}

// mediaIntelligenceConfigResponse carries the tenant's effective config plus the
// code defaults, so the control room can render per-field default hints and a
// "reset to defaults" action without hardcoding numbers.
type mediaIntelligenceConfigResponse struct {
	Config   models.MediaIntelligenceConfig `json:"config"`
	Defaults models.MediaIntelligenceConfig `json:"defaults"`
}

type mediaIntelligenceConfigPatch struct {
	EngagementWeight *float64 `json:"engagement_weight"`
	CompletionWeight *float64 `json:"completion_weight"`
	QualityWeight    *float64 `json:"quality_weight"`
	VelocityWeight   *float64 `json:"velocity_weight"`

	ExplorationSliceEvery   *int `json:"exploration_slice_every"`
	ExploreImpressionTarget *int `json:"explore_impression_target"`
	LegacyExposureViewFloor *int `json:"legacy_exposure_view_floor"`

	DemotionDefaultFactor *float64 `json:"demotion_default_factor"`
	DemotionHalfLifeDays  *int     `json:"demotion_half_life_days"`
}

// GetMediaIntelligenceConfig handles GET /admin/media/intelligence/config —
// the media-value engine's operational tunables. Returns the tenant row (or the
// code defaults when none exists), always sanitized, alongside the defaults.
func GetMediaIntelligenceConfig(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	var cfg models.MediaIntelligenceConfig
	if err := db.Where("tenant_id = ?", principal.TenantID).First(&cfg).Error; err != nil {
		cfg = models.DefaultMediaIntelligenceConfig(principal.TenantID)
	}
	c.JSON(http.StatusOK, mediaIntelligenceConfigResponse{
		Config:   intelligence.SanitizeConfig(cfg),
		Defaults: models.DefaultMediaIntelligenceConfig(principal.TenantID),
	})
}

// UpdateMediaIntelligenceConfig handles PUT /admin/media/intelligence/config.
// Accepts partial patches: omitted fields keep the current tenant value (or the
// code default when no row exists), explicit zero values still apply. Weights
// are normalized to sum 1.0 and every knob is clamped; all-zero weights after
// the patch are rejected. Invalidates the engine's tuning cache so the change
// takes effect within one request (no restart).
func UpdateMediaIntelligenceConfig(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	var patch mediaIntelligenceConfigPatch
	if err := c.ShouldBindJSON(&patch); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid request: " + err.Error(), Code: "INVALID_REQUEST"})
		return
	}

	cfg := loadMediaIntelligenceConfigForPatch(db, principal.TenantID)
	cfg = applyMediaIntelligenceConfigPatch(cfg, patch)
	if cfg.EngagementWeight+cfg.CompletionWeight+cfg.QualityWeight+cfg.VelocityWeight <= 0 {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "At least one signal weight must be positive", Code: "INVALID_WEIGHTS"})
		return
	}

	cfg.TenantID = principal.TenantID
	cfg = intelligence.SanitizeConfig(cfg)
	cfg.ID = 0
	if err := db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "tenant_id"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"engagement_weight", "completion_weight", "quality_weight", "velocity_weight",
			"exploration_slice_every", "explore_impression_target", "legacy_exposure_view_floor",
			"demotion_default_factor", "demotion_half_life_days", "updated_at",
		}),
	}).Create(&cfg).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to save config", Code: "SAVE_FAILED"})
		return
	}
	intelligence.InvalidateTuningCache()
	c.JSON(http.StatusOK, mediaIntelligenceConfigResponse{
		Config:   cfg,
		Defaults: models.DefaultMediaIntelligenceConfig(principal.TenantID),
	})
}

func loadMediaIntelligenceConfigForPatch(db *gorm.DB, tenantID string) models.MediaIntelligenceConfig {
	var cfg models.MediaIntelligenceConfig
	if err := db.Where("tenant_id = ?", tenantID).First(&cfg).Error; err != nil {
		cfg = models.DefaultMediaIntelligenceConfig(tenantID)
	}
	return intelligence.SanitizeConfig(cfg)
}

func applyMediaIntelligenceConfigPatch(cfg models.MediaIntelligenceConfig, patch mediaIntelligenceConfigPatch) models.MediaIntelligenceConfig {
	if patch.EngagementWeight != nil {
		cfg.EngagementWeight = *patch.EngagementWeight
	}
	if patch.CompletionWeight != nil {
		cfg.CompletionWeight = *patch.CompletionWeight
	}
	if patch.QualityWeight != nil {
		cfg.QualityWeight = *patch.QualityWeight
	}
	if patch.VelocityWeight != nil {
		cfg.VelocityWeight = *patch.VelocityWeight
	}
	if patch.ExplorationSliceEvery != nil {
		cfg.ExplorationSliceEvery = *patch.ExplorationSliceEvery
	}
	if patch.ExploreImpressionTarget != nil {
		cfg.ExploreImpressionTarget = *patch.ExploreImpressionTarget
	}
	if patch.LegacyExposureViewFloor != nil {
		cfg.LegacyExposureViewFloor = *patch.LegacyExposureViewFloor
	}
	if patch.DemotionDefaultFactor != nil {
		cfg.DemotionDefaultFactor = *patch.DemotionDefaultFactor
	}
	if patch.DemotionHalfLifeDays != nil {
		cfg.DemotionHalfLifeDays = *patch.DemotionHalfLifeDays
	}
	return cfg
}

type mediaIntelligenceRefreshResult struct {
	Refreshed         int `json:"refreshed"`
	StaleRemaining    int `json:"stale_remaining"`
	UnscoredRemaining int `json:"unscored_remaining"`
}

// TriggerMediaIntelligenceRefresh handles POST /admin/media/intelligence/refresh
// — the control-room "refresh scores now" action. Runs one bounded refresh
// batch synchronously (safe — capped at the default budget) and reports how much
// staleness remains.
func TriggerMediaIntelligenceRefresh(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	engine := intelligence.Engine{DB: db}

	refreshed := engine.RefreshStale(principal.TenantID, 0)
	snapshot := engine.DiagnosticsSnapshot(principal.TenantID)
	c.JSON(http.StatusOK, mediaIntelligenceRefreshResult{
		Refreshed:         refreshed,
		StaleRemaining:    int(snapshot.StaleCount),
		UnscoredRemaining: int(snapshot.UnscoredCount),
	})
}
