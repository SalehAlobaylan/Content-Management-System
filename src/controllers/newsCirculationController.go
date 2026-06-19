package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"encoding/json"
	"math"
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

const defaultCirculationTenant = "default"

type circulationWindow struct {
	Name           string
	PrimaryStart   time.Time
	QueryStart     time.Time
	Now            time.Time
	Location       *time.Location
	CarryoverHours int
}

type circulationContext struct {
	Policy models.NewsCirculationPolicy
	Window circulationWindow
}

func normalizeNewsWindow(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case models.NewsWindowWeek:
		return models.NewsWindowWeek
	case models.NewsWindowMonth:
		return models.NewsWindowMonth
	default:
		return models.NewsWindowToday
	}
}

func loadCirculationPolicy(db *gorm.DB, tenantID string) models.NewsCirculationPolicy {
	var policy models.NewsCirculationPolicy
	if err := db.Where("tenant_id = ?", tenantID).First(&policy).Error; err != nil {
		return models.DefaultNewsCirculationPolicy(tenantID)
	}
	return sanitizeCirculationPolicy(policy)
}

func sanitizeCirculationPolicy(policy models.NewsCirculationPolicy) models.NewsCirculationPolicy {
	if strings.TrimSpace(policy.TenantID) == "" {
		policy.TenantID = defaultCirculationTenant
	}
	if strings.TrimSpace(policy.Preset) == "" {
		policy.Preset = models.NewsCirculationPresetLatestPlus
	}
	if strings.TrimSpace(policy.Timezone) == "" {
		policy.Timezone = "Asia/Riyadh"
	}
	if policy.MinTodayStories < 1 {
		policy.MinTodayStories = 8
	}
	if policy.CarryoverHours < 1 {
		policy.CarryoverHours = 72
	}
	if policy.BreakingMaxAgeMinutes < 1 {
		policy.BreakingMaxAgeMinutes = 180
	}
	if policy.BreakingMinMembers < 1 {
		policy.BreakingMinMembers = 3
	}
	if policy.SourceClaimIntervalMins < 1 {
		policy.SourceClaimIntervalMins = 15
	}
	if policy.SourceMinIntervalMinutes < 1 {
		policy.SourceMinIntervalMinutes = 10
	}
	if policy.SourceMaxIntervalMinutes < policy.SourceMinIntervalMinutes {
		policy.SourceMaxIntervalMinutes = policy.SourceMinIntervalMinutes
	}
	if policy.SourceMaxChangePercent < 1 {
		policy.SourceMaxChangePercent = 50
	}
	switch policy.SourceCadenceMode {
	case models.SourceCadenceModeSuggest, models.SourceCadenceModeAutoApply, models.SourceCadenceModeManual:
	default:
		policy.SourceCadenceMode = models.SourceCadenceModeSuggest
	}
	return policy
}

func applyLatestPlusPolicy(config models.RankingConfig, policy models.NewsCirculationPolicy) models.RankingConfig {
	config.FreshnessWeight = policy.RecencyWeight
	config.EngagementWeight = policy.ImportanceWeight
	config.VelocityWeight = policy.MomentumWeight
	config.SimilarityWeight = 0
	config.QualityWeight = policy.SourceQualityWeight
	config.DiversityWeight = policy.DiversityWeight
	config.TrendingWeight = policy.TrendingWeight
	config.StoryCoverageWeight = policy.CoverageWeight
	config.FreshnessDecayHours = 24
	if config.VelocityWindowHours <= 0 {
		config.VelocityWindowHours = 6
	}
	return config
}

func circulationWindowFor(policy models.NewsCirculationPolicy, window string, now time.Time) circulationWindow {
	loc, err := time.LoadLocation(policy.Timezone)
	if err != nil {
		loc, _ = time.LoadLocation("Asia/Riyadh")
	}
	if loc == nil {
		loc = time.FixedZone("Asia/Riyadh", 3*60*60)
	}
	localNow := now.In(loc)
	startOfDay := time.Date(localNow.Year(), localNow.Month(), localNow.Day(), 0, 0, 0, 0, loc)

	primary := startOfDay
	switch window {
	case models.NewsWindowWeek:
		daysSinceSunday := int(localNow.Weekday())
		primary = startOfDay.AddDate(0, 0, -daysSinceSunday)
	case models.NewsWindowMonth:
		primary = time.Date(localNow.Year(), localNow.Month(), 1, 0, 0, 0, 0, loc)
	}

	queryStart := primary
	if window == models.NewsWindowToday {
		queryStart = primary.Add(-time.Duration(policy.CarryoverHours) * time.Hour)
	}

	return circulationWindow{
		Name:           window,
		PrimaryStart:   primary.UTC(),
		QueryStart:     queryStart.UTC(),
		Now:            now.UTC(),
		Location:       loc,
		CarryoverHours: policy.CarryoverHours,
	}
}

func circulationContextFor(db *gorm.DB, tenantID, rawWindow string, now time.Time) circulationContext {
	policy := loadCirculationPolicy(db, tenantID)
	window := normalizeNewsWindow(rawWindow)
	return circulationContext{
		Policy: policy,
		Window: circulationWindowFor(policy, window, now),
	}
}

func storyLifecycle(policy models.NewsCirculationPolicy, window circulationWindow, lastMemberAt time.Time, memberCount int, isCarryover bool) string {
	if lastMemberAt.IsZero() {
		return models.NewsLifecycleHistorical
	}
	age := window.Now.Sub(lastMemberAt)
	if age < 0 {
		age = 0
	}
	if isCarryover {
		return models.NewsLifecycleCooling
	}
	if age <= time.Duration(policy.BreakingMaxAgeMinutes)*time.Minute && memberCount >= policy.BreakingMinMembers {
		return models.NewsLifecycleBreaking
	}
	if !lastMemberAt.Before(window.PrimaryStart) {
		return models.NewsLifecycleActive
	}
	if age <= time.Duration(policy.CarryoverHours)*time.Hour {
		return models.NewsLifecycleCooling
	}
	return models.NewsLifecycleHistorical
}

func activeStoryOverrides(db *gorm.DB, tenantID string, storyIDs []uuid.UUID, now time.Time) map[uuid.UUID]models.NewsStoryOverride {
	out := make(map[uuid.UUID]models.NewsStoryOverride)
	if len(storyIDs) == 0 {
		return out
	}
	var rows []models.NewsStoryOverride
	db.Where("tenant_id = ? AND story_id IN ?", tenantID, storyIDs).
		Where("expires_at IS NULL OR expires_at > ?", now).
		Find(&rows)
	for _, row := range rows {
		out[row.StoryID] = row
	}
	return out
}

func markAllNewsSnapshotsDirty(db *gorm.DB, tenantID string) {
	db.Model(&models.NewsSnapshot{}).
		Where("tenant_id = ?", tenantID).
		UpdateColumn("dirty", true)
}

func writeCirculationAudit(db *gorm.DB, principal utils.AdminPrincipal, action, resource string, payload map[string]interface{}) {
	entry := models.AuditLog{
		TenantID:       principal.TenantID,
		UserID:         principal.UserID,
		UserEmail:      principal.Email,
		Action:         action,
		TargetService:  "news_circulation",
		TargetResource: resource,
		Status:         "success",
	}
	if payload != nil {
		if raw, err := json.Marshal(payload); err == nil {
			entry.Payload = datatypes.JSON(raw)
		}
	}
	_ = db.Create(&entry).Error
}

// ─── Admin API ─────────────────────────────────────────────────────────────

func GetCirculationPolicy(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	c.JSON(http.StatusOK, loadCirculationPolicy(db, principal.TenantID))
}

func UpdateCirculationPolicy(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	var req models.NewsCirculationPolicy
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid request: " + err.Error(), Code: "INVALID_REQUEST"})
		return
	}
	req.TenantID = principal.TenantID
	req = sanitizeCirculationPolicy(req)
	if req.ID == 0 {
		var existing models.NewsCirculationPolicy
		if err := db.Where("tenant_id = ?", principal.TenantID).First(&existing).Error; err == nil {
			req.ID = existing.ID
			req.CreatedAt = existing.CreatedAt
		}
	}
	if err := db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "tenant_id"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"preset", "timezone", "min_today_stories", "carryover_hours", "carryover_min_score",
			"breaking_max_age_minutes", "breaking_min_members", "recency_weight", "importance_weight",
			"momentum_weight", "coverage_weight", "source_quality_weight", "diversity_weight",
			"trending_weight", "source_cadence_mode", "source_claim_interval_minutes",
			"source_min_interval_minutes", "source_max_interval_minutes", "source_max_change_percent",
			"updated_at",
		}),
	}).Create(&req).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to save policy", Code: "SAVE_FAILED"})
		return
	}
	markAllNewsSnapshotsDirty(db, principal.TenantID)
	writeCirculationAudit(db, principal, "circulation.policy.update", principal.TenantID, map[string]interface{}{
		"preset":              req.Preset,
		"timezone":            req.Timezone,
		"source_cadence_mode": req.SourceCadenceMode,
	})
	triggerCirculationResync(c.GetHeader("Authorization"))
	c.JSON(http.StatusOK, req)
}

func ApplyCirculationPreset(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	preset := strings.TrimSpace(c.Param("preset"))
	if preset == "" {
		preset = models.NewsCirculationPresetLatestPlus
	}
	if preset != models.NewsCirculationPresetLatestPlus {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Unknown circulation preset", Code: "UNKNOWN_PRESET"})
		return
	}
	policy := models.DefaultNewsCirculationPolicy(principal.TenantID)
	if err := db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "tenant_id"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"preset", "timezone", "min_today_stories", "carryover_hours", "carryover_min_score",
			"breaking_max_age_minutes", "breaking_min_members", "recency_weight", "importance_weight",
			"momentum_weight", "coverage_weight", "source_quality_weight", "diversity_weight",
			"trending_weight", "source_cadence_mode", "source_claim_interval_minutes",
			"source_min_interval_minutes", "source_max_interval_minutes", "source_max_change_percent",
			"updated_at",
		}),
	}).Create(&policy).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to apply preset", Code: "SAVE_FAILED"})
		return
	}
	markAllNewsSnapshotsDirty(db, principal.TenantID)
	writeCirculationAudit(db, principal, "circulation.policy.preset", preset, map[string]interface{}{
		"preset": preset,
	})
	triggerCirculationResync(c.GetHeader("Authorization"))
	c.JSON(http.StatusOK, policy)
}

func PreviewCirculation(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "20"))
	if limit < 1 || limit > 60 {
		limit = 20
	}
	config := loadTenantConfig(db, principal.TenantID)
	ctx := circulationContextFor(db, principal.TenantID, c.Query("window"), time.Now())
	slides, _ := assembleStoryNewsFeed(db, principal.TenantID, config, ctx, time.Time{}, uuid.Nil, limit, nil)
	c.JSON(http.StatusOK, StoryNewsResponse{Cursor: nil, Slides: slides})
}

func GetCirculationMetrics(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	policy := loadCirculationPolicy(db, principal.TenantID)
	now := time.Now()

	type windowMetric struct {
		Window      string `json:"window"`
		Stories     int64  `json:"stories"`
		Breaking    int64  `json:"breaking"`
		Active      int64  `json:"active"`
		Cooling     int64  `json:"cooling"`
		Historical  int64  `json:"historical"`
		Carryover   int64  `json:"carryover"`
		PrimaryFrom string `json:"primary_from"`
	}

	metrics := make([]windowMetric, 0, 3)
	for _, w := range []string{models.NewsWindowToday, models.NewsWindowWeek, models.NewsWindowMonth} {
		win := circulationWindowFor(policy, w, now)
		var topics []models.Topic
		db.Select(topicMetaColumns).
			Where("tenant_id = ? AND article_count > 0 AND last_member_at IS NOT NULL AND last_member_at >= ?", principal.TenantID, win.QueryStart).
			Find(&topics)
		m := windowMetric{Window: w, PrimaryFrom: win.PrimaryStart.Format(time.RFC3339)}
		for _, t := range topics {
			if t.LastMemberAt == nil {
				continue
			}
			carry := w == models.NewsWindowToday && t.LastMemberAt.Before(win.PrimaryStart)
			lifecycle := storyLifecycle(policy, win, *t.LastMemberAt, t.ArticleCount, carry)
			m.Stories++
			if carry {
				m.Carryover++
			}
			switch lifecycle {
			case models.NewsLifecycleBreaking:
				m.Breaking++
			case models.NewsLifecycleActive:
				m.Active++
			case models.NewsLifecycleCooling:
				m.Cooling++
			default:
				m.Historical++
			}
		}
		metrics = append(metrics, m)
	}

	var activeSources int64
	db.Model(&models.ContentSource{}).
		Where("tenant_id = ? AND category = ? AND is_active = ?", principal.TenantID, models.SourceCategoryNews, true).
		Count(&activeSources)
	var pendingRecommendations int64
	db.Model(&models.SourceCirculationRecommendation{}).
		Where("tenant_id = ? AND applied = false", principal.TenantID).
		Count(&pendingRecommendations)

	c.JSON(http.StatusOK, gin.H{
		"windows":                 metrics,
		"active_sources":          activeSources,
		"pending_recommendations": pendingRecommendations,
		"policy":                  policy,
	})
}

func ListStoryOverrides(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var rows []models.NewsStoryOverride
	db.Where("tenant_id = ?", principal.TenantID).Order("updated_at DESC").Find(&rows)
	c.JSON(http.StatusOK, gin.H{"data": rows})
}

type storyOverrideRequest struct {
	PinToTop        *bool    `json:"pin_to_top"`
	Suppress        *bool    `json:"suppress"`
	ExcludeFromFeed *bool    `json:"exclude_from_feed"`
	ImportanceBoost *float64 `json:"importance_boost"`
	Notes           string   `json:"notes"`
	ExpiresAt       *string  `json:"expires_at"`
}

func UpsertStoryOverride(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	storyID, err := uuid.Parse(c.Param("story_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid story ID", Code: "INVALID_ID"})
		return
	}
	var topic models.Topic
	if err := db.Where("tenant_id = ? AND public_id = ?", principal.TenantID, storyID).First(&topic).Error; err != nil {
		c.JSON(http.StatusNotFound, authErrorResponse{Message: "Story not found", Code: "NOT_FOUND"})
		return
	}

	var req storyOverrideRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid request", Code: "INVALID_REQUEST"})
		return
	}

	var row models.NewsStoryOverride
	isNew := db.Where("tenant_id = ? AND story_id = ?", principal.TenantID, storyID).First(&row).Error != nil
	if isNew {
		row = models.NewsStoryOverride{TenantID: principal.TenantID, StoryID: storyID, ImportanceBoost: 1.0}
	}
	if req.PinToTop != nil {
		row.PinToTop = *req.PinToTop
	}
	if req.Suppress != nil {
		row.Suppress = *req.Suppress
	}
	if req.ExcludeFromFeed != nil {
		row.ExcludeFromFeed = *req.ExcludeFromFeed
	}
	if req.ImportanceBoost != nil {
		row.ImportanceBoost = *req.ImportanceBoost
	}
	if row.ImportanceBoost <= 0 {
		row.ImportanceBoost = 1.0
	}
	row.Notes = req.Notes
	row.SetBy = principal.Email
	if req.ExpiresAt != nil && strings.TrimSpace(*req.ExpiresAt) != "" {
		if t, err := time.Parse(time.RFC3339, *req.ExpiresAt); err == nil {
			row.ExpiresAt = &t
		}
	} else if req.ExpiresAt != nil {
		row.ExpiresAt = nil
	}

	if err := db.Save(&row).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to save override", Code: "SAVE_FAILED"})
		return
	}
	markAllNewsSnapshotsDirty(db, principal.TenantID)
	writeCirculationAudit(db, principal, "circulation.override.upsert", storyID.String(), map[string]interface{}{
		"pin_to_top":         row.PinToTop,
		"suppress":           row.Suppress,
		"exclude_from_feed":  row.ExcludeFromFeed,
		"importance_boost":   row.ImportanceBoost,
		"expires_at_present": row.ExpiresAt != nil,
	})
	c.JSON(http.StatusOK, row)
}

func DeleteStoryOverride(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	storyID, err := uuid.Parse(c.Param("story_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid story ID", Code: "INVALID_ID"})
		return
	}
	result := db.Where("tenant_id = ? AND story_id = ?", principal.TenantID, storyID).Delete(&models.NewsStoryOverride{})
	if result.RowsAffected == 0 {
		c.JSON(http.StatusNotFound, authErrorResponse{Message: "Override not found", Code: "NOT_FOUND"})
		return
	}
	markAllNewsSnapshotsDirty(db, principal.TenantID)
	writeCirculationAudit(db, principal, "circulation.override.delete", storyID.String(), nil)
	c.JSON(http.StatusOK, gin.H{"message": "Override deleted"})
}

func ListSourceRecommendations(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var rows []models.SourceCirculationRecommendation
	db.Where("tenant_id = ?", principal.TenantID).
		Order("applied ASC, updated_at DESC").
		Limit(100).
		Find(&rows)
	c.JSON(http.StatusOK, gin.H{"data": rows})
}

func GenerateSourceRecommendations(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	policy := loadCirculationPolicy(db, principal.TenantID)
	rows, applied := generateSourceRecommendations(db, policy)
	writeCirculationAudit(db, principal, "circulation.recommendations.generate", principal.TenantID, map[string]interface{}{
		"count":        len(rows),
		"auto_applied": applied,
	})
	c.JSON(http.StatusOK, gin.H{"data": rows, "auto_applied": applied})
}

func ApplySourceRecommendation(c *gin.Context) {
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
	var rec models.SourceCirculationRecommendation
	if err := db.Where("tenant_id = ? AND public_id = ?", principal.TenantID, recID).First(&rec).Error; err != nil {
		c.JSON(http.StatusNotFound, authErrorResponse{Message: "Recommendation not found", Code: "NOT_FOUND"})
		return
	}
	if err := applySourceRecommendation(db, &rec); err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to apply recommendation", Code: "APPLY_FAILED"})
		return
	}
	triggerCirculationResync(c.GetHeader("Authorization"))
	writeCirculationAudit(db, principal, "circulation.recommendation.apply", rec.PublicID.String(), map[string]interface{}{
		"source_id":                    rec.SourceID.String(),
		"current_interval_minutes":     rec.CurrentIntervalMinutes,
		"recommended_interval_minutes": rec.RecommendedIntervalMinutes,
	})
	c.JSON(http.StatusOK, rec)
}

// ─── Internal API used by Aggregation ──────────────────────────────────────

func InternalGetCirculationPolicy(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	tenantID := strings.TrimSpace(c.Query("tenant_id"))
	if tenantID == "" {
		tenantID = defaultCirculationTenant
	}
	c.JSON(http.StatusOK, loadCirculationPolicy(db, tenantID))
}

func InternalClaimCirculationSources(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	tenantID := strings.TrimSpace(c.Query("tenant_id"))
	if tenantID == "" {
		tenantID = defaultCirculationTenant
	}
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "20"))
	if limit < 1 || limit > 100 {
		limit = 20
	}
	policy := loadCirculationPolicy(db, tenantID)
	now := time.Now().UTC()

	var sources []models.ContentSource
	db.Where("tenant_id = ? AND category = ? AND is_active = ?", tenantID, models.SourceCategoryNews, true).
		Order("last_fetched_at ASC NULLS FIRST").
		Limit(200).
		Find(&sources)

	type sourceClaim struct {
		ID                   string                 `json:"id"`
		Name                 string                 `json:"name"`
		Type                 string                 `json:"type"`
		URL                  string                 `json:"url"`
		FetchIntervalMinutes int                    `json:"fetch_interval_minutes"`
		Settings             map[string]interface{} `json:"settings"`
	}
	claims := make([]sourceClaim, 0, limit)
	for _, source := range sources {
		if len(claims) >= limit {
			break
		}
		if source.FeedURL == nil || strings.TrimSpace(*source.FeedURL) == "" {
			continue
		}
		interval := source.FetchIntervalMinutes
		if interval < policy.SourceMinIntervalMinutes {
			interval = policy.SourceMinIntervalMinutes
		}
		due := source.LastFetchedAt == nil || source.LastFetchedAt.Before(now.Add(-time.Duration(interval)*time.Minute))
		if !due {
			continue
		}
		settings := map[string]interface{}{}
		_ = json.Unmarshal(source.APIConfig, &settings)
		claims = append(claims, sourceClaim{
			ID:                   source.PublicID.String(),
			Name:                 source.Name,
			Type:                 string(source.Type),
			URL:                  *source.FeedURL,
			FetchIntervalMinutes: source.FetchIntervalMinutes,
			Settings:             settings,
		})
	}
	if len(claims) > 0 {
		ids := make([]uuid.UUID, 0, len(claims))
		for _, claim := range claims {
			if id, err := uuid.Parse(claim.ID); err == nil {
				ids = append(ids, id)
			}
		}
		db.Model(&models.ContentSource{}).
			Where("tenant_id = ? AND public_id IN ?", tenantID, ids).
			UpdateColumn("last_fetched_at", now)
	}
	c.JSON(http.StatusOK, gin.H{"data": claims, "policy": policy})
}

type sourceRunReportRequest struct {
	TenantID    string                 `json:"tenant_id"`
	SourceID    string                 `json:"source_id"`
	JobID       string                 `json:"job_id"`
	TriggeredBy string                 `json:"triggered_by"`
	Fetched     int                    `json:"fetched"`
	Accepted    int                    `json:"accepted"`
	Duplicates  int                    `json:"duplicates"`
	Filtered    int                    `json:"filtered"`
	Failed      int                    `json:"failed"`
	StartedAt   *string                `json:"started_at"`
	FinishedAt  *string                `json:"finished_at"`
	DurationMs  int                    `json:"duration_ms"`
	Metadata    map[string]interface{} `json:"metadata"`
}

func InternalReportSourceRun(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	var req sourceRunReportRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid request"})
		return
	}
	tenantID := strings.TrimSpace(req.TenantID)
	if tenantID == "" {
		tenantID = defaultCirculationTenant
	}
	sourceID, err := uuid.Parse(req.SourceID)
	if err != nil || strings.TrimSpace(req.JobID) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"message": "source_id and job_id are required"})
		return
	}
	var startedAt, finishedAt *time.Time
	if req.StartedAt != nil {
		if t, err := time.Parse(time.RFC3339, *req.StartedAt); err == nil {
			startedAt = &t
		}
	}
	if req.FinishedAt != nil {
		if t, err := time.Parse(time.RFC3339, *req.FinishedAt); err == nil {
			finishedAt = &t
		}
	}
	rawMeta, _ := json.Marshal(req.Metadata)
	run := models.SourceRunTelemetry{
		TenantID:    tenantID,
		SourceID:    sourceID,
		JobID:       req.JobID,
		TriggeredBy: req.TriggeredBy,
		Fetched:     req.Fetched,
		Accepted:    req.Accepted,
		Duplicates:  req.Duplicates,
		Filtered:    req.Filtered,
		Failed:      req.Failed,
		StartedAt:   startedAt,
		FinishedAt:  finishedAt,
		DurationMs:  req.DurationMs,
		Metadata:    datatypes.JSON(rawMeta),
	}
	if run.TriggeredBy == "" {
		run.TriggeredBy = "schedule"
	}
	if err := db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "job_id"}},
		DoUpdates: clause.Assignments(map[string]interface{}{
			"fetched":     gorm.Expr("GREATEST(source_run_telemetry.fetched, ?)", run.Fetched),
			"accepted":    gorm.Expr("GREATEST(source_run_telemetry.accepted, ?)", run.Accepted),
			"duplicates":  gorm.Expr("GREATEST(source_run_telemetry.duplicates, ?)", run.Duplicates),
			"filtered":    gorm.Expr("GREATEST(source_run_telemetry.filtered, ?)", run.Filtered),
			"failed":      gorm.Expr("GREATEST(source_run_telemetry.failed, ?)", run.Failed),
			"finished_at": run.FinishedAt,
			"duration_ms": gorm.Expr("GREATEST(source_run_telemetry.duration_ms, ?)", run.DurationMs),
			"metadata":    run.Metadata,
			"updated_at":  time.Now(),
		}),
	}).Create(&run).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"message": "Failed to record run"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

// ─── Recommendation helpers ────────────────────────────────────────────────

func generateSourceRecommendations(db *gorm.DB, policy models.NewsCirculationPolicy) ([]models.SourceCirculationRecommendation, int) {
	var sources []models.ContentSource
	db.Where("tenant_id = ? AND category = ? AND is_active = ?", policy.TenantID, models.SourceCategoryNews, true).
		Find(&sources)

	rows := make([]models.SourceCirculationRecommendation, 0)
	autoApplied := 0
	for _, source := range sources {
		rec, ok := recommendationForSource(db, policy, source)
		if !ok {
			continue
		}
		if err := saveSourceRecommendation(db, &rec); err != nil {
			continue
		}
		if policy.SourceCadenceMode == models.SourceCadenceModeAutoApply {
			if err := applySourceRecommendation(db, &rec); err == nil {
				autoApplied++
			}
		}
		rows = append(rows, rec)
	}
	return rows, autoApplied
}

func saveSourceRecommendation(db *gorm.DB, rec *models.SourceCirculationRecommendation) error {
	var existing models.SourceCirculationRecommendation
	if err := db.Where("tenant_id = ? AND source_id = ? AND applied = false", rec.TenantID, rec.SourceID).
		Order("updated_at DESC").
		First(&existing).Error; err == nil {
		rec.ID = existing.ID
		rec.PublicID = existing.PublicID
		rec.CreatedAt = existing.CreatedAt
		if err := db.Save(rec).Error; err != nil {
			return err
		}
	} else {
		if err := db.Create(rec).Error; err != nil {
			return err
		}
	}
	db.Where("tenant_id = ? AND source_id = ? AND applied = false AND public_id <> ?", rec.TenantID, rec.SourceID, rec.PublicID).
		Delete(&models.SourceCirculationRecommendation{})
	return nil
}

func recommendationForSource(db *gorm.DB, policy models.NewsCirculationPolicy, source models.ContentSource) (models.SourceCirculationRecommendation, bool) {
	cutoff := time.Now().AddDate(0, 0, -7)
	var runs []models.SourceRunTelemetry
	db.Where("tenant_id = ? AND source_id = ? AND finished_at > ?", policy.TenantID, source.PublicID, cutoff).
		Find(&runs)
	if len(runs) < 2 {
		return models.SourceCirculationRecommendation{}, false
	}
	var fetched, accepted, failed, duplicates, filtered int
	for _, r := range runs {
		fetched += r.Fetched
		accepted += r.Accepted
		failed += r.Failed
		duplicates += r.Duplicates
		filtered += r.Filtered
	}
	if fetched == 0 && accepted == 0 && failed == 0 {
		return models.SourceCirculationRecommendation{}, false
	}
	yield := 0.0
	if fetched > 0 {
		yield = float64(accepted) / float64(fetched)
	}
	failureRate := 0.0
	if fetched+failed > 0 {
		failureRate = float64(failed) / float64(fetched+failed)
	}
	current := source.FetchIntervalMinutes
	if current <= 0 {
		current = 60
	}
	recommended := current
	reason := ""
	score := yield - failureRate
	switch {
	case failureRate > 0.35 || yield < 0.05:
		recommended = guardedInterval(current, current*2, policy)
		reason = "Low yield or high failure rate; slow this source down."
	case yield > 0.45 && failureRate < 0.15:
		recommended = guardedInterval(current, int(math.Ceil(float64(current)/2)), policy)
		reason = "High yield with low failures; pull this source more often."
	default:
		return models.SourceCirculationRecommendation{}, false
	}
	if recommended == current {
		return models.SourceCirculationRecommendation{}, false
	}
	metrics, _ := json.Marshal(gin.H{
		"runs":         len(runs),
		"fetched":      fetched,
		"accepted":     accepted,
		"duplicates":   duplicates,
		"filtered":     filtered,
		"failed":       failed,
		"yield":        yield,
		"failure_rate": failureRate,
	})
	return models.SourceCirculationRecommendation{
		TenantID:                   policy.TenantID,
		SourceID:                   source.PublicID,
		SourceName:                 source.Name,
		SourceType:                 string(source.Type),
		CurrentIntervalMinutes:     current,
		RecommendedIntervalMinutes: recommended,
		Score:                      score,
		Reason:                     reason,
		Mode:                       policy.SourceCadenceMode,
		Metrics:                    datatypes.JSON(metrics),
	}, true
}

func guardedInterval(current, target int, policy models.NewsCirculationPolicy) int {
	if target < policy.SourceMinIntervalMinutes {
		target = policy.SourceMinIntervalMinutes
	}
	if target > policy.SourceMaxIntervalMinutes {
		target = policy.SourceMaxIntervalMinutes
	}
	maxDelta := int(math.Ceil(float64(current) * float64(policy.SourceMaxChangePercent) / 100.0))
	if maxDelta < 1 {
		maxDelta = 1
	}
	if target > current+maxDelta {
		target = current + maxDelta
	}
	if target < current-maxDelta {
		target = current - maxDelta
	}
	if target < policy.SourceMinIntervalMinutes {
		target = policy.SourceMinIntervalMinutes
	}
	return target
}

func applySourceRecommendation(db *gorm.DB, rec *models.SourceCirculationRecommendation) error {
	now := time.Now()
	if err := db.Model(&models.ContentSource{}).
		Where("tenant_id = ? AND public_id = ?", rec.TenantID, rec.SourceID).
		Update("fetch_interval_minutes", rec.RecommendedIntervalMinutes).Error; err != nil {
		return err
	}
	rec.Applied = true
	rec.AppliedAt = &now
	return db.Save(rec).Error
}

func triggerCirculationResync(authHeader string) {
	aggregationBaseURL := strings.TrimRight(strings.TrimSpace(os.Getenv("AGGREGATION_BASE_URL")), "/")
	if aggregationBaseURL == "" {
		return
	}
	_, _, _ = proxyAggregationRequest(aggregationBaseURL, "/admin/circulation/resync-schedule", authHeader, map[string]interface{}{})
}
