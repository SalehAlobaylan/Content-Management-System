package controllers

import (
	"content-management-system/src/models"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// ================================================================
// Ranking Config endpoints
// ================================================================

// GetRankingConfig handles GET /admin/intelligence/ranking
func GetRankingConfig(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	var config models.RankingConfig
	if err := db.Where("tenant_id = ?", principal.TenantID).First(&config).Error; err != nil {
		config = models.DefaultRankingConfig(principal.TenantID)
	}
	c.JSON(http.StatusOK, config)
}

// UpdateRankingConfig handles PUT /admin/intelligence/ranking
func UpdateRankingConfig(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	var req models.RankingConfig
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid request: " + err.Error(), Code: "INVALID_REQUEST"})
		return
	}

	// Validate weights sum ≈ 1.0 (tolerance ±0.05)
	sum := req.FreshnessWeight + req.EngagementWeight + req.VelocityWeight +
		req.SimilarityWeight + req.QualityWeight + req.DiversityWeight + req.TrendingWeight
	if math.Abs(sum-1.0) > 0.05 {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: fmt.Sprintf("Weights must sum to ~1.0 (got %.3f)", sum),
			Code:    "INVALID_WEIGHTS",
		})
		return
	}

	// Validate individual weights in [0, 1]
	weights := []float64{req.FreshnessWeight, req.EngagementWeight, req.VelocityWeight,
		req.SimilarityWeight, req.QualityWeight, req.DiversityWeight, req.TrendingWeight}
	for _, w := range weights {
		if w < 0 || w > 1 {
			c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Each weight must be between 0 and 1", Code: "INVALID_WEIGHT"})
			return
		}
	}

	// Upsert
	var existing models.RankingConfig
	result := db.Where("tenant_id = ?", principal.TenantID).First(&existing)
	if result.Error != nil {
		req.TenantID = principal.TenantID
		if err := db.Create(&req).Error; err != nil {
			c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to create config", Code: "CREATE_FAILED"})
			return
		}
		c.JSON(http.StatusOK, req)
		return
	}

	// Update existing
	existing.FreshnessWeight = req.FreshnessWeight
	existing.EngagementWeight = req.EngagementWeight
	existing.VelocityWeight = req.VelocityWeight
	existing.SimilarityWeight = req.SimilarityWeight
	existing.QualityWeight = req.QualityWeight
	existing.DiversityWeight = req.DiversityWeight
	existing.TrendingWeight = req.TrendingWeight
	existing.FreshnessDecayHours = req.FreshnessDecayHours
	existing.VelocityWindowHours = req.VelocityWindowHours
	existing.TrendingThresholdMultiplier = req.TrendingThresholdMultiplier
	existing.RecirculationEnabled = req.RecirculationEnabled
	existing.RecirculationMaxAgeDays = req.RecirculationMaxAgeDays
	existing.EngagementNormalization = req.EngagementNormalization
	existing.Mode = req.Mode
	existing.IsActive = req.IsActive

	if err := db.Save(&existing).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to update config", Code: "UPDATE_FAILED"})
		return
	}
	c.JSON(http.StatusOK, existing)
}

// ================================================================
// Mode endpoints
// ================================================================

// GetModes handles GET /admin/intelligence/modes
func GetModes(c *gin.Context) {
	_, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	c.JSON(http.StatusOK, models.ModeDefinitions())
}

// SetMode handles PUT /admin/intelligence/mode
func SetMode(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	var req struct {
		Mode string `json:"mode" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid request: " + err.Error(), Code: "INVALID_REQUEST"})
		return
	}

	// Load or create config
	var config models.RankingConfig
	if err := db.Where("tenant_id = ?", principal.TenantID).First(&config).Error; err != nil {
		config = models.DefaultRankingConfig(principal.TenantID)
	}

	// Apply the preset
	if !config.ApplyPreset(req.Mode) {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: fmt.Sprintf("Unknown mode: %s", req.Mode),
			Code:    "UNKNOWN_MODE",
		})
		return
	}
	config.IsActive = true

	// Upsert
	if config.ID == 0 {
		config.TenantID = principal.TenantID
		if err := db.Create(&config).Error; err != nil {
			c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to create config", Code: "CREATE_FAILED"})
			return
		}
	} else {
		if err := db.Save(&config).Error; err != nil {
			c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to update config", Code: "UPDATE_FAILED"})
			return
		}
	}

	c.JSON(http.StatusOK, config)
}

// ================================================================
// Content Flag endpoints
// ================================================================

type contentFlagWithContent struct {
	models.ContentFlag
	ContentTitle  *string `json:"content_title,omitempty"`
	ContentType   string  `json:"content_type,omitempty"`
	ContentStatus string  `json:"content_status,omitempty"`
}

type flagListResponse struct {
	Data       []contentFlagWithContent `json:"data"`
	Total      int64                    `json:"total"`
	Page       int                      `json:"page"`
	Limit      int                      `json:"limit"`
	TotalPages int                      `json:"total_pages"`
}

// ListContentFlags handles GET /admin/intelligence/flags
func ListContentFlags(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "20"))
	if page <= 0 {
		page = 1
	}
	if limit <= 0 || limit > 100 {
		limit = 20
	}

	var total int64
	db.Model(&models.ContentFlag{}).Where("tenant_id = ?", principal.TenantID).Count(&total)

	var flags []models.ContentFlag
	db.Where("tenant_id = ?", principal.TenantID).
		Order("updated_at DESC").
		Offset((page - 1) * limit).
		Limit(limit).
		Find(&flags)

	// Enrich with content info
	data := make([]contentFlagWithContent, 0, len(flags))
	for _, f := range flags {
		enriched := contentFlagWithContent{ContentFlag: f}
		var item models.ContentItem
		if err := db.Where("public_id = ? AND tenant_id = ?", f.ContentItemID, principal.TenantID).First(&item).Error; err == nil {
			enriched.ContentTitle = item.Title
			enriched.ContentType = string(item.Type)
			enriched.ContentStatus = string(item.Status)
		}
		data = append(data, enriched)
	}

	totalPages := int(total) / limit
	if int(total)%limit > 0 {
		totalPages++
	}

	c.JSON(http.StatusOK, flagListResponse{
		Data:       data,
		Total:      total,
		Page:       page,
		Limit:      limit,
		TotalPages: totalPages,
	})
}

// GetContentFlag handles GET /admin/intelligence/flags/:content_id
func GetContentFlag(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	contentID, err := uuid.Parse(c.Param("content_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid content ID", Code: "INVALID_ID"})
		return
	}

	var flag models.ContentFlag
	if err := db.Where("content_item_id = ? AND tenant_id = ?", contentID, principal.TenantID).First(&flag).Error; err != nil {
		c.JSON(http.StatusNotFound, authErrorResponse{Message: "Flag not found", Code: "NOT_FOUND"})
		return
	}
	c.JSON(http.StatusOK, flag)
}

type upsertFlagRequest struct {
	Boost           *bool    `json:"boost"`
	Suppress        *bool    `json:"suppress"`
	PinToTop        *bool    `json:"pin_to_top"`
	ExcludeFromFeed *bool    `json:"exclude_from_feed"`
	BoostMultiplier *float64 `json:"boost_multiplier"`
	Notes           string   `json:"notes"`
}

// UpsertContentFlag handles PUT /admin/intelligence/flags/:content_id
func UpsertContentFlag(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	contentID, err := uuid.Parse(c.Param("content_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid content ID", Code: "INVALID_ID"})
		return
	}

	// Verify the content item exists
	var item models.ContentItem
	if err := db.Where("public_id = ? AND tenant_id = ?", contentID, principal.TenantID).First(&item).Error; err != nil {
		c.JSON(http.StatusNotFound, authErrorResponse{Message: "Content not found", Code: "CONTENT_NOT_FOUND"})
		return
	}

	var req upsertFlagRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid request", Code: "INVALID_REQUEST"})
		return
	}

	var flag models.ContentFlag
	isNew := db.Where("content_item_id = ? AND tenant_id = ?", contentID, principal.TenantID).First(&flag).Error != nil

	if isNew {
		flag = models.ContentFlag{
			TenantID:        principal.TenantID,
			ContentItemID:   contentID,
			BoostMultiplier: 1.5,
		}
	}

	if req.Boost != nil {
		flag.Boost = *req.Boost
	}
	if req.Suppress != nil {
		flag.Suppress = *req.Suppress
	}
	if req.PinToTop != nil {
		flag.PinToTop = *req.PinToTop
	}
	if req.ExcludeFromFeed != nil {
		flag.ExcludeFromFeed = *req.ExcludeFromFeed
	}
	if req.BoostMultiplier != nil {
		flag.BoostMultiplier = *req.BoostMultiplier
	}
	flag.Notes = req.Notes
	flag.SetBy = principal.Email

	if err := db.Save(&flag).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to save flag", Code: "SAVE_FAILED"})
		return
	}
	c.JSON(http.StatusOK, flag)
}

// DeleteContentFlag handles DELETE /admin/intelligence/flags/:content_id
func DeleteContentFlag(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	contentID, err := uuid.Parse(c.Param("content_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid content ID", Code: "INVALID_ID"})
		return
	}

	result := db.Where("content_item_id = ? AND tenant_id = ?", contentID, principal.TenantID).Delete(&models.ContentFlag{})
	if result.RowsAffected == 0 {
		c.JSON(http.StatusNotFound, authErrorResponse{Message: "Flag not found", Code: "NOT_FOUND"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "Flag deleted"})
}

type bulkFlagRequest struct {
	ContentIDs      []string `json:"content_ids" binding:"required"`
	Boost           *bool    `json:"boost"`
	Suppress        *bool    `json:"suppress"`
	PinToTop        *bool    `json:"pin_to_top"`
	ExcludeFromFeed *bool    `json:"exclude_from_feed"`
	Notes           string   `json:"notes"`
}

// BulkSetFlags handles POST /admin/intelligence/flags/bulk
func BulkSetFlags(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	var req bulkFlagRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid request", Code: "INVALID_REQUEST"})
		return
	}

	var updated int
	for _, idStr := range req.ContentIDs {
		contentID, err := uuid.Parse(idStr)
		if err != nil {
			continue
		}

		var flag models.ContentFlag
		isNew := db.Where("content_item_id = ? AND tenant_id = ?", contentID, principal.TenantID).First(&flag).Error != nil

		if isNew {
			flag = models.ContentFlag{
				TenantID:        principal.TenantID,
				ContentItemID:   contentID,
				BoostMultiplier: 1.5,
			}
		}

		if req.Boost != nil {
			flag.Boost = *req.Boost
		}
		if req.Suppress != nil {
			flag.Suppress = *req.Suppress
		}
		if req.PinToTop != nil {
			flag.PinToTop = *req.PinToTop
		}
		if req.ExcludeFromFeed != nil {
			flag.ExcludeFromFeed = *req.ExcludeFromFeed
		}
		flag.Notes = req.Notes
		flag.SetBy = principal.Email

		if err := db.Save(&flag).Error; err == nil {
			updated++
		}
	}

	c.JSON(http.StatusOK, gin.H{"updated": updated})
}

// ================================================================
// Embeddings Explorer endpoints
// ================================================================

type clusterResult struct {
	Topic          string  `json:"topic"`
	Count          int64   `json:"count"`
	AvgLikes       float64 `json:"avg_likes"`
	AvgViews       float64 `json:"avg_views"`
	AvgShares      float64 `json:"avg_shares"`
}

// GetEmbeddingClusters handles GET /admin/intelligence/embeddings/clusters
func GetEmbeddingClusters(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	var clusters []clusterResult
	db.Raw(`
		SELECT tag AS topic, COUNT(*) AS count,
			AVG(like_count) AS avg_likes,
			AVG(view_count) AS avg_views,
			AVG(share_count) AS avg_shares
		FROM content_items, UNNEST(topic_tags) AS tag
		WHERE tenant_id = ? AND status = ?
		GROUP BY tag
		ORDER BY count DESC
		LIMIT 50
	`, principal.TenantID, models.ContentStatusReady).Scan(&clusters)

	c.JSON(http.StatusOK, clusters)
}

type similarResult struct {
	ID         string  `json:"id"`
	Title      string  `json:"title"`
	Type       string  `json:"type"`
	Similarity float64 `json:"similarity"`
}

// GetSimilarContent handles GET /admin/intelligence/embeddings/similar/:content_id
func GetSimilarContent(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	contentID, err := uuid.Parse(c.Param("content_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid content ID", Code: "INVALID_ID"})
		return
	}

	limitStr := c.DefaultQuery("limit", "10")
	limit, _ := strconv.Atoi(limitStr)
	if limit <= 0 || limit > 50 {
		limit = 10
	}

	// Get the reference item's embedding
	var refItem models.ContentItem
	if err := db.Where("public_id = ? AND tenant_id = ?", contentID, principal.TenantID).First(&refItem).Error; err != nil {
		c.JSON(http.StatusNotFound, authErrorResponse{Message: "Content not found", Code: "NOT_FOUND"})
		return
	}
	if refItem.Embedding == nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Content has no embedding", Code: "NO_EMBEDDING"})
		return
	}

	var results []similarResult
	db.Raw(`
		SELECT public_id::text AS id, title, type,
			1 - (embedding <=> (SELECT embedding FROM content_items WHERE public_id = ? AND tenant_id = ?)) AS similarity
		FROM content_items
		WHERE tenant_id = ? AND status = ? AND embedding IS NOT NULL AND public_id != ?
		ORDER BY embedding <=> (SELECT embedding FROM content_items WHERE public_id = ? AND tenant_id = ?)
		LIMIT ?
	`, contentID, principal.TenantID, principal.TenantID, models.ContentStatusReady, contentID, contentID, principal.TenantID, limit).Scan(&results)

	c.JSON(http.StatusOK, results)
}

type embeddingStats struct {
	TotalReady     int64             `json:"total_ready"`
	WithEmbedding  int64             `json:"with_embedding"`
	Percentage     float64           `json:"percentage"`
	ByType         []typeEmbedStat   `json:"by_type"`
}

type typeEmbedStat struct {
	Type          string `json:"type"`
	Total         int64  `json:"total"`
	WithEmbedding int64  `json:"with_embedding"`
}

// GetEmbeddingStats handles GET /admin/intelligence/embeddings/stats
func GetEmbeddingStats(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	var totalReady, withEmbed int64
	db.Model(&models.ContentItem{}).Where("tenant_id = ? AND status = ?", principal.TenantID, models.ContentStatusReady).Count(&totalReady)
	db.Model(&models.ContentItem{}).Where("tenant_id = ? AND status = ? AND embedding IS NOT NULL", principal.TenantID, models.ContentStatusReady).Count(&withEmbed)

	var pct float64
	if totalReady > 0 {
		pct = float64(withEmbed) / float64(totalReady) * 100
	}

	var byType []typeEmbedStat
	db.Raw(`
		SELECT type,
			COUNT(*) AS total,
			COUNT(embedding) AS with_embedding
		FROM content_items
		WHERE tenant_id = ? AND status = ?
		GROUP BY type
		ORDER BY total DESC
	`, principal.TenantID, models.ContentStatusReady).Scan(&byType)

	c.JSON(http.StatusOK, embeddingStats{
		TotalReady:    totalReady,
		WithEmbedding: withEmbed,
		Percentage:    pct,
		ByType:        byType,
	})
}

// ================================================================
// Analytics endpoints
// ================================================================

type scoreDistBucket struct {
	Range string `json:"range"`
	Count int    `json:"count"`
}

// GetScoreDistribution handles GET /admin/intelligence/analytics/score-distribution
func GetScoreDistribution(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	// Load config + items + flags + velocity, then score
	config := loadTenantConfig(db, principal.TenantID)
	items := loadReadyItems(db, principal.TenantID, 500)
	if len(items) == 0 {
		c.JSON(http.StatusOK, []scoreDistBucket{})
		return
	}

	contentIDs := extractPublicIDs(items)
	flagMap := LoadContentFlags(db, principal.TenantID, contentIDs)
	velocityData := LoadVelocityData(db, contentIDs, config.VelocityWindowHours, time.Now())
	scored := ScoreItems(items, config, flagMap, velocityData, time.Now())

	// Bucket into 10 ranges
	buckets := make([]int, 10) // 0-0.1, 0.1-0.2, ...
	for _, s := range scored {
		idx := int(s.FinalScore * 10)
		if idx >= 10 {
			idx = 9
		}
		if idx < 0 {
			idx = 0
		}
		buckets[idx]++
	}

	result := make([]scoreDistBucket, 10)
	for i := 0; i < 10; i++ {
		result[i] = scoreDistBucket{
			Range: fmt.Sprintf("%.1f-%.1f", float64(i)*0.1, float64(i+1)*0.1),
			Count: buckets[i],
		}
	}
	c.JSON(http.StatusOK, result)
}

type velocityItem struct {
	ID       string  `json:"id"`
	Title    string  `json:"title"`
	Type     string  `json:"type"`
	Velocity float64 `json:"velocity"`
	Count    int     `json:"count"`
}

// GetVelocityLeaderboard handles GET /admin/intelligence/analytics/velocity
func GetVelocityLeaderboard(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	hours, _ := strconv.Atoi(c.DefaultQuery("hours", "24"))
	if hours <= 0 {
		hours = 24
	}
	cutoff := time.Now().Add(-time.Duration(hours) * time.Hour)

	type velRow struct {
		ContentItemID uuid.UUID `gorm:"column:content_item_id"`
		Count         int       `gorm:"column:count"`
	}
	var rows []velRow
	db.Model(&models.UserInteraction{}).
		Select("content_item_id, COUNT(*) as count").
		Where("created_at > ?", cutoff).
		Group("content_item_id").
		Order("count DESC").
		Limit(20).
		Scan(&rows)

	results := make([]velocityItem, 0, len(rows))
	for _, r := range rows {
		var item models.ContentItem
		if err := db.Where("public_id = ? AND tenant_id = ?", r.ContentItemID, principal.TenantID).First(&item).Error; err != nil {
			continue
		}
		title := ""
		if item.Title != nil {
			title = *item.Title
		}
		results = append(results, velocityItem{
			ID:       item.PublicID.String(),
			Title:    title,
			Type:     string(item.Type),
			Velocity: float64(r.Count) / float64(hours),
			Count:    r.Count,
		})
	}
	c.JSON(http.StatusOK, results)
}

type trendingItem struct {
	ID            string  `json:"id"`
	Title         string  `json:"title"`
	Type          string  `json:"type"`
	TrendingScore float64 `json:"trending_score"`
	RecentRate    float64 `json:"recent_rate"`
	AvgRate       float64 `json:"avg_rate"`
}

// GetTrendingItems handles GET /admin/intelligence/analytics/trending
func GetTrendingItems(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	config := loadTenantConfig(db, principal.TenantID)
	items := loadReadyItems(db, principal.TenantID, 200)
	contentIDs := extractPublicIDs(items)
	velocityData := LoadVelocityData(db, contentIDs, config.VelocityWindowHours, time.Now())

	var trending []trendingItem
	now := time.Now()

	for _, item := range items {
		score := computeTrending(item, velocityData, config.VelocityWindowHours, config.TrendingThresholdMultiplier, now)
		if score < 0.3 {
			continue
		}

		var pubTime time.Time
		if item.PublishedAt != nil {
			pubTime = *item.PublishedAt
		} else {
			pubTime = item.CreatedAt
		}
		hoursAge := now.Sub(pubTime).Hours()
		if hoursAge < 1 {
			hoursAge = 1
		}

		totalInteractions := float64(item.LikeCount + item.ViewCount + item.ShareCount + item.CommentCount)
		avgRate := totalInteractions / hoursAge
		recentRate := float64(velocityData[item.PublicID]) / float64(config.VelocityWindowHours)

		title := ""
		if item.Title != nil {
			title = *item.Title
		}
		trending = append(trending, trendingItem{
			ID:            item.PublicID.String(),
			Title:         title,
			Type:          string(item.Type),
			TrendingScore: score,
			RecentRate:    recentRate,
			AvgRate:       avgRate,
		})
	}

	c.JSON(http.StatusOK, trending)
}

type sourcePerformanceItem struct {
	SourceName string  `json:"source_name"`
	SourceType string  `json:"source_type"`
	Count      int64   `json:"count"`
	AvgLikes   float64 `json:"avg_likes"`
	AvgViews   float64 `json:"avg_views"`
	AvgShares  float64 `json:"avg_shares"`
}

// GetSourcePerformance handles GET /admin/intelligence/analytics/source-performance
func GetSourcePerformance(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	var results []sourcePerformanceItem
	db.Raw(`
		SELECT source_name, source AS source_type, COUNT(*) AS count,
			AVG(like_count) AS avg_likes,
			AVG(view_count) AS avg_views,
			AVG(share_count) AS avg_shares
		FROM content_items
		WHERE tenant_id = ? AND status = ? AND source_name IS NOT NULL AND source_name != ''
		GROUP BY source_name, source
		ORDER BY count DESC
		LIMIT 30
	`, principal.TenantID, models.ContentStatusReady).Scan(&results)

	c.JSON(http.StatusOK, results)
}

type signalHealthItem struct {
	Signal     string  `json:"signal"`
	Coverage   int64   `json:"coverage"`
	Total      int64   `json:"total"`
	Percentage float64 `json:"percentage"`
}

// GetSignalHealth handles GET /admin/intelligence/analytics/signal-health
func GetSignalHealth(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	var totalReady int64
	db.Model(&models.ContentItem{}).Where("tenant_id = ? AND status = ?", principal.TenantID, models.ContentStatusReady).Count(&totalReady)

	if totalReady == 0 {
		c.JSON(http.StatusOK, []signalHealthItem{})
		return
	}

	counts := make(map[string]int64)
	signals := []struct {
		name  string
		where string
	}{
		{"embeddings", "embedding IS NOT NULL"},
		{"topic_tags", "topic_tags IS NOT NULL AND array_length(topic_tags, 1) > 0"},
		{"transcripts", "transcript_id IS NOT NULL"},
		{"thumbnails", "thumbnail_url IS NOT NULL AND thumbnail_url != ''"},
		{"engagement", "(like_count + view_count + share_count) > 0"},
		{"excerpts", "excerpt IS NOT NULL AND excerpt != ''"},
	}

	for _, s := range signals {
		var count int64
		db.Model(&models.ContentItem{}).
			Where("tenant_id = ? AND status = ? AND "+s.where, principal.TenantID, models.ContentStatusReady).
			Count(&count)
		counts[s.name] = count
	}

	results := make([]signalHealthItem, 0, len(signals))
	for _, s := range signals {
		cnt := counts[s.name]
		results = append(results, signalHealthItem{
			Signal:     s.name,
			Coverage:   cnt,
			Total:      totalReady,
			Percentage: float64(cnt) / float64(totalReady) * 100,
		})
	}

	c.JSON(http.StatusOK, results)
}

// ================================================================
// Feed Preview endpoints
// ================================================================

type previewFeedItem struct {
	ID             string         `json:"id"`
	Type           string         `json:"type"`
	Title          string         `json:"title"`
	Author         *string        `json:"author,omitempty"`
	SourceName     *string        `json:"source_name,omitempty"`
	PublishedAt    *string        `json:"published_at,omitempty"`
	LikeCount      int            `json:"like_count"`
	ViewCount      int            `json:"view_count"`
	ShareCount     int            `json:"share_count"`
	FinalScore     float64        `json:"final_score"`
	ScoreBreakdown ScoreBreakdown `json:"score_breakdown"`
	ChronPosition  int            `json:"chron_position"`
	RankedPosition int            `json:"ranked_position"`
	PositionChange int            `json:"position_change"`
}

type previewFeedResponse struct {
	Items    []previewFeedItem `json:"items"`
	IsActive bool              `json:"is_active"`
}

// PreviewForYouFeed handles GET /admin/intelligence/preview/foryou
func PreviewForYouFeed(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	config := loadTenantConfig(db, principal.TenantID)

	// Allow temporary weight overrides from query params
	applyWeightOverrides(c, &config)

	// Fetch items (VIDEO + PODCAST, READY)
	var items []models.ContentItem
	db.Where("type IN ? AND status = ? AND tenant_id = ?",
		[]models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast},
		models.ContentStatusReady, principal.TenantID).
		Order("published_at DESC").
		Limit(50).
		Find(&items)

	c.JSON(http.StatusOK, buildPreviewResponse(db, items, config, principal.TenantID))
}

// PreviewNewsFeed handles GET /admin/intelligence/preview/news
func PreviewNewsFeed(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	config := loadTenantConfig(db, principal.TenantID)
	applyWeightOverrides(c, &config)

	var items []models.ContentItem
	db.Where("type = ? AND status = ? AND tenant_id = ?",
		models.ContentTypeArticle, models.ContentStatusReady, principal.TenantID).
		Order("published_at DESC").
		Limit(50).
		Find(&items)

	c.JSON(http.StatusOK, buildPreviewResponse(db, items, config, principal.TenantID))
}

// ================================================================
// Helpers
// ================================================================

func loadTenantConfig(db *gorm.DB, tenantID string) models.RankingConfig {
	var config models.RankingConfig
	if err := db.Where("tenant_id = ?", tenantID).First(&config).Error; err != nil {
		return models.DefaultRankingConfig(tenantID)
	}
	return config
}

func loadReadyItems(db *gorm.DB, tenantID string, limit int) []models.ContentItem {
	var items []models.ContentItem
	db.Where("tenant_id = ? AND status = ?", tenantID, models.ContentStatusReady).
		Order("published_at DESC").
		Limit(limit).
		Find(&items)
	return items
}

func extractPublicIDs(items []models.ContentItem) []uuid.UUID {
	ids := make([]uuid.UUID, len(items))
	for i, item := range items {
		ids[i] = item.PublicID
	}
	return ids
}

func applyWeightOverrides(c *gin.Context, config *models.RankingConfig) {
	if v := c.Query("freshness_weight"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			config.FreshnessWeight = f
		}
	}
	if v := c.Query("engagement_weight"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			config.EngagementWeight = f
		}
	}
	if v := c.Query("velocity_weight"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			config.VelocityWeight = f
		}
	}
	if v := c.Query("similarity_weight"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			config.SimilarityWeight = f
		}
	}
	if v := c.Query("quality_weight"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			config.QualityWeight = f
		}
	}
	if v := c.Query("diversity_weight"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			config.DiversityWeight = f
		}
	}
	if v := c.Query("trending_weight"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			config.TrendingWeight = f
		}
	}
}

func buildPreviewResponse(db *gorm.DB, items []models.ContentItem, config models.RankingConfig, tenantID string) previewFeedResponse {
	if len(items) == 0 {
		return previewFeedResponse{Items: []previewFeedItem{}, IsActive: config.IsActive}
	}

	// Build chronological position map
	chronMap := make(map[uuid.UUID]int)
	for i, item := range items {
		chronMap[item.PublicID] = i + 1
	}

	// Score all items
	contentIDs := extractPublicIDs(items)
	flagMap := LoadContentFlags(db, tenantID, contentIDs)
	velocityData := LoadVelocityData(db, contentIDs, config.VelocityWindowHours, time.Now())
	scored := ScoreItems(items, config, flagMap, velocityData, time.Now())

	result := make([]previewFeedItem, 0, len(scored))
	for i, s := range scored {
		title := ""
		if s.Item.Title != nil {
			title = *s.Item.Title
		}
		var pubAt *string
		if s.Item.PublishedAt != nil {
			t := s.Item.PublishedAt.UTC().Format(time.RFC3339)
			pubAt = &t
		}

		chronPos := chronMap[s.Item.PublicID]
		rankedPos := i + 1

		result = append(result, previewFeedItem{
			ID:             s.Item.PublicID.String(),
			Type:           string(s.Item.Type),
			Title:          title,
			Author:         s.Item.Author,
			SourceName:     s.Item.SourceName,
			PublishedAt:    pubAt,
			LikeCount:      s.Item.LikeCount,
			ViewCount:      s.Item.ViewCount,
			ShareCount:     s.Item.ShareCount,
			FinalScore:     s.FinalScore,
			ScoreBreakdown: s.ScoreBreakdown,
			ChronPosition:  chronPos,
			RankedPosition: rankedPos,
			PositionChange: chronPos - rankedPos,
		})
	}

	return previewFeedResponse{Items: result, IsActive: config.IsActive}
}
