package controllers

import (
	"content-management-system/src/models"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// =============================================================================
// Internal endpoints — Aggregation → CMS
// All gated by InternalAuthMiddleware (CMS_SERVICE_TOKEN).
// =============================================================================

// InternalListQualityRules handles GET /internal/quality/rules?enabled=true
func InternalListQualityRules(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	q := db.Model(&models.QualityRule{})
	if c.Query("enabled") == "true" {
		q = q.Where("enabled = TRUE")
	}
	var rules []models.QualityRule
	if err := q.Order("priority ASC, id ASC").Find(&rules).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to list rules"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": rules})
}

// InternalGetQualityProfile handles GET /internal/quality/profiles/:id
func InternalGetQualityProfile(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid id"})
		return
	}
	var p models.QualityProfile
	if err := db.First(&p, id).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Profile not found"})
		return
	}
	c.JSON(http.StatusOK, p)
}

// InternalGetDefaultQualityProfile handles GET /internal/quality/profiles/default
// Returns the profile flagged IsDefault for the (optional) tenant in query
// param `tenant_id`. Falls back to the global default. Used by the ingest
// pipeline so first-run encodes pick up the operator's choice.
func InternalGetDefaultQualityProfile(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	tenantID := strings.TrimSpace(c.Query("tenant_id"))
	var p models.QualityProfile
	// Prefer per-tenant default, then global default.
	if tenantID != "" {
		if err := db.Where("tenant_id = ? AND is_default = TRUE AND is_active = TRUE", tenantID).First(&p).Error; err == nil {
			c.JSON(http.StatusOK, p)
			return
		}
	}
	if err := db.Where("tenant_id IS NULL AND is_default = TRUE AND is_active = TRUE").First(&p).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "No default profile configured"})
		return
	}
	c.JSON(http.StatusOK, p)
}

// InternalListQualityCandidates handles GET /internal/quality/candidates?rule_id=N&limit=K
// Mirrors GetQualityCandidates but returns the raw ContentItem rows the worker
// needs (with public_id, current size, current profile, storage tier).
func InternalListQualityCandidates(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	ruleIDRaw := strings.TrimSpace(c.Query("rule_id"))
	if ruleIDRaw == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "rule_id is required"})
		return
	}
	ruleID, err := strconv.ParseUint(ruleIDRaw, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid rule_id"})
		return
	}
	var r models.QualityRule
	if err := db.First(&r, ruleID).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Rule not found"})
		return
	}
	tenantID := r.TenantID
	if tenantID == nil {
		// Global rule applies to all tenants — caller should iterate per tenant.
		// For simplicity: caller passes tenant_id explicitly.
		t := strings.TrimSpace(c.Query("tenant_id"))
		if t == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "tenant_id required for global rules"})
			return
		}
		tenantID = &t
	}
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "50"))
	if limit <= 0 || limit > 500 {
		limit = 50
	}

	var maxViews *int
	if r.MaxViewCount != nil {
		v := *r.MaxViewCount
		maxViews = &v
	}
	q := buildQualityCandidateQuery(db, qualityFilter{
		tenantID:         *tenantID,
		minAgeDays:       r.MinAgeDays,
		maxViewCount:     maxViews,
		contentType:      r.ContentType,
		targetProfileID:  r.TargetProfileID,
		onlyIfHigherThan: r.OnlyIfHigherThan,
	})
	var items []models.ContentItem
	if err := q.Order("view_count ASC, created_at ASC").Limit(limit).Find(&items).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch candidates"})
		return
	}

	type itemRef struct {
		ContentItemID    string  `json:"content_item_id"`
		StorageTier      *string `json:"storage_tier,omitempty"`
		FileSizeBytes    int64   `json:"file_size_bytes"`
		MediaURL         *string `json:"media_url,omitempty"`
		MediaVersion     int     `json:"media_version"`
		CurrentProfileID *uint   `json:"current_quality_profile_id,omitempty"`
	}
	out := make([]itemRef, 0, len(items))
	for _, it := range items {
		out = append(out, itemRef{
			ContentItemID:    it.PublicID.String(),
			StorageTier:      it.StorageTier,
			FileSizeBytes:    it.FileSizeBytes,
			MediaURL:         it.MediaURL,
			MediaVersion:     it.MediaVersion,
			CurrentProfileID: it.CurrentQualityProfileID,
		})
	}

	// Touch LastSweepAt — the worker is about to process this batch.
	now := time.Now().UTC()
	r.LastSweepAt = &now
	_ = db.Save(&r).Error

	c.JSON(http.StatusOK, gin.H{
		"data":              out,
		"target_profile_id": r.TargetProfileID,
		"rule_id":           r.ID,
		"tenant_id":         *tenantID,
	})
}

// =============================================================================
// History write-back
// =============================================================================

type internalQualityHistoryRequest struct {
	ContentItemID       string  `json:"content_item_id"`
	TenantID            string  `json:"tenant_id"`
	FromProfileID       *uint   `json:"from_profile_id"`
	ToProfileID         uint    `json:"to_profile_id"`
	OriginalSizeBytes   int64   `json:"original_size_bytes"`
	NewSizeBytes        int64   `json:"new_size_bytes"`
	OriginalBitrateKbps int     `json:"original_bitrate_kbps"`
	NewBitrateKbps      int     `json:"new_bitrate_kbps"`
	DurationMs          int     `json:"duration_ms"`
	Trigger             string  `json:"trigger"`
	RuleID              *uint   `json:"rule_id"`
	Error               string  `json:"error"`
}

// InternalWriteQualityHistory handles POST /internal/quality/history
func InternalWriteQualityHistory(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	var req internalQualityHistoryRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}
	id, err := uuid.Parse(req.ContentItemID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid content_item_id"})
		return
	}
	row := models.QualityHistory{
		ContentItemID:       id,
		TenantID:            req.TenantID,
		FromProfileID:       req.FromProfileID,
		ToProfileID:         req.ToProfileID,
		OriginalSizeBytes:   req.OriginalSizeBytes,
		NewSizeBytes:        req.NewSizeBytes,
		SavingsBytes:        req.OriginalSizeBytes - req.NewSizeBytes,
		OriginalBitrateKbps: req.OriginalBitrateKbps,
		NewBitrateKbps:      req.NewBitrateKbps,
		DurationMs:          req.DurationMs,
		Trigger:             req.Trigger,
		RuleID:              req.RuleID,
		Error:               req.Error,
	}
	if err := db.Create(&row).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to record history"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"id": row.ID})
}

// =============================================================================
// Per-item quality update (bumps media_version, swaps URL & bitrate)
// =============================================================================

type internalUpdateItemQualityRequest struct {
	MediaURL                *string `json:"media_url"`
	FileSizeBytes           *int64  `json:"file_size_bytes"`
	CurrentBitrateKbps      *int    `json:"current_bitrate_kbps"`
	CurrentQualityProfileID *uint   `json:"current_quality_profile_id"`
	BumpVersion             bool    `json:"bump_version"`
}

// InternalUpdateContentItemQuality handles PATCH /internal/content-items/:id/quality
func InternalUpdateContentItemQuality(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid id"})
		return
	}
	var req internalUpdateItemQualityRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}
	var item models.ContentItem
	if err := db.Where("public_id = ?", id).First(&item).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Content not found"})
		return
	}
	if req.MediaURL != nil {
		item.MediaURL = req.MediaURL
	}
	if req.FileSizeBytes != nil {
		item.FileSizeBytes = *req.FileSizeBytes
	}
	if req.CurrentBitrateKbps != nil {
		v := *req.CurrentBitrateKbps
		item.CurrentBitrateKbps = &v
	}
	if req.CurrentQualityProfileID != nil {
		v := *req.CurrentQualityProfileID
		item.CurrentQualityProfileID = &v
	}
	if req.BumpVersion {
		item.MediaVersion++
	}
	if err := db.Save(&item).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update"})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"success":       true,
		"media_version": item.MediaVersion,
	})
}
