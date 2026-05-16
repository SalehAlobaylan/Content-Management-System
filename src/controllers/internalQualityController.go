package controllers

import (
	"content-management-system/src/models"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// =============================================================================
// Internal endpoints — Aggregation → CMS
//
// Phase 7 surface: the only things Aggregation needs from CMS for the Quality
// system are
//   - resolve a profile for (tenant, source_type) on every ingest job
//   - fetch a profile by id (used by the re-encode worker invoked from Storage)
//   - patch per-item quality fields after a re-encode
// All gated by InternalAuthMiddleware (CMS_SERVICE_TOKEN).
// =============================================================================

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

// InternalResolveQualityProfile handles GET /internal/quality/profiles/resolve?tenant_id=X&source_type=Y
//
// Returns the most-specific matching profile or 404 if no rung matches and
// there's no global default. Aggregation's media worker calls this on every
// fresh job; a 60-second per-process cache lives on the Aggregation side.
func InternalResolveQualityProfile(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	tenantID := strings.TrimSpace(c.Query("tenant_id"))
	sourceType := strings.ToUpper(strings.TrimSpace(c.Query("source_type")))

	profile, matched := resolveProfile(db, tenantID, sourceType)
	if profile == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "no matching profile (no global default configured)"})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"profile":    profile,
		"matched_on": matched,
	})
}

// =============================================================================
// Per-item quality update (bumps media_version, swaps URL & bitrate)
// Called by the re-encode worker after a successful encode.
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
