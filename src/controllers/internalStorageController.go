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

// -----------------------------------------------------------------------------
// Aggregation reads candidates and policy through these endpoints.
// They run under /internal/* with the CMS_SERVICE_TOKEN gate.
// -----------------------------------------------------------------------------

type internalListPoliciesResponse struct {
	Global    *models.StoragePolicy  `json:"global"`
	Overrides []models.StoragePolicy `json:"overrides"`
	All       []models.StoragePolicy `json:"all"`
}

// InternalListStoragePolicies handles GET /internal/storage/policies
func InternalListStoragePolicies(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	var all []models.StoragePolicy
	if err := db.Find(&all).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to list policies"})
		return
	}
	resp := internalListPoliciesResponse{All: all}
	for i := range all {
		if all[i].TenantID == nil {
			p := all[i]
			resp.Global = &p
			continue
		}
		resp.Overrides = append(resp.Overrides, all[i])
	}
	c.JSON(http.StatusOK, resp)
}

type internalCandidatesResponse struct {
	Data       []internalCandidate `json:"data"`
	Total      int64               `json:"total"`
	TotalBytes int64               `json:"total_bytes"`
}

type internalCandidate struct {
	ID            string  `json:"id"`
	TenantID      string  `json:"tenant_id"`
	Type          string  `json:"type"`
	Status        string  `json:"status"`
	MediaURL      *string `json:"media_url,omitempty"`
	ThumbnailURL  *string `json:"thumbnail_url,omitempty"`
	FileSizeBytes int64   `json:"file_size_bytes"`
	ViewCount     int     `json:"view_count"`
	CreatedAt     string  `json:"created_at"`
}

// InternalListStorageCandidates handles GET /internal/storage/candidates
// Query params: tenant_id, min_age_days, max_view_count, limit,
// delete_failed_immediately (bool), max_bytes
func InternalListStorageCandidates(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	tenantID := strings.TrimSpace(c.Query("tenant_id"))
	if tenantID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "tenant_id required"})
		return
	}

	minAgeDays := atoiDefault(c.Query("min_age_days"), defaultMinAgeDays)
	maxViewCount := atoiDefault(c.Query("max_view_count"), defaultMinViewCountForKeep)
	limit := atoiDefault(c.Query("limit"), 200)
	if limit > 1000 {
		limit = 1000
	}
	deleteFailed := strings.EqualFold(strings.TrimSpace(c.DefaultQuery("delete_failed_immediately", "true")), "true")
	protectTopN := atoiDefault(c.Query("protect_top_n_by_views"), 0)
	protectWindow := atoiDefault(c.Query("protect_top_n_window_days"), 30)

	q := buildCandidateQuery(db, candidateFilter{
		tenantID:                tenantID,
		minAgeDays:              minAgeDays,
		maxViewCount:            maxViewCount,
		deleteFailedImmediately: deleteFailed,
		protectTopNByViews:      protectTopN,
		protectTopNWindowDays:   protectWindow,
		excludeColdTier:         true,
	})

	var total int64
	q.Model(&models.ContentItem{}).Count(&total)

	var totalBytes int64
	q.Model(&models.ContentItem{}).Select("COALESCE(SUM(file_size_bytes),0)").Scan(&totalBytes)

	var items []models.ContentItem
	if err := q.Order("view_count ASC, created_at ASC").Limit(limit).Find(&items).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to list candidates"})
		return
	}

	out := make([]internalCandidate, 0, len(items))
	for _, it := range items {
		out = append(out, internalCandidate{
			ID:            it.PublicID.String(),
			TenantID:      it.TenantID,
			Type:          string(it.Type),
			Status:        string(it.Status),
			MediaURL:      it.MediaURL,
			ThumbnailURL:  it.ThumbnailURL,
			FileSizeBytes: it.FileSizeBytes,
			ViewCount:     it.ViewCount,
			CreatedAt:     it.CreatedAt.UTC().Format(time.RFC3339),
		})
	}

	// Trim by max_bytes if set
	if raw := strings.TrimSpace(c.Query("max_bytes")); raw != "" {
		if maxBytes, err := strconv.ParseInt(raw, 10, 64); err == nil && maxBytes > 0 {
			var running int64
			cut := 0
			for i, it := range out {
				running += it.FileSizeBytes
				cut = i + 1
				if running >= maxBytes {
					break
				}
			}
			out = out[:cut]
		}
	}

	c.JSON(http.StatusOK, internalCandidatesResponse{
		Data:       out,
		Total:      total,
		TotalBytes: totalBytes,
	})
}

type internalArchiveItemsRequest struct {
	IDs                []string `json:"ids"`
	PreserveThumbnails bool     `json:"preserve_thumbnails"`
}

type internalArchiveItemsResponse struct {
	UpdatedCount int   `json:"updated_count"`
	FreedBytes   int64 `json:"freed_bytes"`
}

// InternalArchiveItems handles POST /internal/storage/archive
// Marks items archived (status=ARCHIVED), nulls media_url, zeroes
// file_size_bytes, sets archived_at.
func InternalArchiveItems(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	var req internalArchiveItemsRequest
	if err := c.ShouldBindJSON(&req); err != nil || len(req.IDs) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "ids required"})
		return
	}

	ids := make([]uuid.UUID, 0, len(req.IDs))
	for _, raw := range req.IDs {
		if id, err := uuid.Parse(raw); err == nil {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no valid ids"})
		return
	}

	var freed int64
	db.Model(&models.ContentItem{}).
		Where("public_id IN ?", ids).
		Select("COALESCE(SUM(file_size_bytes),0)").
		Scan(&freed)

	now := time.Now().UTC()
	updates := map[string]interface{}{
		"status":          models.ContentStatusArchived,
		"archived_at":     &now,
		"file_size_bytes": 0,
		"media_url":       nil,
	}
	if !req.PreserveThumbnails {
		updates["thumbnail_url"] = nil
	}
	res := db.Model(&models.ContentItem{}).Where("public_id IN ?", ids).Updates(updates)
	if res.Error != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to archive"})
		return
	}

	c.JSON(http.StatusOK, internalArchiveItemsResponse{
		UpdatedCount: int(res.RowsAffected),
		FreedBytes:   freed,
	})
}

type internalMoveToColdItem struct {
	ID              string  `json:"id"`
	MediaURL        *string `json:"media_url"`
	ThumbnailURL    *string `json:"thumbnail_url"`
	NewSizeBytes    *int64  `json:"new_size_bytes"`
}

type internalMoveToColdRequest struct {
	Items []internalMoveToColdItem `json:"items"`
}

type internalMoveToColdResponse struct {
	UpdatedCount int   `json:"updated_count"`
	FreedBytes   int64 `json:"freed_bytes"`
}

// InternalMoveItemsToCold handles POST /internal/storage/move-to-cold
// Bulk-updates items to storage_tier='cold' with new media URLs pointing at the
// cold bucket. Status stays READY so playback continues from the cold tier.
func InternalMoveItemsToCold(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	var req internalMoveToColdRequest
	if err := c.ShouldBindJSON(&req); err != nil || len(req.Items) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "items required"})
		return
	}

	cold := "cold"
	now := time.Now().UTC()
	updated := 0
	var freed int64

	for _, it := range req.Items {
		id, err := uuid.Parse(it.ID)
		if err != nil {
			continue
		}

		var item models.ContentItem
		if err := db.Where("public_id = ?", id).First(&item).Error; err != nil {
			continue
		}

		oldSize := item.FileSizeBytes
		updates := map[string]interface{}{
			"storage_tier":       &cold,
			"last_storage_check": &now,
		}
		if it.MediaURL != nil {
			updates["media_url"] = it.MediaURL
		}
		if it.ThumbnailURL != nil {
			updates["thumbnail_url"] = it.ThumbnailURL
		}
		if it.NewSizeBytes != nil {
			updates["file_size_bytes"] = *it.NewSizeBytes
			freed += oldSize - *it.NewSizeBytes
		} else {
			// No size change reported; the cold copy is the same bytes.
			// We "freed" nothing on the bucket totals but the primary tier
			// shed `oldSize` bytes — which is what circulation cares about.
			freed += oldSize
		}

		if err := db.Model(&models.ContentItem{}).Where("id = ?", item.ID).Updates(updates).Error; err == nil {
			updated++
		}
	}

	c.JSON(http.StatusOK, internalMoveToColdResponse{
		UpdatedCount: updated,
		FreedBytes:   freed,
	})
}

type internalSweepRunRequest struct {
	TenantID         string  `json:"tenant_id"`
	StartedAt        string  `json:"started_at"`
	FinishedAt       *string `json:"finished_at"`
	DeletedCount     int     `json:"deleted_count"`
	MovedToColdCount int     `json:"moved_to_cold_count"`
	ReEncodedCount   int     `json:"re_encoded_count"`
	FreedBytes       int64   `json:"freed_bytes"`
	Trigger          string  `json:"trigger"`
	Error            string  `json:"error,omitempty"`
}

// InternalCreateSweepRun handles POST /internal/storage/sweep-runs
func InternalCreateSweepRun(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	var req internalSweepRunRequest
	if err := c.ShouldBindJSON(&req); err != nil || strings.TrimSpace(req.TenantID) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	started, err := time.Parse(time.RFC3339, req.StartedAt)
	if err != nil {
		started = time.Now().UTC()
	}
	var finished *time.Time
	if req.FinishedAt != nil && *req.FinishedAt != "" {
		if f, err := time.Parse(time.RFC3339, *req.FinishedAt); err == nil {
			finished = &f
		}
	}

	trigger := strings.TrimSpace(req.Trigger)
	if trigger == "" {
		trigger = "auto"
	}

	run := models.StorageSweepRun{
		TenantID:         req.TenantID,
		StartedAt:        started,
		FinishedAt:       finished,
		DeletedCount:     req.DeletedCount,
		MovedToColdCount: req.MovedToColdCount,
		ReEncodedCount:   req.ReEncodedCount,
		FreedBytes:       req.FreedBytes,
		Trigger:          trigger,
		Error:            req.Error,
	}
	if err := db.Create(&run).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to record sweep run"})
		return
	}

	// Touch the policy's last_sweep_at
	if req.Error == "" {
		_ = db.Model(&models.StoragePolicy{}).
			Where("tenant_id = ? OR tenant_id IS NULL", req.TenantID).
			Update("last_sweep_at", started).Error
	}

	c.JSON(http.StatusOK, run)
}
