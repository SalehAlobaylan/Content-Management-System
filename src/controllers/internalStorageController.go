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
	// AutopilotTenants lists tenants where the Media Circulation Autopilot is
	// enabled (stage 5, G4 single-actor rule): the storage worker defers its
	// self-scheduled repeatable sweep tick for these tenants — Autopilot runs
	// trigger bounded sweeps instead. Manual sweeps are unaffected.
	AutopilotTenants []string `json:"autopilot_tenants"`
}

// InternalListStoragePolicies handles GET /internal/storage/policies
func InternalListStoragePolicies(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	var all []models.StoragePolicy
	if err := db.Find(&all).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to list policies"})
		return
	}
	resp := internalListPoliciesResponse{All: all, AutopilotTenants: []string{}}
	for i := range all {
		if all[i].TenantID == nil {
			p := all[i]
			resp.Global = &p
			continue
		}
		resp.Overrides = append(resp.Overrides, all[i])
	}
	// The Autopilot-managed tenant list gates the sweep worker's single-actor
	// rule (G4). If this query fails we must NOT return an empty list — that
	// would read as "no tenant is autopilot-managed" and let the legacy sweep
	// run alongside Autopilot. Fail the request so the worker fails closed
	// (skips its tick) rather than double-acting on stale/incomplete data.
	var autopilotTenants []string
	if err := db.Model(&models.MediaCirculationPolicy{}).
		Where("enabled = ? AND autopilot_enabled = ?", true, true).
		Pluck("tenant_id", &autopilotTenants).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to resolve autopilot-managed tenants"})
		return
	}
	if autopilotTenants != nil {
		resp.AutopilotTenants = autopilotTenants
	}
	c.JSON(http.StatusOK, resp)
}

type internalCandidatesResponse struct {
	Data       []internalCandidate `json:"data"`
	Total      int64               `json:"total"`
	TotalBytes int64               `json:"total_bytes"`
}

type internalCandidate struct {
	ID                  string  `json:"id"`
	TenantID            string  `json:"tenant_id"`
	Type                string  `json:"type"`
	Status              string  `json:"status"`
	MediaURL            *string `json:"media_url,omitempty"`
	ThumbnailURL        *string `json:"thumbnail_url,omitempty"`
	FileSizeBytes       int64   `json:"file_size_bytes"`
	ViewCount           int     `json:"view_count"`
	CreatedAt           string  `json:"created_at"`
	ParentContentItemID *string `json:"parent_content_item_id,omitempty"`
	IsFeedUnit          bool    `json:"is_feed_unit"`
	FeedVisibility      string  `json:"feed_visibility"`
	DurationSec         *int    `json:"duration_sec,omitempty"`
	OriginalURL         *string `json:"original_url,omitempty"`
	SourceFeedURL       *string `json:"source_feed_url,omitempty"`
	SourceEpisodeID     *string `json:"source_episode_id,omitempty"`
	MediaSuitability    string  `json:"media_suitability"`
	ContentRole         string  `json:"content_role"`
	ProtectionReason    string  `json:"protection_reason,omitempty"`
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
	includeAtomizedParents := strings.EqualFold(strings.TrimSpace(c.DefaultQuery("include_atomized_parents", "false")), "true")
	archiveAction := strings.ToLower(strings.TrimSpace(c.DefaultQuery("archive_action", "re_encode")))
	filterIDs := parseStorageCandidateIDs(c.Query("ids"))
	if strings.TrimSpace(c.Query("ids")) != "" && len(filterIDs) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "ids must contain at least one valid UUID"})
		return
	}

	q := buildCandidateQuery(db, candidateFilter{
		tenantID:                tenantID,
		minAgeDays:              minAgeDays,
		maxViewCount:            maxViewCount,
		deleteFailedImmediately: deleteFailed,
		protectTopNByViews:      protectTopN,
		protectTopNWindowDays:   protectWindow,
		excludeColdTier:         true,
		includeAtomizedParents:  includeAtomizedParents,
		archiveAction:           archiveAction,
	})
	if len(filterIDs) > 0 {
		q = q.Where("public_id IN ?", filterIDs)
		if limit > len(filterIDs) {
			limit = len(filterIDs)
		}
	}

	var total int64
	q.Model(&models.ContentItem{}).Count(&total)

	var totalBytes int64
	q.Model(&models.ContentItem{}).Select("COALESCE(SUM(file_size_bytes),0)").Scan(&totalBytes)

	var items []models.ContentItem
	if err := q.Order(storageValueOrderExpr).Limit(limit).Find(&items).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to list candidates"})
		return
	}

	out := make([]internalCandidate, 0, len(items))
	for _, it := range items {
		role, reason := storageRoleForContentItem(it)
		var parentID *string
		if it.ParentContentItemID != nil {
			s := it.ParentContentItemID.String()
			parentID = &s
		}
		out = append(out, internalCandidate{
			ID:                  it.PublicID.String(),
			TenantID:            it.TenantID,
			Type:                string(it.Type),
			Status:              string(it.Status),
			MediaURL:            it.MediaURL,
			ThumbnailURL:        it.ThumbnailURL,
			FileSizeBytes:       it.FileSizeBytes,
			ViewCount:           it.ViewCount,
			CreatedAt:           it.CreatedAt.UTC().Format(time.RFC3339),
			ParentContentItemID: parentID,
			IsFeedUnit:          it.IsFeedUnit,
			FeedVisibility:      it.FeedVisibility,
			DurationSec:         it.DurationSec,
			OriginalURL:         it.OriginalURL,
			SourceFeedURL:       it.SourceFeedURL,
			SourceEpisodeID:     it.SourceEpisodeID,
			MediaSuitability:    it.MediaSuitability,
			ContentRole:         role,
			ProtectionReason:    reason,
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

func parseStorageCandidateIDs(raw string) []uuid.UUID {
	parts := strings.Split(raw, ",")
	out := make([]uuid.UUID, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if id, err := uuid.Parse(part); err == nil {
			out = append(out, id)
		}
	}
	return out
}

type internalArchiveItemsRequest struct {
	IDs                []string `json:"ids"`
	PreserveThumbnails bool     `json:"preserve_thumbnails"`
}

type internalArchiveItemsResponse struct {
	UpdatedCount int   `json:"updated_count"`
	FreedBytes   int64 `json:"freed_bytes"`
}

// InternalArchiveItems handles POST /internal/storage/archive.
// It deletes artifact references and records storage_state without using
// content status as the artifact lifecycle marker.
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

	var items []models.ContentItem
	db.Where("public_id IN ?", ids).Find(&items)
	var freed int64
	for _, item := range items {
		freed += item.FileSizeBytes
	}

	now := time.Now().UTC()
	updates := map[string]interface{}{
		"file_size_bytes":         0,
		"media_url":               nil,
		"storage_state":           models.StorageStateRecoverableDeleted,
		"storage_state_reason":    "storage_archive",
		"storage_recovery_status": models.StorageRecoveryRecoverable,
		"storage_deleted_at":      &now,
	}
	if !req.PreserveThumbnails {
		updates["thumbnail_url"] = nil
	}
	res := db.Model(&models.ContentItem{}).Where("public_id IN ?", ids).Updates(updates)
	if res.Error != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to archive"})
		return
	}
	for _, item := range items {
		_, _ = createStorageArtifactEvent(db, storageArtifactEventInput{
			TenantID:              item.TenantID,
			ContentItemID:         item.PublicID,
			ParentContentItemID:   item.ParentContentItemID,
			EventType:             models.StorageArtifactEventRecoverableDeleted,
			Status:                models.StorageArtifactEventStatusSuccess,
			Reason:                "Archived by storage sweep",
			Trigger:               "auto",
			Source:                "aggregation",
			OldMediaURL:           stringValue(item.MediaURL),
			OldSizeBytes:          item.FileSizeBytes,
			DeletedBytes:          item.FileSizeBytes,
			FreedBytes:            item.FileSizeBytes,
			RecoveryPayload:       storageRecoveryPayloadForItem(item),
			StorageState:          models.StorageStateRecoverableDeleted,
			StorageStateReason:    "storage_archive",
			StorageRecoveryStatus: models.StorageRecoveryRecoverable,
		})
	}

	c.JSON(http.StatusOK, internalArchiveItemsResponse{
		UpdatedCount: int(res.RowsAffected),
		FreedBytes:   freed,
	})
}

type internalMoveToColdItem struct {
	ID           string  `json:"id"`
	MediaURL     *string `json:"media_url"`
	ThumbnailURL *string `json:"thumbnail_url"`
	NewSizeBytes *int64  `json:"new_size_bytes"`
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
			"storage_tier":             &cold,
			"last_storage_check":       &now,
			"storage_state":            models.StorageStateCold,
			"storage_state_reason":     "moved_to_cold",
			"storage_recovery_status":  models.StorageRecoveryRecoverable,
			"storage_last_verified_at": &now,
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
			newSize := int64Value(it.NewSizeBytes, oldSize)
			eventFreed := oldSize - newSize
			if it.NewSizeBytes == nil {
				eventFreed = oldSize
			}
			if eventFreed < 0 {
				eventFreed = 0
			}
			_, _ = createStorageArtifactEvent(db, storageArtifactEventInput{
				TenantID:              item.TenantID,
				ContentItemID:         item.PublicID,
				ParentContentItemID:   item.ParentContentItemID,
				EventType:             models.StorageArtifactEventMovedCold,
				Status:                models.StorageArtifactEventStatusSuccess,
				Reason:                "Moved to cold storage by storage sweep",
				Trigger:               "auto",
				Source:                "aggregation",
				StorageTier:           "cold",
				OldStorageTier:        tierFromItem(item),
				OldMediaURL:           stringValue(item.MediaURL),
				NewMediaURL:           stringValue(it.MediaURL),
				OldSizeBytes:          oldSize,
				NewSizeBytes:          newSize,
				FreedBytes:            eventFreed,
				RecoveryPayload:       storageRecoveryPayloadForItem(item),
				StorageState:          models.StorageStateCold,
				StorageStateReason:    "moved_to_cold",
				StorageRecoveryStatus: models.StorageRecoveryRecoverable,
			})
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
