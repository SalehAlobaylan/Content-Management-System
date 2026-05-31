package controllers

import (
	"bytes"
	"content-management-system/src/models"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

const (
	defaultMaxStorageBytes      int64 = 5 * 1024 * 1024 * 1024 // 5 GB
	defaultTargetUtilizationPct       = 80
	defaultMinAgeDays                 = 14
	defaultMinViewCountForKeep        = 5
	defaultSweepIntervalMinutes       = 60
	purgeIDsLimit                     = 500
)

// -----------------------------------------------------------------------------
// Storage stats
// -----------------------------------------------------------------------------

type storageStatsResponse struct {
	UsedBytes        int64                  `json:"used_bytes"`
	ObjectCount      int64                  `json:"object_count"`
	QuotaBytes       int64                  `json:"quota_bytes"`
	UtilizationPct   float64                `json:"utilization_pct"`
	ByArtifactType   map[string]int64       `json:"by_artifact_type"`
	ByContentType    map[string]contentSize `json:"by_content_type"`
	DBTrackedBytes   int64                  `json:"db_tracked_bytes"`
	LiveStatsAt      string                 `json:"live_stats_at"`
	AggregationError string                 `json:"aggregation_error,omitempty"`
}

type contentSize struct {
	Bytes int64 `json:"bytes"`
	Count int64 `json:"count"`
}

// aggStatsResponse mirrors the shape Aggregation-Service returns from
// GET /admin/storage/stats.
type aggStatsResponse struct {
	UsedBytes      int64            `json:"used_bytes"`
	ObjectCount    int64            `json:"object_count"`
	ByArtifactType map[string]int64 `json:"by_artifact_type"`
}

// GetStorageStats handles GET /admin/storage/stats
func GetStorageStats(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}

	db := c.MustGet("db").(*gorm.DB)
	policy := loadEffectiveStoragePolicy(db, principal.TenantID)

	resp := storageStatsResponse{
		QuotaBytes:     policy.MaxStorageBytes,
		ByArtifactType: map[string]int64{},
		ByContentType:  map[string]contentSize{},
		LiveStatsAt:    time.Now().UTC().Format(time.RFC3339),
	}

	// CMS-side aggregates from DB
	type contentTypeAgg struct {
		Type  string
		Bytes int64
		Count int64
	}
	var rows []contentTypeAgg
	if err := db.Model(&models.ContentItem{}).
		Select("type, COALESCE(SUM(file_size_bytes),0) as bytes, COUNT(*) as count").
		Where("tenant_id = ? AND status != ?", principal.TenantID, models.ContentStatusArchived).
		Group("type").
		Scan(&rows).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{
			Message: "Failed to aggregate content sizes",
			Code:    "STATS_FAILED",
		})
		return
	}
	for _, r := range rows {
		resp.ByContentType[r.Type] = contentSize{Bytes: r.Bytes, Count: r.Count}
		resp.DBTrackedBytes += r.Bytes
	}

	// Live numbers from Aggregation
	live, err := callAggregationStorageStats(c.GetHeader("Authorization"))
	if err != nil {
		// Soft-fail: report DB-side numbers and surface the error so the UI
		// can show a warning instead of breaking.
		resp.AggregationError = err.Error()
		resp.UsedBytes = resp.DBTrackedBytes
	} else {
		resp.UsedBytes = live.UsedBytes
		resp.ObjectCount = live.ObjectCount
		resp.ByArtifactType = live.ByArtifactType
	}

	if resp.QuotaBytes > 0 {
		resp.UtilizationPct = float64(resp.UsedBytes) / float64(resp.QuotaBytes) * 100.0
	}

	c.JSON(http.StatusOK, resp)
}

// -----------------------------------------------------------------------------
// Candidates (worst-first list of purgeable items)
// -----------------------------------------------------------------------------

type storageCandidate struct {
	ID            string  `json:"id"`
	Type          string  `json:"type"`
	Status        string  `json:"status"`
	Title         string  `json:"title"`
	SourceName    *string `json:"source_name,omitempty"`
	ViewCount     int     `json:"view_count"`
	FileSizeBytes int64   `json:"file_size_bytes"`
	CreatedAt     string  `json:"created_at"`
	PublishedAt   *string `json:"published_at,omitempty"`
	MediaURL      *string `json:"media_url,omitempty"`
	ThumbnailURL  *string `json:"thumbnail_url,omitempty"`
}

type storageCandidatesResponse struct {
	Data       []storageCandidate `json:"data"`
	Total      int64              `json:"total"`
	Limit      int                `json:"limit"`
	TotalBytes int64              `json:"total_bytes"`
}

// GetStorageCandidates handles GET /admin/storage/candidates
func GetStorageCandidates(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}

	db := c.MustGet("db").(*gorm.DB)
	policy := loadEffectiveStoragePolicy(db, principal.TenantID)

	minAgeDays := atoiDefault(c.Query("min_age_days"), policy.MinAgeDays)
	maxViewCount := atoiDefault(c.Query("max_view_count"), policy.MinViewCountForKeep)
	limit := atoiDefault(c.Query("limit"), 100)
	if limit > 500 {
		limit = 500
	}
	statusFilter := strings.ToUpper(strings.TrimSpace(c.Query("status")))
	sourceName := strings.TrimSpace(c.Query("source_name"))

	query := buildCandidateQuery(db, candidateFilter{
		tenantID:                principal.TenantID,
		minAgeDays:              minAgeDays,
		maxViewCount:            maxViewCount,
		status:                  statusFilter,
		sourceName:              sourceName,
		deleteFailedImmediately: policy.DeleteFailedImmediately,
		protectTopNByViews:      policy.ProtectTopNByViews,
		protectTopNWindowDays:   policy.ProtectTopNWindowDays,
		excludeColdTier:         true,
	})

	var total int64
	query.Model(&models.ContentItem{}).Count(&total)

	var totalBytes int64
	query.Model(&models.ContentItem{}).Select("COALESCE(SUM(file_size_bytes),0)").Scan(&totalBytes)

	var items []models.ContentItem
	if err := query.
		Order("view_count ASC, created_at ASC").
		Limit(limit).
		Find(&items).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{
			Message: "Failed to fetch candidates",
			Code:    "CANDIDATES_FAILED",
		})
		return
	}

	out := make([]storageCandidate, 0, len(items))
	for _, it := range items {
		out = append(out, mapStorageCandidate(it))
	}
	c.JSON(http.StatusOK, storageCandidatesResponse{
		Data:       out,
		Total:      total,
		Limit:      limit,
		TotalBytes: totalBytes,
	})
}

// candidateFilter captures every variable the candidate query needs. Pulled
// out so the candidate, preview, purge, and internal endpoints can share one
// definition.
type candidateFilter struct {
	tenantID                string
	minAgeDays              int
	maxViewCount            int
	status                  string
	sourceName              string
	deleteFailedImmediately bool
	protectTopNByViews      int
	protectTopNWindowDays   int
	excludeColdTier         bool
}

func buildCandidateQuery(db *gorm.DB, f candidateFilter) *gorm.DB {
	cutoff := time.Now().UTC().AddDate(0, 0, -f.minAgeDays)

	q := db.Model(&models.ContentItem{}).
		Where("tenant_id = ?", f.tenantID).
		Where("(media_url IS NOT NULL OR thumbnail_url IS NOT NULL)").
		Where("status != ?", models.ContentStatusArchived)

	if f.excludeColdTier {
		// Cold-tier items already paid the savings dividend; don't re-process them.
		q = q.Where("(storage_tier IS NULL OR storage_tier != 'cold')")
	}

	switch {
	case f.status != "":
		q = q.Where("status = ?", f.status)
	case f.deleteFailedImmediately:
		q = q.Where(
			db.Where("status = ?", models.ContentStatusFailed).
				Or(db.Where("created_at < ? AND view_count <= ?", cutoff, f.maxViewCount)),
		)
	default:
		q = q.Where("created_at < ? AND view_count <= ?", cutoff, f.maxViewCount)
	}

	if f.sourceName != "" {
		q = q.Where("source_name = ?", f.sourceName)
	}

	// Hot-content protection: subquery returning the IDs of the top-N most-viewed
	// items in the recent window. Anything in that set is exempt from purge.
	if f.protectTopNByViews > 0 && f.protectTopNWindowDays >= 0 {
		windowCutoff := time.Now().UTC().AddDate(0, 0, -f.protectTopNWindowDays)
		protectedIDs := db.Model(&models.ContentItem{}).
			Select("id").
			Where("tenant_id = ?", f.tenantID).
			Where("created_at > ?", windowCutoff).
			Order("view_count DESC, created_at DESC").
			Limit(f.protectTopNByViews)
		q = q.Where("id NOT IN (?)", protectedIDs)
	}

	// Return a fresh session so callers can run several finishers off this base
	// (Count, SUM(...).Scan, Order(...).Find) without clauses leaking between
	// them. Without this, the ORDER BY view_count from the Find chain bleeds
	// into the aggregate SUM query and Postgres rejects it with
	// "column must appear in the GROUP BY clause" (SQLSTATE 42803).
	return q.Session(&gorm.Session{})
}

// filterFromPolicy is the canonical filter the worker uses for an auto sweep.
func filterFromPolicy(p models.StoragePolicy, tenantID, status, sourceName string) candidateFilter {
	return candidateFilter{
		tenantID:                tenantID,
		minAgeDays:              p.MinAgeDays,
		maxViewCount:            p.MinViewCountForKeep,
		status:                  status,
		sourceName:              sourceName,
		deleteFailedImmediately: p.DeleteFailedImmediately,
		protectTopNByViews:      p.ProtectTopNByViews,
		protectTopNWindowDays:   p.ProtectTopNWindowDays,
		excludeColdTier:         true,
	}
}

func mapStorageCandidate(it models.ContentItem) storageCandidate {
	title := ""
	if it.Title != nil {
		title = *it.Title
	}
	var pub *string
	if it.PublishedAt != nil {
		s := it.PublishedAt.UTC().Format(time.RFC3339)
		pub = &s
	}
	return storageCandidate{
		ID:            it.PublicID.String(),
		Type:          string(it.Type),
		Status:        string(it.Status),
		Title:         title,
		SourceName:    it.SourceName,
		ViewCount:     it.ViewCount,
		FileSizeBytes: it.FileSizeBytes,
		CreatedAt:     it.CreatedAt.UTC().Format(time.RFC3339),
		PublishedAt:   pub,
		MediaURL:      it.MediaURL,
		ThumbnailURL:  it.ThumbnailURL,
	}
}

// -----------------------------------------------------------------------------
// Purge (archive in DB + delete objects in S3 via Aggregation)
// -----------------------------------------------------------------------------

type purgeFilters struct {
	MinAgeDays   *int    `json:"min_age_days"`
	MaxViewCount *int    `json:"max_view_count"`
	Status       string  `json:"status"`
	SourceName   string  `json:"source_name"`
	MaxBytes     *int64  `json:"max_bytes"`
	Trigger      *string `json:"trigger"`
}

type storagePurgeRequest struct {
	IDs                []string      `json:"ids"`
	Filters            *purgeFilters `json:"filters"`
	DryRun             bool          `json:"dry_run"`
	PreserveThumbnails *bool         `json:"preserve_thumbnails"`
}

type storagePurgeResponse struct {
	DeletedCount int    `json:"deleted_count"`
	FreedBytes   int64  `json:"freed_bytes"`
	DryRun       bool   `json:"dry_run"`
	Message      string `json:"message"`
}

// PurgeStorage handles POST /admin/storage/purge
func PurgeStorage(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	var req storagePurgeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid request: " + err.Error(), Code: "INVALID_REQUEST"})
		return
	}

	if len(req.IDs) > purgeIDsLimit {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: fmt.Sprintf("Too many ids; maximum is %d", purgeIDsLimit),
			Code:    "TOO_MANY_IDS",
		})
		return
	}

	policy := loadEffectiveStoragePolicy(db, principal.TenantID)
	preserveThumbs := policy.PreserveThumbnails
	if req.PreserveThumbnails != nil {
		preserveThumbs = *req.PreserveThumbnails
	}

	items, err := resolvePurgeTargets(db, principal.TenantID, req, policy)
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: err.Error(), Code: "BAD_FILTERS"})
		return
	}

	if req.DryRun {
		var freed int64
		for _, it := range items {
			freed += it.FileSizeBytes
		}
		c.JSON(http.StatusOK, storagePurgeResponse{
			DeletedCount: len(items),
			FreedBytes:   freed,
			DryRun:       true,
			Message:      "Dry run — no objects deleted",
		})
		return
	}

	// Build object keys to delete and ask Aggregation to remove them.
	contentIDs := make([]string, 0, len(items))
	for _, it := range items {
		contentIDs = append(contentIDs, it.PublicID.String())
	}
	artifacts := []string{"processed", "original"}
	if !preserveThumbs {
		artifacts = append(artifacts, "thumbnail")
	}

	delResp, err := callAggregationDeleteObjects(c.GetHeader("Authorization"), aggDeleteRequest{
		ContentIDs: contentIDs,
		Artifacts:  artifacts,
	})
	if err != nil {
		c.JSON(http.StatusBadGateway, authErrorResponse{
			Message: "Aggregation refused delete: " + err.Error(),
			Code:    "AGGREGATION_DELETE_FAILED",
		})
		return
	}

	now := time.Now().UTC()
	var freed int64
	for _, it := range items {
		freed += it.FileSizeBytes
		updates := map[string]interface{}{
			"status":          models.ContentStatusArchived,
			"archived_at":     &now,
			"file_size_bytes": 0,
			"media_url":       nil,
		}
		if !preserveThumbs {
			updates["thumbnail_url"] = nil
		}
		if err := db.Model(&models.ContentItem{}).Where("id = ?", it.ID).Updates(updates).Error; err != nil {
			// Don't abort the whole batch; just log and continue
			fmt.Println("storage purge: failed to update content item", it.PublicID, err)
		}
	}

	// Best-effort sweep-run record for manual purges
	trigger := "manual"
	if req.Filters != nil && req.Filters.Trigger != nil && strings.TrimSpace(*req.Filters.Trigger) != "" {
		trigger = *req.Filters.Trigger
	}
	finished := time.Now().UTC()
	_ = db.Create(&models.StorageSweepRun{
		TenantID:     principal.TenantID,
		StartedAt:    now,
		FinishedAt:   &finished,
		DeletedCount: delResp.DeletedCount,
		FreedBytes:   freed,
		Trigger:      trigger,
	}).Error

	c.JSON(http.StatusOK, storagePurgeResponse{
		DeletedCount: delResp.DeletedCount,
		FreedBytes:   freed,
		Message:      "Purge complete",
	})
}

func resolvePurgeTargets(db *gorm.DB, tenantID string, req storagePurgeRequest, policy models.StoragePolicy) ([]models.ContentItem, error) {
	if len(req.IDs) > 0 {
		ids := make([]uuid.UUID, 0, len(req.IDs))
		for _, raw := range req.IDs {
			id, err := uuid.Parse(raw)
			if err != nil {
				return nil, fmt.Errorf("invalid id %q", raw)
			}
			ids = append(ids, id)
		}
		var items []models.ContentItem
		if err := db.Where("tenant_id = ? AND public_id IN ?", tenantID, ids).Find(&items).Error; err != nil {
			return nil, err
		}
		return items, nil
	}

	if req.Filters == nil {
		return nil, errors.New("either ids or filters is required")
	}

	minAge := policy.MinAgeDays
	if req.Filters.MinAgeDays != nil {
		minAge = *req.Filters.MinAgeDays
	}
	maxViews := policy.MinViewCountForKeep
	if req.Filters.MaxViewCount != nil {
		maxViews = *req.Filters.MaxViewCount
	}

	q := buildCandidateQuery(db, candidateFilter{
		tenantID:                tenantID,
		minAgeDays:              minAge,
		maxViewCount:            maxViews,
		status:                  strings.ToUpper(strings.TrimSpace(req.Filters.Status)),
		sourceName:              strings.TrimSpace(req.Filters.SourceName),
		deleteFailedImmediately: policy.DeleteFailedImmediately,
		protectTopNByViews:      policy.ProtectTopNByViews,
		protectTopNWindowDays:   policy.ProtectTopNWindowDays,
		excludeColdTier:         true,
	}).
		Order("view_count ASC, created_at ASC")

	if req.Filters.MaxBytes != nil && *req.Filters.MaxBytes > 0 {
		// Caller wants to free up at most N bytes — fetch enough rows to cover that.
		var items []models.ContentItem
		if err := q.Find(&items).Error; err != nil {
			return nil, err
		}
		var running int64
		out := make([]models.ContentItem, 0, len(items))
		for _, it := range items {
			out = append(out, it)
			running += it.FileSizeBytes
			if running >= *req.Filters.MaxBytes {
				break
			}
		}
		return out, nil
	}

	var items []models.ContentItem
	if err := q.Limit(purgeIDsLimit).Find(&items).Error; err != nil {
		return nil, err
	}
	return items, nil
}

// -----------------------------------------------------------------------------
// Restore — flips a purged item back to PENDING and asks Aggregation to re-fetch
// -----------------------------------------------------------------------------

type restoreResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

// RestoreStorageItem handles POST /admin/storage/restore/:id
func RestoreStorageItem(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid id", Code: "INVALID_ID"})
		return
	}

	var item models.ContentItem
	if err := db.Where("public_id = ? AND tenant_id = ?", id, principal.TenantID).First(&item).Error; err != nil {
		c.JSON(http.StatusNotFound, authErrorResponse{Message: "Content not found", Code: "NOT_FOUND"})
		return
	}

	now := time.Now().UTC()
	item.Status = models.ContentStatusPending
	item.ArchivedAt = nil
	item.LastRestoredAt = &now
	if err := db.Save(&item).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to flip status", Code: "UPDATE_FAILED"})
		return
	}

	// Ask Aggregation to enqueue a media job for this id.
	if _, err := callAggregationRetryPending(c.GetHeader("Authorization"), 1); err != nil {
		c.JSON(http.StatusBadGateway, authErrorResponse{Message: "Aggregation retry failed: " + err.Error(), Code: "RETRY_FAILED"})
		return
	}

	c.JSON(http.StatusOK, restoreResponse{Success: true, Message: "Restore enqueued"})
}

// -----------------------------------------------------------------------------
// Policy CRUD
// -----------------------------------------------------------------------------

type policyTenantInfo struct {
	TenantID *string `json:"tenant_id"`
	Scope    string  `json:"scope"` // "global" | "tenant"
}

type policyResponse struct {
	models.StoragePolicy
	Effective policyTenantInfo `json:"effective"`
}

// GetStoragePolicy handles GET /admin/storage/policy
// Optional query: ?scope=global|tenant — defaults to "tenant" (effective)
func GetStoragePolicy(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	scope := strings.ToLower(strings.TrimSpace(c.DefaultQuery("scope", "tenant")))

	var p models.StoragePolicy
	switch scope {
	case "global":
		p = loadOrCreateGlobalPolicy(db)
		c.JSON(http.StatusOK, policyResponse{StoragePolicy: p, Effective: policyTenantInfo{Scope: "global"}})
	case "tenant":
		p = loadEffectiveStoragePolicy(db, principal.TenantID)
		var info policyTenantInfo
		if p.TenantID != nil {
			info = policyTenantInfo{TenantID: p.TenantID, Scope: "tenant"}
		} else {
			info = policyTenantInfo{Scope: "global"}
		}
		c.JSON(http.StatusOK, policyResponse{StoragePolicy: p, Effective: info})
	default:
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "scope must be 'global' or 'tenant'", Code: "INVALID_SCOPE"})
	}
}

type updatePolicyRequest struct {
	Scope                   string  `json:"scope"` // "global" | "tenant"
	TenantID                string  `json:"tenant_id"`
	Enabled                 *bool   `json:"enabled"`
	MaxStorageBytes         *int64  `json:"max_storage_bytes"`
	TargetUtilizationPct    *int    `json:"target_utilization_pct"`
	MinAgeDays              *int    `json:"min_age_days"`
	MinViewCountForKeep     *int    `json:"min_view_count_for_keep"`
	SweepIntervalMinutes    *int    `json:"sweep_interval_minutes"`
	DeleteFailedImmediately *bool   `json:"delete_failed_immediately"`
	PreserveThumbnails      *bool   `json:"preserve_thumbnails"`
	ProtectTopNByViews      *int    `json:"protect_top_n_by_views"`
	ProtectTopNWindowDays   *int    `json:"protect_top_n_window_days"`
	ArchiveAction           *string `json:"archive_action"`
	// Re-encode target — which QualityProfile id to shrink down to when
	// ArchiveAction='re_encode'. NULL means "auto" (each item uses its own
	// resolved ingest profile by source_type).
	ReEncodeTargetProfileID *uint `json:"re_encode_target_profile_id"`
	// Operation budget fields
	ClassAFreeBudget *int64 `json:"class_a_free_budget"`
	ClassBFreeBudget *int64 `json:"class_b_free_budget"`
	ClassAWarnPct    *int   `json:"class_a_warn_pct"`
	ClassACapPct     *int   `json:"class_a_cap_pct"`
	ClassBWarnPct    *int   `json:"class_b_warn_pct"`
	ClassBCapPct     *int   `json:"class_b_cap_pct"`
}

// UpdateStoragePolicy handles PUT /admin/storage/policy
func UpdateStoragePolicy(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	var req updatePolicyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid request", Code: "INVALID_REQUEST"})
		return
	}

	scope := strings.ToLower(strings.TrimSpace(req.Scope))
	if scope == "" {
		scope = "tenant"
	}

	var p models.StoragePolicy
	switch scope {
	case "global":
		p = loadOrCreateGlobalPolicy(db)
	case "tenant":
		tenantID := principal.TenantID
		if strings.TrimSpace(req.TenantID) != "" {
			tenantID = req.TenantID
		}
		p = loadOrCreateTenantPolicy(db, tenantID)
	default:
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "scope must be 'global' or 'tenant'", Code: "INVALID_SCOPE"})
		return
	}

	if req.Enabled != nil {
		p.Enabled = *req.Enabled
	}
	if req.MaxStorageBytes != nil && *req.MaxStorageBytes > 0 {
		p.MaxStorageBytes = *req.MaxStorageBytes
	}
	if req.TargetUtilizationPct != nil {
		p.TargetUtilizationPct = clampInt(*req.TargetUtilizationPct, 10, 100)
	}
	if req.MinAgeDays != nil {
		p.MinAgeDays = clampInt(*req.MinAgeDays, 0, 365*5)
	}
	if req.MinViewCountForKeep != nil {
		p.MinViewCountForKeep = clampInt(*req.MinViewCountForKeep, 0, 1_000_000)
	}
	if req.SweepIntervalMinutes != nil {
		p.SweepIntervalMinutes = clampInt(*req.SweepIntervalMinutes, 1, 24*60)
	}
	if req.DeleteFailedImmediately != nil {
		p.DeleteFailedImmediately = *req.DeleteFailedImmediately
	}
	if req.PreserveThumbnails != nil {
		p.PreserveThumbnails = *req.PreserveThumbnails
	}
	if req.ProtectTopNByViews != nil {
		p.ProtectTopNByViews = clampInt(*req.ProtectTopNByViews, 0, 10000)
	}
	if req.ProtectTopNWindowDays != nil {
		p.ProtectTopNWindowDays = clampInt(*req.ProtectTopNWindowDays, 0, 365)
	}
	if req.ArchiveAction != nil {
		action := strings.ToLower(strings.TrimSpace(*req.ArchiveAction))
		if action != "delete" && action != "move_to_cold" && action != "re_encode" {
			c.JSON(http.StatusBadRequest, authErrorResponse{
				Message: "archive_action must be 'delete', 'move_to_cold', or 're_encode'",
				Code:    "INVALID_ACTION",
			})
			return
		}
		// re_encode requires either an explicit target or at least one global
		// ingest profile to fall back on. Reject early if neither exists —
		// otherwise the worker would have nothing to use and silently skip.
		//
		// IMPORTANT: the Console sends `re_encode_target_profile_id: 0` to
		// mean "switch to Auto". A pointer-to-zero is NOT nil, so we must
		// normalise here — otherwise the guard passes and the policy saves
		// in a broken state (action=re_encode + target=NULL + no global).
		if action == "re_encode" {
			var effectiveTarget *uint = p.ReEncodeTargetProfileID
			if req.ReEncodeTargetProfileID != nil {
				if *req.ReEncodeTargetProfileID == 0 {
					effectiveTarget = nil
				} else {
					effectiveTarget = req.ReEncodeTargetProfileID
				}
			}
			if effectiveTarget == nil {
				var globalCount int64
				db.Model(&models.QualityProfile{}).
					Where("tenant_id IS NULL AND source_type IS NULL AND is_active = TRUE").
					Count(&globalCount)
				if globalCount == 0 {
					c.JSON(http.StatusBadRequest, authErrorResponse{
						Message: "archive_action='re_encode' requires either re_encode_target_profile_id or at least one active global ingest profile (tenant_id=NULL, source_type=NULL)",
						Code:    "NO_REENCODE_TARGET",
					})
					return
				}
			}
		}
		p.ArchiveAction = action
	}

	// Re-encode target profile id — accepts an id or 0/null to clear (auto).
	if req.ReEncodeTargetProfileID != nil {
		if *req.ReEncodeTargetProfileID == 0 {
			p.ReEncodeTargetProfileID = nil
		} else {
			// Confirm the target exists.
			var qp models.QualityProfile
			if err := db.First(&qp, *req.ReEncodeTargetProfileID).Error; err != nil {
				c.JSON(http.StatusBadRequest, authErrorResponse{
					Message: "re_encode_target_profile_id does not match any QualityProfile",
					Code:    "INVALID_REENCODE_TARGET",
				})
				return
			}
			v := *req.ReEncodeTargetProfileID
			p.ReEncodeTargetProfileID = &v
		}
	}

	// Operation budgets — 0 disables the soft cap entirely.
	if req.ClassAFreeBudget != nil && *req.ClassAFreeBudget >= 0 {
		p.ClassAFreeBudget = *req.ClassAFreeBudget
	}
	if req.ClassBFreeBudget != nil && *req.ClassBFreeBudget >= 0 {
		p.ClassBFreeBudget = *req.ClassBFreeBudget
	}
	if req.ClassAWarnPct != nil {
		p.ClassAWarnPct = clampInt(*req.ClassAWarnPct, 0, 100)
	}
	if req.ClassACapPct != nil {
		p.ClassACapPct = clampInt(*req.ClassACapPct, 0, 100)
	}
	if req.ClassBWarnPct != nil {
		p.ClassBWarnPct = clampInt(*req.ClassBWarnPct, 0, 100)
	}
	if req.ClassBCapPct != nil {
		p.ClassBCapPct = clampInt(*req.ClassBCapPct, 0, 100)
	}

	if err := db.Save(&p).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to save policy", Code: "SAVE_FAILED"})
		return
	}

	// Notify Aggregation that the policy changed so it can register/update its
	// repeatable sweeper job.
	go func(authHeader string) {
		_ = callAggregationPolicyChanged(authHeader)
	}(c.GetHeader("Authorization"))

	c.JSON(http.StatusOK, p)
}

type listOverridesResponse struct {
	Global    models.StoragePolicy   `json:"global"`
	Overrides []models.StoragePolicy `json:"overrides"`
}

// ListStoragePolicyOverrides handles GET /admin/storage/policy/overrides
func ListStoragePolicyOverrides(c *gin.Context) {
	if _, ok := requireAdminPrincipal(c); !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	global := loadOrCreateGlobalPolicy(db)
	var overrides []models.StoragePolicy
	if err := db.Where("tenant_id IS NOT NULL").Order("tenant_id ASC").Find(&overrides).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to list overrides", Code: "LIST_FAILED"})
		return
	}
	c.JSON(http.StatusOK, listOverridesResponse{Global: global, Overrides: overrides})
}

// DeleteStoragePolicyOverride handles DELETE /admin/storage/policy/overrides/:tenant_id
func DeleteStoragePolicyOverride(c *gin.Context) {
	if _, ok := requireAdminPrincipal(c); !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	tenantID := strings.TrimSpace(c.Param("tenant_id"))
	if tenantID == "" {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "tenant_id required", Code: "INVALID"})
		return
	}
	if err := db.Where("tenant_id = ?", tenantID).Delete(&models.StoragePolicy{}).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to delete override", Code: "DELETE_FAILED"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true})
}

// -----------------------------------------------------------------------------
// Pre-purge preview — what would the next sweep do, right now?
// -----------------------------------------------------------------------------

type sweepPreviewResponse struct {
	Enabled         bool    `json:"enabled"`
	NextRunAt       *string `json:"next_run_at,omitempty"`
	CandidatesCount int64   `json:"candidates_count"`
	BytesToFree     int64   `json:"bytes_to_free"`
	ArchiveAction   string  `json:"archive_action"`
	ProtectedCount  int64   `json:"protected_count"`
	ProtectedBytes  int64   `json:"protected_bytes"`
}

// GetSweepPreview handles GET /admin/storage/preview
// Reports what the next auto-sweep would do, applying the *current* policy.
func GetSweepPreview(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	policy := loadEffectiveStoragePolicy(db, principal.TenantID)

	q := buildCandidateQuery(db, filterFromPolicy(policy, principal.TenantID, "", ""))

	var count int64
	q.Model(&models.ContentItem{}).Count(&count)

	var bytes int64
	q.Model(&models.ContentItem{}).Select("COALESCE(SUM(file_size_bytes),0)").Scan(&bytes)

	resp := sweepPreviewResponse{
		Enabled:         policy.Enabled,
		CandidatesCount: count,
		BytesToFree:     bytes,
		ArchiveAction:   policy.ArchiveAction,
	}

	// How big is the protected set? Useful diagnostic — "we'd purge more if you
	// dropped your protected count."
	if policy.ProtectTopNByViews > 0 {
		windowCutoff := time.Now().UTC().AddDate(0, 0, -policy.ProtectTopNWindowDays)
		protectedSub := db.Model(&models.ContentItem{}).
			Select("id, file_size_bytes").
			Where("tenant_id = ?", principal.TenantID).
			Where("created_at > ?", windowCutoff).
			Order("view_count DESC, created_at DESC").
			Limit(policy.ProtectTopNByViews)

		var protectedCount int64
		var protectedBytes int64
		db.Table("(?) as p", protectedSub).Count(&protectedCount)
		db.Table("(?) as p", protectedSub).Select("COALESCE(SUM(file_size_bytes),0)").Scan(&protectedBytes)
		resp.ProtectedCount = protectedCount
		resp.ProtectedBytes = protectedBytes
	}

	if policy.Enabled {
		var next time.Time
		if policy.LastSweepAt != nil {
			next = policy.LastSweepAt.Add(time.Duration(policy.SweepIntervalMinutes) * time.Minute)
		} else {
			next = time.Now().UTC().Add(time.Duration(policy.SweepIntervalMinutes) * time.Minute)
		}
		formatted := next.UTC().Format(time.RFC3339)
		resp.NextRunAt = &formatted
	}

	c.JSON(http.StatusOK, resp)
}

// -----------------------------------------------------------------------------
// Manual sweep + sweep-runs history
// -----------------------------------------------------------------------------

// RunSweepNow handles POST /admin/storage/policy/run-now
func RunSweepNow(c *gin.Context) {
	if _, ok := requireAdminPrincipal(c); !ok {
		return
	}
	if err := callAggregationRunSweep(c.GetHeader("Authorization")); err != nil {
		c.JSON(http.StatusBadGateway, authErrorResponse{Message: "Aggregation rejected sweep: " + err.Error(), Code: "SWEEP_FAILED"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "message": "Sweep enqueued"})
}

type sweepRunsResponse struct {
	Data  []models.StorageSweepRun `json:"data"`
	Total int64                    `json:"total"`
}

// ListSweepRuns handles GET /admin/storage/sweep-runs
func ListSweepRuns(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	limit := atoiDefault(c.Query("limit"), 50)
	if limit > 200 {
		limit = 200
	}

	var total int64
	db.Model(&models.StorageSweepRun{}).Where("tenant_id = ?", principal.TenantID).Count(&total)

	var runs []models.StorageSweepRun
	if err := db.Where("tenant_id = ?", principal.TenantID).
		Order("started_at DESC").
		Limit(limit).
		Find(&runs).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to list sweep runs", Code: "LIST_FAILED"})
		return
	}
	c.JSON(http.StatusOK, sweepRunsResponse{Data: runs, Total: total})
}

// -----------------------------------------------------------------------------
// Reconcile (S3 LIST vs DB)
// -----------------------------------------------------------------------------

type reconcileResponse struct {
	OrphanKeys     []string `json:"orphan_keys"`     // in S3, not in DB
	MissingObjects []string `json:"missing_objects"` // in DB (media_url set), not in S3
	OrphanCount    int      `json:"orphan_count"`
	MissingCount   int      `json:"missing_count"`
}

// ReconcileStorage handles POST /admin/storage/reconcile
func ReconcileStorage(c *gin.Context) {
	if _, ok := requireAdminPrincipal(c); !ok {
		return
	}
	body, status, err := proxyAggregationGet(c.GetHeader("Authorization"), "/admin/storage/reconcile")
	if err != nil {
		c.JSON(http.StatusBadGateway, authErrorResponse{Message: err.Error(), Code: "RECONCILE_FAILED"})
		return
	}
	if status >= 300 {
		c.Data(status, "application/json", body)
		return
	}
	c.Data(http.StatusOK, "application/json", body)
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

func loadEffectiveStoragePolicy(db *gorm.DB, tenantID string) models.StoragePolicy {
	var p models.StoragePolicy
	if err := db.Where("tenant_id = ?", tenantID).First(&p).Error; err == nil {
		return p
	}
	return loadOrCreateGlobalPolicy(db)
}

func loadOrCreateGlobalPolicy(db *gorm.DB) models.StoragePolicy {
	var p models.StoragePolicy
	if err := db.Where("tenant_id IS NULL").First(&p).Error; err == nil {
		return p
	}
	p = models.StoragePolicy{
		TenantID:                nil,
		Enabled:                 false,
		MaxStorageBytes:         defaultMaxStorageBytes,
		TargetUtilizationPct:    defaultTargetUtilizationPct,
		MinAgeDays:              defaultMinAgeDays,
		MinViewCountForKeep:     defaultMinViewCountForKeep,
		SweepIntervalMinutes:    defaultSweepIntervalMinutes,
		DeleteFailedImmediately: true,
		PreserveThumbnails:      true,
		ProtectTopNByViews:      50,
		ProtectTopNWindowDays:   30,
		ArchiveAction:           "delete",
		// R2 free-tier defaults — admin can override per tenant.
		ClassAFreeBudget: 1_000_000,
		ClassBFreeBudget: 10_000_000,
		ClassAWarnPct:    80,
		ClassACapPct:     95,
		ClassBWarnPct:    80,
		ClassBCapPct:     95,
	}
	_ = db.Create(&p).Error
	return p
}

func loadOrCreateTenantPolicy(db *gorm.DB, tenantID string) models.StoragePolicy {
	var p models.StoragePolicy
	if err := db.Where("tenant_id = ?", tenantID).First(&p).Error; err == nil {
		return p
	}
	base := loadOrCreateGlobalPolicy(db)
	p = models.StoragePolicy{
		TenantID:                &tenantID,
		Enabled:                 base.Enabled,
		MaxStorageBytes:         base.MaxStorageBytes,
		TargetUtilizationPct:    base.TargetUtilizationPct,
		MinAgeDays:              base.MinAgeDays,
		MinViewCountForKeep:     base.MinViewCountForKeep,
		SweepIntervalMinutes:    base.SweepIntervalMinutes,
		DeleteFailedImmediately: base.DeleteFailedImmediately,
		PreserveThumbnails:      base.PreserveThumbnails,
		ProtectTopNByViews:      base.ProtectTopNByViews,
		ProtectTopNWindowDays:   base.ProtectTopNWindowDays,
		ArchiveAction:           base.ArchiveAction,
		ClassAFreeBudget:        base.ClassAFreeBudget,
		ClassBFreeBudget:        base.ClassBFreeBudget,
		ClassAWarnPct:           base.ClassAWarnPct,
		ClassACapPct:            base.ClassACapPct,
		ClassBWarnPct:           base.ClassBWarnPct,
		ClassBCapPct:            base.ClassBCapPct,
	}
	_ = db.Create(&p).Error
	return p
}

func atoiDefault(raw string, def int) int {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return def
	}
	return n
}

func clampInt(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// -----------------------------------------------------------------------------
// Aggregation HTTP client
// -----------------------------------------------------------------------------

type aggDeleteRequest struct {
	ContentIDs []string `json:"content_ids,omitempty"`
	Keys       []string `json:"keys,omitempty"`
	Artifacts  []string `json:"artifacts,omitempty"`
}

type aggDeleteResponse struct {
	DeletedCount int      `json:"deleted_count"`
	FreedBytes   int64    `json:"freed_bytes"`
	Errors       []string `json:"errors,omitempty"`
}

func aggregationBaseURL() (string, error) {
	u := strings.TrimRight(strings.TrimSpace(os.Getenv("AGGREGATION_BASE_URL")), "/")
	if u == "" {
		return "", errors.New("AGGREGATION_BASE_URL is not configured")
	}
	return u, nil
}

func callAggregationStorageStats(authHeader string) (aggStatsResponse, error) {
	body, status, err := proxyAggregationGet(authHeader, "/admin/storage/stats")
	if err != nil {
		return aggStatsResponse{}, err
	}
	if status >= 300 {
		return aggStatsResponse{}, fmt.Errorf("aggregation stats responded with %d", status)
	}
	var out aggStatsResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return aggStatsResponse{}, err
	}
	return out, nil
}

func callAggregationDeleteObjects(authHeader string, payload aggDeleteRequest) (aggDeleteResponse, error) {
	body, status, err := proxyAggregationPost(authHeader, "/admin/storage/delete-objects", payload)
	if err != nil {
		return aggDeleteResponse{}, err
	}
	if status >= 300 {
		return aggDeleteResponse{}, fmt.Errorf("aggregation delete responded with %d: %s", status, string(body))
	}
	var out aggDeleteResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return aggDeleteResponse{}, err
	}
	return out, nil
}

func callAggregationRunSweep(authHeader string) error {
	body, status, err := proxyAggregationPost(authHeader, "/admin/storage/sweep", map[string]any{"trigger": "manual"})
	if err != nil {
		return err
	}
	if status >= 300 {
		return fmt.Errorf("aggregation sweep responded with %d: %s", status, string(body))
	}
	return nil
}

func callAggregationPolicyChanged(authHeader string) error {
	_, status, err := proxyAggregationPost(authHeader, "/admin/storage/policy-changed", map[string]any{})
	if err != nil {
		return err
	}
	if status >= 300 {
		return fmt.Errorf("aggregation policy-changed responded with %d", status)
	}
	return nil
}

func callAggregationRetryPending(authHeader string, limit int) ([]byte, error) {
	body, status, err := proxyAggregationPost(authHeader, "/admin/retry-pending", map[string]any{"limit": limit})
	if err != nil {
		return nil, err
	}
	if status >= 300 {
		return nil, fmt.Errorf("aggregation retry-pending responded with %d: %s", status, string(body))
	}
	return body, nil
}

func proxyAggregationPost(authHeader, path string, payload any) ([]byte, int, error) {
	base, err := aggregationBaseURL()
	if err != nil {
		return nil, 0, err
	}
	buf, err := json.Marshal(payload)
	if err != nil {
		return nil, 0, err
	}
	req, err := http.NewRequest(http.MethodPost, base+path, bytes.NewReader(buf))
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	if strings.TrimSpace(authHeader) != "" {
		req.Header.Set("Authorization", authHeader)
	}
	if t := strings.TrimSpace(os.Getenv("AGGREGATION_SERVICE_TOKEN")); t != "" {
		req.Header.Set("X-Service-Token", t)
	}
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, err
	}
	return respBody, resp.StatusCode, nil
}

func proxyAggregationGet(authHeader, path string) ([]byte, int, error) {
	base, err := aggregationBaseURL()
	if err != nil {
		return nil, 0, err
	}
	req, err := http.NewRequest(http.MethodGet, base+path, nil)
	if err != nil {
		return nil, 0, err
	}
	if strings.TrimSpace(authHeader) != "" {
		req.Header.Set("Authorization", authHeader)
	}
	if t := strings.TrimSpace(os.Getenv("AGGREGATION_SERVICE_TOKEN")); t != "" {
		req.Header.Set("X-Service-Token", t)
	}
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, err
	}
	return respBody, resp.StatusCode, nil
}
