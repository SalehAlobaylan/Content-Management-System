package controllers

import (
	"bytes"
	"content-management-system/src/intelligence"
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
	defaultMaxStorageBytes            int64 = 5 * 1024 * 1024 * 1024 // 5 GB
	defaultTargetUtilizationPct             = 80
	defaultMinAgeDays                       = 14
	defaultMinViewCountForKeep              = 5
	defaultSweepIntervalMinutes             = 60
	purgeIDsLimit                           = 500
	storageRoleHotFeedUnit                  = "hot_feed_unit"
	storageRoleNormalFeedUnit               = "normal_feed_unit"
	storageRoleDormantFeedUnit              = "dormant_feed_unit"
	storageRoleAtomizedParentSource         = "atomized_parent_source"
	storageRoleUnsuitableMedia              = "unsuitable_media"
	storageRoleFailedOrOrphanArtifact       = "failed_or_orphan_artifact"
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
	ColdEnabled      bool                   `json:"cold_enabled"`
	Cold             *aggColdStats          `json:"cold,omitempty"`
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
	ColdEnabled    bool             `json:"cold_enabled"`
	Cold           *aggColdStats    `json:"cold,omitempty"`
}

type aggColdStats struct {
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
		resp.ColdEnabled = live.ColdEnabled
		resp.Cold = live.Cold
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
	ID                  string  `json:"id"`
	Type                string  `json:"type"`
	Status              string  `json:"status"`
	Title               string  `json:"title"`
	SourceName          *string `json:"source_name,omitempty"`
	ViewCount           int     `json:"view_count"`
	FileSizeBytes       int64   `json:"file_size_bytes"`
	CreatedAt           string  `json:"created_at"`
	PublishedAt         *string `json:"published_at,omitempty"`
	MediaURL            *string `json:"media_url,omitempty"`
	ThumbnailURL        *string `json:"thumbnail_url,omitempty"`
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

	// Ranking/Intelligence value surface (stage 4) — why this candidate sits
	// where it does in the worst-first ordering. Nil = never scored.
	Value            *float64 `json:"value,omitempty"`
	ValueConfidence  *float64 `json:"value_confidence,omitempty"`
	ExplorationState *string  `json:"exploration_state,omitempty"`
	ValueReasons     []string `json:"value_reasons,omitempty"`
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
		Order(storageValueOrderExpr).
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
	attachCandidateValues(db, items, out)
	c.JSON(http.StatusOK, storageCandidatesResponse{
		Data:       out,
		Total:      total,
		Limit:      limit,
		TotalBytes: totalBytes,
	})
}

// attachCandidateValues decorates a candidate page with the persisted
// intelligence scores (value, confidence, exploration state, reasons) so the
// Console — and the stage-5 ledger — can explain every pick. One bounded query
// per page.
func attachCandidateValues(db *gorm.DB, items []models.ContentItem, out []storageCandidate) {
	if len(items) == 0 {
		return
	}
	ids := make([]uuid.UUID, len(items))
	for i, it := range items {
		ids[i] = it.PublicID
	}
	var rows []models.MediaIntelligenceScore
	db.Where("content_item_id IN ?", ids).Find(&rows)
	byID := make(map[string]models.MediaIntelligenceScore, len(rows))
	for _, r := range rows {
		byID[r.ContentItemID.String()] = r
	}
	for i := range out {
		row, ok := byID[out[i].ID]
		if !ok {
			continue
		}
		value := row.Value
		confidence := row.Confidence
		state := row.ExplorationState
		out[i].Value = &value
		out[i].ValueConfidence = &confidence
		out[i].ExplorationState = &state
		if len(row.Reasons) > 0 {
			var reasons []string
			if err := json.Unmarshal(row.Reasons, &reasons); err == nil {
				out[i].ValueReasons = reasons
			}
		}
	}
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
	includeAtomizedParents  bool
	archiveAction           string
}

// storageValueOrderExpr orders candidates worst-value-first from the persisted
// intelligence score (stage 4 — grilling Q3). Unscored rows order at the
// neutral prior rather than zero so "never scored" is never mistaken for
// "worthless"; view_count/created_at stay as deterministic tie-breakers (and
// as the whole ordering for non-feed liabilities that have no score).
const storageValueOrderExpr = `COALESCE((SELECT s.value FROM media_intelligence_scores s WHERE s.content_item_id = content_items.public_id), 0.4) ASC, view_count ASC, created_at ASC`

func buildCandidateQuery(db *gorm.DB, f candidateFilter) *gorm.DB {
	cutoff := time.Now().UTC().AddDate(0, 0, -f.minAgeDays)

	q := db.Model(&models.ContentItem{}).
		Where("tenant_id = ?", f.tenantID).
		Where("(media_url IS NOT NULL OR thumbnail_url IS NOT NULL)").
		Where("status != ?", models.ContentStatusArchived)

	// Exploration guard (W1): a READY feed unit that is still exploring — or
	// was never scored at all — is "unseen", not "unpopular". Destroying its
	// exposure (delete, cold-move) would destroy the exploration valve, so it
	// is not a candidate for those actions. Re-encode is exempt: it shrinks
	// bytes while the item stays READY and servable, so exploration continues.
	// Failed/hidden artifacts are storage liabilities regardless and stay in.
	if f.archiveAction != "re_encode" {
		q = q.Where(`NOT (is_feed_unit = TRUE AND status = ? AND NOT EXISTS (
			SELECT 1 FROM media_intelligence_scores s
			WHERE s.content_item_id = content_items.public_id AND s.exploration_state = 'established'
		))`, models.ContentStatusReady)
	}

	if f.archiveAction == "re_encode" {
		// Re-encode only saves media bytes when a primary media artifact exists.
		// Failed/missing rows with only thumbnails are storage-accounting
		// liabilities, not quality-worker candidates.
		q = q.Where("media_url IS NOT NULL").
			Where("status != ?", models.ContentStatusFailed).
			Where("(storage_state IS NULL OR storage_state NOT IN ?)", []string{
				models.StorageStateMissing,
				models.StorageStateRecoverableDeleted,
				models.StorageStateUnrecoverable,
			})
	}

	if f.excludeColdTier {
		// Cold-tier items already paid the savings dividend; don't re-process them.
		q = q.Where("(storage_tier IS NULL OR storage_tier != 'cold')")
	}

	// Value-aware eligibility (stage 4): past the age floor (a safety invariant
	// that holds unconditionally), an item is a candidate when the legacy views
	// proxy marks it dormant OR the intelligence model is confident it is
	// low-value. Low-confidence value never widens eligibility — it falls back
	// to the views rule alone.
	const confidentlyLowValue = `EXISTS (
		SELECT 1 FROM media_intelligence_scores s
		WHERE s.content_item_id = content_items.public_id
			AND s.confidence >= ? AND s.value <= ?
	)`
	var eligibility *gorm.DB
	switch {
	case f.status != "":
		eligibility = db.Where("status = ?", f.status)
	case f.deleteFailedImmediately:
		eligibility = db.Where("status = ?", models.ContentStatusFailed).
			Or(db.Where("created_at < ? AND (view_count <= ? OR "+confidentlyLowValue+")",
				cutoff, f.maxViewCount, intelligence.StorageEligibilityMinConfidence, intelligence.StorageEligibilityValueFloor))
	default:
		eligibility = db.Where("created_at < ? AND (view_count <= ? OR "+confidentlyLowValue+")",
			cutoff, f.maxViewCount, intelligence.StorageEligibilityMinConfidence, intelligence.StorageEligibilityValueFloor)
	}
	if f.includeAtomizedParents {
		atomizedParent := db.Where("parent_content_item_id IS NULL").
			Where("type IN ?", []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}).
			Where("duration_sec IS NOT NULL AND duration_sec > ?", forYouHardMaxDurationSec).
			Where("EXISTS (SELECT 1 FROM content_items child WHERE child.parent_content_item_id = content_items.public_id AND child.status = ? AND child.is_feed_unit = TRUE)", models.ContentStatusReady)
		eligibility = db.Where(eligibility).Or(atomizedParent)
	}
	lowSuitability := db.Where("media_suitability IN ?", []string{
		models.MediaSuitabilityVisualDependent,
		models.MediaSuitabilityUnsuitable,
	})
	eligibility = db.Where(eligibility).Or(lowSuitability)
	q = q.Where(eligibility)

	if f.archiveAction == "delete" {
		hasRecoveryPointer := db.Where("original_url IS NOT NULL").Or("source_feed_url IS NOT NULL").Or("source_episode_id IS NOT NULL").Or("idempotency_key IS NOT NULL")
		atomizedParentDelete := db.Where("parent_content_item_id IS NULL").
			Where("is_feed_unit = FALSE").
			Where("duration_sec IS NOT NULL AND duration_sec > ?", forYouHardMaxDurationSec).
			Where("EXISTS (SELECT 1 FROM content_items child WHERE child.parent_content_item_id = content_items.public_id AND child.status = ? AND child.is_feed_unit = TRUE AND child.feed_visibility = 'visible' AND child.media_url IS NOT NULL)", models.ContentStatusReady)
		failedNeverReady := db.Where("status = ?", models.ContentStatusFailed)
		unsuitableHidden := db.Where("is_feed_unit = FALSE").
			Where("feed_visibility != 'visible'").
			Where("media_suitability IN ?", []string{models.MediaSuitabilityVisualDependent, models.MediaSuitabilityUnsuitable})
		q = q.Where(hasRecoveryPointer).
			Where("NOT (is_feed_unit = TRUE AND status = ?)", models.ContentStatusReady).
			Where("media_suitability IS NULL OR media_suitability != ?", models.MediaSuitabilityUnknown).
			Where(db.Where(atomizedParentDelete).Or(failedNeverReady).Or(unsuitableHidden))
	}

	if f.sourceName != "" {
		q = q.Where("source_name = ?", f.sourceName)
	}

	// Hot-content protection: hybrid deterministic model. Feed performance only
	// changes storage protection; it does not mutate feed visibility or ranking.
	if f.protectTopNByViews > 0 && f.protectTopNWindowDays >= 0 {
		protectedIDs := protectedStorageItemsQuery(db, f.tenantID, models.StoragePolicy{
			ProtectTopNByViews:    f.protectTopNByViews,
			ProtectTopNWindowDays: f.protectTopNWindowDays,
		}).Select("id")
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
		includeAtomizedParents:  true,
		archiveAction:           p.ArchiveAction,
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
	var parentID *string
	if it.ParentContentItemID != nil {
		s := it.ParentContentItemID.String()
		parentID = &s
	}
	role, reason := storageRoleForContentItem(it)
	return storageCandidate{
		ID:                  it.PublicID.String(),
		Type:                string(it.Type),
		Status:              string(it.Status),
		Title:               title,
		SourceName:          it.SourceName,
		ViewCount:           it.ViewCount,
		FileSizeBytes:       it.FileSizeBytes,
		CreatedAt:           it.CreatedAt.UTC().Format(time.RFC3339),
		PublishedAt:         pub,
		MediaURL:            it.MediaURL,
		ThumbnailURL:        it.ThumbnailURL,
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
	}
}

func storageRoleForContentItem(it models.ContentItem) (string, string) {
	if it.Status == models.ContentStatusFailed {
		return storageRoleFailedOrOrphanArtifact, "failed ingest artifact"
	}
	if it.MediaSuitability == models.MediaSuitabilityVisualDependent || it.MediaSuitability == models.MediaSuitabilityUnsuitable {
		return storageRoleUnsuitableMedia, "unsuitable media has low storage protection"
	}
	if it.ParentContentItemID == nil && !it.IsFeedUnit && it.DurationSec != nil && *it.DurationSec > forYouHardMaxDurationSec {
		return storageRoleAtomizedParentSource, "parent source after atomization"
	}
	if it.IsFeedUnit && it.FeedVisibility == "visible" && it.Status == models.ContentStatusReady {
		if it.ViewCount <= defaultMinViewCountForKeep {
			return storageRoleDormantFeedUnit, "old or low-engagement feed unit"
		}
		return storageRoleNormalFeedUnit, "visible feed unit"
	}
	return storageRoleDormantFeedUnit, "storage candidate"
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
			"file_size_bytes":         0,
			"media_url":               nil,
			"storage_state":           models.StorageStateRecoverableDeleted,
			"storage_state_reason":    "manual_purge",
			"storage_recovery_status": models.StorageRecoveryRecoverable,
			"storage_deleted_at":      &now,
		}
		if !preserveThumbs {
			updates["thumbnail_url"] = nil
		}
		if err := db.Model(&models.ContentItem{}).Where("id = ?", it.ID).Updates(updates).Error; err != nil {
			// Don't abort the whole batch; just log and continue
			fmt.Println("storage purge: failed to update content item", it.PublicID, err)
		} else {
			_, _ = createStorageArtifactEvent(db, storageArtifactEventInput{
				TenantID:              it.TenantID,
				ContentItemID:         it.PublicID,
				ParentContentItemID:   it.ParentContentItemID,
				EventType:             models.StorageArtifactEventRecoverableDeleted,
				Status:                models.StorageArtifactEventStatusSuccess,
				Reason:                "Manual storage purge",
				Trigger:               triggerFromPurge(req),
				Source:                "cms_admin",
				OldMediaURL:           stringValue(it.MediaURL),
				OldSizeBytes:          it.FileSizeBytes,
				DeletedBytes:          it.FileSizeBytes,
				FreedBytes:            it.FileSizeBytes,
				RecoveryPayload:       storageRecoveryPayloadForItem(it),
				StorageState:          models.StorageStateRecoverableDeleted,
				StorageStateReason:    "manual_purge",
				StorageRecoveryStatus: models.StorageRecoveryRecoverable,
			})
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

func triggerFromPurge(req storagePurgeRequest) string {
	if req.Filters != nil && req.Filters.Trigger != nil && strings.TrimSpace(*req.Filters.Trigger) != "" {
		return *req.Filters.Trigger
	}
	return "manual"
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
		archiveAction:           policy.ArchiveAction,
	}).
		Order(storageValueOrderExpr)

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
	item.StorageState = models.StorageStateRecoveryPending
	reason := "restore_requested"
	item.StorageStateReason = &reason
	item.StorageRecoveryStatus = models.StorageRecoveryPending
	if err := db.Save(&item).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to flip status", Code: "UPDATE_FAILED"})
		return
	}
	_, _ = createStorageArtifactEvent(db, storageArtifactEventInput{
		TenantID:              item.TenantID,
		ContentItemID:         item.PublicID,
		ParentContentItemID:   item.ParentContentItemID,
		EventType:             models.StorageArtifactEventRestoreRequested,
		Status:                models.StorageArtifactEventStatusSuccess,
		Reason:                "Restore requested from storage cockpit",
		Trigger:               "manual",
		Source:                "cms_admin",
		RecoveryPayload:       storageRecoveryPayloadForItem(item),
		StorageState:          models.StorageStateRecoveryPending,
		StorageStateReason:    "restore_requested",
		StorageRecoveryStatus: models.StorageRecoveryPending,
	})

	// Ask Aggregation to enqueue a media job for this exact id.
	if _, err := callAggregationRetryPending(c.GetHeader("Authorization"), 1, item.PublicID.String()); err != nil {
		item.StorageRecoveryStatus = models.StorageRecoveryFailed
		failReason := "reingest_queue_failed"
		item.StorageStateReason = &failReason
		_ = db.Save(&item).Error
		_, _ = createStorageArtifactEvent(db, storageArtifactEventInput{
			TenantID:              item.TenantID,
			ContentItemID:         item.PublicID,
			ParentContentItemID:   item.ParentContentItemID,
			EventType:             models.StorageArtifactEventRecoveryFailed,
			Status:                models.StorageArtifactEventStatusError,
			Reason:                "Failed to queue best-effort re-ingestion",
			Trigger:               "manual",
			Source:                "cms_admin",
			Error:                 err.Error(),
			RecoveryPayload:       storageRecoveryPayloadForItem(item),
			StorageState:          models.StorageStateRecoveryPending,
			StorageStateReason:    "reingest_queue_failed",
			StorageRecoveryStatus: models.StorageRecoveryFailed,
		})
		c.JSON(http.StatusBadGateway, authErrorResponse{Message: "Aggregation retry failed: " + err.Error(), Code: "RETRY_FAILED"})
		return
	}
	_, _ = createStorageArtifactEvent(db, storageArtifactEventInput{
		TenantID:              item.TenantID,
		ContentItemID:         item.PublicID,
		ParentContentItemID:   item.ParentContentItemID,
		EventType:             models.StorageArtifactEventReingestQueued,
		Status:                models.StorageArtifactEventStatusSuccess,
		Reason:                "Best-effort re-ingestion queued",
		Trigger:               "manual",
		Source:                "cms_admin",
		RecoveryPayload:       storageRecoveryPayloadForItem(item),
		StorageState:          models.StorageStateRecoveryPending,
		StorageStateReason:    "reingest_queued",
		StorageRecoveryStatus: models.StorageRecoveryPending,
	})

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
	Preset                  *string `json:"preset"`
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
	if req.Preset != nil {
		preset := strings.ToLower(strings.TrimSpace(*req.Preset))
		if preset == "" {
			preset = "balanced"
		}
		switch preset {
		case "balanced", "conservative", "storage_saver", "critical_pressure":
			p.Preset = preset
		default:
			c.JSON(http.StatusBadRequest, authErrorResponse{
				Message: "preset must be 'balanced', 'conservative', 'storage_saver', or 'critical_pressure'",
				Code:    "INVALID_PRESET",
			})
			return
		}
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
		protectedSub := protectedStorageItemsQuery(db, principal.TenantID, policy).
			Select("id, file_size_bytes")

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
	OrphanKeys          []string `json:"orphan_keys"`     // in S3, not in DB
	MissingObjects      []string `json:"missing_objects"` // in DB (media_url set), not in S3
	OrphanCount         int      `json:"orphan_count"`
	MissingCount        int      `json:"missing_count"`
	ScannedObjectCount  int      `json:"scanned_object_count,omitempty"`
	ScannedCMSItemCount int      `json:"scanned_cms_item_count,omitempty"`
	Partial             bool     `json:"partial,omitempty"`
	TruncatedReason     string   `json:"truncated_reason,omitempty"`
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
	result := db.Where("tenant_id = ?", tenantID).Limit(1).Find(&p)
	if result.Error == nil && result.RowsAffected > 0 {
		return p
	}
	return loadOrCreateGlobalPolicy(db)
}

func loadOrCreateGlobalPolicy(db *gorm.DB) models.StoragePolicy {
	var p models.StoragePolicy
	result := db.Where("tenant_id IS NULL").Limit(1).Find(&p)
	if result.Error == nil && result.RowsAffected > 0 {
		return p
	}
	p = models.StoragePolicy{
		TenantID:                nil,
		Enabled:                 false,
		Preset:                  "balanced",
		MaxStorageBytes:         defaultMaxStorageBytes,
		TargetUtilizationPct:    defaultTargetUtilizationPct,
		MinAgeDays:              defaultMinAgeDays,
		MinViewCountForKeep:     defaultMinViewCountForKeep,
		SweepIntervalMinutes:    defaultSweepIntervalMinutes,
		DeleteFailedImmediately: true,
		PreserveThumbnails:      true,
		ProtectTopNByViews:      50,
		ProtectTopNWindowDays:   30,
		ArchiveAction:           "re_encode",
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
	result := db.Where("tenant_id = ?", tenantID).Limit(1).Find(&p)
	if result.Error == nil && result.RowsAffected > 0 {
		return p
	}
	base := loadOrCreateGlobalPolicy(db)
	p = models.StoragePolicy{
		TenantID:                &tenantID,
		Enabled:                 base.Enabled,
		Preset:                  base.Preset,
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

func callAggregationRetryPending(authHeader string, limit int, ids ...string) ([]byte, error) {
	payload := map[string]any{"limit": limit}
	if len(ids) > 0 {
		payload["ids"] = ids
	}
	body, status, err := proxyAggregationPost(authHeader, "/admin/retry-pending", payload)
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
