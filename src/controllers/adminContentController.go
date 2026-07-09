package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type adminContentListResponse struct {
	Data       []adminContentItemResponse `json:"data"`
	Total      int64                      `json:"total"`
	Page       int                        `json:"page"`
	Limit      int                        `json:"limit"`
	TotalPages int                        `json:"total_pages"`
}

type mediaSizeAggregateResponse struct {
	Count int64 `json:"count"`
	Bytes int64 `json:"bytes"`
}

type mediaSizeLargestItemResponse struct {
	ID            string  `json:"id"`
	Type          string  `json:"type"`
	Status        string  `json:"status"`
	Title         string  `json:"title"`
	SourceName    *string `json:"source_name,omitempty"`
	FileSizeBytes int64   `json:"file_size_bytes"`
	StorageTier   *string `json:"storage_tier,omitempty"`
	MediaURL      *string `json:"media_url,omitempty"`
	ThumbnailURL  *string `json:"thumbnail_url,omitempty"`
	PublishedAt   *string `json:"published_at,omitempty"`
	UpdatedAt     string  `json:"updated_at"`
}

type mediaSizeStatsResponse struct {
	TotalCount     int64                                 `json:"total_count"`
	TrackedCount   int64                                 `json:"tracked_count"`
	UntrackedCount int64                                 `json:"untracked_count"`
	TotalBytes     int64                                 `json:"total_bytes"`
	AvgBytes       int64                                 `json:"avg_bytes"`
	MaxBytes       int64                                 `json:"max_bytes"`
	LargestItems   []mediaSizeLargestItemResponse        `json:"largest_items"`
	ByType         map[string]mediaSizeAggregateResponse `json:"by_type"`
	BySource       map[string]mediaSizeAggregateResponse `json:"by_source"`
	ByStatus       map[string]mediaSizeAggregateResponse `json:"by_status"`
	SizeBuckets    map[string]mediaSizeAggregateResponse `json:"size_buckets"`
}

type contentDailyPoint struct {
	Day    string `json:"day"`
	Count  int64  `json:"count"`
	Failed int64  `json:"failed"`
}

type contentSourceStat struct {
	SourceName string `json:"source_name"`
	Count      int64  `json:"count"`
	Ready      int64  `json:"ready"`
	Failed     int64  `json:"failed"`
}

type contentEngagement struct {
	Likes  int64 `json:"likes"`
	Views  int64 `json:"views"`
	Shares int64 `json:"shares"`
}

type contentFreshness struct {
	// OldestUnprocessed is the created_at of the oldest PENDING/PROCESSING item
	// (ISO string), or nil when the pipeline is empty.
	OldestUnprocessed   *string `json:"oldest_unprocessed"`
	StuckCount          int64   `json:"stuck_count"`
	StuckThresholdHours int     `json:"stuck_threshold_hours"`
}

type contentFailureReason struct {
	Reason string `json:"reason"`
	Count  int64  `json:"count"`
}

type contentStatsResponse struct {
	Total          int64                       `json:"total"`
	ByStatus       map[string]int64            `json:"by_status"`
	ByType         map[string]int64            `json:"by_type"`
	ByTypeStatus   map[string]map[string]int64 `json:"by_type_status"`
	ByCaptionState map[string]int64            `json:"by_caption_state"`
	Daily          []contentDailyPoint         `json:"daily"`
	TopSources     []contentSourceStat         `json:"top_sources"`
	FailureReasons []contentFailureReason      `json:"failure_reasons"`
	Engagement     contentEngagement           `json:"engagement"`
	Freshness      contentFreshness            `json:"freshness"`
	RangeDays      int                         `json:"range_days"`
}

// stuckThresholdHours flags PENDING/PROCESSING items older than this as stuck.
// Code default (not env) per Config Discipline — promote to an admin-config row
// if operators need to tune it.
const stuckThresholdHours = 24

type adminContentItemResponse struct {
	ID                         string                 `json:"id"`
	Type                       string                 `json:"type"`
	Status                     string                 `json:"status"`
	Title                      string                 `json:"title"`
	BodyText                   *string                `json:"body_text,omitempty"`
	Excerpt                    *string                `json:"excerpt,omitempty"`
	Author                     *string                `json:"author,omitempty"`
	SourceID                   *string                `json:"source_id,omitempty"`
	SourceName                 *string                `json:"source_name,omitempty"`
	MediaURL                   *string                `json:"media_url,omitempty"`
	ThumbnailURL               *string                `json:"thumbnail_url,omitempty"`
	OriginalURL                *string                `json:"original_url,omitempty"`
	DurationSec                *int                   `json:"duration_sec,omitempty"`
	FileSizeBytes              int64                  `json:"file_size_bytes"`
	StorageTier                *string                `json:"storage_tier,omitempty"`
	MediaSuitability           string                 `json:"media_suitability"`
	MediaSuitabilityConfidence *float64               `json:"media_suitability_confidence,omitempty"`
	MediaSuitabilityReasons    []string               `json:"media_suitability_reasons,omitempty"`
	TopicTags                  []string               `json:"topic_tags,omitempty"`
	PublishedAt                *string                `json:"published_at,omitempty"`
	CreatedAt                  string                 `json:"created_at"`
	UpdatedAt                  string                 `json:"updated_at"`
	LikeCount                  int                    `json:"like_count"`
	ViewCount                  int                    `json:"view_count"`
	ShareCount                 int                    `json:"share_count"`
	Metadata                   map[string]interface{} `json:"metadata,omitempty"`
	// Caption-first state (Media tab badges + STT action gating).
	CaptionState           *string                    `json:"caption_state,omitempty"`
	TranscriptSource       *string                    `json:"transcript_source,omitempty"`
	HasTranscript          bool                       `json:"has_transcript"`
	TranscriptApprovedAt   *string                    `json:"transcript_approved_at,omitempty"`
	TranscriptApprovedBy   *string                    `json:"transcript_approved_by,omitempty"`
	LatestTranscriptionJob *transcriptionJobResponse  `json:"latest_transcription_job,omitempty"`
	TranscriptQuality      *transcriptQualityResponse `json:"transcript_quality,omitempty"`
}

type updateContentStatusRequest struct {
	Status string `json:"status"`
}

type updateContentSuitabilityRequest struct {
	MediaSuitability           string   `json:"media_suitability"`
	MediaSuitabilityConfidence *float64 `json:"media_suitability_confidence"`
	MediaSuitabilityReasons    []string `json:"media_suitability_reasons"`
}

var contentAdminQueryConfig = utils.QueryConfig{
	DefaultLimit: 20,
	MaxLimit:     100,
	DefaultSort: []utils.SortParam{{
		Field:     "published_at",
		Direction: "desc",
	}},
	SortableFields: map[string]string{
		"created_at":      "content_items.created_at",
		"updated_at":      "content_items.updated_at",
		"published_at":    "content_items.published_at",
		"title":           "content_items.title",
		"type":            "content_items.type",
		"status":          "content_items.status",
		"duration_sec":    "content_items.duration_sec",
		"file_size_bytes": "content_items.file_size_bytes",
		"source_name":     "content_items.source_name",
		"like_count":      "content_items.like_count",
		"view_count":      "content_items.view_count",
		"share_count":     "content_items.share_count",
	},
	FilterableFields: map[string]string{
		"status":        "content_items.status",
		"type":          "content_items.type",
		"caption_state": "content_items.caption_state",
		"source_id":     "content_items.source_feed_url",
		"source_name":   "content_items.source_name",
		"created_at":    "content_items.created_at",
		"published_at":  "content_items.published_at",
	},
	SearchableFields: map[string]string{
		"title":   "content_items.title",
		"excerpt": "content_items.excerpt",
		"author":  "content_items.author",
	},
	DefaultSearchFields: []string{"title", "excerpt", "author"},
	FieldDefaultOperators: map[string]string{
		"title":   "contains",
		"excerpt": "contains",
		"author":  "contains",
	},
}

// ListContentItems handles GET /admin/content
func ListContentItems(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}

	db := c.MustGet("db").(*gorm.DB)

	params, err := utils.ParseQueryParams(c, contentAdminQueryConfig)
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: err.Error(),
			Code:    "INVALID_QUERY",
		})
		return
	}

	query := db.Model(&models.ContentItem{}).Where("tenant_id = ?", principal.TenantID)
	query = utils.ApplyQuery(query, params, contentAdminQueryConfig)
	query, err = applyAdminContentSpecialFilters(c, query)
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: err.Error(),
			Code:    "INVALID_QUERY",
		})
		return
	}

	var items []models.ContentItem
	meta, err := utils.FetchWithPagination(query, params, &items)
	if err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{
			Message: "Failed to fetch content",
			Code:    "FETCH_FAILED",
		})
		return
	}

	data := make([]adminContentItemResponse, 0, len(items))
	for _, item := range items {
		data = append(data, mapAdminContentItemResponse(item))
	}
	// Batched (3 queries total) — the per-row variant is 3 queries per item.
	populateAdminContentTranscriptionBatch(db, items, data)

	c.JSON(http.StatusOK, adminContentListResponse{
		Data:       data,
		Total:      meta.Total,
		Page:       meta.Page,
		Limit:      meta.Limit,
		TotalPages: meta.TotalPages,
	})
}

// GetMediaSizeStats handles GET /admin/content/media-size-stats
func GetMediaSizeStats(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}

	db := c.MustGet("db").(*gorm.DB)
	params, err := utils.ParseQueryParams(c, contentAdminQueryConfig)
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: err.Error(),
			Code:    "INVALID_QUERY",
		})
		return
	}
	params.Sort = nil

	query := db.Model(&models.ContentItem{}).Where("tenant_id = ?", principal.TenantID)
	if strings.TrimSpace(c.Query("type")) == "" {
		query = query.Where("content_items.type IN ?", []models.ContentType{
			models.ContentTypeVideo,
			models.ContentTypePodcast,
		})
	}
	query = utils.ApplyQuery(query, params, contentAdminQueryConfig)
	query, err = applyAdminContentSpecialFilters(c, query)
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: err.Error(),
			Code:    "INVALID_QUERY",
		})
		return
	}

	var totals struct {
		TotalCount     int64
		TrackedCount   int64
		UntrackedCount int64
		TotalBytes     int64
		AvgBytes       float64
		MaxBytes       int64
	}
	if err := query.Session(&gorm.Session{}).
		Select(`
			COUNT(*) AS total_count,
			COALESCE(SUM(CASE WHEN COALESCE(file_size_bytes, 0) > 0 THEN 1 ELSE 0 END), 0) AS tracked_count,
			COALESCE(SUM(CASE WHEN COALESCE(file_size_bytes, 0) <= 0 THEN 1 ELSE 0 END), 0) AS untracked_count,
			COALESCE(SUM(CASE WHEN file_size_bytes > 0 THEN file_size_bytes ELSE 0 END), 0) AS total_bytes,
			COALESCE(AVG(NULLIF(CASE WHEN file_size_bytes > 0 THEN file_size_bytes ELSE 0 END, 0)), 0) AS avg_bytes,
			COALESCE(MAX(CASE WHEN file_size_bytes > 0 THEN file_size_bytes ELSE 0 END), 0) AS max_bytes`).
		Scan(&totals).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{
			Message: "Failed to aggregate media sizes",
			Code:    "STATS_FAILED",
		})
		return
	}

	resp := mediaSizeStatsResponse{
		TotalCount:     totals.TotalCount,
		TrackedCount:   totals.TrackedCount,
		UntrackedCount: totals.UntrackedCount,
		TotalBytes:     totals.TotalBytes,
		AvgBytes:       int64(math.Round(totals.AvgBytes)),
		MaxBytes:       totals.MaxBytes,
		LargestItems:   []mediaSizeLargestItemResponse{},
		ByType:         map[string]mediaSizeAggregateResponse{},
		BySource:       map[string]mediaSizeAggregateResponse{},
		ByStatus:       map[string]mediaSizeAggregateResponse{},
		SizeBuckets:    map[string]mediaSizeAggregateResponse{},
	}

	if err := fillMediaSizeAggregate(query, "type", resp.ByType); err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{
			Message: "Failed to aggregate media sizes by type",
			Code:    "STATS_FAILED",
		})
		return
	}
	if err := fillMediaSizeAggregate(query, "COALESCE(NULLIF(source_name, ''), 'Unknown source')", resp.BySource); err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{
			Message: "Failed to aggregate media sizes by source",
			Code:    "STATS_FAILED",
		})
		return
	}
	if err := fillMediaSizeAggregate(query, "status", resp.ByStatus); err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{
			Message: "Failed to aggregate media sizes by status",
			Code:    "STATS_FAILED",
		})
		return
	}
	if err := fillMediaSizeBuckets(query, resp.SizeBuckets); err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{
			Message: "Failed to aggregate media size buckets",
			Code:    "STATS_FAILED",
		})
		return
	}

	var largest []models.ContentItem
	if err := query.Session(&gorm.Session{}).
		Where("file_size_bytes > 0").
		Order("file_size_bytes DESC").
		Limit(8).
		Find(&largest).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{
			Message: "Failed to fetch largest media items",
			Code:    "STATS_FAILED",
		})
		return
	}
	for _, item := range largest {
		resp.LargestItems = append(resp.LargestItems, mapMediaSizeLargestItem(item))
	}

	c.JSON(http.StatusOK, resp)
}

// GetContentStats handles GET /admin/content/stats
// Returns filter-scoped analytics aggregates powering the content-monitoring
// dashboard. Honors the same filters as the content list (type, status,
// source_name, created_at, published_at) so the console can scope a single
// endpoint to All / News / Media. The `range_days` query param (default 30,
// cap 90) only windows the daily ingestion series.
func GetContentStats(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}

	db := c.MustGet("db").(*gorm.DB)
	params, err := utils.ParseQueryParams(c, contentAdminQueryConfig)
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: err.Error(),
			Code:    "INVALID_QUERY",
		})
		return
	}
	params.Sort = nil

	rangeDays := 30
	if raw := strings.TrimSpace(c.Query("range_days")); raw != "" {
		if parsed, convErr := strconv.Atoi(raw); convErr == nil && parsed > 0 {
			rangeDays = parsed
		}
	}
	if rangeDays > 90 {
		rangeDays = 90
	}

	query := db.Model(&models.ContentItem{}).Where("tenant_id = ?", principal.TenantID)
	query = utils.ApplyQuery(query, params, contentAdminQueryConfig)
	query, err = applyAdminContentSpecialFilters(c, query)
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: err.Error(),
			Code:    "INVALID_QUERY",
		})
		return
	}

	resp := contentStatsResponse{
		ByStatus:       map[string]int64{"PENDING": 0, "PROCESSING": 0, "READY": 0, "FAILED": 0, "ARCHIVED": 0},
		ByType:         map[string]int64{},
		ByTypeStatus:   map[string]map[string]int64{},
		ByCaptionState: map[string]int64{},
		Daily:          []contentDailyPoint{},
		TopSources:     []contentSourceStat{},
		FailureReasons: []contentFailureReason{},
		Freshness:      contentFreshness{StuckThresholdHours: stuckThresholdHours},
		RangeDays:      rangeDays,
	}
	for _, t := range []models.ContentType{
		models.ContentTypeNews, models.ContentTypeArticle, models.ContentTypeVideo,
		models.ContentTypePodcast, models.ContentTypeTweet, models.ContentTypeComment,
	} {
		resp.ByType[string(t)] = 0
	}

	// Type × status matrix — one query feeds Total, ByType, ByStatus, ByTypeStatus.
	type matrixRow struct {
		Type   string
		Status string
		Count  int64
	}
	var matrix []matrixRow
	if err := query.Session(&gorm.Session{}).
		Select("type, status, COUNT(*) AS count").
		Group("type, status").
		Scan(&matrix).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{
			Message: "Failed to aggregate content counts",
			Code:    "STATS_FAILED",
		})
		return
	}
	for _, row := range matrix {
		resp.Total += row.Count
		resp.ByType[row.Type] += row.Count
		resp.ByStatus[row.Status] += row.Count
		if resp.ByTypeStatus[row.Type] == nil {
			resp.ByTypeStatus[row.Type] = map[string]int64{}
		}
		resp.ByTypeStatus[row.Type][row.Status] += row.Count
	}

	// Daily ingestion velocity over the requested window.
	cutoff := time.Now().AddDate(0, 0, -rangeDays)
	if err := query.Session(&gorm.Session{}).
		Select("TO_CHAR(date_trunc('day', created_at), 'YYYY-MM-DD') AS day, COUNT(*) AS count, COALESCE(SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END), 0) AS failed").
		Where("created_at >= ?", cutoff).
		Group("date_trunc('day', created_at)").
		Order("date_trunc('day', created_at)").
		Scan(&resp.Daily).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{
			Message: "Failed to aggregate ingestion velocity",
			Code:    "STATS_FAILED",
		})
		return
	}

	// Top sources by item count, with ready/failed split.
	if err := query.Session(&gorm.Session{}).
		Select("COALESCE(NULLIF(source_name, ''), 'Unknown source') AS source_name, COUNT(*) AS count, COALESCE(SUM(CASE WHEN status = 'READY' THEN 1 ELSE 0 END), 0) AS ready, COALESCE(SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END), 0) AS failed").
		Group("COALESCE(NULLIF(source_name, ''), 'Unknown source')").
		Order("count DESC").
		Limit(10).
		Scan(&resp.TopSources).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{
			Message: "Failed to aggregate top sources",
			Code:    "STATS_FAILED",
		})
		return
	}

	// Engagement totals.
	if err := query.Session(&gorm.Session{}).
		Select("COALESCE(SUM(like_count), 0) AS likes, COALESCE(SUM(view_count), 0) AS views, COALESCE(SUM(share_count), 0) AS shares").
		Scan(&resp.Engagement).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{
			Message: "Failed to aggregate engagement",
			Code:    "STATS_FAILED",
		})
		return
	}

	// Caption-state coverage (drives the Media transcript-coverage panel).
	type captionRow struct {
		Key   string
		Count int64
	}
	var captionRows []captionRow
	if err := query.Session(&gorm.Session{}).
		Select("COALESCE(NULLIF(caption_state, ''), 'none') AS key, COUNT(*) AS count").
		Group("COALESCE(NULLIF(caption_state, ''), 'none')").
		Scan(&captionRows).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{
			Message: "Failed to aggregate caption coverage",
			Code:    "STATS_FAILED",
		})
		return
	}
	for _, row := range captionRows {
		resp.ByCaptionState[row.Key] = row.Count
	}

	// Top failure reasons (stored in metadata.failure_reason).
	if err := query.Session(&gorm.Session{}).
		Select("COALESCE(NULLIF(metadata->>'failure_reason', ''), 'Unknown') AS reason, COUNT(*) AS count").
		Where("status = ?", models.ContentStatusFailed).
		Group("COALESCE(NULLIF(metadata->>'failure_reason', ''), 'Unknown')").
		Order("count DESC").
		Limit(6).
		Scan(&resp.FailureReasons).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{
			Message: "Failed to aggregate failure reasons",
			Code:    "STATS_FAILED",
		})
		return
	}

	// Pipeline freshness — oldest unprocessed item + stuck count.
	var freshness struct {
		Oldest *time.Time
		Stuck  int64
	}
	stuckBefore := time.Now().Add(-time.Duration(stuckThresholdHours) * time.Hour)
	if err := query.Session(&gorm.Session{}).
		Where("status IN ?", []models.ContentStatus{models.ContentStatusPending, models.ContentStatusProcessing}).
		Select("MIN(created_at) AS oldest, COALESCE(SUM(CASE WHEN created_at < ? THEN 1 ELSE 0 END), 0) AS stuck", stuckBefore).
		Scan(&freshness).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{
			Message: "Failed to aggregate pipeline freshness",
			Code:    "STATS_FAILED",
		})
		return
	}
	resp.Freshness.StuckCount = freshness.Stuck
	if freshness.Oldest != nil {
		iso := freshness.Oldest.UTC().Format(time.RFC3339)
		resp.Freshness.OldestUnprocessed = &iso
	}

	c.JSON(http.StatusOK, resp)
}

// GetAdminContentItem handles GET /admin/content/:id
func GetAdminContentItem(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}

	db := c.MustGet("db").(*gorm.DB)
	publicID := c.Param("id")
	id, err := uuid.Parse(publicID)
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "Invalid content ID",
			Code:    "INVALID_ID",
		})
		return
	}

	var item models.ContentItem
	if err := db.Where("public_id = ? AND tenant_id = ?", id, principal.TenantID).First(&item).Error; err != nil {
		c.JSON(http.StatusNotFound, authErrorResponse{
			Message: "Content not found",
			Code:    "NOT_FOUND",
		})
		return
	}

	resp := mapAdminContentItemResponse(item)
	populateAdminContentTranscription(db, item.PublicID, &resp)
	c.JSON(http.StatusOK, resp)
}

func applyAdminContentSpecialFilters(c *gin.Context, query *gorm.DB) (*gorm.DB, error) {
	// Topic filter — topic_tags is a text[] column not handled by the generic
	// query builder, so apply array membership directly (same pattern as the
	// public RSS feed controller).
	if topic := strings.TrimSpace(c.Query("topic")); topic != "" {
		query = query.Where("? = ANY(topic_tags)", topic)
	}
	// First-class topic filter (the News manager board fetches by story_id).
	// The sentinel "none" selects unclassified articles (story_id IS NULL).
	if topicID := strings.TrimSpace(c.Query("story_id")); topicID != "" {
		if strings.EqualFold(topicID, "none") {
			query = query.Where("story_id IS NULL")
		} else {
			query = query.Where("story_id = ?", topicID)
		}
	}
	if tStatus := strings.TrimSpace(c.Query("transcription_status")); tStatus != "" {
		query = query.Where(`(
			SELECT tj.status FROM transcription_jobs tj
			WHERE tj.content_item_id = content_items.public_id
			ORDER BY tj.created_at DESC
			LIMIT 1
		) = ?`, tStatus)
	}
	if tTrigger := strings.TrimSpace(c.Query("transcription_trigger")); tTrigger != "" {
		query = query.Where(`(
			SELECT tj.trigger_source FROM transcription_jobs tj
			WHERE tj.content_item_id = content_items.public_id
			ORDER BY tj.created_at DESC
			LIMIT 1
		) = ?`, tTrigger)
	}
	if qStatus := strings.TrimSpace(c.Query("quality_status")); qStatus != "" {
		query = query.Where(`EXISTS (
			SELECT 1 FROM transcript_quality tq
			WHERE tq.content_item_id = content_items.public_id AND tq.status = ?
		)`, qStatus)
	}
	if minSize, ok, err := parseOptionalInt64Query(c, "min_size_bytes"); err != nil {
		return query, err
	} else if ok {
		query = query.Where("file_size_bytes >= ?", minSize)
	}
	if maxSize, ok, err := parseOptionalInt64Query(c, "max_size_bytes"); err != nil {
		return query, err
	} else if ok {
		query = query.Where("file_size_bytes <= ?", maxSize)
	}
	switch strings.ToLower(strings.TrimSpace(c.Query("size_tracked"))) {
	case "", "all":
	case "tracked":
		query = query.Where("COALESCE(file_size_bytes, 0) > 0")
	case "untracked":
		query = query.Where("COALESCE(file_size_bytes, 0) <= 0")
	default:
		return query, fmt.Errorf("invalid size_tracked parameter")
	}
	return query, nil
}

func parseOptionalInt64Query(c *gin.Context, key string) (int64, bool, error) {
	raw := strings.TrimSpace(c.Query(key))
	if raw == "" {
		return 0, false, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < 0 {
		return 0, false, fmt.Errorf("invalid %s parameter", key)
	}
	return value, true, nil
}

func fillMediaSizeAggregate(query *gorm.DB, groupExpr string, target map[string]mediaSizeAggregateResponse) error {
	type row struct {
		Key   string
		Count int64
		Bytes int64
	}
	var rows []row
	if err := query.Session(&gorm.Session{}).
		Select(fmt.Sprintf("%s AS key, COUNT(*) AS count, COALESCE(SUM(CASE WHEN file_size_bytes > 0 THEN file_size_bytes ELSE 0 END), 0) AS bytes", groupExpr)).
		Group(groupExpr).
		Scan(&rows).Error; err != nil {
		return err
	}
	for _, r := range rows {
		target[r.Key] = mediaSizeAggregateResponse{Count: r.Count, Bytes: r.Bytes}
	}
	return nil
}

func fillMediaSizeBuckets(query *gorm.DB, target map[string]mediaSizeAggregateResponse) error {
	buckets := []struct {
		key   string
		where string
	}{
		{"untracked", "COALESCE(file_size_bytes, 0) <= 0"},
		{"<100MB", "file_size_bytes > 0 AND file_size_bytes < 104857600"},
		{"100-500MB", "file_size_bytes >= 104857600 AND file_size_bytes < 524288000"},
		{"500MB-1GB", "file_size_bytes >= 524288000 AND file_size_bytes < 1073741824"},
		{"1-2GB", "file_size_bytes >= 1073741824 AND file_size_bytes < 2147483648"},
		{">2GB", "file_size_bytes >= 2147483648"},
	}
	for _, bucket := range buckets {
		var agg mediaSizeAggregateResponse
		if err := query.Session(&gorm.Session{}).
			Select("COUNT(*) AS count, COALESCE(SUM(CASE WHEN file_size_bytes > 0 THEN file_size_bytes ELSE 0 END), 0) AS bytes").
			Where(bucket.where).
			Scan(&agg).Error; err != nil {
			return err
		}
		target[bucket.key] = agg
	}
	return nil
}

func mapMediaSizeLargestItem(item models.ContentItem) mediaSizeLargestItemResponse {
	title := ""
	if item.Title != nil {
		title = *item.Title
	}
	return mediaSizeLargestItemResponse{
		ID:            item.PublicID.String(),
		Type:          string(item.Type),
		Status:        string(item.Status),
		Title:         title,
		SourceName:    item.SourceName,
		FileSizeBytes: item.FileSizeBytes,
		StorageTier:   item.StorageTier,
		MediaURL:      item.MediaURL,
		ThumbnailURL:  item.ThumbnailURL,
		PublishedAt:   formatTimePtr(item.PublishedAt),
		UpdatedAt:     item.UpdatedAt.UTC().Format(time.RFC3339),
	}
}

// UpdateContentStatus handles PATCH /admin/content/:id/status
func UpdateContentStatus(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}

	db := c.MustGet("db").(*gorm.DB)
	publicID := c.Param("id")
	id, err := uuid.Parse(publicID)
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "Invalid content ID",
			Code:    "INVALID_ID",
		})
		return
	}

	var req updateContentStatusRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "Invalid request",
			Code:    "INVALID_REQUEST",
		})
		return
	}

	status := strings.ToUpper(strings.TrimSpace(req.Status))
	if status == "" {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "Status is required",
			Code:    "STATUS_REQUIRED",
		})
		return
	}

	var item models.ContentItem
	if err := db.Where("public_id = ? AND tenant_id = ?", id, principal.TenantID).First(&item).Error; err != nil {
		c.JSON(http.StatusNotFound, authErrorResponse{
			Message: "Content not found",
			Code:    "NOT_FOUND",
		})
		return
	}

	item.Status = models.ContentStatus(status)
	if err := db.Save(&item).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{
			Message: "Failed to update status",
			Code:    "UPDATE_FAILED",
		})
		return
	}

	resp := mapAdminContentItemResponse(item)
	populateAdminContentTranscription(db, item.PublicID, &resp)
	c.JSON(http.StatusOK, resp)
}

// UpdateContentSuitability handles PATCH /admin/content/:id/suitability.
func UpdateContentSuitability(c *gin.Context) {
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
	var req updateContentSuitabilityRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid request", Code: "INVALID_REQUEST"})
		return
	}
	verdict := normalizeMediaSuitability(req.MediaSuitability)
	if verdict == models.MediaSuitabilityUnknown && strings.TrimSpace(req.MediaSuitability) != "" && !strings.EqualFold(strings.TrimSpace(req.MediaSuitability), models.MediaSuitabilityUnknown) {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid media_suitability", Code: "INVALID_SUITABILITY"})
		return
	}
	var item models.ContentItem
	if err := db.Where("public_id = ? AND tenant_id = ?", id, principal.TenantID).First(&item).Error; err != nil {
		c.JSON(http.StatusNotFound, authErrorResponse{Message: "Content not found", Code: "NOT_FOUND"})
		return
	}
	item.MediaSuitability = verdict
	if req.MediaSuitabilityConfidence != nil {
		conf := *req.MediaSuitabilityConfidence
		if conf < 0 {
			conf = 0
		}
		if conf > 1 {
			conf = 1
		}
		item.MediaSuitabilityConfidence = &conf
	}
	if req.MediaSuitabilityReasons != nil {
		if raw, err := json.Marshal(req.MediaSuitabilityReasons); err == nil {
			item.MediaSuitabilityReasons = raw
		}
	}
	now := time.Now().UTC()
	item.MediaSuitabilityReviewedAt = &now
	if reviewer, err := uuid.Parse(principal.UserID); err == nil {
		item.MediaSuitabilityReviewedBy = &reviewer
	}
	if err := db.Save(&item).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to update suitability", Code: "UPDATE_FAILED"})
		return
	}
	resp := mapAdminContentItemResponse(item)
	populateAdminContentTranscription(db, item.PublicID, &resp)
	c.JSON(http.StatusOK, resp)
}

// populateAdminContentTranscriptionBatch fills transcription job / quality /
// approval fields for a whole page of rows with 3 queries total — the per-row
// variant below costs 3 queries PER ROW, which at 50 rows × 30s auto-refresh
// is a real latency problem against a remote Postgres.
func populateAdminContentTranscriptionBatch(db *gorm.DB, items []models.ContentItem, rows []adminContentItemResponse) {
	if len(items) == 0 || len(items) != len(rows) {
		return
	}
	ids := make([]uuid.UUID, 0, len(items))
	for _, it := range items {
		ids = append(ids, it.PublicID)
	}

	// Latest transcription job per item.
	var jobs []models.TranscriptionJob
	db.Raw(`SELECT DISTINCT ON (content_item_id) * FROM transcription_jobs
		WHERE content_item_id IN ?
		ORDER BY content_item_id, created_at DESC`, ids).Scan(&jobs)
	jobByContent := make(map[uuid.UUID]models.TranscriptionJob, len(jobs))
	for _, j := range jobs {
		jobByContent[j.ContentItemID] = j
	}

	// Quality is upserted one-row-per-item.
	var quals []models.TranscriptQuality
	db.Where("content_item_id IN ?", ids).Find(&quals)
	qualByContent := make(map[uuid.UUID]models.TranscriptQuality, len(quals))
	for _, q := range quals {
		qualByContent[q.ContentItemID] = q
	}

	// Approval state of each item's ACTIVE transcript.
	type approvedRow struct {
		ContentID  uuid.UUID  `gorm:"column:content_id"`
		ApprovedAt *time.Time `gorm:"column:approved_at"`
		ApprovedBy *string    `gorm:"column:approved_by"`
	}
	var approved []approvedRow
	db.Raw(`SELECT ci.public_id AS content_id, t.approved_at, t.approved_by
		FROM content_items ci
		JOIN transcripts t ON t.public_id = ci.transcript_id
		WHERE ci.public_id IN ? AND t.approved_at IS NOT NULL`, ids).Scan(&approved)
	approvedByContent := make(map[uuid.UUID]approvedRow, len(approved))
	for _, a := range approved {
		approvedByContent[a.ContentID] = a
	}

	for i := range rows {
		cid := items[i].PublicID
		if j, ok := jobByContent[cid]; ok {
			mapped := mapTranscriptionJob(j)
			rows[i].LatestTranscriptionJob = &mapped
		}
		if q, ok := qualByContent[cid]; ok {
			mapped := mapTranscriptQuality(q)
			rows[i].TranscriptQuality = &mapped
		}
		if a, ok := approvedByContent[cid]; ok {
			rows[i].TranscriptApprovedAt = formatTimePtr(a.ApprovedAt)
			rows[i].TranscriptApprovedBy = a.ApprovedBy
		}
	}
}

func populateAdminContentTranscription(db *gorm.DB, contentID uuid.UUID, resp *adminContentItemResponse) {
	if job := latestTranscriptionJob(db, contentID); job != nil {
		mapped := mapTranscriptionJob(*job)
		resp.LatestTranscriptionJob = &mapped
	}
	if q := latestTranscriptQuality(db, contentID); q != nil {
		mapped := mapTranscriptQuality(*q)
		resp.TranscriptQuality = &mapped
	}
	var transcript models.Transcript
	if err := db.Where(`public_id = (
		SELECT transcript_id FROM content_items WHERE public_id = ?
	) AND approved_at IS NOT NULL`, contentID).
		Order("created_at DESC").First(&transcript).Error; err == nil {
		resp.TranscriptApprovedAt = formatTimePtr(transcript.ApprovedAt)
		resp.TranscriptApprovedBy = transcript.ApprovedBy
	}
}

func mapAdminContentItemResponse(item models.ContentItem) adminContentItemResponse {
	var title string
	if item.Title != nil {
		title = *item.Title
	}

	var publishedAt *string
	if item.PublishedAt != nil {
		formatted := item.PublishedAt.UTC().Format(time.RFC3339)
		publishedAt = &formatted
	}

	metadata := map[string]interface{}{}
	if len(item.Metadata) > 0 {
		_ = json.Unmarshal(item.Metadata, &metadata)
	}
	var suitabilityReasons []string
	if len(item.MediaSuitabilityReasons) > 0 {
		_ = json.Unmarshal(item.MediaSuitabilityReasons, &suitabilityReasons)
	}

	return adminContentItemResponse{
		ID:                         item.PublicID.String(),
		Type:                       string(item.Type),
		Status:                     string(item.Status),
		Title:                      title,
		BodyText:                   item.BodyText,
		Excerpt:                    item.Excerpt,
		Author:                     item.Author,
		SourceID:                   item.SourceFeedURL,
		SourceName:                 item.SourceName,
		MediaURL:                   item.MediaURL,
		ThumbnailURL:               item.ThumbnailURL,
		OriginalURL:                item.OriginalURL,
		DurationSec:                item.DurationSec,
		FileSizeBytes:              item.FileSizeBytes,
		StorageTier:                item.StorageTier,
		MediaSuitability:           item.MediaSuitability,
		MediaSuitabilityConfidence: item.MediaSuitabilityConfidence,
		MediaSuitabilityReasons:    suitabilityReasons,
		TopicTags:                  item.TopicTags,
		PublishedAt:                publishedAt,
		CreatedAt:                  item.CreatedAt.UTC().Format(time.RFC3339),
		UpdatedAt:                  item.UpdatedAt.UTC().Format(time.RFC3339),
		LikeCount:                  item.LikeCount,
		ViewCount:                  item.ViewCount,
		ShareCount:                 item.ShareCount,
		Metadata:                   metadata,
		CaptionState:               item.CaptionState,
		TranscriptSource:           item.TranscriptSource,
		HasTranscript:              item.TranscriptID != nil,
	}
}

type bulkDeleteContentRequest struct {
	Status        string   `json:"status"`
	SourceName    string   `json:"source_name"`
	Type          string   `json:"type"`
	Topic         string   `json:"topic"`
	StoryID       string   `json:"story_id"`
	CreatedBefore string   `json:"created_before"`
	IDs           []string `json:"ids"`
	DryRun        bool     `json:"dry_run"`
}

const bulkDeleteIDsLimit = 500

type bulkDeleteContentResponse struct {
	DeletedCount int64  `json:"deleted_count"`
	Message      string `json:"message"`
}

// ListDistinctSourceNames handles GET /admin/content/source-names
func ListDistinctSourceNames(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}

	db := c.MustGet("db").(*gorm.DB)

	var names []string
	db.Model(&models.ContentItem{}).
		Where("tenant_id = ? AND source_name IS NOT NULL AND source_name != ''", principal.TenantID).
		Distinct("source_name").
		Order("source_name").
		Pluck("source_name", &names)

	c.JSON(http.StatusOK, gin.H{"source_names": names})
}

// BulkDeleteContent handles POST /admin/content/bulk-delete
func BulkDeleteContent(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}

	db := c.MustGet("db").(*gorm.DB)

	var req bulkDeleteContentRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "Invalid request: " + err.Error(),
			Code:    "INVALID_REQUEST",
		})
		return
	}

	hasIDs := len(req.IDs) > 0
	if !hasIDs && req.Status == "" && req.SourceName == "" && req.Type == "" && req.Topic == "" && req.StoryID == "" && req.CreatedBefore == "" {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "At least one filter is required (ids, status, type, topic, story_id, source_name, or created_before)",
			Code:    "FILTER_REQUIRED",
		})
		return
	}

	if hasIDs && len(req.IDs) > bulkDeleteIDsLimit {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: fmt.Sprintf("Too many ids; maximum is %d", bulkDeleteIDsLimit),
			Code:    "TOO_MANY_IDS",
		})
		return
	}

	query := db.Where("tenant_id = ?", principal.TenantID)

	if hasIDs {
		// When ids are provided, ignore the other filters — they're an
		// explicit row-selection from the UI.
		query = query.Where("public_id IN ?", req.IDs)
	} else {
		if req.Status != "" {
			query = query.Where("status = ?", strings.ToUpper(req.Status))
		}

		if req.SourceName != "" {
			query = query.Where("source_name = ?", req.SourceName)
		}

		if req.Type != "" {
			query = query.Where("type = ?", strings.ToUpper(req.Type))
		}

		if req.Topic != "" {
			query = query.Where("? = ANY(topic_tags)", req.Topic)
		}

		if req.StoryID != "" {
			if strings.EqualFold(req.StoryID, "none") {
				query = query.Where("story_id IS NULL")
			} else {
				query = query.Where("story_id = ?", req.StoryID)
			}
		}

		if req.CreatedBefore != "" {
			parsedTime, err := time.Parse(time.RFC3339, req.CreatedBefore)
			if err != nil {
				c.JSON(http.StatusBadRequest, authErrorResponse{
					Message: "Invalid created_before format. Use RFC3339 (e.g., 2026-03-14T00:00:00Z)",
					Code:    "INVALID_DATE",
				})
				return
			}
			query = query.Where("created_at < ?", parsedTime)
		}
	}

	if req.DryRun {
		var count int64
		query.Model(&models.ContentItem{}).Count(&count)
		c.JSON(http.StatusOK, bulkDeleteContentResponse{
			DeletedCount: count,
			Message:      "Dry run - no items deleted",
		})
		return
	}

	result := query.Delete(&models.ContentItem{})
	if result.Error != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{
			Message: "Failed to delete content: " + result.Error.Error(),
			Code:    "DELETE_FAILED",
		})
		return
	}

	c.JSON(http.StatusOK, bulkDeleteContentResponse{
		DeletedCount: result.RowsAffected,
		Message:      "Successfully deleted content items",
	})
}

// GetStatusCounts handles GET /admin/content/status-counts
// Returns a map of status → count for all content items in the tenant.
func GetStatusCounts(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}

	db := c.MustGet("db").(*gorm.DB)

	type statusRow struct {
		Status string `gorm:"column:status"`
		Count  int64  `gorm:"column:count"`
	}

	var rows []statusRow
	if err := db.Model(&models.ContentItem{}).
		Select("status, COUNT(*) as count").
		Where("tenant_id = ?", principal.TenantID).
		Group("status").
		Scan(&rows).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{
			Message: "Failed to fetch status counts",
			Code:    "FETCH_FAILED",
		})
		return
	}

	counts := map[string]int64{
		"PENDING":    0,
		"PROCESSING": 0,
		"READY":      0,
		"FAILED":     0,
		"ARCHIVED":   0,
	}
	for _, row := range rows {
		counts[row.Status] = row.Count
	}

	c.JSON(http.StatusOK, counts)
}

type bulkStatusChangeRequest struct {
	// Selection mode A — explicit rows from the UI. When ids are present the
	// filter fields (from_status/source_name/type/created_before) are ignored.
	IDs []string `json:"ids"`

	// Selection mode B — filter-based. from_status is required in this mode;
	// created_before enables age-based rotation (e.g. archive news older than N
	// days), mirroring bulk-delete.
	FromStatus    string `json:"from_status"`
	SourceName    string `json:"source_name"`
	Type          string `json:"type"`
	Topic         string `json:"topic"`
	StoryID       string `json:"story_id"`
	CreatedBefore string `json:"created_before"`

	// Always required — the status to move matching items into.
	ToStatus string `json:"to_status" binding:"required"`

	Limit  int  `json:"limit"`
	DryRun bool `json:"dry_run"`
}

const bulkStatusIDsLimit = 500

type bulkStatusChangeResponse struct {
	UpdatedCount int64  `json:"updated_count"`
	Message      string `json:"message"`
}

var validContentStatuses = map[string]bool{
	"PENDING": true, "PROCESSING": true, "READY": true, "FAILED": true, "ARCHIVED": true,
}

// BulkStatusChange handles POST /admin/content/bulk-status
// Moves content items into to_status, selected either by explicit ids or by a
// filter (from_status [+ source_name/type/created_before]).
func BulkStatusChange(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}

	db := c.MustGet("db").(*gorm.DB)

	var req bulkStatusChangeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "Invalid request: to_status is required",
			Code:    "INVALID_REQUEST",
		})
		return
	}

	toStatus := strings.ToUpper(strings.TrimSpace(req.ToStatus))
	if !validContentStatuses[toStatus] {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "Invalid to_status. Must be one of: PENDING, PROCESSING, READY, FAILED, ARCHIVED",
			Code:    "INVALID_STATUS",
		})
		return
	}

	// ── Mode A: explicit ids (UI row selection) ──────────────────
	if len(req.IDs) > 0 {
		if len(req.IDs) > bulkStatusIDsLimit {
			c.JSON(http.StatusBadRequest, authErrorResponse{
				Message: fmt.Sprintf("Too many ids; maximum is %d", bulkStatusIDsLimit),
				Code:    "TOO_MANY_IDS",
			})
			return
		}

		query := db.Model(&models.ContentItem{}).
			Where("tenant_id = ? AND public_id IN ?", principal.TenantID, req.IDs)

		if req.DryRun {
			var count int64
			query.Count(&count)
			c.JSON(http.StatusOK, bulkStatusChangeResponse{
				UpdatedCount: count,
				Message:      "Dry run — no items updated",
			})
			return
		}

		result := query.Update("status", toStatus)
		if result.Error != nil {
			c.JSON(http.StatusInternalServerError, authErrorResponse{
				Message: "Failed to update status: " + result.Error.Error(),
				Code:    "UPDATE_FAILED",
			})
			return
		}

		c.JSON(http.StatusOK, bulkStatusChangeResponse{
			UpdatedCount: result.RowsAffected,
			Message:      "Updated selected items to " + strings.ToLower(toStatus),
		})
		return
	}

	// ── Mode B: filter-based selection ───────────────────────────
	fromStatus := strings.ToUpper(strings.TrimSpace(req.FromStatus))
	if !validContentStatuses[fromStatus] {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "Invalid from_status. Must be one of: PENDING, PROCESSING, READY, FAILED, ARCHIVED",
			Code:    "INVALID_STATUS",
		})
		return
	}
	if fromStatus == toStatus {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "from_status and to_status must be different",
			Code:    "SAME_STATUS",
		})
		return
	}

	var createdBefore *time.Time
	if strings.TrimSpace(req.CreatedBefore) != "" {
		parsed, err := time.Parse(time.RFC3339, req.CreatedBefore)
		if err != nil {
			c.JSON(http.StatusBadRequest, authErrorResponse{
				Message: "Invalid created_before format. Use RFC3339 (e.g., 2026-03-14T00:00:00Z)",
				Code:    "INVALID_DATE",
			})
			return
		}
		createdBefore = &parsed
	}

	// Cap is applied ONLY when the caller passes an explicit positive limit.
	// With no limit the whole matching set is updated in one statement — topic
	// rotations routinely touch thousands of rows, so the old hardcoded 500 cap
	// would silently leave most of a topic untouched.
	limit := req.Limit
	if limit < 0 {
		limit = 0
	}

	applyFilters := func(q *gorm.DB) *gorm.DB {
		q = q.Where("tenant_id = ? AND status = ?", principal.TenantID, fromStatus)
		if req.SourceName != "" {
			q = q.Where("source_name = ?", req.SourceName)
		}
		if req.Type != "" {
			q = q.Where("type = ?", strings.ToUpper(req.Type))
		}
		if req.Topic != "" {
			q = q.Where("? = ANY(topic_tags)", req.Topic)
		}
		if req.StoryID != "" {
			if strings.EqualFold(req.StoryID, "none") {
				q = q.Where("story_id IS NULL")
			} else {
				q = q.Where("story_id = ?", req.StoryID)
			}
		}
		if createdBefore != nil {
			q = q.Where("created_at < ?", *createdBefore)
		}
		return q
	}

	if req.DryRun {
		var count int64
		applyFilters(db.Model(&models.ContentItem{})).Count(&count)
		if limit > 0 && int64(limit) < count {
			count = int64(limit)
		}
		c.JSON(http.StatusOK, bulkStatusChangeResponse{
			UpdatedCount: count,
			Message:      "Dry run — no items updated",
		})
		return
	}

	var result *gorm.DB
	if limit > 0 {
		// Bounded update — subquery caps the number of rows touched.
		subQuery := applyFilters(db.Model(&models.ContentItem{}).Select("id")).Limit(limit)
		result = db.Model(&models.ContentItem{}).
			Where("id IN (?)", subQuery).
			Update("status", toStatus)
	} else {
		// Uncapped — update the entire matching set in a single statement.
		result = applyFilters(db.Model(&models.ContentItem{})).Update("status", toStatus)
	}

	if result.Error != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{
			Message: "Failed to update status: " + result.Error.Error(),
			Code:    "UPDATE_FAILED",
		})
		return
	}

	c.JSON(http.StatusOK, bulkStatusChangeResponse{
		UpdatedCount: result.RowsAffected,
		Message:      "Updated " + strings.ToLower(fromStatus) + " items to " + strings.ToLower(toStatus),
	})
}
