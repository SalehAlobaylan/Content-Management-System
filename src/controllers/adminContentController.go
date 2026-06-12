package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"encoding/json"
	"fmt"
	"net/http"
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

type adminContentItemResponse struct {
	ID           string                 `json:"id"`
	Type         string                 `json:"type"`
	Status       string                 `json:"status"`
	Title        string                 `json:"title"`
	BodyText     *string                `json:"body_text,omitempty"`
	Excerpt      *string                `json:"excerpt,omitempty"`
	Author       *string                `json:"author,omitempty"`
	SourceID     *string                `json:"source_id,omitempty"`
	SourceName   *string                `json:"source_name,omitempty"`
	MediaURL     *string                `json:"media_url,omitempty"`
	ThumbnailURL *string                `json:"thumbnail_url,omitempty"`
	OriginalURL  *string                `json:"original_url,omitempty"`
	DurationSec  *int                   `json:"duration_sec,omitempty"`
	TopicTags    []string               `json:"topic_tags,omitempty"`
	PublishedAt  *string                `json:"published_at,omitempty"`
	CreatedAt    string                 `json:"created_at"`
	UpdatedAt    string                 `json:"updated_at"`
	LikeCount    int                    `json:"like_count"`
	ViewCount    int                    `json:"view_count"`
	ShareCount   int                    `json:"share_count"`
	Metadata     map[string]interface{} `json:"metadata,omitempty"`
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

var contentAdminQueryConfig = utils.QueryConfig{
	DefaultLimit: 20,
	MaxLimit:     100,
	DefaultSort: []utils.SortParam{{
		Field:     "published_at",
		Direction: "desc",
	}},
	SortableFields: map[string]string{
		"created_at":   "content_items.created_at",
		"updated_at":   "content_items.updated_at",
		"published_at": "content_items.published_at",
		"title":        "content_items.title",
		"type":         "content_items.type",
		"status":       "content_items.status",
		"duration_sec": "content_items.duration_sec",
		"source_name":  "content_items.source_name",
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

	// Topic filter — topic_tags is a text[] column not handled by the generic
	// query builder, so apply array membership directly (same pattern as the
	// public RSS feed controller).
	if topic := strings.TrimSpace(c.Query("topic")); topic != "" {
		query = query.Where("? = ANY(topic_tags)", topic)
	}
	// First-class topic filter (the News manager board fetches by topic_id).
	// The sentinel "none" selects unclassified articles (topic_id IS NULL).
	if topicID := strings.TrimSpace(c.Query("topic_id")); topicID != "" {
		if strings.EqualFold(topicID, "none") {
			query = query.Where("topic_id IS NULL")
		} else {
			query = query.Where("topic_id = ?", topicID)
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

	return adminContentItemResponse{
		ID:               item.PublicID.String(),
		Type:             string(item.Type),
		Status:           string(item.Status),
		Title:            title,
		BodyText:         item.BodyText,
		Excerpt:          item.Excerpt,
		Author:           item.Author,
		SourceID:         item.SourceFeedURL,
		SourceName:       item.SourceName,
		MediaURL:         item.MediaURL,
		ThumbnailURL:     item.ThumbnailURL,
		OriginalURL:      item.OriginalURL,
		DurationSec:      item.DurationSec,
		TopicTags:        item.TopicTags,
		PublishedAt:      publishedAt,
		CreatedAt:        item.CreatedAt.UTC().Format(time.RFC3339),
		UpdatedAt:        item.UpdatedAt.UTC().Format(time.RFC3339),
		LikeCount:        item.LikeCount,
		ViewCount:        item.ViewCount,
		ShareCount:       item.ShareCount,
		Metadata:         metadata,
		CaptionState:     item.CaptionState,
		TranscriptSource: item.TranscriptSource,
		HasTranscript:    item.TranscriptID != nil,
	}
}

type bulkDeleteContentRequest struct {
	Status        string   `json:"status"`
	SourceName    string   `json:"source_name"`
	Type          string   `json:"type"`
	Topic         string   `json:"topic"`
	TopicID       string   `json:"topic_id"`
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
	if !hasIDs && req.Status == "" && req.SourceName == "" && req.Type == "" && req.Topic == "" && req.TopicID == "" && req.CreatedBefore == "" {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "At least one filter is required (ids, status, type, topic, topic_id, source_name, or created_before)",
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

		if req.TopicID != "" {
			if strings.EqualFold(req.TopicID, "none") {
				query = query.Where("topic_id IS NULL")
			} else {
				query = query.Where("topic_id = ?", req.TopicID)
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
	TopicID       string `json:"topic_id"`
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
		if req.TopicID != "" {
			if strings.EqualFold(req.TopicID, "none") {
				q = q.Where("topic_id IS NULL")
			} else {
				q = q.Where("topic_id = ?", req.TopicID)
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
