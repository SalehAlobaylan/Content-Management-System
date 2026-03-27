package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"encoding/json"
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
	},
	FilterableFields: map[string]string{
		"status":       "content_items.status",
		"type":         "content_items.type",
		"source_id":    "content_items.source_feed_url",
		"source_name":  "content_items.source_name",
		"created_at":   "content_items.created_at",
		"published_at": "content_items.published_at",
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

	c.JSON(http.StatusOK, mapAdminContentItemResponse(item))
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

	c.JSON(http.StatusOK, mapAdminContentItemResponse(item))
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
		ID:           item.PublicID.String(),
		Type:         string(item.Type),
		Status:       string(item.Status),
		Title:        title,
		BodyText:     item.BodyText,
		Excerpt:      item.Excerpt,
		Author:       item.Author,
		SourceID:     item.SourceFeedURL,
		SourceName:   item.SourceName,
		MediaURL:     item.MediaURL,
		ThumbnailURL: item.ThumbnailURL,
		OriginalURL:  item.OriginalURL,
		DurationSec:  item.DurationSec,
		TopicTags:    item.TopicTags,
		PublishedAt:  publishedAt,
		CreatedAt:    item.CreatedAt.UTC().Format(time.RFC3339),
		UpdatedAt:    item.UpdatedAt.UTC().Format(time.RFC3339),
		LikeCount:    item.LikeCount,
		ViewCount:    item.ViewCount,
		ShareCount:   item.ShareCount,
		Metadata:     metadata,
	}
}

type bulkDeleteContentRequest struct {
	Status        string `json:"status"`
	SourceName    string `json:"source_name"`
	CreatedBefore string `json:"created_before"`
	DryRun        bool   `json:"dry_run"`
}

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

	if req.Status == "" && req.SourceName == "" && req.CreatedBefore == "" {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "At least one filter is required (status, source_name, or created_before)",
			Code:    "FILTER_REQUIRED",
		})
		return
	}

	query := db.Where("tenant_id = ?", principal.TenantID)

	if req.Status != "" {
		query = query.Where("status = ?", strings.ToUpper(req.Status))
	}

	if req.SourceName != "" {
		query = query.Where("source_name = ?", req.SourceName)
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
	FromStatus string `json:"from_status" binding:"required"`
	ToStatus   string `json:"to_status" binding:"required"`
	SourceName string `json:"source_name"`
	Type       string `json:"type"`
	Limit      int    `json:"limit"`
	DryRun     bool   `json:"dry_run"`
}

type bulkStatusChangeResponse struct {
	UpdatedCount int64  `json:"updated_count"`
	Message      string `json:"message"`
}

// BulkStatusChange handles POST /admin/content/bulk-status
// Changes the status of content items matching the given filters.
func BulkStatusChange(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}

	db := c.MustGet("db").(*gorm.DB)

	var req bulkStatusChangeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "Invalid request: from_status and to_status are required",
			Code:    "INVALID_REQUEST",
		})
		return
	}

	fromStatus := strings.ToUpper(strings.TrimSpace(req.FromStatus))
	toStatus := strings.ToUpper(strings.TrimSpace(req.ToStatus))

	validStatuses := map[string]bool{
		"PENDING": true, "PROCESSING": true, "READY": true, "FAILED": true, "ARCHIVED": true,
	}
	if !validStatuses[fromStatus] || !validStatuses[toStatus] {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "Invalid status value. Must be one of: PENDING, PROCESSING, READY, FAILED, ARCHIVED",
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

	limit := req.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}

	query := db.Model(&models.ContentItem{}).
		Where("tenant_id = ? AND status = ?", principal.TenantID, fromStatus)

	if req.SourceName != "" {
		query = query.Where("source_name = ?", req.SourceName)
	}
	if req.Type != "" {
		query = query.Where("type = ?", strings.ToUpper(req.Type))
	}

	if req.DryRun {
		var count int64
		query.Count(&count)
		if int64(limit) < count {
			count = int64(limit)
		}
		c.JSON(http.StatusOK, bulkStatusChangeResponse{
			UpdatedCount: count,
			Message:      "Dry run — no items updated",
		})
		return
	}

	// Use a subquery to limit the number of rows updated
	subQuery := db.Model(&models.ContentItem{}).
		Select("id").
		Where("tenant_id = ? AND status = ?", principal.TenantID, fromStatus)
	if req.SourceName != "" {
		subQuery = subQuery.Where("source_name = ?", req.SourceName)
	}
	if req.Type != "" {
		subQuery = subQuery.Where("type = ?", strings.ToUpper(req.Type))
	}
	subQuery = subQuery.Limit(limit)

	result := db.Model(&models.ContentItem{}).
		Where("id IN (?)", subQuery).
		Update("status", toStatus)

	if result.Error != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{
			Message: "Failed to update status: " + result.Error.Error(),
			Code:    "UPDATE_FAILED",
		})
		return
	}

	c.JSON(http.StatusOK, bulkStatusChangeResponse{
		UpdatedCount: result.RowsAffected,
		Message:      "Updated " + strings.ToLower(req.FromStatus) + " items to " + strings.ToLower(req.ToStatus),
	})
}
