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
