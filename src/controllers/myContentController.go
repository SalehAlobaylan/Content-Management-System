package controllers

import (
	"net/http"
	"strings"

	"content-management-system/src/models"
	"content-management-system/src/utils"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// MyContentItem is a slim representation for the profile "My Audio" /
// "My Writes" tabs. It mirrors ForYouItem but drops the engagement-state
// fields that require an interactions join — the user can always tap
// through to the full feed view if they want them.
type MyContentItem struct {
	ID           uuid.UUID `json:"id"`
	Type         string    `json:"type"`
	Status       string    `json:"status"`
	Title        string    `json:"title,omitempty"`
	Excerpt      string    `json:"excerpt,omitempty"`
	MediaURL     string    `json:"media_url,omitempty"`
	ThumbnailURL string    `json:"thumbnail_url,omitempty"`
	DurationSec  int       `json:"duration_sec,omitempty"`
	LikeCount    int       `json:"like_count"`
	CommentCount int       `json:"comment_count"`
	PublishedAt  string    `json:"published_at,omitempty"`
}

// MyContentResponse mirrors the For You/News cursor envelope.
type MyContentResponse struct {
	Cursor *string         `json:"cursor"`
	Items  []MyContentItem `json:"items"`
}

// GetMyContent returns the authenticated user's submitted content with
// cursor pagination. Filter by `type=podcast|article|video` (case-insensitive).
//
// GET /api/v1/content/mine?type=podcast&cursor=...&limit=20
// Requires UserAuthMiddleware (sets `user_id` in context).
func GetMyContent(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	rawUserID, exists := c.Get("user_id")
	if !exists {
		c.JSON(http.StatusUnauthorized, utils.HTTPError{
			Code:    http.StatusUnauthorized,
			Message: "Authentication required",
		})
		return
	}
	userID, err := uuid.Parse(rawUserID.(string))
	if err != nil {
		c.JSON(http.StatusUnauthorized, utils.HTTPError{
			Code:    http.StatusUnauthorized,
			Message: "Token user id is not a valid uuid",
		})
		return
	}

	pagination, err := utils.ParseCursorParams(c.Query("cursor"), c.Query("limit"))
	if err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code:    http.StatusBadRequest,
			Message: "Invalid cursor: " + err.Error(),
		})
		return
	}

	query := db.Model(&models.ContentItem{}).
		Where("author_id = ?", userID).
		// PENDING/PROCESSING items are useful to show the user ("uploading…");
		// FAILED is also visible so they know the pipeline rejected it.
		Where("status IN ?", []models.ContentStatus{
			models.ContentStatusReady,
			models.ContentStatusPending,
			models.ContentStatusProcessing,
			models.ContentStatusFailed,
			models.ContentStatusArchived,
		}).
		Order("COALESCE(published_at, created_at) DESC, public_id DESC")

	if typeFilter := strings.ToUpper(strings.TrimSpace(c.Query("type"))); typeFilter != "" {
		query = query.Where("type = ?", typeFilter)
	}

	if !pagination.Timestamp.IsZero() {
		query = query.Where(
			"(COALESCE(published_at, created_at) < ? OR (COALESCE(published_at, created_at) = ? AND public_id < ?))",
			pagination.Timestamp, pagination.Timestamp, pagination.LastID,
		)
	}

	var items []models.ContentItem
	if err := query.Limit(pagination.Limit + 1).Find(&items).Error; err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{
			Code:    http.StatusInternalServerError,
			Message: "Failed to fetch user content",
		})
		return
	}

	var nextCursor *string
	hasMore := len(items) > pagination.Limit
	if hasMore {
		items = items[:pagination.Limit]
		last := items[len(items)-1]
		ts := last.CreatedAt
		if last.PublishedAt != nil {
			ts = *last.PublishedAt
		}
		cursor := utils.EncodeCursor(ts, last.PublicID)
		nextCursor = &cursor
	}

	response := MyContentResponse{
		Cursor: nextCursor,
		Items:  make([]MyContentItem, len(items)),
	}
	for i, it := range items {
		response.Items[i] = mapToMyContentItem(it)
	}
	c.JSON(http.StatusOK, response)
}

func mapToMyContentItem(item models.ContentItem) MyContentItem {
	out := MyContentItem{
		ID:           item.PublicID,
		Type:         string(item.Type),
		Status:       string(item.Status),
		LikeCount:    item.LikeCount,
		CommentCount: item.CommentCount,
	}
	if item.Title != nil {
		out.Title = *item.Title
	}
	if item.Excerpt != nil {
		out.Excerpt = *item.Excerpt
	}
	if item.MediaURL != nil {
		out.MediaURL = *item.MediaURL
	}
	if item.ThumbnailURL != nil {
		out.ThumbnailURL = *item.ThumbnailURL
	}
	if item.DurationSec != nil {
		out.DurationSec = *item.DurationSec
	}
	if item.PublishedAt != nil {
		out.PublishedAt = item.PublishedAt.UTC().Format("2006-01-02T15:04:05Z")
	}
	return out
}
