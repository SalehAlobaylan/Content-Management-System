package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// ContentItemResponse is the API response for a single content item
type ContentItemResponse struct {
	ID           uuid.UUID `json:"id"`
	Type         string    `json:"type"`
	Source       string    `json:"source"`
	Title        string    `json:"title,omitempty"`
	BodyText     string    `json:"body_text,omitempty"`
	Excerpt      string    `json:"excerpt,omitempty"`
	MediaURL     string    `json:"media_url,omitempty"`
	ThumbnailURL string    `json:"thumbnail_url,omitempty"`
	OriginalURL  string    `json:"original_url,omitempty"`
	DurationSec  int       `json:"duration_sec,omitempty"`
	Author       string    `json:"author,omitempty"`
	SourceName   string    `json:"source_name,omitempty"`
	TopicTags    []string  `json:"topic_tags,omitempty"`
	LikeCount    int       `json:"like_count"`
	CommentCount int       `json:"comment_count"`
	ShareCount   int       `json:"share_count"`
	ViewCount    int       `json:"view_count"`
	PublishedAt  string    `json:"published_at,omitempty"`
	CreatedAt    string    `json:"created_at"`
	IsLiked      bool      `json:"is_liked"`
	IsBookmarked bool      `json:"is_bookmarked"`
}

// GetContentItem returns a single content item by ID
// GET /api/v1/content/:id
func GetContentItem(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	contentIDStr := c.Param("id")
	contentID, err := uuid.Parse(contentIDStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code:    http.StatusBadRequest,
			Message: "Invalid content ID",
		})
		return
	}

	var item models.ContentItem
	if err := db.Where("public_id = ?", contentID).First(&item).Error; err != nil {
		c.JSON(http.StatusNotFound, utils.HTTPError{
			Code:    http.StatusNotFound,
			Message: "Content not found",
		})
		return
	}

	// Get interaction status if session/user provided
	sessionID := c.Query("session_id")
	userIDStr := c.Query("user_id")
	isLiked, isBookmarked := false, false

	if sessionID != "" || userIDStr != "" {
		isLiked, isBookmarked = getSingleInteractionStatus(db, item.PublicID, sessionID, userIDStr)
	}

	// Map to response
	response := mapToContentItemResponse(item, isLiked, isBookmarked)

	c.JSON(http.StatusOK, utils.ResponseMessage{
		Code:    http.StatusOK,
		Message: "Content fetched successfully",
		Data:    response,
	})
}

func getSingleInteractionStatus(db *gorm.DB, contentID uuid.UUID, sessionID, userIDStr string) (bool, bool) {
	isLiked, isBookmarked := false, false

	var interactions []models.UserInteraction
	query := db.Model(&models.UserInteraction{}).
		Where("content_item_id = ?", contentID).
		Where("type IN ?", []models.InteractionType{models.InteractionTypeLike, models.InteractionTypeBookmark})

	if sessionID != "" {
		query = query.Where("session_id = ?", sessionID)
	}
	if userIDStr != "" {
		if userID, err := uuid.Parse(userIDStr); err == nil {
			query = query.Or("user_id = ?", userID)
		}
	}

	query.Find(&interactions)

	for _, interaction := range interactions {
		if interaction.Type == models.InteractionTypeLike {
			isLiked = true
		}
		if interaction.Type == models.InteractionTypeBookmark {
			isBookmarked = true
		}
	}

	return isLiked, isBookmarked
}

func mapToContentItemResponse(item models.ContentItem, isLiked, isBookmarked bool) ContentItemResponse {
	response := ContentItemResponse{
		ID:           item.PublicID,
		Type:         string(item.Type),
		Source:       string(item.Source),
		LikeCount:    item.LikeCount,
		CommentCount: item.CommentCount,
		ShareCount:   item.ShareCount,
		ViewCount:    item.ViewCount,
		CreatedAt:    item.CreatedAt.Format("2006-01-02T15:04:05Z"),
		IsLiked:      isLiked,
		IsBookmarked: isBookmarked,
	}

	if item.Title != nil {
		response.Title = *item.Title
	}
	if item.BodyText != nil {
		response.BodyText = *item.BodyText
	}
	if item.Excerpt != nil {
		response.Excerpt = *item.Excerpt
	}
	if item.MediaURL != nil {
		response.MediaURL = *item.MediaURL
	}
	if item.ThumbnailURL != nil {
		response.ThumbnailURL = *item.ThumbnailURL
	}
	if item.OriginalURL != nil {
		response.OriginalURL = *item.OriginalURL
	}
	if item.DurationSec != nil {
		response.DurationSec = *item.DurationSec
	}
	if item.Author != nil {
		response.Author = *item.Author
	}
	if item.SourceName != nil {
		response.SourceName = *item.SourceName
	}
	if item.TopicTags != nil {
		response.TopicTags = item.TopicTags
	}
	if item.PublishedAt != nil {
		response.PublishedAt = item.PublishedAt.Format("2006-01-02T15:04:05Z")
	}

	return response
}
