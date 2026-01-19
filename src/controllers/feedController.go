package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// ForYouResponse is the API response for the For You feed
type ForYouResponse struct {
	Cursor *string      `json:"cursor"`
	Items  []ForYouItem `json:"items"`
}

// ForYouItem represents a single item in the For You feed
type ForYouItem struct {
	ID           uuid.UUID `json:"id"`
	Type         string    `json:"type"`
	Title        string    `json:"title"`
	MediaURL     string    `json:"media_url"`
	ThumbnailURL string    `json:"thumbnail_url,omitempty"`
	DurationSec  int       `json:"duration_sec,omitempty"`
	Author       string    `json:"author,omitempty"`
	SourceName   string    `json:"source_name,omitempty"`
	LikeCount    int       `json:"like_count"`
	CommentCount int       `json:"comment_count"`
	ShareCount   int       `json:"share_count"`
	PublishedAt  time.Time `json:"published_at"`
	IsLiked      bool      `json:"is_liked"`
	IsBookmarked bool      `json:"is_bookmarked"`
}

// NewsResponse is the API response for the News feed
type NewsResponse struct {
	Cursor *string     `json:"cursor"`
	Slides []NewsSlide `json:"slides"`
}

// NewsSlide represents a single slide in the News feed
type NewsSlide struct {
	SlideID  uuid.UUID     `json:"slide_id"`
	Featured NewsFeatured  `json:"featured"`
	Related  []NewsRelated `json:"related"`
}

// NewsFeatured is the main article in a news slide
type NewsFeatured struct {
	ID           uuid.UUID `json:"id"`
	Type         string    `json:"type"`
	Title        string    `json:"title"`
	Excerpt      string    `json:"excerpt,omitempty"`
	ThumbnailURL string    `json:"thumbnail_url,omitempty"`
	Author       string    `json:"author,omitempty"`
	PublishedAt  time.Time `json:"published_at"`
}

// NewsRelated is a related item in a news slide
type NewsRelated struct {
	ID       uuid.UUID `json:"id"`
	Type     string    `json:"type"`
	Title    string    `json:"title,omitempty"`
	BodyText string    `json:"body_text,omitempty"`
	Excerpt  string    `json:"excerpt,omitempty"`
	Author   string    `json:"author,omitempty"`
}

// GetForYouFeed returns the For You feed with cursor-based pagination
// GET /api/v1/feed/foryou?cursor=xxx&limit=20
func GetForYouFeed(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	// Parse cursor pagination
	pagination, err := utils.ParseCursorParams(c.Query("cursor"), c.Query("limit"))
	if err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code:    http.StatusBadRequest,
			Message: "Invalid cursor: " + err.Error(),
		})
		return
	}

	// Get session/user ID for interaction status (optional)
	sessionID := c.Query("session_id")
	userIDStr := c.Query("user_id")

	// Query for VIDEO and PODCAST content
	query := db.Model(&models.ContentItem{}).
		Where("type IN ?", []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}).
		Where("status = ?", models.ContentStatusReady).
		Order("published_at DESC, id DESC")

	// Apply cursor if provided
	if !pagination.Timestamp.IsZero() {
		query = query.Where(
			"(published_at < ? OR (published_at = ? AND public_id < ?))",
			pagination.Timestamp, pagination.Timestamp, pagination.LastID,
		)
	}

	// Fetch items + 1 to check for next page
	var items []models.ContentItem
	if err := query.Limit(pagination.Limit + 1).Find(&items).Error; err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{
			Code:    http.StatusInternalServerError,
			Message: "Failed to fetch feed: " + err.Error(),
		})
		return
	}

	// Determine if there's a next page
	var nextCursor *string
	hasMore := len(items) > pagination.Limit
	if hasMore {
		items = items[:pagination.Limit] // trim to limit
	}

	// Get last item for cursor
	if len(items) > 0 && hasMore {
		lastItem := items[len(items)-1]
		cursor := utils.EncodeCursor(*lastItem.PublishedAt, lastItem.PublicID)
		nextCursor = &cursor
	}

	// Get interaction status if session/user provided
	likedMap := make(map[uuid.UUID]bool)
	bookmarkedMap := make(map[uuid.UUID]bool)
	if sessionID != "" || userIDStr != "" {
		likedMap, bookmarkedMap = getInteractionStatus(db, items, sessionID, userIDStr)
	}

	// Map to response
	responseItems := make([]ForYouItem, len(items))
	for i, item := range items {
		responseItems[i] = mapToForYouItem(item, likedMap[item.PublicID], bookmarkedMap[item.PublicID])
	}

	c.JSON(http.StatusOK, ForYouResponse{
		Cursor: nextCursor,
		Items:  responseItems,
	})
}

// GetNewsFeed returns the News feed with cursor-based pagination
// GET /api/v1/feed/news?cursor=xxx&limit=10
func GetNewsFeed(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	// Parse cursor pagination
	pagination, err := utils.ParseCursorParams(c.Query("cursor"), c.Query("limit"))
	if err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code:    http.StatusBadRequest,
			Message: "Invalid cursor: " + err.Error(),
		})
		return
	}

	// For news, we want slides (1 featured + 3 related each)
	// Limit means number of slides
	slideLimit := pagination.Limit
	if slideLimit > 20 {
		slideLimit = 20
	}
	if slideLimit <= 0 {
		slideLimit = 10
	}

	// Query for ARTICLE content (featured items)
	query := db.Model(&models.ContentItem{}).
		Where("type = ?", models.ContentTypeArticle).
		Where("status = ?", models.ContentStatusReady).
		Order("published_at DESC, id DESC")

	// Apply cursor if provided
	if !pagination.Timestamp.IsZero() {
		query = query.Where(
			"(published_at < ? OR (published_at = ? AND public_id < ?))",
			pagination.Timestamp, pagination.Timestamp, pagination.LastID,
		)
	}

	// Fetch featured articles + 1 to check for next page
	var featuredItems []models.ContentItem
	if err := query.Limit(slideLimit + 1).Find(&featuredItems).Error; err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{
			Code:    http.StatusInternalServerError,
			Message: "Failed to fetch feed: " + err.Error(),
		})
		return
	}

	// Determine if there's a next page
	var nextCursor *string
	hasMore := len(featuredItems) > slideLimit
	if hasMore {
		featuredItems = featuredItems[:slideLimit]
	}

	// Get last item for cursor
	if len(featuredItems) > 0 && hasMore {
		lastItem := featuredItems[len(featuredItems)-1]
		cursor := utils.EncodeCursor(*lastItem.PublishedAt, lastItem.PublicID)
		nextCursor = &cursor
	}

	// Query for related items (TWEET, COMMENT)
	var relatedItems []models.ContentItem
	if err := db.Model(&models.ContentItem{}).
		Where("type IN ?", []models.ContentType{models.ContentTypeTweet, models.ContentTypeComment}).
		Where("status = ?", models.ContentStatusReady).
		Order("published_at DESC").
		Limit(slideLimit * 3). // 3 related per slide
		Find(&relatedItems).Error; err != nil {
		// Non-fatal: continue with empty related
		relatedItems = []models.ContentItem{}
	}

	// Build slides
	slides := make([]NewsSlide, len(featuredItems))
	for i, featured := range featuredItems {
		// Get 3 related items for this slide
		related := make([]NewsRelated, 0, 3)
		startIdx := i * 3
		for j := 0; j < 3 && startIdx+j < len(relatedItems); j++ {
			related = append(related, mapToNewsRelated(relatedItems[startIdx+j]))
		}

		slides[i] = NewsSlide{
			SlideID:  uuid.New(),
			Featured: mapToNewsFeatured(featured),
			Related:  related,
		}
	}

	c.JSON(http.StatusOK, NewsResponse{
		Cursor: nextCursor,
		Slides: slides,
	})
}

// Helper functions

func getInteractionStatus(db *gorm.DB, items []models.ContentItem, sessionID, userIDStr string) (map[uuid.UUID]bool, map[uuid.UUID]bool) {
	likedMap := make(map[uuid.UUID]bool)
	bookmarkedMap := make(map[uuid.UUID]bool)

	if len(items) == 0 {
		return likedMap, bookmarkedMap
	}

	// Collect content IDs
	contentIDs := make([]uuid.UUID, len(items))
	for i, item := range items {
		contentIDs[i] = item.PublicID
	}

	// Query interactions
	var interactions []models.UserInteraction
	query := db.Model(&models.UserInteraction{}).
		Where("content_item_id IN ?", contentIDs).
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
			likedMap[interaction.ContentItemID] = true
		}
		if interaction.Type == models.InteractionTypeBookmark {
			bookmarkedMap[interaction.ContentItemID] = true
		}
	}

	return likedMap, bookmarkedMap
}

func mapToForYouItem(item models.ContentItem, isLiked, isBookmarked bool) ForYouItem {
	result := ForYouItem{
		ID:           item.PublicID,
		Type:         string(item.Type),
		LikeCount:    item.LikeCount,
		CommentCount: item.CommentCount,
		ShareCount:   item.ShareCount,
		IsLiked:      isLiked,
		IsBookmarked: isBookmarked,
	}

	if item.Title != nil {
		result.Title = *item.Title
	}
	if item.MediaURL != nil {
		result.MediaURL = *item.MediaURL
	}
	if item.ThumbnailURL != nil {
		result.ThumbnailURL = *item.ThumbnailURL
	}
	if item.DurationSec != nil {
		result.DurationSec = *item.DurationSec
	}
	if item.Author != nil {
		result.Author = *item.Author
	}
	if item.SourceName != nil {
		result.SourceName = *item.SourceName
	}
	if item.PublishedAt != nil {
		result.PublishedAt = *item.PublishedAt
	}

	return result
}

func mapToNewsFeatured(item models.ContentItem) NewsFeatured {
	result := NewsFeatured{
		ID:   item.PublicID,
		Type: string(item.Type),
	}

	if item.Title != nil {
		result.Title = *item.Title
	}
	if item.Excerpt != nil {
		result.Excerpt = *item.Excerpt
	}
	if item.ThumbnailURL != nil {
		result.ThumbnailURL = *item.ThumbnailURL
	}
	if item.Author != nil {
		result.Author = *item.Author
	}
	if item.PublishedAt != nil {
		result.PublishedAt = *item.PublishedAt
	}

	return result
}

func mapToNewsRelated(item models.ContentItem) NewsRelated {
	result := NewsRelated{
		ID:   item.PublicID,
		Type: string(item.Type),
	}

	if item.Title != nil {
		result.Title = *item.Title
	}
	if item.BodyText != nil {
		result.BodyText = *item.BodyText
	}
	if item.Excerpt != nil {
		result.Excerpt = *item.Excerpt
	}
	if item.Author != nil {
		result.Author = *item.Author
	}

	return result
}
