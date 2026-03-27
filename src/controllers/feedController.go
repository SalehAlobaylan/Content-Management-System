package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"fmt"
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
	TranscriptID *string   `json:"transcript_id,omitempty"`
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
	excludeSeen := c.Query("exclude_seen") == "true"

	// Pre-fetch IDs the user has already viewed (used by both paths below)
	var seenIDs []uuid.UUID
	if excludeSeen && (sessionID != "" || userIDStr != "") {
		seenIDs = fetchSeenIDs(db, sessionID, userIDStr)
	}

	// Load ranking config (uses "default" tenant for public feeds)
	config := loadTenantConfig(db, "default")

	// ------ Ranked path (when intelligence is active) ------
	if config.IsActive {
		// Fetch items for ranking — try time window first, then fall back to all
		var allItems []models.ContentItem
		baseQuery := db.Model(&models.ContentItem{}).
			Where("type IN ?", []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}).
			Where("status = ?", models.ContentStatusReady).
			Where("media_url IS NOT NULL AND media_url != ''")

		// First try: items from the configured freshness window (minimum 30 days)
		windowDays := config.FreshnessDecayHours / 24
		if windowDays < 30 {
			windowDays = 30
		}
		broadQuery := baseQuery.Session(&gorm.Session{}).
			Where("COALESCE(published_at, created_at) > ?", time.Now().AddDate(0, 0, -windowDays)).
			Order("COALESCE(published_at, created_at) DESC").Limit(200)
		if err := broadQuery.Find(&allItems).Error; err != nil {
			c.JSON(http.StatusInternalServerError, utils.HTTPError{Code: http.StatusInternalServerError, Message: "Failed to fetch feed: " + err.Error()})
			return
		}

		// Fallback: if not enough items to fill multiple pages, fetch all READY items
		if len(allItems) < 200 {
			baseQuery.Session(&gorm.Session{}).Order("COALESCE(published_at, created_at) DESC").Limit(200).Find(&allItems)
		}

		// Score items
		contentIDs := extractPublicIDs(allItems)
		flagMap := LoadContentFlags(db, "default", contentIDs)
		velocityData := LoadVelocityData(db, contentIDs, config.VelocityWindowHours, time.Now())
		scored := ScoreItems(allItems, config, flagMap, velocityData, time.Now())

		// Filter out already-seen items
		if len(seenIDs) > 0 {
			seenSet := make(map[uuid.UUID]bool, len(seenIDs))
			for _, id := range seenIDs {
				seenSet[id] = true
			}
			filtered := scored[:0]
			for _, s := range scored {
				if !seenSet[s.Item.PublicID] {
					filtered = append(filtered, s)
				}
			}
			scored = filtered
		}

		// Apply cursor-based pagination over scored results
		startIdx := 0
		if !pagination.Timestamp.IsZero() {
			found := false
			for i, s := range scored {
				if s.Item.PublicID == pagination.LastID {
					startIdx = i + 1
					found = true
					break
				}
			}
			// Fallback: if the cursor item wasn't found (scores shifted between requests),
			// find the closest position by timestamp to avoid restarting from page 1
			if !found {
				for i, s := range scored {
					var itemTs time.Time
					if s.Item.PublishedAt != nil {
						itemTs = *s.Item.PublishedAt
					} else {
						itemTs = s.Item.CreatedAt
					}
					if !itemTs.After(pagination.Timestamp) {
						startIdx = i
						break
					}
				}
			}
		}

		endIdx := startIdx + pagination.Limit
		var nextCursor *string
		hasMore := endIdx < len(scored)
		if endIdx > len(scored) {
			endIdx = len(scored)
		}

		pageItems := scored[startIdx:endIdx]
		if hasMore && len(pageItems) > 0 {
			lastItem := pageItems[len(pageItems)-1].Item
			var ts time.Time
			if lastItem.PublishedAt != nil {
				ts = *lastItem.PublishedAt
			} else {
				ts = lastItem.CreatedAt
			}
			cursor := utils.EncodeCursor(ts, lastItem.PublicID)
			nextCursor = &cursor
		}

		// Extract items for interaction lookup
		items := make([]models.ContentItem, len(pageItems))
		for i, s := range pageItems {
			items[i] = s.Item
		}

		likedMap := make(map[uuid.UUID]bool)
		bookmarkedMap := make(map[uuid.UUID]bool)
		if sessionID != "" || userIDStr != "" {
			likedMap, bookmarkedMap = getInteractionStatus(db, items, sessionID, userIDStr)
		}

		responseItems := make([]ForYouItem, len(items))
		for i, item := range items {
			responseItems[i] = mapToForYouItem(item, likedMap[item.PublicID], bookmarkedMap[item.PublicID])
		}

		c.JSON(http.StatusOK, ForYouResponse{Cursor: nextCursor, Items: responseItems})
		return
	}

	// ------ Chronological path (default) ------

	// Query for VIDEO and PODCAST content with a valid media URL.
	// Use COALESCE(published_at, created_at) so items with NULL published_at
	// are still ordered and reachable by cursor pagination.
	query := db.Model(&models.ContentItem{}).
		Where("type IN ?", []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}).
		Where("status = ?", models.ContentStatusReady).
		Where("media_url IS NOT NULL AND media_url != ''").
		Order("COALESCE(published_at, created_at) DESC, public_id DESC")

	// Apply cursor if provided
	if !pagination.Timestamp.IsZero() {
		query = query.Where(
			"(COALESCE(published_at, created_at) < ? OR (COALESCE(published_at, created_at) = ? AND public_id < ?))",
			pagination.Timestamp, pagination.Timestamp, pagination.LastID,
		)
	}

	// Exclude already-seen items
	if len(seenIDs) > 0 {
		query = query.Where("public_id NOT IN ?", seenIDs)
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
		var ts time.Time
		if lastItem.PublishedAt != nil {
			ts = *lastItem.PublishedAt
		} else {
			ts = lastItem.CreatedAt
		}
		cursor := utils.EncodeCursor(ts, lastItem.PublicID)
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

	sessionID := c.Query("session_id")
	userIDStr := c.Query("user_id")
	excludeSeen := c.Query("exclude_seen") == "true"

	var seenIDs []uuid.UUID
	if excludeSeen && (sessionID != "" || userIDStr != "") {
		seenIDs = fetchSeenIDs(db, sessionID, userIDStr)
	}

	// Load ranking config
	config := loadTenantConfig(db, "default")

	var featuredItems []models.ContentItem

	if config.IsActive {
		// ------ Ranked path ------
		var allArticles []models.ContentItem
		windowDays := config.FreshnessDecayHours / 24
		if windowDays < 30 {
			windowDays = 30
		}
		baseQuery := db.Where("type = ? AND status = ?", models.ContentTypeArticle, models.ContentStatusReady)

		baseQuery.Session(&gorm.Session{}).
			Where("COALESCE(published_at, created_at) > ?", time.Now().AddDate(0, 0, -windowDays)).
			Order("COALESCE(published_at, created_at) DESC").Limit(200).Find(&allArticles)

		// Fallback: if not enough articles to fill multiple pages, fetch all READY articles
		if len(allArticles) < 200 {
			baseQuery.Session(&gorm.Session{}).
				Order("COALESCE(published_at, created_at) DESC").Limit(200).Find(&allArticles)
		}

		contentIDs := extractPublicIDs(allArticles)
		flagMap := LoadContentFlags(db, "default", contentIDs)
		velocityData := LoadVelocityData(db, contentIDs, config.VelocityWindowHours, time.Now())
		scored := ScoreItems(allArticles, config, flagMap, velocityData, time.Now())

		// Filter out already-seen slides
		if len(seenIDs) > 0 {
			seenSet := make(map[uuid.UUID]bool, len(seenIDs))
			for _, id := range seenIDs {
				seenSet[id] = true
			}
			filtered := scored[:0]
			for _, s := range scored {
				if !seenSet[s.Item.PublicID] {
					filtered = append(filtered, s)
				}
			}
			scored = filtered
		}

		// Cursor-based pagination with fallback when order shifts between requests
		startIdx := 0
		if !pagination.Timestamp.IsZero() {
			found := false
			for i, s := range scored {
				if s.Item.PublicID == pagination.LastID {
					startIdx = i + 1
					found = true
					break
				}
			}
			if !found {
				for i, s := range scored {
					var itemTs time.Time
					if s.Item.PublishedAt != nil {
						itemTs = *s.Item.PublishedAt
					} else {
						itemTs = s.Item.CreatedAt
					}
					if !itemTs.After(pagination.Timestamp) {
						startIdx = i
						break
					}
				}
			}
		}
		endIdx := startIdx + slideLimit
		if endIdx > len(scored) {
			endIdx = len(scored)
		}
		for _, s := range scored[startIdx:endIdx] {
			featuredItems = append(featuredItems, s.Item)
		}
	} else {
		// ------ Chronological path ------
		query := db.Model(&models.ContentItem{}).
			Where("type = ?", models.ContentTypeArticle).
			Where("status = ?", models.ContentStatusReady).
			Order("COALESCE(published_at, created_at) DESC, public_id DESC")

		if !pagination.Timestamp.IsZero() {
			query = query.Where(
				"(COALESCE(published_at, created_at) < ? OR (COALESCE(published_at, created_at) = ? AND public_id < ?))",
				pagination.Timestamp, pagination.Timestamp, pagination.LastID,
			)
		}

		if len(seenIDs) > 0 {
			query = query.Where("public_id NOT IN ?", seenIDs)
		}

		var fetched []models.ContentItem
		query.Limit(slideLimit + 1).Find(&fetched)

		if len(fetched) > slideLimit {
			featuredItems = fetched[:slideLimit]
		} else {
			featuredItems = fetched
		}
	}

	// Determine next cursor
	var nextCursor *string
	if len(featuredItems) == slideLimit {
		lastItem := featuredItems[len(featuredItems)-1]
		var ts time.Time
		if lastItem.PublishedAt != nil {
			ts = *lastItem.PublishedAt
		} else {
			ts = lastItem.CreatedAt
		}
		cursor := utils.EncodeCursor(ts, lastItem.PublicID)
		nextCursor = &cursor
	}

	// Build slides — fetch semantically related items per featured article
	slides := make([]NewsSlide, len(featuredItems))
	for i, featured := range featuredItems {
		related := fetchRelatedItems(db, featured, 3)
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

	// Build identity condition: match session_id OR user_id (both scoped to content_item_id IN above)
	if sessionID != "" && userIDStr != "" {
		if userID, err := uuid.Parse(userIDStr); err == nil {
			query = query.Where("session_id = ? OR user_id = ?", sessionID, userID)
		} else {
			query = query.Where("session_id = ?", sessionID)
		}
	} else if sessionID != "" {
		query = query.Where("session_id = ?", sessionID)
	} else if userIDStr != "" {
		if userID, err := uuid.Parse(userIDStr); err == nil {
			query = query.Where("user_id = ?", userID)
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
	if item.TranscriptID != nil {
		tid := item.TranscriptID.String()
		result.TranscriptID = &tid
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

// fetchRelatedItems returns up to `limit` TWEET/COMMENT items semantically related to the
// featured article. If the article has an embedding, uses pgvector cosine similarity (<=>).
// Falls back to date-ordered if no embedding is available.
func fetchRelatedItems(db *gorm.DB, featured models.ContentItem, limit int) []NewsRelated {
	var items []models.ContentItem

	if featured.Embedding != nil {
		// Semantic path: order by cosine distance to article embedding
		embStr := pgvectorToLiteral(featured.Embedding.Slice())
		err := db.Model(&models.ContentItem{}).
			Where("type IN ?", []models.ContentType{models.ContentTypeTweet, models.ContentTypeComment}).
			Where("status = ?", models.ContentStatusReady).
			Where("embedding IS NOT NULL").
			Order(fmt.Sprintf("embedding <=> '%s'", embStr)).
			Limit(limit).
			Find(&items).Error
		if err != nil || len(items) == 0 {
			// fall through to date fallback
			items = nil
		}
	}

	// Fallback: date-ordered
	if len(items) == 0 {
		db.Model(&models.ContentItem{}).
			Where("type IN ?", []models.ContentType{models.ContentTypeTweet, models.ContentTypeComment}).
			Where("status = ?", models.ContentStatusReady).
			Order("published_at DESC").
			Limit(limit).
			Find(&items)
	}

	result := make([]NewsRelated, 0, len(items))
	for _, item := range items {
		result = append(result, mapToNewsRelated(item))
	}
	return result
}

// pgvectorToLiteral converts a float32 slice to a PostgreSQL vector literal string.
// e.g. [0.1, 0.2, 0.3] → '[0.1,0.2,0.3]'
func pgvectorToLiteral(v []float32) string {
	if len(v) == 0 {
		return "[]"
	}
	s := "["
	for i, f := range v {
		if i > 0 {
			s += ","
		}
		s += fmt.Sprintf("%g", f)
	}
	s += "]"
	return s
}
