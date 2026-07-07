package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// maxCommentLength caps comment text to keep payloads and rendering sane.
const maxCommentLength = 1000

// commentMetadata is the expected Metadata shape for comment interactions.
type commentMetadata struct {
	Text   string `json:"text"`
	Author string `json:"author,omitempty"`
}

// authedUserID returns the authenticated user's UUID when the request carried a
// valid JWT (set as "user_id" in the context by OptionalUserAuthMiddleware).
// The bool reports whether the caller is authenticated. A client-supplied
// user_id is never consulted here — authorization must derive identity only
// from the verified token.
func authedUserID(c *gin.Context) (uuid.UUID, bool) {
	raw, ok := c.Get("user_id")
	if !ok {
		return uuid.Nil, false
	}
	s, ok := raw.(string)
	if !ok {
		return uuid.Nil, false
	}
	uid, err := uuid.Parse(s)
	if err != nil {
		return uuid.Nil, false
	}
	return uid, true
}

// readIdentity returns the (userIDStr, sessionID) pair a read handler should
// scope personalization to. When the caller is authenticated, identity is the
// verified user only and the client-supplied session_id is ignored — otherwise
// an authenticated caller could pass ?session_id=<victim> to read another
// (anonymous) user's session-scoped flags. Anonymous callers fall back to their
// own session_id.
func readIdentity(c *gin.Context) (userIDStr string, sessionID string) {
	if uid, ok := authedUserID(c); ok {
		return uid.String(), ""
	}
	return "", c.Query("session_id")
}

func isIdempotentInteraction(interactionType models.InteractionType) bool {
	return interactionType == models.InteractionTypeLike || interactionType == models.InteractionTypeBookmark
}

func scopedInteractionQuery(db *gorm.DB, contentItemID uuid.UUID, interactionType models.InteractionType, interaction models.UserInteraction) *gorm.DB {
	query := db.Where("content_item_id = ?", contentItemID).
		Where("type = ?", interactionType)
	if interaction.UserID != nil {
		return query.Where("user_id = ?", *interaction.UserID)
	}
	return query.Where("session_id = ?", *interaction.SessionID)
}

func interactionLockKey(contentItemID uuid.UUID, interactionType models.InteractionType, interaction models.UserInteraction) string {
	if interaction.UserID != nil {
		return "user:" + interaction.UserID.String() + ":" + contentItemID.String() + ":" + string(interactionType)
	}
	return "session:" + *interaction.SessionID + ":" + contentItemID.String() + ":" + string(interactionType)
}

// CreateInteraction records a user interaction (like, bookmark, view, share, complete)
// POST /api/v1/interactions
func CreateInteraction(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	var req models.CreateInteractionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code:    http.StatusBadRequest,
			Message: "Invalid request body: " + err.Error(),
		})
		return
	}

	// Parse content item ID
	contentItemID, err := uuid.Parse(req.ContentItemID)
	if err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code:    http.StatusBadRequest,
			Message: "Invalid content_item_id",
		})
		return
	}

	// Verify content item exists
	var contentItem models.ContentItem
	if err := db.Where("public_id = ?", contentItemID).First(&contentItem).Error; err != nil {
		c.JSON(http.StatusNotFound, utils.HTTPError{
			Code:    http.StatusNotFound,
			Message: "Content item not found",
		})
		return
	}

	// Comments must carry non-blank text (length-capped)
	if req.InteractionType == models.InteractionTypeComment {
		var meta commentMetadata
		if err := json.Unmarshal(req.Metadata, &meta); err != nil || strings.TrimSpace(meta.Text) == "" {
			c.JSON(http.StatusBadRequest, utils.HTTPError{
				Code:    http.StatusBadRequest,
				Message: "Comment requires metadata.text",
			})
			return
		}
		if len([]rune(meta.Text)) > maxCommentLength {
			c.JSON(http.StatusBadRequest, utils.HTTPError{
				Code:    http.StatusBadRequest,
				Message: "Comment text exceeds maximum length",
			})
			return
		}
	}

	// Build interaction
	interaction := models.UserInteraction{
		ContentItemID: contentItemID,
		Type:          req.InteractionType,
		Metadata:      req.Metadata,
	}

	// Identity: prefer the authenticated user (verified JWT). Never trust the
	// client-supplied req.UserID — it is ignored entirely. Anonymous callers
	// fall back to a session id they provide.
	if uid, ok := authedUserID(c); ok {
		interaction.UserID = &uid
	} else if req.SessionID != nil && strings.TrimSpace(*req.SessionID) != "" {
		interaction.SessionID = req.SessionID
	} else {
		c.JSON(http.StatusUnauthorized, utils.HTTPError{
			Code:    http.StatusUnauthorized,
			Message: "Authentication or session_id required",
		})
		return
	}

	if isIdempotentInteraction(req.InteractionType) {
		var saved models.UserInteraction
		created := false
		err := db.Transaction(func(tx *gorm.DB) error {
			lockKey := interactionLockKey(contentItemID, req.InteractionType, interaction)
			if err := tx.Exec("SELECT pg_advisory_xact_lock(hashtext(?)::bigint)", lockKey).Error; err != nil {
				return err
			}

			query := scopedInteractionQuery(tx, contentItemID, req.InteractionType, interaction)
			var existing models.UserInteraction
			if err := query.First(&existing).Error; err == nil {
				saved = existing
				return nil
			} else if err != gorm.ErrRecordNotFound {
				return err
			}

			if err := tx.Create(&interaction).Error; err != nil {
				return err
			}
			saved = interaction
			created = true
			return nil
		})
		if err != nil {
			c.JSON(http.StatusInternalServerError, utils.HTTPError{
				Code:    http.StatusInternalServerError,
				Message: "Failed to create interaction: " + err.Error(),
			})
			return
		}
		if !created {
			c.JSON(http.StatusOK, utils.ResponseMessage{
				Code:    http.StatusOK,
				Message: "Interaction already exists",
				Data:    saved,
			})
			return
		}
		if err := updateEngagementCount(db, contentItemID, req.InteractionType, 1); err != nil {
			log.Printf("failed to update engagement counter for interaction %s on content %s: %v", req.InteractionType, contentItemID, err)
		}
		c.JSON(http.StatusCreated, utils.ResponseMessage{
			Code:    http.StatusCreated,
			Message: "Interaction created successfully",
			Data:    saved,
		})
		return
	}

	if err := db.Create(&interaction).Error; err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{
			Code:    http.StatusInternalServerError,
			Message: "Failed to create interaction: " + err.Error(),
		})
		return
	}

	if err := updateEngagementCount(db, contentItemID, req.InteractionType, 1); err != nil {
		log.Printf("failed to update engagement counter for interaction %s on content %s: %v", req.InteractionType, contentItemID, err)
	}

	c.JSON(http.StatusCreated, utils.ResponseMessage{
		Code:    http.StatusCreated,
		Message: "Interaction created successfully",
		Data:    interaction,
	})
}

// GetBookmarks returns the user's bookmarked content
// GET /api/v1/interactions/bookmarks?session_id=xxx&user_id=xxx&cursor=xxx&limit=20
func GetBookmarks(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	pagination, err := utils.ParseCursorParams(c.Query("cursor"), c.Query("limit"))
	if err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code:    http.StatusBadRequest,
			Message: "Invalid cursor: " + err.Error(),
		})
		return
	}

	sortParam := c.DefaultQuery("sort", "saved_desc")
	sortAsc := sortParam == "saved_asc"
	if sortParam != "saved_desc" && sortParam != "saved_asc" {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code:    http.StatusBadRequest,
			Message: "sort must be saved_desc or saved_asc",
		})
		return
	}

	feedParam := c.DefaultQuery("feed", "all")
	if feedParam != "all" && feedParam != "foryou" && feedParam != "news" {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code:    http.StatusBadRequest,
			Message: "feed must be all, foryou, or news",
		})
		return
	}

	// Query bookmarks, scoped strictly to the caller's own identity. Identity
	// comes from the verified JWT when present, otherwise the caller's session.
	query := db.Model(&models.UserInteraction{}).
		Select("user_interactions.*").
		Joins("JOIN content_items ON content_items.public_id = user_interactions.content_item_id").
		Where("user_interactions.type = ?", models.InteractionTypeBookmark).
		Where("content_items.status = ?", models.ContentStatusReady)

	if uid, ok := authedUserID(c); ok {
		query = query.Where("user_interactions.user_id = ?", uid)
	} else if sessionID := c.Query("session_id"); sessionID != "" {
		query = query.Where("user_interactions.session_id = ?", sessionID)
	} else {
		c.JSON(http.StatusUnauthorized, utils.HTTPError{
			Code:    http.StatusUnauthorized,
			Message: "Authentication or session_id required",
		})
		return
	}

	switch feedParam {
	case "foryou":
		query = query.Where("content_items.type IN ?", []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast})
	case "news":
		query = query.Where("content_items.type IN ?", []models.ContentType{
			models.ContentTypeNews,
			models.ContentTypeArticle,
			models.ContentTypeTweet,
			models.ContentTypeComment,
		})
	}

	if q := strings.TrimSpace(c.Query("q")); q != "" {
		like := "%" + q + "%"
		query = query.Where(
			"content_items.title ILIKE ? OR content_items.source_name ILIKE ? OR content_items.author ILIKE ?",
			like,
			like,
			like,
		)
	}

	if !pagination.Timestamp.IsZero() {
		if sortAsc {
			query = query.Where(
				"user_interactions.created_at > ? OR (user_interactions.created_at = ? AND user_interactions.public_id > ?)",
				pagination.Timestamp,
				pagination.Timestamp,
				pagination.LastID,
			)
		} else {
			query = query.Where(
				"user_interactions.created_at < ? OR (user_interactions.created_at = ? AND user_interactions.public_id < ?)",
				pagination.Timestamp,
				pagination.Timestamp,
				pagination.LastID,
			)
		}
	}

	orderDirection := "DESC"
	if sortAsc {
		orderDirection = "ASC"
	}

	var interactions []models.UserInteraction
	if err := query.
		Order("user_interactions.created_at " + orderDirection + ", user_interactions.public_id " + orderDirection).
		Limit(pagination.Limit + 1).
		Find(&interactions).Error; err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{
			Code:    http.StatusInternalServerError,
			Message: "Failed to fetch bookmarks: " + err.Error(),
		})
		return
	}

	// Check for next page
	var nextCursor *string
	hasMore := len(interactions) > pagination.Limit
	if hasMore {
		interactions = interactions[:pagination.Limit]
	}
	if len(interactions) > 0 && hasMore {
		lastItem := interactions[len(interactions)-1]
		cursor := utils.EncodeCursor(lastItem.CreatedAt, lastItem.PublicID)
		nextCursor = &cursor
	}

	// Get full content items
	contentIDs := make([]uuid.UUID, len(interactions))
	for i, interaction := range interactions {
		contentIDs[i] = interaction.ContentItemID
	}

	var contentItems []models.ContentItem
	if len(contentIDs) > 0 {
		db.Where("public_id IN ?", contentIDs).Find(&contentItems)
	}

	contentMap := make(map[uuid.UUID]models.ContentItem, len(contentItems))
	for _, item := range contentItems {
		contentMap[item.PublicID] = item
	}

	items := make([]ForYouItem, 0, len(interactions))
	for _, interaction := range interactions {
		item, ok := contentMap[interaction.ContentItemID]
		if !ok {
			continue
		}
		mapped := mapToForYouItem(item, false, true)
		bookmarkedAt := interaction.CreatedAt
		mapped.BookmarkedAt = &bookmarkedAt
		items = append(items, mapped)
	}

	c.JSON(http.StatusOK, gin.H{
		"cursor": nextCursor,
		"items":  items,
	})
}

// GetLikes returns the content the user has liked, newest-like first.
// GET /api/v1/interactions/likes?session_id=xxx&cursor=xxx&limit=20
//
// Mirrors GetBookmarks but scopes to like interactions. Items carry is_liked
// (always true here) and the real is_bookmarked flag so the row can render
// engagement state consistently with the Saved tab.
func GetLikes(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	pagination, err := utils.ParseCursorParams(c.Query("cursor"), c.Query("limit"))
	if err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code:    http.StatusBadRequest,
			Message: "Invalid cursor: " + err.Error(),
		})
		return
	}

	// Scope strictly to the caller's own identity (verified JWT, else session).
	query := db.Model(&models.UserInteraction{}).
		Select("user_interactions.*").
		Joins("JOIN content_items ON content_items.public_id = user_interactions.content_item_id").
		Where("user_interactions.type = ?", models.InteractionTypeLike).
		Where("content_items.status = ?", models.ContentStatusReady)

	if uid, ok := authedUserID(c); ok {
		query = query.Where("user_interactions.user_id = ?", uid)
	} else if sessionID := c.Query("session_id"); sessionID != "" {
		query = query.Where("user_interactions.session_id = ?", sessionID)
	} else {
		c.JSON(http.StatusUnauthorized, utils.HTTPError{
			Code:    http.StatusUnauthorized,
			Message: "Authentication or session_id required",
		})
		return
	}

	if !pagination.Timestamp.IsZero() {
		query = query.Where(
			"user_interactions.created_at < ? OR (user_interactions.created_at = ? AND user_interactions.public_id < ?)",
			pagination.Timestamp,
			pagination.Timestamp,
			pagination.LastID,
		)
	}

	var interactions []models.UserInteraction
	if err := query.
		Order("user_interactions.created_at DESC, user_interactions.public_id DESC").
		Limit(pagination.Limit + 1).
		Find(&interactions).Error; err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{
			Code:    http.StatusInternalServerError,
			Message: "Failed to fetch likes: " + err.Error(),
		})
		return
	}

	var nextCursor *string
	hasMore := len(interactions) > pagination.Limit
	if hasMore {
		interactions = interactions[:pagination.Limit]
	}
	if len(interactions) > 0 && hasMore {
		lastItem := interactions[len(interactions)-1]
		cursor := utils.EncodeCursor(lastItem.CreatedAt, lastItem.PublicID)
		nextCursor = &cursor
	}

	contentIDs := make([]uuid.UUID, len(interactions))
	for i, interaction := range interactions {
		contentIDs[i] = interaction.ContentItemID
	}

	var contentItems []models.ContentItem
	if len(contentIDs) > 0 {
		db.Where("public_id IN ?", contentIDs).Find(&contentItems)
	}

	contentMap := make(map[uuid.UUID]models.ContentItem, len(contentItems))
	for _, item := range contentItems {
		contentMap[item.PublicID] = item
	}

	// Resolve the bookmark flag for each liked item, scoped to the caller.
	userIDStr, sessionID := readIdentity(c)
	_, bookmarkedMap := getInteractionStatus(db, contentItems, sessionID, userIDStr)

	items := make([]ForYouItem, 0, len(interactions))
	for _, interaction := range interactions {
		item, ok := contentMap[interaction.ContentItemID]
		if !ok {
			continue
		}
		items = append(items, mapToForYouItem(item, true, bookmarkedMap[item.PublicID]))
	}

	c.JSON(http.StatusOK, gin.H{
		"cursor": nextCursor,
		"items":  items,
	})
}

// UserStats is the aggregate engagement summary shown on the profile hub.
type UserStats struct {
	Saved    int64 `json:"saved"`
	Likes    int64 `json:"likes"`
	Listened int64 `json:"listened"`
	Created  int64 `json:"created"`
}

// GetUserStats returns the authenticated user's profile counts. Each count is
// aligned with the list it heads: saved/likes are joined to READY content (so
// they match the Saved/Likes tabs), listened is distinct viewed content (matches
// the de-duped history), created mirrors GetMyContent's author scoping.
// GET /api/v1/interactions/stats
func GetUserStats(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	uid, ok := authedUserID(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, utils.HTTPError{
			Code:    http.StatusUnauthorized,
			Message: "Authentication required",
		})
		return
	}

	var stats UserStats

	db.Model(&models.UserInteraction{}).
		Joins("JOIN content_items ON content_items.public_id = user_interactions.content_item_id").
		Where("user_interactions.user_id = ?", uid).
		Where("user_interactions.type = ?", models.InteractionTypeBookmark).
		Where("content_items.status = ?", models.ContentStatusReady).
		Count(&stats.Saved)

	db.Model(&models.UserInteraction{}).
		Joins("JOIN content_items ON content_items.public_id = user_interactions.content_item_id").
		Where("user_interactions.user_id = ?", uid).
		Where("user_interactions.type = ?", models.InteractionTypeLike).
		Where("content_items.status = ?", models.ContentStatusReady).
		Count(&stats.Likes)

	db.Model(&models.UserInteraction{}).
		Where("user_id = ?", uid).
		Where("type = ?", models.InteractionTypeView).
		Distinct("content_item_id").
		Count(&stats.Listened)

	db.Model(&models.ContentItem{}).
		Where("author_id = ?", uid).
		Where("status IN ?", []models.ContentStatus{
			models.ContentStatusReady,
			models.ContentStatusPending,
			models.ContentStatusProcessing,
			models.ContentStatusFailed,
			models.ContentStatusArchived,
		}).
		Count(&stats.Created)

	c.JSON(http.StatusOK, stats)
}

// DeleteInteraction removes an interaction (unlike, unbookmark)
// DELETE /api/v1/interactions/:id
func DeleteInteraction(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	interactionIDStr := c.Param("id")
	interactionID, err := uuid.Parse(interactionIDStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code:    http.StatusBadRequest,
			Message: "Invalid interaction ID",
		})
		return
	}

	var interaction models.UserInteraction
	if err := db.Where("public_id = ?", interactionID).First(&interaction).Error; err != nil {
		c.JSON(http.StatusNotFound, utils.HTTPError{
			Code:    http.StatusNotFound,
			Message: "Interaction not found",
		})
		return
	}

	// Ownership check: the caller may only delete their own interaction. A 404
	// (rather than 403) is returned on mismatch to avoid leaking existence.
	if uid, ok := authedUserID(c); ok {
		if interaction.UserID == nil || *interaction.UserID != uid {
			c.JSON(http.StatusNotFound, utils.HTTPError{
				Code:    http.StatusNotFound,
				Message: "Interaction not found",
			})
			return
		}
	} else {
		sessionID := c.Query("session_id")
		if sessionID == "" || interaction.SessionID == nil || *interaction.SessionID != sessionID {
			c.JSON(http.StatusNotFound, utils.HTTPError{
				Code:    http.StatusNotFound,
				Message: "Interaction not found",
			})
			return
		}
	}

	if err := updateEngagementCount(db, interaction.ContentItemID, interaction.Type, -1); err != nil {
		log.Printf("failed to decrement engagement counter for interaction %s on content %s: %v", interaction.Type, interaction.ContentItemID, err)
	}

	// Delete the interaction
	if err := db.Delete(&interaction).Error; err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{
			Code:    http.StatusInternalServerError,
			Message: "Failed to delete interaction: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, utils.ResponseMessage{
		Code:    http.StatusOK,
		Message: "Interaction deleted successfully",
	})
}

// DeleteInteractionByContext removes an interaction by content + type + user/session.
// DELETE /api/v1/interactions?content_item_id=...&type=like|bookmark&user_id=...|session_id=...
func DeleteInteractionByContext(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	contentItemIDStr := c.Query("content_item_id")
	interactionTypeStr := c.Query("type")

	if contentItemIDStr == "" || interactionTypeStr == "" {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code:    http.StatusBadRequest,
			Message: "content_item_id and type are required",
		})
		return
	}

	contentItemID, err := uuid.Parse(contentItemIDStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code:    http.StatusBadRequest,
			Message: "Invalid content_item_id",
		})
		return
	}

	interactionType := models.InteractionType(interactionTypeStr)
	if interactionType != models.InteractionTypeLike && interactionType != models.InteractionTypeBookmark {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code:    http.StatusBadRequest,
			Message: "type must be like or bookmark",
		})
		return
	}

	query := db.Where("content_item_id = ?", contentItemID).
		Where("type = ?", interactionType)

	// Scope deletion to the caller's own identity (verified JWT or session).
	if uid, ok := authedUserID(c); ok {
		query = query.Where("user_id = ?", uid)
	} else if sessionID := c.Query("session_id"); sessionID != "" {
		query = query.Where("session_id = ?", sessionID)
	} else {
		c.JSON(http.StatusUnauthorized, utils.HTTPError{
			Code:    http.StatusUnauthorized,
			Message: "Authentication or session_id required",
		})
		return
	}

	var interaction models.UserInteraction
	if err := query.First(&interaction).Error; err != nil {
		c.JSON(http.StatusNotFound, utils.HTTPError{
			Code:    http.StatusNotFound,
			Message: "Interaction not found",
		})
		return
	}

	if err := updateEngagementCount(db, interaction.ContentItemID, interaction.Type, -1); err != nil {
		log.Printf("failed to decrement engagement counter for interaction %s on content %s: %v", interaction.Type, interaction.ContentItemID, err)
	}

	if err := db.Delete(&interaction).Error; err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{
			Code:    http.StatusInternalServerError,
			Message: "Failed to delete interaction: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, utils.ResponseMessage{
		Code:    http.StatusOK,
		Message: "Interaction deleted successfully",
	})
}

// CommentItem is a single comment in a content item's comment list
type CommentItem struct {
	ID        uuid.UUID `json:"id"`
	Text      string    `json:"text"`
	Author    string    `json:"author,omitempty"`
	IsMine    bool      `json:"is_mine"`
	CreatedAt time.Time `json:"created_at"`
}

// GetContentComments lists comments for a content item, newest first.
// GET /api/v1/content/:id/comments?cursor=xxx&limit=20&session_id=xxx&user_id=xxx
// session_id / user_id are optional and only used to mark the caller's own
// comments (is_mine) so the client can label them.
func GetContentComments(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	contentID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code:    http.StatusBadRequest,
			Message: "Invalid content ID",
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

	query := db.Model(&models.UserInteraction{}).
		Where("content_item_id = ?", contentID).
		Where("type = ?", models.InteractionTypeComment).
		Order("created_at DESC")

	if !pagination.Timestamp.IsZero() {
		query = query.Where("created_at < ?", pagination.Timestamp)
	}

	var interactions []models.UserInteraction
	if err := query.Limit(pagination.Limit + 1).Find(&interactions).Error; err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{
			Code:    http.StatusInternalServerError,
			Message: "Failed to fetch comments: " + err.Error(),
		})
		return
	}

	hasMore := len(interactions) > pagination.Limit
	if hasMore {
		interactions = interactions[:pagination.Limit]
	}

	sessionID := c.Query("session_id")
	var callerUserID *uuid.UUID
	if uid, ok := authedUserID(c); ok {
		callerUserID = &uid
	}

	items := make([]CommentItem, 0, len(interactions))
	for _, in := range interactions {
		var meta commentMetadata
		if err := json.Unmarshal(in.Metadata, &meta); err != nil || strings.TrimSpace(meta.Text) == "" {
			continue // skip malformed rows rather than failing the whole list
		}
		isMine := (callerUserID != nil && in.UserID != nil && *in.UserID == *callerUserID) ||
			(sessionID != "" && in.SessionID != nil && *in.SessionID == sessionID)
		items = append(items, CommentItem{
			ID:        in.PublicID,
			Text:      meta.Text,
			Author:    meta.Author,
			IsMine:    isMine,
			CreatedAt: in.CreatedAt,
		})
	}

	var nextCursor *string
	if hasMore && len(interactions) > 0 {
		last := interactions[len(interactions)-1]
		cursor := utils.EncodeCursor(last.CreatedAt, last.PublicID)
		nextCursor = &cursor
	}

	c.JSON(http.StatusOK, gin.H{
		"cursor": nextCursor,
		"items":  items,
	})
}

// HistoryItem is a single entry in the user's watch history
type HistoryItem struct {
	ContentID    uuid.UUID `json:"content_id"`
	ViewedAt     time.Time `json:"viewed_at"`
	Type         string    `json:"type"`
	Title        string    `json:"title,omitempty"`
	ThumbnailURL *string   `json:"thumbnail_url,omitempty"`
	MediaURL     *string   `json:"media_url,omitempty"`
	DurationSec  *int      `json:"duration_sec,omitempty"`
	Author       *string   `json:"author,omitempty"`
	SourceName   *string   `json:"source_name,omitempty"`
}

// GetWatchHistory returns a user's watch history (view interactions) with content details.
// GET /api/v1/interactions/history?session_id=xxx&user_id=xxx&cursor=xxx&limit=20
func GetWatchHistory(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	pagination, err := utils.ParseCursorParams(c.Query("cursor"), c.Query("limit"))
	if err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{Code: http.StatusBadRequest, Message: "Invalid cursor: " + err.Error()})
		return
	}

	// Build a latest-view-per-content subquery, scoped to the caller's own
	// identity (verified JWT when present, otherwise the caller's session).
	latestViews := db.Model(&models.UserInteraction{}).
		Select("DISTINCT ON (content_item_id) user_interactions.*").
		Where("type = ?", models.InteractionTypeView)

	if uid, ok := authedUserID(c); ok {
		latestViews = latestViews.Where("user_id = ?", uid)
	} else if sessionID := c.Query("session_id"); sessionID != "" {
		latestViews = latestViews.Where("session_id = ?", sessionID)
	} else {
		c.JSON(http.StatusUnauthorized, utils.HTTPError{
			Code:    http.StatusUnauthorized,
			Message: "Authentication or session_id required",
		})
		return
	}

	latestViews = latestViews.Order("content_item_id, created_at DESC, id DESC")

	// Cursor pagination over the deduped latest views, ordered by view time.
	query := db.Table("(?) AS latest_views", latestViews)
	if !pagination.Timestamp.IsZero() {
		query = query.Where(
			"created_at < ? OR (created_at = ? AND content_item_id < ?)",
			pagination.Timestamp,
			pagination.Timestamp,
			pagination.LastID,
		)
	}

	var interactions []models.UserInteraction
	if err := query.
		Order("created_at DESC, content_item_id DESC").
		Limit(pagination.Limit + 1).
		Scan(&interactions).Error; err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{Code: http.StatusInternalServerError, Message: err.Error()})
		return
	}

	hasMore := len(interactions) > pagination.Limit
	if hasMore {
		interactions = interactions[:pagination.Limit]
	}

	// Fetch content details for this batch
	contentIDs := make([]uuid.UUID, len(interactions))
	for i, v := range interactions {
		contentIDs[i] = v.ContentItemID
	}

	var contentItems []models.ContentItem
	if len(contentIDs) > 0 {
		db.Where("public_id IN ?", contentIDs).Find(&contentItems)
	}

	contentMap := make(map[uuid.UUID]models.ContentItem, len(contentItems))
	for _, item := range contentItems {
		contentMap[item.PublicID] = item
	}

	// Build response — preserve the view-time order from interactions
	items := make([]HistoryItem, 0, len(interactions))
	for _, v := range interactions {
		item, ok := contentMap[v.ContentItemID]
		if !ok {
			continue
		}
		h := HistoryItem{
			ContentID:    item.PublicID,
			ViewedAt:     v.CreatedAt,
			Type:         string(item.Type),
			ThumbnailURL: item.ThumbnailURL,
			MediaURL:     item.MediaURL,
			DurationSec:  item.DurationSec,
			Author:       item.Author,
			SourceName:   item.SourceName,
		}
		if item.Title != nil {
			h.Title = *item.Title
		}
		items = append(items, h)
	}

	// Next cursor based on the oldest view in this page
	var nextCursor *string
	if hasMore && len(interactions) > 0 {
		last := interactions[len(interactions)-1]
		cursor := utils.EncodeCursor(last.CreatedAt, last.ContentItemID)
		nextCursor = &cursor
	}

	c.JSON(http.StatusOK, gin.H{
		"cursor": nextCursor,
		"items":  items,
	})
}

// DeleteWatchHistory clears all view interactions for a user/session.
// DELETE /api/v1/interactions/history
func DeleteWatchHistory(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	// Scope deletion to the caller's own identity (verified JWT or session).
	query := db.Where("type = ?", models.InteractionTypeView)
	if uid, ok := authedUserID(c); ok {
		query = query.Where("user_id = ?", uid)
	} else if sessionID := c.Query("session_id"); sessionID != "" {
		query = query.Where("session_id = ?", sessionID)
	} else {
		c.JSON(http.StatusUnauthorized, utils.HTTPError{Code: http.StatusUnauthorized, Message: "Authentication or session_id required"})
		return
	}

	if err := query.Delete(&models.UserInteraction{}).Error; err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{Code: http.StatusInternalServerError, Message: err.Error()})
		return
	}

	c.JSON(http.StatusOK, utils.ResponseMessage{Code: http.StatusOK, Message: "Watch history cleared"})
}

// fetchSeenIDs returns content IDs already viewed by the given user/session.
func fetchSeenIDs(db *gorm.DB, sessionID, userIDStr string) []uuid.UUID {
	query := db.Model(&models.UserInteraction{}).
		Select("content_item_id").
		Where("type = ?", models.InteractionTypeView)

	if userIDStr != "" {
		if uid, err := uuid.Parse(userIDStr); err == nil {
			query = query.Where("user_id = ?", uid)
		}
	} else if sessionID != "" {
		query = query.Where("session_id = ?", sessionID)
	} else {
		return nil
	}

	var views []struct {
		ContentItemID uuid.UUID `gorm:"column:content_item_id"`
	}
	query.Scan(&views)

	ids := make([]uuid.UUID, len(views))
	for i, v := range views {
		ids[i] = v.ContentItemID
	}
	return ids
}

// updateEngagementCount updates the like/share count on a content item
func updateEngagementCount(db *gorm.DB, contentItemID uuid.UUID, interactionType models.InteractionType, delta int) error {
	var field string
	switch interactionType {
	case models.InteractionTypeLike:
		field = "like_count"
	case models.InteractionTypeShare:
		field = "share_count"
	case models.InteractionTypeView:
		field = "view_count"
	case models.InteractionTypeComment:
		field = "comment_count"
	default:
		return nil
	}

	return db.Model(&models.ContentItem{}).
		Where("public_id = ?", contentItemID).
		UpdateColumn(field, gorm.Expr(field+" + ?", delta)).Error
}
