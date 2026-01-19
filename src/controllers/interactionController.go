package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

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

	// Build interaction
	interaction := models.UserInteraction{
		ContentItemID: contentItemID,
		Type:          req.InteractionType,
		Metadata:      req.Metadata,
	}

	// Set user/session
	if req.SessionID != nil {
		interaction.SessionID = req.SessionID
	}
	if req.UserID != nil {
		if userID, err := uuid.Parse(*req.UserID); err == nil {
			interaction.UserID = &userID
		}
	}

	// Check for duplicate (like/bookmark should be unique per user per content)
	if req.InteractionType == models.InteractionTypeLike || req.InteractionType == models.InteractionTypeBookmark {
		var existing models.UserInteraction
		query := db.Where("content_item_id = ?", contentItemID).
			Where("type = ?", req.InteractionType)

		if interaction.SessionID != nil {
			query = query.Where("session_id = ?", *interaction.SessionID)
		}
		if interaction.UserID != nil {
			query = query.Where("user_id = ?", *interaction.UserID)
		}

		if err := query.First(&existing).Error; err == nil {
			// Already exists - return success (idempotent)
			c.JSON(http.StatusOK, utils.ResponseMessage{
				Code:    http.StatusOK,
				Message: "Interaction already exists",
				Data:    existing,
			})
			return
		}
	}

	// Create the interaction
	if err := db.Create(&interaction).Error; err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{
			Code:    http.StatusInternalServerError,
			Message: "Failed to create interaction: " + err.Error(),
		})
		return
	}

	// Update engagement counters
	updateEngagementCount(db, contentItemID, req.InteractionType, 1)

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

	sessionID := c.Query("session_id")
	userIDStr := c.Query("user_id")

	if sessionID == "" && userIDStr == "" {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code:    http.StatusBadRequest,
			Message: "Either session_id or user_id is required",
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

	// Query bookmarks
	query := db.Model(&models.UserInteraction{}).
		Where("type = ?", models.InteractionTypeBookmark).
		Order("created_at DESC")

	if sessionID != "" {
		query = query.Where("session_id = ?", sessionID)
	}
	if userIDStr != "" {
		if userID, err := uuid.Parse(userIDStr); err == nil {
			query = query.Or("user_id = ?", userID)
		}
	}

	// Apply cursor
	if !pagination.Timestamp.IsZero() {
		query = query.Where("created_at < ?", pagination.Timestamp)
	}

	var interactions []models.UserInteraction
	if err := query.Limit(pagination.Limit + 1).Find(&interactions).Error; err != nil {
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

	// Map to response
	items := make([]ForYouItem, 0, len(contentItems))
	for _, item := range contentItems {
		items = append(items, mapToForYouItem(item, false, true)) // is_bookmarked = true
	}

	c.JSON(http.StatusOK, gin.H{
		"cursor": nextCursor,
		"items":  items,
	})
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

	// Decrement engagement counter
	updateEngagementCount(db, interaction.ContentItemID, interaction.Type, -1)

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

// updateEngagementCount updates the like/share count on a content item
func updateEngagementCount(db *gorm.DB, contentItemID uuid.UUID, interactionType models.InteractionType, delta int) {
	var field string
	switch interactionType {
	case models.InteractionTypeLike:
		field = "like_count"
	case models.InteractionTypeShare:
		field = "share_count"
	case models.InteractionTypeView:
		field = "view_count"
	default:
		return
	}

	db.Model(&models.ContentItem{}).
		Where("public_id = ?", contentItemID).
		UpdateColumn(field, gorm.Expr(field+" + ?", delta))
}
