package controllers

import (
	"content-management-system/src/models"
	"encoding/json"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type internalCreateTranscriptRequest struct {
	ContentItemID  string                   `json:"content_item_id"`
	FullText       string                   `json:"full_text"`
	Summary        *string                  `json:"summary"`
	WordTimestamps []map[string]interface{} `json:"word_timestamps"`
	Language       *string                  `json:"language"`
}

type internalCreateTranscriptResponse struct {
	ID        string `json:"id"`
	CreatedAt string `json:"created_at"`
}

// InternalCreateTranscript handles POST /internal/transcripts
func InternalCreateTranscript(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	var req internalCreateTranscriptRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	if req.ContentItemID == "" || req.FullText == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "content_item_id and full_text are required"})
		return
	}

	contentUUID, err := uuid.Parse(req.ContentItemID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid content_item_id"})
		return
	}

	transcript := models.Transcript{
		ContentItemID: contentUUID,
		FullText:      req.FullText,
		Summary:       req.Summary,
		Language:      req.Language,
	}

	if req.WordTimestamps != nil {
		if raw, err := json.Marshal(req.WordTimestamps); err == nil {
			transcript.WordTimestamps = datatypes.JSON(raw)
		}
	}

	if err := db.Create(&transcript).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create transcript"})
		return
	}

	c.JSON(http.StatusOK, internalCreateTranscriptResponse{
		ID:        transcript.PublicID.String(),
		CreatedAt: transcript.CreatedAt.UTC().Format(time.RFC3339),
	})
}
