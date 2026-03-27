package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"encoding/json"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// TranscriptResponse is the public API response for a transcript
type TranscriptResponse struct {
	ID             string      `json:"id"`
	ContentItemID  string      `json:"content_item_id"`
	FullText       string      `json:"full_text"`
	Summary        *string     `json:"summary,omitempty"`
	WordTimestamps interface{} `json:"word_timestamps,omitempty"`
	Language       *string     `json:"language,omitempty"`
	CreatedAt      string      `json:"created_at"`
}

// GetTranscript returns a transcript by its public ID
// GET /api/v1/transcripts/:id
func GetTranscript(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	transcriptIDStr := c.Param("id")
	transcriptID, err := uuid.Parse(transcriptIDStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code:    http.StatusBadRequest,
			Message: "Invalid transcript ID",
		})
		return
	}

	var transcript models.Transcript
	if err := db.Where("public_id = ?", transcriptID).First(&transcript).Error; err != nil {
		c.JSON(http.StatusNotFound, utils.HTTPError{
			Code:    http.StatusNotFound,
			Message: "Transcript not found",
		})
		return
	}

	// Unmarshal word_timestamps from JSONB for clean response
	var wordTimestamps interface{}
	if transcript.WordTimestamps != nil {
		_ = json.Unmarshal([]byte(transcript.WordTimestamps), &wordTimestamps)
	}

	c.JSON(http.StatusOK, utils.ResponseMessage{
		Code:    http.StatusOK,
		Message: "Transcript fetched successfully",
		Data: TranscriptResponse{
			ID:             transcript.PublicID.String(),
			ContentItemID:  transcript.ContentItemID.String(),
			FullText:       transcript.FullText,
			Summary:        transcript.Summary,
			WordTimestamps: wordTimestamps,
			Language:       transcript.Language,
			CreatedAt:      transcript.CreatedAt.UTC().Format("2006-01-02T15:04:05Z"),
		},
	})
}
