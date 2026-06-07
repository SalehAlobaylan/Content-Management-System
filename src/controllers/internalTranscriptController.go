package controllers

import (
	"content-management-system/src/models"
	"encoding/json"
	"net/http"
	"strings"
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
	// Caption-first additions (all optional for backward compat):
	Segments []map[string]interface{} `json:"segments"` // [{start,end,text}]
	Chapters []map[string]interface{} `json:"chapters"` // [{start,end,title,source}]
	Source   *string                  `json:"source"`   // youtube_human|youtube_auto|stt_deepgram|stt_whisper
	Provider *string                  `json:"provider"` // concrete engine name
	Language *string                  `json:"language"`
}

type internalCreateTranscriptResponse struct {
	ID        string `json:"id"`
	CreatedAt string `json:"created_at"`
}

// InternalCreateTranscript handles POST /internal/transcripts.
//
// Beyond creating the transcript row it now (a) persists segments/chapters/source/
// provider, (b) links the transcript to its content item and sets the lightweight
// caption_state + transcript_source in one pass, and (c) fires the model-agnostic
// re-enrichment cascade when this is an STT upgrade so search reflects the new text.
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
		Source:        req.Source,
		Provider:      req.Provider,
	}

	if req.WordTimestamps != nil {
		if raw, err := json.Marshal(req.WordTimestamps); err == nil {
			transcript.WordTimestamps = datatypes.JSON(raw)
		}
	}
	if req.Segments != nil {
		if raw, err := json.Marshal(req.Segments); err == nil {
			transcript.Segments = datatypes.JSON(raw)
		}
	}
	if req.Chapters != nil {
		if raw, err := json.Marshal(req.Chapters); err == nil {
			transcript.Chapters = datatypes.JSON(raw)
		}
	}

	if err := db.Create(&transcript).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create transcript"})
		return
	}

	// Link the transcript + set the lightweight caption_state/transcript_source on
	// the content item so feed/console can filter+badge without joining transcripts.
	// Derive source/state with a sane default for legacy callers (no source field).
	source := models.TranscriptSourceSTTWhisper
	if req.Source != nil && *req.Source != "" {
		source = *req.Source
	}
	captionState := models.CaptionStateForSource(source)

	var item models.ContentItem
	if err := db.Where("public_id = ?", contentUUID).First(&item).Error; err == nil {
		tid := transcript.PublicID
		item.TranscriptID = &tid
		item.CaptionState = &captionState
		item.TranscriptSource = &source
		_ = db.Save(&item).Error

		// Re-enrichment cascade (model-agnostic): when STT upgrades an ALREADY-READY
		// item's transcript, re-trigger embedding so retrieval reflects the better
		// text. The embedding model/composition is owned by the Enrichment migration
		// — we only signal.
		//
		// Gate on READY: this is the manual-upgrade path. During fresh ingestion the
		// item is still PROCESSING and the Aggregation AI worker runs the embedding
		// step itself (with the transcript) right after STT — firing the cascade
		// there would race that write and waste an embedding call.
		if strings.HasPrefix(source, "stt_") && item.Status == models.ContentStatusReady {
			itemCopy := item
			go func() {
				if text := buildEmbeddingText(&itemCopy); text != "" {
					_ = triggerEmbedding(text, itemCopy.PublicID.String(), true)
				}
			}()
		}
	}

	c.JSON(http.StatusOK, internalCreateTranscriptResponse{
		ID:        transcript.PublicID.String(),
		CreatedAt: transcript.CreatedAt.UTC().Format(time.RFC3339),
	})
}
