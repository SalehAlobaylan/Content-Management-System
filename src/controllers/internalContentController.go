package controllers

import (
	"content-management-system/src/models"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/pgvector/pgvector-go"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type internalCreateContentItemRequest struct {
	IdempotencyKey string                 `json:"idempotency_key"`
	Type           string                 `json:"type"`
	Source         string                 `json:"source"`
	Status         string                 `json:"status"`
	Title          string                 `json:"title"`
	BodyText       *string                `json:"body_text"`
	Excerpt        *string                `json:"excerpt"`
	Author         *string                `json:"author"`
	SourceName     string                 `json:"source_name"`
	SourceFeedURL  *string                `json:"source_feed_url"`
	OriginalURL    string                 `json:"original_url"`
	MediaURL       *string                `json:"media_url"`
	ThumbnailURL   *string                `json:"thumbnail_url"`
	DurationSec    *int                   `json:"duration_sec"`
	TopicTags      []string               `json:"topic_tags"`
	Metadata       map[string]interface{} `json:"metadata"`
	PublishedAt    *string                `json:"published_at"`
}

type internalCreateContentItemResponse struct {
	ID        string `json:"id"`
	Status    string `json:"status"`
	Created   bool   `json:"created"`
	CreatedAt string `json:"created_at"`
}

type internalUpdateContentItemRequest struct {
	Title       *string                `json:"title"`
	BodyText    *string                `json:"body_text"`
	Excerpt     *string                `json:"excerpt"`
	Author      *string                `json:"author"`
	SourceName  *string                `json:"source_name"`
	SourceFeed  *string                `json:"source_feed_url"`
	OriginalURL *string                `json:"original_url"`
	PublishedAt *string                `json:"published_at"`
	Metadata    map[string]interface{} `json:"metadata"`
}

type internalUpdateStatusRequest struct {
	Status        string  `json:"status"`
	FailureReason *string `json:"failure_reason"`
}

type internalUpdateArtifactsRequest struct {
	MediaURL     *string `json:"media_url"`
	ThumbnailURL *string `json:"thumbnail_url"`
	DurationSec  *int    `json:"duration_sec"`
}

type internalUpdateEmbeddingRequest struct {
	Embedding []float32 `json:"embedding"`
	TopicTags []string  `json:"topic_tags"`
}

type internalLinkTranscriptRequest struct {
	TranscriptID string `json:"transcript_id"`
}

const maxIdempotencyKeyLength = 512

func normalizeIdempotencyKey(key string) string {
	normalized := strings.TrimSpace(key)
	if utf8.RuneCountInString(normalized) <= maxIdempotencyKeyLength {
		return normalized
	}

	// Keep deterministic de-duplication for very long URLs/keys without DB length errors.
	sum := sha256.Sum256([]byte(normalized))
	return "sha256:" + hex.EncodeToString(sum[:])
}

// InternalCreateContentItem handles POST /internal/content-items
func InternalCreateContentItem(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	var req internalCreateContentItemRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	if strings.TrimSpace(req.IdempotencyKey) == "" || req.Type == "" || req.Source == "" || req.Status == "" || req.Title == "" || req.OriginalURL == "" || req.SourceName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Missing required fields"})
		return
	}

	idempotencyKey := normalizeIdempotencyKey(req.IdempotencyKey)

	// Check for existing item by idempotency key
	var existing models.ContentItem
	if err := db.Where("idempotency_key = ?", idempotencyKey).First(&existing).Error; err == nil {
		c.JSON(http.StatusOK, internalCreateContentItemResponse{
			ID:        existing.PublicID.String(),
			Status:    string(existing.Status),
			Created:   false,
			CreatedAt: existing.CreatedAt.UTC().Format(time.RFC3339),
		})
		return
	} else if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to check idempotency"})
		return
	}

	var publishedAt *time.Time
	if req.PublishedAt != nil && *req.PublishedAt != "" {
		if parsed, err := time.Parse(time.RFC3339, *req.PublishedAt); err == nil {
			publishedAt = &parsed
		}
	}

	metadataJSON, _ := json.Marshal(req.Metadata)

	item := models.ContentItem{
		Type:           models.ContentType(strings.ToUpper(req.Type)),
		Source:         models.SourceType(strings.ToUpper(req.Source)),
		Status:         models.ContentStatus(strings.ToUpper(req.Status)),
		IdempotencyKey: &idempotencyKey,
		Title:          &req.Title,
		BodyText:       req.BodyText,
		Excerpt:        req.Excerpt,
		Author:         req.Author,
		SourceName:     &req.SourceName,
		SourceFeedURL:  req.SourceFeedURL,
		MediaURL:       req.MediaURL,
		ThumbnailURL:   req.ThumbnailURL,
		OriginalURL:    &req.OriginalURL,
		DurationSec:    req.DurationSec,
		TopicTags:      req.TopicTags,
		Metadata:       datatypes.JSON(metadataJSON),
		PublishedAt:    publishedAt,
	}

	if err := db.Create(&item).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create content item"})
		return
	}

	c.JSON(http.StatusOK, internalCreateContentItemResponse{
		ID:        item.PublicID.String(),
		Status:    string(item.Status),
		Created:   true,
		CreatedAt: item.CreatedAt.UTC().Format(time.RFC3339),
	})
}

// InternalUpdateContentItem handles PUT /internal/content-items/:id
func InternalUpdateContentItem(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	publicID := c.Param("id")
	id, err := uuid.Parse(publicID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid content ID"})
		return
	}

	var req internalUpdateContentItemRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	var item models.ContentItem
	if err := db.Where("public_id = ?", id).First(&item).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Content not found"})
		return
	}

	if req.Title != nil {
		item.Title = req.Title
	}
	if req.BodyText != nil {
		item.BodyText = req.BodyText
	}
	if req.Excerpt != nil {
		item.Excerpt = req.Excerpt
	}
	if req.Author != nil {
		item.Author = req.Author
	}
	if req.SourceName != nil {
		item.SourceName = req.SourceName
	}
	if req.SourceFeed != nil {
		item.SourceFeedURL = req.SourceFeed
	}
	if req.OriginalURL != nil {
		item.OriginalURL = req.OriginalURL
	}
	if req.PublishedAt != nil && *req.PublishedAt != "" {
		if parsed, err := time.Parse(time.RFC3339, *req.PublishedAt); err == nil {
			item.PublishedAt = &parsed
		}
	}
	if req.Metadata != nil {
		if raw, err := json.Marshal(req.Metadata); err == nil {
			item.Metadata = datatypes.JSON(raw)
		}
	}

	if err := db.Save(&item).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update content item"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true})
}

// InternalUpdateContentStatus handles PATCH /internal/content-items/:id/status
func InternalUpdateContentStatus(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	publicID := c.Param("id")
	id, err := uuid.Parse(publicID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid content ID"})
		return
	}

	var req internalUpdateStatusRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	if req.Status == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Status is required"})
		return
	}

	var item models.ContentItem
	if err := db.Where("public_id = ?", id).First(&item).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Content not found"})
		return
	}

	item.Status = models.ContentStatus(strings.ToUpper(req.Status))

	if req.FailureReason != nil {
		metadata := map[string]interface{}{}
		if len(item.Metadata) > 0 {
			_ = json.Unmarshal(item.Metadata, &metadata)
		}
		metadata["failure_reason"] = *req.FailureReason
		if raw, err := json.Marshal(metadata); err == nil {
			item.Metadata = datatypes.JSON(raw)
		}
	}

	if err := db.Save(&item).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update status"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true})
}

// InternalUpdateContentArtifacts handles PATCH /internal/content-items/:id/artifacts
func InternalUpdateContentArtifacts(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	publicID := c.Param("id")
	id, err := uuid.Parse(publicID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid content ID"})
		return
	}

	var req internalUpdateArtifactsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	var item models.ContentItem
	if err := db.Where("public_id = ?", id).First(&item).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Content not found"})
		return
	}

	if req.MediaURL != nil {
		item.MediaURL = req.MediaURL
	}
	if req.ThumbnailURL != nil {
		item.ThumbnailURL = req.ThumbnailURL
	}
	if req.DurationSec != nil {
		item.DurationSec = req.DurationSec
	}

	if err := db.Save(&item).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update artifacts"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true})
}

// InternalUpdateContentEmbedding handles PATCH /internal/content-items/:id/embedding
func InternalUpdateContentEmbedding(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	publicID := c.Param("id")
	id, err := uuid.Parse(publicID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid content ID"})
		return
	}

	var req internalUpdateEmbeddingRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	if len(req.Embedding) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Embedding is required"})
		return
	}

	var item models.ContentItem
	if err := db.Where("public_id = ?", id).First(&item).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Content not found"})
		return
	}

	vec := pgvector.NewVector(req.Embedding)
	item.Embedding = &vec
	if len(req.TopicTags) > 0 {
		item.TopicTags = req.TopicTags
	}

	if err := db.Save(&item).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update embedding"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true})
}

// InternalLinkTranscript handles PATCH /internal/content-items/:id/transcript
func InternalLinkTranscript(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	publicID := c.Param("id")
	id, err := uuid.Parse(publicID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid content ID"})
		return
	}

	var req internalLinkTranscriptRequest
	if err := c.ShouldBindJSON(&req); err != nil || req.TranscriptID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "transcript_id is required"})
		return
	}

	transcriptUUID, err := uuid.Parse(req.TranscriptID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid transcript ID"})
		return
	}

	var item models.ContentItem
	if err := db.Where("public_id = ?", id).First(&item).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Content not found"})
		return
	}

	item.TranscriptID = &transcriptUUID

	if err := db.Save(&item).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to link transcript"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true})
}
