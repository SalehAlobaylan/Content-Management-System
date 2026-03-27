package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// ── Response types ──────────────────────────────────────────

type enrichmentStatsResponse struct {
	TotalMedia        int64 `json:"total_media"`
	WithTranscript    int64 `json:"with_transcript"`
	MissingTranscript int64 `json:"missing_transcript"`
	WithEmbedding     int64 `json:"with_embedding"`
	MissingEmbedding  int64 `json:"missing_embedding"`
	TotalReady        int64 `json:"total_ready"`
}

type missingEnrichmentItem struct {
	ID            string `json:"id"`
	Title         string `json:"title"`
	Type          string `json:"type"`
	SourceName    string `json:"source_name"`
	Status        string `json:"status"`
	HasTranscript bool   `json:"has_transcript"`
	HasEmbedding  bool   `json:"has_embedding"`
	MediaURL      string `json:"media_url"`
	CreatedAt     string `json:"created_at"`
}

type triggerEnrichmentRequest struct {
	Types []string `json:"types" binding:"required"`
}

type triggerBatchRequest struct {
	ContentIDs []string `json:"content_ids" binding:"required"`
	Types      []string `json:"types" binding:"required"`
}

type triggerResultItem struct {
	ContentID string `json:"content_id"`
	Status    string `json:"status"`
	Error     string `json:"error,omitempty"`
}

// ── GET /admin/enrichment/stats ─────────────────────────────

func GetEnrichmentStats(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	var stats enrichmentStatsResponse

	// Single efficient query using PostgreSQL FILTER
	row := db.Raw(`
		SELECT
			COUNT(*) FILTER (WHERE type IN ('VIDEO','PODCAST')) as total_media,
			COUNT(*) FILTER (WHERE type IN ('VIDEO','PODCAST') AND transcript_id IS NOT NULL) as with_transcript,
			COUNT(*) FILTER (WHERE type IN ('VIDEO','PODCAST') AND transcript_id IS NULL AND status = 'READY') as missing_transcript,
			COUNT(*) FILTER (WHERE embedding IS NOT NULL) as with_embedding,
			COUNT(*) FILTER (WHERE embedding IS NULL AND status = 'READY') as missing_embedding,
			COUNT(*) FILTER (WHERE status = 'READY') as total_ready
		FROM content_items
		WHERE status != 'ARCHIVED'
	`).Row()

	if err := row.Scan(
		&stats.TotalMedia,
		&stats.WithTranscript,
		&stats.MissingTranscript,
		&stats.WithEmbedding,
		&stats.MissingEmbedding,
		&stats.TotalReady,
	); err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{
			Code:    http.StatusInternalServerError,
			Message: "Failed to fetch enrichment stats: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, utils.ResponseMessage{
		Code:    http.StatusOK,
		Message: "Enrichment stats fetched successfully",
		Data:    stats,
	})
}

// ── GET /admin/enrichment/missing ───────────────────────────

func GetMissingEnrichments(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	// Parse query params
	missingParam := c.DefaultQuery("missing", "transcript")
	contentType := c.Query("type")
	status := c.DefaultQuery("status", "READY")
	limitStr := c.DefaultQuery("limit", "50")
	offsetStr := c.DefaultQuery("offset", "0")

	limit, _ := strconv.Atoi(limitStr)
	offset, _ := strconv.Atoi(offsetStr)
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}

	missingTypes := strings.Split(missingParam, ",")

	// Build query
	query := db.Model(&models.ContentItem{}).Where("status != ?", "ARCHIVED")

	if status != "" {
		query = query.Where("status = ?", status)
	}

	if contentType != "" {
		query = query.Where("type = ?", contentType)
	}

	// Filter by what's missing
	conditions := []string{}
	for _, mt := range missingTypes {
		switch strings.TrimSpace(mt) {
		case "transcript":
			conditions = append(conditions, "transcript_id IS NULL AND type IN ('VIDEO','PODCAST')")
		case "embedding":
			conditions = append(conditions, "embedding IS NULL")
		}
	}

	if len(conditions) > 0 {
		combined := "(" + strings.Join(conditions, " OR ") + ")"
		query = query.Where(combined)
	}

	// Count total
	var total int64
	query.Count(&total)

	// Fetch items
	var items []models.ContentItem
	query.Order("created_at DESC").Limit(limit).Offset(offset).Find(&items)

	// Map to response
	responseItems := make([]missingEnrichmentItem, 0, len(items))
	for _, item := range items {
		resp := missingEnrichmentItem{
			ID:            item.PublicID.String(),
			Type:          string(item.Type),
			Status:        string(item.Status),
			HasTranscript: item.TranscriptID != nil,
			HasEmbedding:  item.Embedding != nil,
			CreatedAt:     item.CreatedAt.Format("2006-01-02T15:04:05Z"),
		}
		if item.Title != nil {
			resp.Title = *item.Title
		}
		if item.SourceName != nil {
			resp.SourceName = *item.SourceName
		}
		if item.MediaURL != nil {
			resp.MediaURL = *item.MediaURL
		}
		responseItems = append(responseItems, resp)
	}

	c.JSON(http.StatusOK, utils.ResponseMessage{
		Code:    http.StatusOK,
		Message: "Missing enrichments fetched successfully",
		Data: gin.H{
			"items":  responseItems,
			"total":  total,
			"limit":  limit,
			"offset": offset,
		},
	})
}

// ── POST /admin/enrichment/trigger/:id ──────────────────────

func TriggerEnrichment(c *gin.Context) {
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

	var req triggerEnrichmentRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code:    http.StatusBadRequest,
			Message: "Invalid request body: " + err.Error(),
		})
		return
	}

	// Look up content item
	var item models.ContentItem
	if err := db.Where("public_id = ?", contentID).First(&item).Error; err != nil {
		c.JSON(http.StatusNotFound, utils.HTTPError{
			Code:    http.StatusNotFound,
			Message: "Content item not found",
		})
		return
	}

	results := make([]string, 0)
	errors := make([]string, 0)

	for _, enrichType := range req.Types {
		switch enrichType {
		case "transcript":
			if item.TranscriptID != nil {
				results = append(results, "transcript: already exists")
				continue
			}
			if item.Type != models.ContentTypeVideo && item.Type != models.ContentTypePodcast {
				results = append(results, "transcript: skipped (not VIDEO/PODCAST)")
				continue
			}
			if item.MediaURL == nil || *item.MediaURL == "" {
				errors = append(errors, "transcript: no media_url available")
				continue
			}
			if err := triggerTranscription(*item.MediaURL, item.PublicID.String()); err != nil {
				errors = append(errors, "transcript: "+err.Error())
			} else {
				results = append(results, "transcript: triggered")
			}

		case "embedding":
			if item.Embedding != nil {
				results = append(results, "embedding: already exists")
				continue
			}
			// Build text for embedding from available content
			text := buildEmbeddingText(&item)
			if text == "" {
				errors = append(errors, "embedding: no text content available")
				continue
			}
			if err := triggerEmbedding(text, item.PublicID.String()); err != nil {
				errors = append(errors, "embedding: "+err.Error())
			} else {
				results = append(results, "embedding: triggered")
			}
		}
	}

	c.JSON(http.StatusOK, utils.ResponseMessage{
		Code:    http.StatusOK,
		Message: "Enrichment trigger completed",
		Data: gin.H{
			"content_id": contentIDStr,
			"results":    results,
			"errors":     errors,
		},
	})
}

// ── POST /admin/enrichment/trigger-batch ────────────────────

func TriggerBatchEnrichment(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	var req triggerBatchRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code:    http.StatusBadRequest,
			Message: "Invalid request body: " + err.Error(),
		})
		return
	}

	// Cap at 10 items per batch to prevent overload
	if len(req.ContentIDs) > 10 {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code:    http.StatusBadRequest,
			Message: "Maximum 10 items per batch request",
		})
		return
	}

	results := make([]triggerResultItem, 0, len(req.ContentIDs))

	for _, idStr := range req.ContentIDs {
		contentID, err := uuid.Parse(idStr)
		if err != nil {
			results = append(results, triggerResultItem{ContentID: idStr, Status: "error", Error: "invalid ID"})
			continue
		}

		var item models.ContentItem
		if err := db.Where("public_id = ?", contentID).First(&item).Error; err != nil {
			results = append(results, triggerResultItem{ContentID: idStr, Status: "error", Error: "not found"})
			continue
		}

		itemErrors := []string{}
		for _, enrichType := range req.Types {
			switch enrichType {
			case "transcript":
				if item.TranscriptID != nil {
					continue // already has transcript
				}
				if item.Type != models.ContentTypeVideo && item.Type != models.ContentTypePodcast {
					continue // not applicable
				}
				if item.MediaURL == nil || *item.MediaURL == "" {
					itemErrors = append(itemErrors, "no media_url")
					continue
				}
				if err := triggerTranscription(*item.MediaURL, item.PublicID.String()); err != nil {
					itemErrors = append(itemErrors, "transcript: "+err.Error())
				}

			case "embedding":
				if item.Embedding != nil {
					continue // already has embedding
				}
				text := buildEmbeddingText(&item)
				if text == "" {
					itemErrors = append(itemErrors, "no text for embedding")
					continue
				}
				if err := triggerEmbedding(text, item.PublicID.String()); err != nil {
					itemErrors = append(itemErrors, "embedding: "+err.Error())
				}
			}
		}

		if len(itemErrors) > 0 {
			results = append(results, triggerResultItem{
				ContentID: idStr,
				Status:    "partial",
				Error:     strings.Join(itemErrors, "; "),
			})
		} else {
			results = append(results, triggerResultItem{ContentID: idStr, Status: "triggered"})
		}
	}

	c.JSON(http.StatusOK, utils.ResponseMessage{
		Code:    http.StatusOK,
		Message: "Batch enrichment completed",
		Data:    results,
	})
}

// ── GET /admin/enrichment/health ────────────────────────────

func GetEnrichmentServiceHealth(c *gin.Context) {
	health, err := checkEnrichmentHealth()
	if err != nil {
		c.JSON(http.StatusOK, utils.ResponseMessage{
			Code:    http.StatusOK,
			Message: "Enrichment service status",
			Data: gin.H{
				"status":  "unreachable",
				"error":   err.Error(),
				"models":  nil,
			},
		})
		return
	}

	c.JSON(http.StatusOK, utils.ResponseMessage{
		Code:    http.StatusOK,
		Message: "Enrichment service status",
		Data: gin.H{
			"status":       health.Status,
			"models":       health.Models,
			"dependencies": health.Dependencies,
		},
	})
}

// ── Helpers ─────────────────────────────────────────────────

// buildEmbeddingText constructs a text string from content item fields for embedding.
func buildEmbeddingText(item *models.ContentItem) string {
	parts := []string{}
	if item.Title != nil && *item.Title != "" {
		parts = append(parts, *item.Title)
	}
	if item.Excerpt != nil && *item.Excerpt != "" {
		parts = append(parts, *item.Excerpt)
	}
	if item.BodyText != nil && *item.BodyText != "" {
		// Truncate body text to first 500 runes for embedding (UTF-8 safe)
		body := *item.BodyText
		runes := []rune(body)
		if len(runes) > 500 {
			body = string(runes[:500])
		}
		parts = append(parts, body)
	}
	return strings.Join(parts, " ")
}
