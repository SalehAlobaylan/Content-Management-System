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
	TotalMedia            int64 `json:"total_media"`
	WithTranscript        int64 `json:"with_transcript"`
	MissingTranscript     int64 `json:"missing_transcript"`
	WithEmbedding         int64 `json:"with_embedding"`
	MissingEmbedding      int64 `json:"missing_embedding"`
	WithSparse            int64 `json:"with_sparse"`
	MissingSparse         int64 `json:"missing_sparse"`
	WithImageEmbedding    int64 `json:"with_image_embedding"`
	MissingImageEmbedding int64 `json:"missing_image_embedding"`
	TotalReady            int64 `json:"total_ready"`
}

type missingEnrichmentItem struct {
	ID                string `json:"id"`
	Title             string `json:"title"`
	Type              string `json:"type"`
	SourceName        string `json:"source_name"`
	Status            string `json:"status"`
	HasTranscript     bool   `json:"has_transcript"`
	HasEmbedding      bool   `json:"has_embedding"`
	HasSparse         bool   `json:"has_sparse"`
	HasImageEmbedding bool   `json:"has_image_embedding"`
	MediaURL          string `json:"media_url"`
	ThumbnailURL      string `json:"thumbnail_url"`
	CreatedAt         string `json:"created_at"`
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
			COUNT(*) FILTER (WHERE embedding_sparse IS NOT NULL) as with_sparse,
			COUNT(*) FILTER (WHERE embedding IS NOT NULL AND embedding_sparse IS NULL AND status = 'READY') as missing_sparse,
			COUNT(*) FILTER (WHERE image_embedding IS NOT NULL) as with_image_embedding,
			COUNT(*) FILTER (WHERE thumbnail_url IS NOT NULL AND image_embedding IS NULL AND status = 'READY') as missing_image_embedding,
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
		&stats.WithSparse,
		&stats.MissingSparse,
		&stats.WithImageEmbedding,
		&stats.MissingImageEmbedding,
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
		// Accept a comma-separated list so a panel can scope to several types
		// at once (e.g. the Videos panel queries VIDEO,PODCAST together).
		types := strings.Split(contentType, ",")
		for i := range types {
			types[i] = strings.TrimSpace(types[i])
		}
		query = query.Where("type IN ?", types)
	}

	// Filter by what's missing
	conditions := []string{}
	for _, mt := range missingTypes {
		switch strings.TrimSpace(mt) {
		case "transcript":
			conditions = append(conditions, "transcript_id IS NULL AND type IN ('VIDEO','PODCAST')")
		case "embedding":
			conditions = append(conditions, "embedding IS NULL")
		case "sparse":
			// Has a dense vector but no sparse lexical weights.
			conditions = append(conditions, "embedding IS NOT NULL AND embedding_sparse IS NULL")
		case "image":
			// Has a thumbnail but no CLIP image embedding.
			conditions = append(conditions, "image_embedding IS NULL AND thumbnail_url IS NOT NULL")
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
			ID:                item.PublicID.String(),
			Type:              string(item.Type),
			Status:            string(item.Status),
			HasTranscript:     item.TranscriptID != nil,
			HasEmbedding:      item.Embedding != nil,
			HasSparse:         item.EmbeddingSparse != nil,
			HasImageEmbedding: item.ImageEmbedding != nil,
			CreatedAt:         item.CreatedAt.Format("2006-01-02T15:04:05Z"),
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
		if item.ThumbnailURL != nil {
			resp.ThumbnailURL = *item.ThumbnailURL
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
			// extract_sparse=true → populates dense + sparse together.
			if err := triggerEmbedding(text, item.PublicID.String(), true); err != nil {
				errors = append(errors, "embedding: "+err.Error())
			} else {
				results = append(results, "embedding: triggered")
			}

		case "sparse":
			if item.EmbeddingSparse != nil {
				results = append(results, "sparse: already exists")
				continue
			}
			text := buildEmbeddingText(&item)
			if text == "" {
				errors = append(errors, "sparse: no text content available")
				continue
			}
			// Re-embed with sparse on — re-writes dense too (harmless, same value).
			if err := triggerEmbedding(text, item.PublicID.String(), true); err != nil {
				errors = append(errors, "sparse: "+err.Error())
			} else {
				results = append(results, "sparse: triggered")
			}

		case "image":
			if item.ImageEmbedding != nil {
				results = append(results, "image: already exists")
				continue
			}
			if item.ThumbnailURL == nil || *item.ThumbnailURL == "" {
				errors = append(errors, "image: no thumbnail_url available")
				continue
			}
			if err := triggerImageEmbedding(*item.ThumbnailURL, item.PublicID.String()); err != nil {
				errors = append(errors, "image: "+err.Error())
			} else {
				results = append(results, "image: triggered")
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
				if err := triggerEmbedding(text, item.PublicID.String(), true); err != nil {
					itemErrors = append(itemErrors, "embedding: "+err.Error())
				}

			case "sparse":
				if item.EmbeddingSparse != nil {
					continue // already has sparse
				}
				text := buildEmbeddingText(&item)
				if text == "" {
					itemErrors = append(itemErrors, "no text for sparse")
					continue
				}
				if err := triggerEmbedding(text, item.PublicID.String(), true); err != nil {
					itemErrors = append(itemErrors, "sparse: "+err.Error())
				}

			case "image":
				if item.ImageEmbedding != nil {
					continue // already has image embedding
				}
				if item.ThumbnailURL == nil || *item.ThumbnailURL == "" {
					itemErrors = append(itemErrors, "no thumbnail_url")
					continue
				}
				if err := triggerImageEmbedding(*item.ThumbnailURL, item.PublicID.String()); err != nil {
					itemErrors = append(itemErrors, "image: "+err.Error())
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
//
// After the Media-Service split, this endpoint reports the health of BOTH
// AI services so Platform-Console can render a unified card. The legacy
// top-level "status"/"models"/"dependencies" fields are preserved for the
// Enrichment-Service so older console builds keep working; the new
// "services" map is the canonical shape.

func GetEnrichmentServiceHealth(c *gin.Context) {
	type perServiceView struct {
		Status       string          `json:"status"`
		Error        string          `json:"error,omitempty"`
		Models       map[string]bool `json:"models"`
		Dependencies map[string]bool `json:"dependencies,omitempty"`
	}

	view := func(h *serviceHealthResponse, err error) perServiceView {
		if err != nil {
			return perServiceView{Status: "unreachable", Error: err.Error()}
		}
		return perServiceView{
			Status:       h.Status,
			Models:       h.Models,
			Dependencies: h.Dependencies,
		}
	}

	enrichment := view(checkEnrichmentHealth())
	media := view(checkMediaHealth())

	// Aggregate top-level status: ok only when both are ok.
	overallStatus := "ok"
	if enrichment.Status != "ok" || media.Status != "ok" {
		overallStatus = "not_ready"
	}

	// Legacy fields surface the Enrichment-Service so the existing Console
	// build keeps rendering even before the dual-service UI update lands.
	c.JSON(http.StatusOK, utils.ResponseMessage{
		Code:    http.StatusOK,
		Message: "AI services status",
		Data: gin.H{
			"status":       overallStatus,
			"models":       enrichment.Models,
			"dependencies": enrichment.Dependencies,
			"services": gin.H{
				"media":      media,
				"enrichment": enrichment,
			},
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
