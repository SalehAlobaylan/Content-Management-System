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

	"github.com/gin-gonic/gin"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// createAdminContentRequest is the payload for the News-rotation manual-create
// flow. Only title + original_url are required; everything else has an
// editorial default (READY ARTICLE from a "Manual" source).
type createAdminContentRequest struct {
	Title        string                 `json:"title"`
	Excerpt      *string                `json:"excerpt"`
	BodyText     *string                `json:"body_text"`
	OriginalURL  string                 `json:"original_url"`
	SourceName   *string                `json:"source_name"`
	Author       *string                `json:"author"`
	ThumbnailURL *string                `json:"thumbnail_url"`
	Type         string                 `json:"type"`
	Status       string                 `json:"status"`
	TopicTags    []string               `json:"topic_tags"`
	PublishedAt  *string                `json:"published_at"`
	Metadata     map[string]interface{} `json:"metadata"`
}

// CreateAdminContent handles POST /admin/content.
//
// Admins (News rotation) create an editorial item directly — there is no source
// to scrape. We mirror the InternalCreateContentItem create logic but scope the
// row to the caller's tenant, force source=MANUAL, default to a published
// (READY) ARTICLE, and kick off a fire-and-forget embedding so the News slide's
// related-items semantic search works for hand-authored articles too.
func CreateAdminContent(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}

	db := c.MustGet("db").(*gorm.DB)

	var req createAdminContentRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "Invalid request",
			Code:    "INVALID_REQUEST",
		})
		return
	}

	title := strings.TrimSpace(req.Title)
	originalURL := strings.TrimSpace(req.OriginalURL)
	if title == "" || originalURL == "" {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "title and original_url are required",
			Code:    "MISSING_FIELDS",
		})
		return
	}

	contentType := models.ContentTypeArticle
	if t := strings.ToUpper(strings.TrimSpace(req.Type)); t != "" {
		contentType = models.ContentType(t)
	}

	status := models.ContentStatusReady
	if s := strings.ToUpper(strings.TrimSpace(req.Status)); s != "" {
		status = models.ContentStatus(s)
	}

	sourceName := "Manual"
	if req.SourceName != nil && strings.TrimSpace(*req.SourceName) != "" {
		sourceName = strings.TrimSpace(*req.SourceName)
	}

	// Deterministic idempotency key keyed on the URL so an accidental
	// double-submit of the same article doesn't create duplicate rows.
	sum := sha256.Sum256([]byte("manual:" + strings.ToLower(originalURL)))
	idempotencyKey := "manual:" + hex.EncodeToString(sum[:])

	var existing models.ContentItem
	if err := db.Where("idempotency_key = ? AND tenant_id = ?", idempotencyKey, principal.TenantID).
		First(&existing).Error; err == nil {
		// Already created for this URL — return the existing row so the UI is
		// idempotent rather than erroring.
		c.JSON(http.StatusOK, mapAdminContentItemResponse(existing))
		return
	} else if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		c.JSON(http.StatusInternalServerError, authErrorResponse{
			Message: "Failed to check idempotency",
			Code:    "IDEMPOTENCY_CHECK_FAILED",
		})
		return
	}

	// Default published_at to now so new editorial items surface at the top of
	// the chronological News feed immediately.
	var publishedAt *time.Time
	if req.PublishedAt != nil && strings.TrimSpace(*req.PublishedAt) != "" {
		if parsed, err := time.Parse(time.RFC3339, *req.PublishedAt); err == nil {
			publishedAt = &parsed
		}
	}
	if publishedAt == nil {
		now := time.Now().UTC()
		publishedAt = &now
	}

	metadataJSON, _ := json.Marshal(req.Metadata)

	item := models.ContentItem{
		TenantID:       principal.TenantID,
		Type:           contentType,
		Source:         models.SourceTypeManual,
		Status:         status,
		IdempotencyKey: &idempotencyKey,
		Title:          &title,
		BodyText:       req.BodyText,
		Excerpt:        req.Excerpt,
		Author:         req.Author,
		SourceName:     &sourceName,
		ThumbnailURL:   req.ThumbnailURL,
		OriginalURL:    &originalURL,
		TopicTags:      req.TopicTags,
		Metadata:       datatypes.JSON(metadataJSON),
		PublishedAt:    publishedAt,
	}

	if err := db.Create(&item).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{
			Message: "Failed to create content item",
			Code:    "CREATE_FAILED",
		})
		return
	}

	// Fire-and-forget embedding. Only meaningful once the item is READY; a
	// failure here is non-fatal because the News slide falls back to
	// date-ordered related items when an article has no embedding.
	if status == models.ContentStatusReady {
		if text := buildEmbeddingText(&item); strings.TrimSpace(text) != "" {
			id := item.PublicID.String()
			go func() {
				_ = triggerEmbedding(text, id, true)
			}()
		}
	}

	c.JSON(http.StatusCreated, mapAdminContentItemResponse(item))
}

// extractContentURLRequest is the payload for POST /admin/content/extract-url.
type extractContentURLRequest struct {
	URL string `json:"url"`
}

// ExtractContentURL handles POST /admin/content/extract-url.
//
// Thin proxy to Enrichment-Service's stealth web extraction (POST /v1/extract).
// It only reads the page and returns title/excerpt/body/image so the News
// "Add by URL" tab can prefill the compose form — nothing is written. The admin
// reviews the result then publishes via POST /admin/content.
func ExtractContentURL(c *gin.Context) {
	if _, ok := requireAdminPrincipal(c); !ok {
		return
	}

	var req extractContentURLRequest
	if err := c.ShouldBindJSON(&req); err != nil || strings.TrimSpace(req.URL) == "" {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "url is required",
			Code:    "URL_REQUIRED",
		})
		return
	}

	result, err := extractURLViaEnrichment(strings.TrimSpace(req.URL))
	if err != nil {
		c.JSON(http.StatusBadGateway, authErrorResponse{
			Message: "Failed to extract URL: " + err.Error(),
			Code:    "EXTRACT_FAILED",
		})
		return
	}

	c.JSON(http.StatusOK, result)
}
