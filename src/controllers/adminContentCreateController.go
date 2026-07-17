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

// parseFlexibleTime parses a timestamp from the common web/feed formats
// (RFC3339, RSS pubDate / RFC1123, plain dates).
func parseFlexibleTime(s string) *time.Time {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	for _, layout := range []string{
		time.RFC3339,
		time.RFC1123Z,
		time.RFC1123,
		"2006-01-02T15:04:05Z07:00",
		"2006-01-02 15:04:05",
		"2006-01-02",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return &t
		}
	}
	return nil
}

// createManualArticle creates — or returns the existing — manual content item
// for a URL, keyed by a deterministic idempotency key. Returns (item, created,
// err). Newly-created READY items get a fire-and-forget embedding, which in turn
// triggers first-class topic classification. Shared by single create + feed import.
func createManualArticle(db *gorm.DB, tenantID string, req createAdminContentRequest) (models.ContentItem, bool, error) {
	title := strings.TrimSpace(req.Title)
	originalURL := strings.TrimSpace(req.OriginalURL)
	if title == "" || originalURL == "" {
		return models.ContentItem{}, false, errors.New("title and original_url are required")
	}

	// Default to a NEWS article. Legacy/explicit ARTICLE|TWEET|COMMENT fold into
	// the NEWS kind with a format sub-classification; VIDEO/PODCAST/NEWS pass
	// through with no format.
	contentType := models.ContentTypeNews
	defaultFormat := string(models.ContentFormatArticle)
	contentFormat := &defaultFormat
	if t := strings.ToUpper(strings.TrimSpace(req.Type)); t != "" {
		switch models.ContentType(t) {
		case models.ContentTypeArticle, models.ContentTypeTweet, models.ContentTypeComment:
			f := t
			contentFormat = &f
		case models.ContentTypeNews:
			// keep the default article format
		default:
			contentType = models.ContentType(t)
			contentFormat = nil
		}
	}
	status := models.ContentStatusReady
	if s := strings.ToUpper(strings.TrimSpace(req.Status)); s != "" {
		status = models.ContentStatus(s)
	}
	sourceName := "Manual"
	if req.SourceName != nil && strings.TrimSpace(*req.SourceName) != "" {
		sourceName = strings.TrimSpace(*req.SourceName)
	}

	sum := sha256.Sum256([]byte("manual:" + strings.ToLower(originalURL)))
	idempotencyKey := "manual:" + hex.EncodeToString(sum[:])

	var existing models.ContentItem
	if err := db.Where("idempotency_key = ? AND tenant_id = ?", idempotencyKey, tenantID).
		First(&existing).Error; err == nil {
		return existing, false, nil
	} else if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return models.ContentItem{}, false, err
	}

	pub := ""
	if req.PublishedAt != nil {
		pub = *req.PublishedAt
	}
	publishedAt := parseFlexibleTime(pub)
	if publishedAt == nil {
		now := time.Now().UTC()
		publishedAt = &now
	}

	metadataJSON, _ := json.Marshal(req.Metadata)

	item := models.ContentItem{
		TenantID:       tenantID,
		Type:           contentType,
		Format:         contentFormat,
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
		return models.ContentItem{}, false, err
	}

	if status == models.ContentStatusReady {
		if text := buildEmbeddingText(&item); strings.TrimSpace(text) != "" {
			id := item.PublicID.String()
			go func() {
				_ = triggerEmbedding(text, id)
			}()
		}
	}
	return item, true, nil
}

// CreateAdminContent handles POST /admin/content — manual single-article create.
func CreateAdminContent(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	var req createAdminContentRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid request", Code: "INVALID_REQUEST"})
		return
	}
	if strings.TrimSpace(req.Title) == "" || strings.TrimSpace(req.OriginalURL) == "" {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "title and original_url are required", Code: "MISSING_FIELDS"})
		return
	}

	item, created, err := createManualArticle(db, principal.TenantID, req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to create content item: " + err.Error(), Code: "CREATE_FAILED"})
		return
	}
	if !created {
		c.JSON(http.StatusOK, mapAdminContentItemResponse(item))
		return
	}
	c.JSON(http.StatusCreated, mapAdminContentItemResponse(item))
}

type importFeedRequest struct {
	URL    string `json:"url"`
	Status string `json:"status"`
}

type importFeedResponse struct {
	IsFeed   bool   `json:"is_feed"`
	Imported int    `json:"imported"`
	Skipped  int    `json:"skipped"`
	Total    int    `json:"total"`
	SiteName string `json:"site_name"`
}

func optStr(s string) *string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return &s
}

// ImportFeed handles POST /admin/content/import-feed — fetch an RSS/Atom feed
// (stealth, via Enrichment) and bulk-create a content item for EVERY item.
// Deduplicated by article URL; created READY items auto-classify into topics.
func ImportFeed(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	var req importFeedRequest
	if err := c.ShouldBindJSON(&req); err != nil || strings.TrimSpace(req.URL) == "" {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "url is required", Code: "URL_REQUIRED"})
		return
	}

	feed, err := extractFeedViaEnrichment(strings.TrimSpace(req.URL))
	if err != nil {
		c.JSON(http.StatusBadGateway, authErrorResponse{Message: "Failed to read feed: " + err.Error(), Code: "FEED_EXTRACT_FAILED"})
		return
	}

	status := strings.ToUpper(strings.TrimSpace(req.Status))
	if status == "" {
		status = string(models.ContentStatusReady)
	}

	imported, skipped := 0, 0
	for _, it := range feed.Items {
		title := strings.TrimSpace(it.Title)
		itemURL := strings.TrimSpace(it.URL)
		if title == "" || itemURL == "" {
			skipped++
			continue
		}
		cr := createAdminContentRequest{
			Title:        title,
			OriginalURL:  itemURL,
			Excerpt:      optStr(it.Excerpt),
			BodyText:     optStr(it.Text),
			ThumbnailURL: optStr(it.ImageURL),
			Author:       optStr(it.Author),
			SourceName:   optStr(feed.SiteName),
			PublishedAt:  optStr(it.PublishedAt),
			Type:         string(models.ContentTypeArticle),
			Status:       status,
		}
		if _, wasCreated, cerr := createManualArticle(db, principal.TenantID, cr); cerr != nil || !wasCreated {
			skipped++
		} else {
			imported++
		}
	}

	c.JSON(http.StatusOK, importFeedResponse{
		IsFeed:   feed.IsFeed,
		Imported: imported,
		Skipped:  skipped,
		Total:    len(feed.Items),
		SiteName: feed.SiteName,
	})
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
