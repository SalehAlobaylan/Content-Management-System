package controllers

import (
	"content-management-system/src/models"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type rssFeedResponse struct {
	models.RSSFeed
	RSSURL  string `json:"rss_url"`
	AtomURL string `json:"atom_url"`
	JSONURL string `json:"json_url"`
}

type rssFeedListResponse struct {
	Data       []rssFeedResponse `json:"data"`
	PublicBase string            `json:"public_base"`
}

var slugNonWord = regexp.MustCompile(`[^a-z0-9]+`)

func slugify(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = slugNonWord.ReplaceAllString(s, "-")
	s = strings.Trim(s, "-")
	if len(s) > 120 {
		s = strings.Trim(s[:120], "-")
	}
	return s
}

// publicBaseFor returns the absolute origin for feed links — PUBLIC_BASE_URL
// env if set, else the request host (works for direct CMS calls in dev).
func publicBaseFor(c *gin.Context) string {
	if b := strings.TrimRight(strings.TrimSpace(os.Getenv("PUBLIC_BASE_URL")), "/"); b != "" {
		return b
	}
	return publicBaseURL(c)
}

func feedToResponse(base string, f models.RSSFeed) rssFeedResponse {
	saved := base + "/api/v1/feed/saved/" + f.Slug
	return rssFeedResponse{
		RSSFeed: f,
		RSSURL:  saved,
		AtomURL: saved + "?format=atom",
		JSONURL: saved + "?format=json",
	}
}

// uniqueFeedSlug ensures the slug is unique within the tenant, appending -2,-3…
// excludeID skips a feed's own row (used on update).
func uniqueFeedSlug(db *gorm.DB, tenant, base string, excludeID *uuid.UUID) string {
	if base == "" {
		base = "feed-" + uuid.NewString()[:8]
	}
	slug := base
	for i := 2; ; i++ {
		q := db.Model(&models.RSSFeed{}).Where("tenant_id = ? AND slug = ?", tenant, slug)
		if excludeID != nil {
			q = q.Where("public_id <> ?", *excludeID)
		}
		var count int64
		q.Count(&count)
		if count == 0 {
			return slug
		}
		slug = base + "-" + strconv.Itoa(i)
	}
}

func clampLimit(n int) int {
	if n <= 0 || n > 200 {
		return 50
	}
	return n
}

// ListRSSFeeds handles GET /admin/feeds.
func ListRSSFeeds(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	var feeds []models.RSSFeed
	if err := db.Where("tenant_id = ?", principal.TenantID).
		Order("created_at DESC").Find(&feeds).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to list feeds", Code: "FEEDS_LIST_FAILED"})
		return
	}

	base := publicBaseFor(c)
	data := make([]rssFeedResponse, 0, len(feeds))
	for _, f := range feeds {
		data = append(data, feedToResponse(base, f))
	}
	c.JSON(http.StatusOK, rssFeedListResponse{Data: data, PublicBase: base})
}

type createRSSFeedRequest struct {
	Name        string  `json:"name"`
	Title       string  `json:"title"`
	Description string  `json:"description"`
	TopicID     *string `json:"topic_id"`
	ContentType string  `json:"content_type"`
	ItemLimit   int     `json:"item_limit"`
	Slug        string  `json:"slug"`
}

// CreateRSSFeed handles POST /admin/feeds.
func CreateRSSFeed(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	var req createRSSFeedRequest
	if err := c.ShouldBindJSON(&req); err != nil || strings.TrimSpace(req.Name) == "" {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "name is required", Code: "NAME_REQUIRED"})
		return
	}

	feed := models.RSSFeed{
		TenantID:    principal.TenantID,
		Name:        strings.TrimSpace(req.Name),
		Title:       strings.TrimSpace(req.Title),
		Description: strings.TrimSpace(req.Description),
		ContentType: strings.ToUpper(strings.TrimSpace(req.ContentType)),
		ItemLimit:   clampLimit(req.ItemLimit),
		Enabled:     true,
	}
	if req.TopicID != nil {
		if tid, err := uuid.Parse(strings.TrimSpace(*req.TopicID)); err == nil {
			feed.TopicID = &tid
		}
	}
	slugBase := req.Slug
	if strings.TrimSpace(slugBase) == "" {
		slugBase = req.Name
	}
	feed.Slug = uniqueFeedSlug(db, principal.TenantID, slugify(slugBase), nil)

	if err := db.Create(&feed).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to create feed: " + err.Error(), Code: "FEED_CREATE_FAILED"})
		return
	}
	c.JSON(http.StatusCreated, feedToResponse(publicBaseFor(c), feed))
}

type updateRSSFeedRequest struct {
	Name        *string `json:"name"`
	Title       *string `json:"title"`
	Description *string `json:"description"`
	TopicID     *string `json:"topic_id"` // "" / "null" clears; uuid sets
	ContentType *string `json:"content_type"`
	ItemLimit   *int    `json:"item_limit"`
	Enabled     *bool   `json:"enabled"`
	Slug        *string `json:"slug"`
}

// UpdateRSSFeed handles PUT /admin/feeds/:id.
func UpdateRSSFeed(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid feed id", Code: "INVALID_ID"})
		return
	}

	var feed models.RSSFeed
	if err := db.Where("public_id = ? AND tenant_id = ?", id, principal.TenantID).First(&feed).Error; err != nil {
		c.JSON(http.StatusNotFound, authErrorResponse{Message: "Feed not found", Code: "NOT_FOUND"})
		return
	}

	var req updateRSSFeedRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid request", Code: "INVALID_REQUEST"})
		return
	}

	updates := map[string]interface{}{}
	if req.Name != nil {
		updates["name"] = strings.TrimSpace(*req.Name)
	}
	if req.Title != nil {
		updates["title"] = strings.TrimSpace(*req.Title)
	}
	if req.Description != nil {
		updates["description"] = strings.TrimSpace(*req.Description)
	}
	if req.ContentType != nil {
		updates["content_type"] = strings.ToUpper(strings.TrimSpace(*req.ContentType))
	}
	if req.ItemLimit != nil {
		updates["item_limit"] = clampLimit(*req.ItemLimit)
	}
	if req.Enabled != nil {
		updates["enabled"] = *req.Enabled
	}
	if req.TopicID != nil {
		v := strings.TrimSpace(*req.TopicID)
		if v == "" || strings.EqualFold(v, "null") {
			updates["topic_id"] = nil
		} else if tid, perr := uuid.Parse(v); perr == nil {
			updates["topic_id"] = tid
		}
	}
	if req.Slug != nil {
		updates["slug"] = uniqueFeedSlug(db, principal.TenantID, slugify(*req.Slug), &id)
	}

	if len(updates) > 0 {
		if err := db.Model(&models.RSSFeed{}).
			Where("public_id = ? AND tenant_id = ?", id, principal.TenantID).
			Updates(updates).Error; err != nil {
			c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to update feed: " + err.Error(), Code: "FEED_UPDATE_FAILED"})
			return
		}
	}

	db.Where("public_id = ? AND tenant_id = ?", id, principal.TenantID).First(&feed)
	c.JSON(http.StatusOK, feedToResponse(publicBaseFor(c), feed))
}

// DeleteRSSFeed handles DELETE /admin/feeds/:id.
func DeleteRSSFeed(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid feed id", Code: "INVALID_ID"})
		return
	}

	res := db.Where("public_id = ? AND tenant_id = ?", id, principal.TenantID).Delete(&models.RSSFeed{})
	if res.Error != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to delete feed", Code: "FEED_DELETE_FAILED"})
		return
	}
	if res.RowsAffected == 0 {
		c.JSON(http.StatusNotFound, authErrorResponse{Message: "Feed not found", Code: "NOT_FOUND"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true})
}
