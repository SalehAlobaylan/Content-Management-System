package controllers

import (
	"content-management-system/src/models"
	"encoding/xml"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type rssDocument struct {
	XMLName xml.Name   `xml:"rss"`
	Version string     `xml:"version,attr"`
	AtomNS  string     `xml:"xmlns:atom,attr,omitempty"`
	Channel rssChannel `xml:"channel"`
}

type rssChannel struct {
	Title       string    `xml:"title"`
	Link        string    `xml:"link"`
	Description string    `xml:"description"`
	Language    string    `xml:"language,omitempty"`
	LastBuild   string    `xml:"lastBuildDate,omitempty"`
	Items       []rssItem `xml:"item"`
}

type rssItem struct {
	Title       string        `xml:"title"`
	Link        string        `xml:"link,omitempty"`
	GUID        rssGUID       `xml:"guid"`
	Description string        `xml:"description,omitempty"`
	PubDate     string        `xml:"pubDate,omitempty"`
	Author      string        `xml:"author,omitempty"`
	Category    []rssCategory `xml:"category,omitempty"`
}

type rssGUID struct {
	IsPermaLink bool   `xml:"isPermaLink,attr"`
	Value       string `xml:",chardata"`
}

type rssCategory struct {
	Value string `xml:",chardata"`
}

// GetRSSFeed returns a public RSS 2.0 feed for READY content.
// GET /api/v1/feed/rss.xml?type=ARTICLE&topic=tech&limit=50
func GetRSSFeed(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	contentType := strings.TrimSpace(strings.ToUpper(c.Query("type")))
	topic := strings.TrimSpace(c.Query("topic"))
	limit := 50
	if raw := strings.TrimSpace(c.Query("limit")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil {
			if parsed > 0 && parsed <= 200 {
				limit = parsed
			}
		}
	}

	query := db.Model(&models.ContentItem{}).
		Where("status = ?", models.ContentStatusReady).
		Order("published_at DESC NULLS LAST, created_at DESC").
		Limit(limit)

	if contentType != "" {
		query = query.Where("type = ?", models.ContentType(contentType))
	}
	if topic != "" {
		query = query.Where("? = ANY(topic_tags)", topic)
	}

	var items []models.ContentItem
	if err := query.Find(&items).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"message": "failed to build rss feed",
		})
		return
	}

	channelItems := make([]rssItem, 0, len(items))
	for _, item := range items {
		title := "Untitled"
		if item.Title != nil && strings.TrimSpace(*item.Title) != "" {
			title = *item.Title
		}

		description := ""
		if item.Excerpt != nil {
			description = *item.Excerpt
		} else if item.BodyText != nil {
			description = *item.BodyText
		}

		link := ""
		if item.OriginalURL != nil {
			link = *item.OriginalURL
		} else if item.MediaURL != nil {
			link = *item.MediaURL
		}

		publishedAt := item.CreatedAt
		if item.PublishedAt != nil {
			publishedAt = *item.PublishedAt
		}

		rssEntry := rssItem{
			Title:       title,
			Link:        link,
			GUID:        rssGUID{IsPermaLink: false, Value: item.PublicID.String()},
			Description: description,
			PubDate:     publishedAt.Format(time.RFC1123Z),
		}
		if item.Author != nil && strings.TrimSpace(*item.Author) != "" {
			rssEntry.Author = *item.Author
		}
		for _, tag := range item.TopicTags {
			if strings.TrimSpace(tag) == "" {
				continue
			}
			rssEntry.Category = append(rssEntry.Category, rssCategory{Value: tag})
		}

		channelItems = append(channelItems, rssEntry)
	}

	baseLink := strings.TrimRight(c.Request.Host, "/")
	scheme := "https"
	if c.Request.TLS == nil {
		scheme = "http"
	}
	channelLink := scheme + "://" + baseLink + "/api/v1/feed/rss.xml"

	doc := rssDocument{
		Version: "2.0",
		AtomNS:  "http://www.w3.org/2005/Atom",
		Channel: rssChannel{
			Title:       "Wahb Content Feed",
			Link:        channelLink,
			Description: "Latest READY content from the Wahb platform.",
			Language:    "en",
			LastBuild:   time.Now().UTC().Format(time.RFC1123Z),
			Items:       channelItems,
		},
	}

	c.Header("Content-Type", "application/rss+xml; charset=utf-8")
	c.XML(http.StatusOK, doc)
}
