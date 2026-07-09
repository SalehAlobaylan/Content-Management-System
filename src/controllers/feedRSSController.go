package controllers

import (
	"content-management-system/src/models"
	"encoding/json"
	"encoding/xml"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// ─── Normalized feed item + meta (format-agnostic) ──────────

type feedItem struct {
	ID          string
	Title       string
	Link        string
	Description string
	Author      string
	Published   time.Time
	Categories  []string
}

type feedMeta struct {
	Title       string
	Description string
	SelfURL     string
}

type feedQuery struct {
	TenantID    string // "" = all tenants (public ad-hoc); set for saved feeds
	StoryID     string // first-class topic UUID, or ""
	Topic       string // legacy free-form tag, or ""
	ContentType string
	Limit       int
}

// fetchFeedItems pulls READY content for a feed, newest first, normalized into
// format-agnostic feedItems shared by the RSS/Atom/JSON renderers.
func fetchFeedItems(db *gorm.DB, q feedQuery) ([]feedItem, error) {
	limit := q.Limit
	if limit <= 0 || limit > 200 {
		limit = 50
	}

	query := db.Model(&models.ContentItem{}).
		Where("status = ?", models.ContentStatusReady).
		Order("published_at DESC NULLS LAST, created_at DESC").
		Limit(limit)

	if q.TenantID != "" {
		query = query.Where("tenant_id = ?", q.TenantID)
	}
	if q.ContentType != "" {
		query = query.Where("type = ?", models.ContentType(strings.ToUpper(q.ContentType)))
	}
	if q.StoryID != "" {
		query = query.Where("story_id = ?", q.StoryID)
	}
	if q.Topic != "" {
		query = query.Where("? = ANY(topic_tags)", q.Topic)
	}

	var items []models.ContentItem
	if err := query.Find(&items).Error; err != nil {
		return nil, err
	}

	out := make([]feedItem, 0, len(items))
	for _, it := range items {
		fi := feedItem{ID: it.PublicID.String(), Title: "Untitled", Published: it.CreatedAt}
		if it.Title != nil && strings.TrimSpace(*it.Title) != "" {
			fi.Title = *it.Title
		}
		if it.Excerpt != nil && strings.TrimSpace(*it.Excerpt) != "" {
			fi.Description = *it.Excerpt
		} else if it.BodyText != nil {
			fi.Description = *it.BodyText
		}
		if it.OriginalURL != nil {
			fi.Link = *it.OriginalURL
		} else if it.MediaURL != nil {
			fi.Link = *it.MediaURL
		}
		if it.PublishedAt != nil {
			fi.Published = *it.PublishedAt
		}
		if it.Author != nil {
			fi.Author = strings.TrimSpace(*it.Author)
		}
		for _, t := range it.TopicTags {
			if strings.TrimSpace(t) != "" {
				fi.Categories = append(fi.Categories, t)
			}
		}
		out = append(out, fi)
	}
	return out, nil
}

// publicBaseURL is the absolute origin for building feed self-links. Prefers the
// PUBLIC_BASE_URL env (e.g. https://cms.salehspace.dev), else the request host.
func publicBaseURL(c *gin.Context) string {
	if b := strings.TrimRight(strings.TrimSpace(os.Getenv("PUBLIC_BASE_URL")), "/"); b != "" {
		return b
	}
	scheme := "https"
	if c.Request.TLS == nil {
		scheme = "http"
	}
	return scheme + "://" + strings.TrimRight(c.Request.Host, "/")
}

func selfURL(c *gin.Context) string {
	return publicBaseURL(c) + c.Request.URL.RequestURI()
}

// ─── RSS 2.0 ────────────────────────────────────────────────

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

func renderRSS(c *gin.Context, meta feedMeta, items []feedItem) {
	ch := rssChannel{
		Title:       meta.Title,
		Link:        meta.SelfURL,
		Description: meta.Description,
		Language:    "en",
		LastBuild:   time.Now().UTC().Format(time.RFC1123Z),
	}
	for _, fi := range items {
		e := rssItem{
			Title:       fi.Title,
			Link:        fi.Link,
			GUID:        rssGUID{IsPermaLink: false, Value: fi.ID},
			Description: fi.Description,
			PubDate:     fi.Published.Format(time.RFC1123Z),
		}
		if fi.Author != "" {
			e.Author = fi.Author
		}
		for _, cat := range fi.Categories {
			e.Category = append(e.Category, rssCategory{Value: cat})
		}
		ch.Items = append(ch.Items, e)
	}
	doc := rssDocument{Version: "2.0", AtomNS: "http://www.w3.org/2005/Atom", Channel: ch}
	c.Header("Content-Type", "application/rss+xml; charset=utf-8")
	c.XML(http.StatusOK, doc)
}

// ─── Atom 1.0 ───────────────────────────────────────────────

type atomFeed struct {
	XMLName  xml.Name    `xml:"feed"`
	Xmlns    string      `xml:"xmlns,attr"`
	Title    string      `xml:"title"`
	Subtitle string      `xml:"subtitle,omitempty"`
	ID       string      `xml:"id"`
	Updated  string      `xml:"updated"`
	Links    []atomLink  `xml:"link"`
	Entries  []atomEntry `xml:"entry"`
}

type atomLink struct {
	Href string `xml:"href,attr"`
	Rel  string `xml:"rel,attr,omitempty"`
}

type atomEntry struct {
	Title     string      `xml:"title"`
	ID        string      `xml:"id"`
	Updated   string      `xml:"updated"`
	Published string      `xml:"published,omitempty"`
	Link      atomLink    `xml:"link"`
	Summary   string      `xml:"summary,omitempty"`
	Author    *atomAuthor `xml:"author,omitempty"`
}

type atomAuthor struct {
	Name string `xml:"name"`
}

func renderAtom(c *gin.Context, meta feedMeta, items []feedItem) {
	f := atomFeed{
		Xmlns:    "http://www.w3.org/2005/Atom",
		Title:    meta.Title,
		Subtitle: meta.Description,
		ID:       meta.SelfURL,
		Updated:  time.Now().UTC().Format(time.RFC3339),
		Links:    []atomLink{{Href: meta.SelfURL, Rel: "self"}},
	}
	for _, fi := range items {
		e := atomEntry{
			Title:     fi.Title,
			ID:        "urn:uuid:" + fi.ID,
			Updated:   fi.Published.UTC().Format(time.RFC3339),
			Published: fi.Published.UTC().Format(time.RFC3339),
			Link:      atomLink{Href: fi.Link},
			Summary:   fi.Description,
		}
		if fi.Author != "" {
			e.Author = &atomAuthor{Name: fi.Author}
		}
		f.Entries = append(f.Entries, e)
	}
	c.Header("Content-Type", "application/atom+xml; charset=utf-8")
	c.XML(http.StatusOK, f)
}

// ─── JSON Feed 1.1 ──────────────────────────────────────────

type jsonFeed struct {
	Version     string         `json:"version"`
	Title       string         `json:"title"`
	Description string         `json:"description,omitempty"`
	FeedURL     string         `json:"feed_url,omitempty"`
	Items       []jsonFeedItem `json:"items"`
}

type jsonFeedItem struct {
	ID            string           `json:"id"`
	URL           string           `json:"url,omitempty"`
	Title         string           `json:"title"`
	ContentText   string           `json:"content_text,omitempty"`
	DatePublished string           `json:"date_published,omitempty"`
	Authors       []jsonFeedAuthor `json:"authors,omitempty"`
	Tags          []string         `json:"tags,omitempty"`
}

type jsonFeedAuthor struct {
	Name string `json:"name"`
}

func renderJSONFeed(c *gin.Context, meta feedMeta, items []feedItem) {
	jf := jsonFeed{
		Version:     "https://jsonfeed.org/version/1.1",
		Title:       meta.Title,
		Description: meta.Description,
		FeedURL:     meta.SelfURL,
		Items:       make([]jsonFeedItem, 0, len(items)),
	}
	for _, fi := range items {
		ji := jsonFeedItem{
			ID:            fi.ID,
			URL:           fi.Link,
			Title:         fi.Title,
			ContentText:   fi.Description,
			DatePublished: fi.Published.UTC().Format(time.RFC3339),
		}
		if fi.Author != "" {
			ji.Authors = []jsonFeedAuthor{{Name: fi.Author}}
		}
		if len(fi.Categories) > 0 {
			ji.Tags = fi.Categories
		}
		jf.Items = append(jf.Items, ji)
	}
	body, err := json.Marshal(jf)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"message": "failed to build feed"})
		return
	}
	c.Data(http.StatusOK, "application/feed+json; charset=utf-8", body)
}

// ─── Ad-hoc public feeds (power per-topic feeds) ────────────

// adhocFeedData builds items + meta from query params shared by all 3 formats.
func adhocFeedData(c *gin.Context) ([]feedItem, feedMeta, bool) {
	db := c.MustGet("db").(*gorm.DB)

	q := feedQuery{
		StoryID:     strings.TrimSpace(c.Query("story_id")),
		Topic:       strings.TrimSpace(c.Query("topic")),
		ContentType: strings.TrimSpace(c.Query("type")),
	}
	if raw := strings.TrimSpace(c.Query("limit")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil {
			q.Limit = n
		}
	}

	items, err := fetchFeedItems(db, q)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"message": "failed to build feed"})
		return nil, feedMeta{}, false
	}

	title := strings.TrimSpace(c.Query("title"))
	if title == "" {
		title = "Wahb Content Feed"
		if q.StoryID != "" {
			var t models.Story
			if db.Select("label").Where("public_id = ?", q.StoryID).First(&t).Error == nil &&
				strings.TrimSpace(t.Label) != "" {
				title = "Wahb · " + t.Label
			}
		}
	}

	return items, feedMeta{
		Title:       title,
		Description: "Latest published content from the Wahb platform.",
		SelfURL:     selfURL(c),
	}, true
}

// GetRSSFeed handles GET /api/v1/feed/rss.xml?story_id=&type=&limit=&title=
func GetRSSFeed(c *gin.Context) {
	items, meta, ok := adhocFeedData(c)
	if !ok {
		return
	}
	renderRSS(c, meta, items)
}

// GetAtomFeed handles GET /api/v1/feed/atom.xml (same params as RSS).
func GetAtomFeed(c *gin.Context) {
	items, meta, ok := adhocFeedData(c)
	if !ok {
		return
	}
	renderAtom(c, meta, items)
}

// GetJSONFeed handles GET /api/v1/feed/feed.json (same params as RSS).
func GetJSONFeed(c *gin.Context) {
	items, meta, ok := adhocFeedData(c)
	if !ok {
		return
	}
	renderJSONFeed(c, meta, items)
}

// ─── Saved feeds ────────────────────────────────────────────

// GetSavedFeed handles GET /api/v1/feed/saved/:slug?format=rss|atom|json.
func GetSavedFeed(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	slug := strings.TrimSpace(c.Param("slug"))

	var feed models.RSSFeed
	if err := db.Where("slug = ? AND enabled = ?", slug, true).First(&feed).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"message": "feed not found"})
		return
	}

	q := feedQuery{
		TenantID:    feed.TenantID,
		ContentType: feed.ContentType,
		Limit:       feed.ItemLimit,
	}
	if feed.StoryID != nil {
		q.StoryID = feed.StoryID.String()
	}

	items, err := fetchFeedItems(db, q)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"message": "failed to build feed"})
		return
	}

	title := strings.TrimSpace(feed.Title)
	if title == "" {
		title = feed.Name
	}
	meta := feedMeta{Title: title, Description: feed.Description, SelfURL: selfURL(c)}

	switch strings.ToLower(strings.TrimSpace(c.Query("format"))) {
	case "atom":
		renderAtom(c, meta, items)
	case "json":
		renderJSONFeed(c, meta, items)
	default:
		renderRSS(c, meta, items)
	}
}
