package controllers

import (
	"bytes"
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type adminSourceListResponse struct {
	Data       []contentSourceResponse `json:"data"`
	Total      int64                   `json:"total"`
	Page       int                     `json:"page"`
	Limit      int                     `json:"limit"`
	TotalPages int                     `json:"total_pages"`
}

type contentSourceResponse struct {
	ID                   string          `json:"id"`
	Name                 string          `json:"name"`
	Type                 string          `json:"type"`
	Category             string          `json:"category"`
	FeedURL              *string         `json:"feed_url,omitempty"`
	ImageURL             *string         `json:"image_url,omitempty"`
	APIConfig            json.RawMessage `json:"api_config,omitempty"`
	IsActive             bool            `json:"is_active"`
	FetchIntervalMinutes int             `json:"fetch_interval_minutes"`
	LastFetchedAt        *string         `json:"last_fetched_at,omitempty"`
	Metadata             json.RawMessage `json:"metadata,omitempty"`
	CreatedAt            string          `json:"created_at"`
	UpdatedAt            string          `json:"updated_at"`
}

type createContentSourceRequest struct {
	Name                 string                 `json:"name"`
	Type                 string                 `json:"type"`
	Category             *string                `json:"category,omitempty"`
	FeedURL              *string                `json:"feed_url,omitempty"`
	ImageURL             *string                `json:"image_url,omitempty"`
	APIConfig            map[string]interface{} `json:"api_config,omitempty"`
	IsActive             *bool                  `json:"is_active,omitempty"`
	FetchIntervalMinutes *int                   `json:"fetch_interval_minutes,omitempty"`
	Metadata             map[string]interface{} `json:"metadata,omitempty"`
}

type updateContentSourceRequest struct {
	Name                 *string                `json:"name,omitempty"`
	Type                 *string                `json:"type,omitempty"`
	Category             *string                `json:"category,omitempty"`
	FeedURL              *string                `json:"feed_url,omitempty"`
	ImageURL             *string                `json:"image_url,omitempty"`
	APIConfig            map[string]interface{} `json:"api_config,omitempty"`
	IsActive             *bool                  `json:"is_active,omitempty"`
	FetchIntervalMinutes *int                   `json:"fetch_interval_minutes,omitempty"`
	Metadata             map[string]interface{} `json:"metadata,omitempty"`
}

type bulkCreateContentSourcesRequest struct {
	Sources []createContentSourceRequest `json:"sources"`
}

type bulkCreateFailure struct {
	Index   int    `json:"index"`
	Name    string `json:"name,omitempty"`
	Message string `json:"message"`
}

type bulkCreateContentSourcesResponse struct {
	Created  []contentSourceResponse `json:"created"`
	Failed   []bulkCreateFailure     `json:"failed"`
	Total    int                     `json:"total"`
	Accepted int                     `json:"accepted"`
}

type runSourceResponse struct {
	Message string `json:"message"`
	JobID   string `json:"job_id,omitempty"`
}

type discoverFeedsRequest struct {
	URL string `json:"url"`
}

type discoverFeedItem struct {
	URL   string `json:"url"`
	Title string `json:"title,omitempty"`
	Type  string `json:"type"`
}

type discoverFeedsResponse struct {
	Success bool               `json:"success"`
	Feeds   []discoverFeedItem `json:"feeds"`
	Message string             `json:"message"`
}

type previewSourceRequest struct {
	SourceType string                 `json:"sourceType"`
	URL        string                 `json:"url"`
	Name       string                 `json:"name,omitempty"`
	Settings   map[string]interface{} `json:"settings,omitempty"`
	Limit      int                    `json:"limit,omitempty"`
}

type previewSourceItem struct {
	IdempotencyKey string  `json:"idempotencyKey"`
	Type           string  `json:"type"`
	Title          string  `json:"title"`
	Excerpt        *string `json:"excerpt,omitempty"`
	Author         *string `json:"author,omitempty"`
	OriginalURL    string  `json:"originalUrl"`
	PublishedAt    *string `json:"publishedAt,omitempty"`
	// Rich media metadata (carried through from the aggregation preview) so the
	// "Add media source" wizard can show thumbnails + durations.
	ThumbnailURL *string `json:"thumbnailUrl,omitempty"`
	DurationSec  *int    `json:"durationSec,omitempty"`
}

// --- Media-add discovery proxies (podcast search, youtube resolve) -----------

type podcastSearchResult struct {
	ID       int64  `json:"id"`
	Name     string `json:"name"`
	FeedURL  string `json:"feed_url"`
	ImageURL string `json:"image_url,omitempty"`
}

type podcastSearchResponse struct {
	Results []podcastSearchResult `json:"results"`
}

type itunesAggResponse struct {
	Results []struct {
		CollectionID   int64  `json:"collectionId"`
		CollectionName string `json:"collectionName"`
		FeedURL        string `json:"feedUrl"`
		ArtworkURL600  string `json:"artworkUrl600"`
	} `json:"results"`
}

type youtubeResolveResponse struct {
	ChannelID       string `json:"channel_id"`
	Title           string `json:"title"`
	Thumbnail       string `json:"thumbnail,omitempty"`
	SubscriberCount int64  `json:"subscriber_count,omitempty"`
}

type youtubeAggResponse struct {
	ChannelID       string `json:"channelId"`
	Title           string `json:"title"`
	Thumbnail       string `json:"thumbnail"`
	SubscriberCount int64  `json:"subscriberCount"`
}

type previewSourceResponse struct {
	Success    bool                `json:"success"`
	Message    string              `json:"message"`
	Fetched    int                 `json:"fetched"`
	Normalized int                 `json:"normalized"`
	Skipped    int                 `json:"skipped"`
	Errors     int                 `json:"errors"`
	Items      []previewSourceItem `json:"items"`
}

type aggregationTriggerRequest struct {
	SourceType string                 `json:"sourceType"`
	URL        string                 `json:"url"`
	Name       string                 `json:"name,omitempty"`
	Settings   map[string]interface{} `json:"settings,omitempty"`
	// SourceID is the source's public UUID. Aggregation uses it as the run's
	// sourceId so fetch/normalize telemetry reports back to source_run_telemetry
	// (the workers skip reporting for non-UUID synthetic ids).
	SourceID string `json:"sourceId,omitempty"`
}

type aggregationTriggerResponse struct {
	Success bool   `json:"success"`
	JobID   string `json:"jobId,omitempty"`
	Message string `json:"message"`
}

var contentSourceQueryConfig = utils.QueryConfig{
	DefaultLimit: 20,
	MaxLimit:     100,
	DefaultSort: []utils.SortParam{{
		Field:     "created_at",
		Direction: "desc",
	}},
	SortableFields: map[string]string{
		"created_at":      "content_sources.created_at",
		"updated_at":      "content_sources.updated_at",
		"name":            "content_sources.name",
		"type":            "content_sources.type",
		"last_fetched_at": "content_sources.last_fetched_at",
	},
	FilterableFields: map[string]string{
		"name":       "content_sources.name",
		"type":       "content_sources.type",
		"category":   "content_sources.category",
		"is_active":  "content_sources.is_active",
		"created_at": "content_sources.created_at",
		"updated_at": "content_sources.updated_at",
	},
	SearchableFields: map[string]string{
		"name":     "content_sources.name",
		"feed_url": "content_sources.feed_url",
	},
	DefaultSearchFields: []string{"name", "feed_url"},
	FieldDefaultOperators: map[string]string{
		"name":     "contains",
		"feed_url": "contains",
	},
}

// ListContentSources handles GET /admin/sources
func ListContentSources(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}

	db := c.MustGet("db").(*gorm.DB)

	limitParam := c.Query("limit")
	pageParam := c.Query("page")
	fmt.Printf("[DEBUG] ListContentSources: limit=%s, page=%s\n", limitParam, pageParam)

	params, err := utils.ParseQueryParams(c, contentSourceQueryConfig)
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: err.Error(),
			Code:    "INVALID_QUERY",
		})
		return
	}
	fmt.Printf("[DEBUG] ListContentSources: parsed params.Page=%d, params.Limit=%d\n", params.Pagination.Page, params.Pagination.Limit)

	query := db.Model(&models.ContentSource{}).Where("tenant_id = ?", principal.TenantID)
	query = utils.ApplyQuery(query, params, contentSourceQueryConfig)

	var sources []models.ContentSource
	meta, err := utils.FetchWithPagination(query, params, &sources)
	if err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{
			Message: "Failed to fetch sources",
			Code:    "FETCH_FAILED",
		})
		return
	}

	data := make([]contentSourceResponse, 0, len(sources))
	for _, source := range sources {
		data = append(data, mapContentSourceResponse(source))
	}

	c.JSON(http.StatusOK, adminSourceListResponse{
		Data:       data,
		Total:      meta.Total,
		Page:       meta.Page,
		Limit:      meta.Limit,
		TotalPages: meta.TotalPages,
	})
}

// --- Source monitoring stats -------------------------------------------------

type sourceFreshness struct {
	OverdueCount  int64   `json:"overdue_count"`
	DueSoonCount  int64   `json:"due_soon_count"`
	FreshCount    int64   `json:"fresh_count"`
	NeverRunCount int64   `json:"never_run_count"`
	OldestFetched *string `json:"oldest_fetched_at"`
}

type sourceOutputStat struct {
	Name       string  `json:"name"`
	Type       string  `json:"type"`
	Category   string  `json:"category"`
	Items      int64   `json:"items"`
	Ready      int64   `json:"ready"`
	Failed     int64   `json:"failed"`
	LastItemAt *string `json:"last_item_at,omitempty"`
}

type sourceAttention struct {
	ID            string  `json:"id"`
	Name          string  `json:"name"`
	Type          string  `json:"type"`
	Category      string  `json:"category"`
	Health        string  `json:"health"`
	LastFetchedAt *string `json:"last_fetched_at,omitempty"`
}

type sourceStatsResponse struct {
	Total          int64                       `json:"total"`
	ByCategory     map[string]int64            `json:"by_category"`
	ByType         map[string]int64            `json:"by_type"`
	ByStatus       map[string]int64            `json:"by_status"`
	ByHealth       map[string]int64            `json:"by_health"`
	ByTypeHealth   map[string]map[string]int64 `json:"by_type_health"`
	Freshness      sourceFreshness             `json:"freshness"`
	TopSources     []sourceOutputStat          `json:"top_sources"`
	RecentFailures []sourceAttention           `json:"recent_failures"`
}

// computeSourceHealth mirrors the frontend sourceHealth helper
// (Platform-Console/src/lib/sources/health.ts) so the dashboard and the source
// list agree on what "stale" means.
func computeSourceHealth(s models.ContentSource, now time.Time) string {
	if !s.IsActive {
		return "disabled"
	}
	if s.LastFetchedAt == nil {
		return "never_run"
	}
	interval := time.Duration(s.FetchIntervalMinutes) * time.Minute
	if now.Sub(*s.LastFetchedAt) > interval {
		return "stale"
	}
	return "healthy"
}

func isMediaConsoleSource(s models.ContentSource) bool {
	if s.Category == models.SourceCategoryMedia {
		return true
	}
	if s.Type != models.SourceTypeTelegram || len(s.APIConfig) == 0 {
		return false
	}
	var cfg map[string]interface{}
	if err := json.Unmarshal(s.APIConfig, &cfg); err != nil {
		return false
	}
	rawTypes, ok := cfg["media_types"].([]interface{})
	if !ok {
		return false
	}
	for _, raw := range rawTypes {
		kind, ok := raw.(string)
		if !ok {
			continue
		}
		switch strings.ToLower(kind) {
		case "audio", "voice", "video":
			return true
		}
	}
	return false
}

// GetSourceStats handles GET /admin/sources/stats. It returns source-centric
// aggregates powering the Sources monitoring dashboard. Honors an optional
// `category` filter (news|media) so the dashboard can scope All / News / Media.
// Health is derived (not stored) so it tracks the same rules as the list UI.
func GetSourceStats(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}

	db := c.MustGet("db").(*gorm.DB)
	categoryFilter := strings.ToLower(strings.TrimSpace(c.Query("category")))

	resp, err := buildSourceStats(db, principal.TenantID, categoryFilter)
	if err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{
			Message: "Failed to fetch source stats",
			Code:    "STATS_FAILED",
		})
		return
	}

	c.JSON(http.StatusOK, resp)
}

func buildSourceStats(db *gorm.DB, tenantID string, categoryFilter string) (sourceStatsResponse, error) {
	now := time.Now()
	categoryFilter = strings.ToLower(strings.TrimSpace(categoryFilter))

	query := db.Model(&models.ContentSource{}).Where("tenant_id = ?", tenantID)
	if categoryFilter != "" && categoryFilter != models.SourceCategoryMedia {
		query = query.Where("category = ?", categoryFilter)
	}

	var sources []models.ContentSource
	if err := query.Find(&sources).Error; err != nil {
		return sourceStatsResponse{}, err
	}
	if categoryFilter == models.SourceCategoryMedia {
		filtered := make([]models.ContentSource, 0, len(sources))
		for _, s := range sources {
			if isMediaConsoleSource(s) {
				filtered = append(filtered, s)
			}
		}
		sources = filtered
	}

	resp := sourceStatsResponse{
		Total:          int64(len(sources)),
		ByCategory:     map[string]int64{models.SourceCategoryNews: 0, models.SourceCategoryMedia: 0},
		ByType:         map[string]int64{},
		ByStatus:       map[string]int64{"active": 0, "disabled": 0},
		ByHealth:       map[string]int64{"healthy": 0, "stale": 0, "never_run": 0, "disabled": 0},
		ByTypeHealth:   map[string]map[string]int64{},
		TopSources:     []sourceOutputStat{},
		RecentFailures: []sourceAttention{},
	}

	// names maps a source name to its meta (type/category) for enriching the
	// per-source output query, which joins content_items by source_name.
	type sourceMeta struct {
		Type     string
		Category string
	}
	nameMeta := make(map[string]sourceMeta, len(sources))
	names := make([]string, 0, len(sources))
	var oldestFetched *time.Time
	// Sources needing attention (active but stale / never run), oldest first.
	type attentionRow struct {
		src    models.ContentSource
		health string
		ageKey int64 // larger = needs attention sooner; sentinel for never_run
	}
	var attention []attentionRow

	for _, s := range sources {
		typeStr := string(s.Type)
		resp.ByType[typeStr]++

		cat := s.Category
		if cat == "" {
			cat = models.SourceCategoryNews
		}
		if categoryFilter == models.SourceCategoryMedia && isMediaConsoleSource(s) {
			cat = models.SourceCategoryMedia
		}
		resp.ByCategory[cat]++

		if s.IsActive {
			resp.ByStatus["active"]++
		} else {
			resp.ByStatus["disabled"]++
		}

		health := computeSourceHealth(s, now)
		resp.ByHealth[health]++
		if resp.ByTypeHealth[typeStr] == nil {
			resp.ByTypeHealth[typeStr] = map[string]int64{}
		}
		resp.ByTypeHealth[typeStr][health]++

		// Freshness buckets (active sources only).
		switch health {
		case "never_run":
			resp.Freshness.NeverRunCount++
		case "stale":
			resp.Freshness.OverdueCount++
		case "healthy":
			// Due soon = within the last quarter of the fetch interval.
			interval := time.Duration(s.FetchIntervalMinutes) * time.Minute
			if s.LastFetchedAt != nil && now.Sub(*s.LastFetchedAt) > interval*3/4 {
				resp.Freshness.DueSoonCount++
			} else {
				resp.Freshness.FreshCount++
			}
		}

		if s.LastFetchedAt != nil && (oldestFetched == nil || s.LastFetchedAt.Before(*oldestFetched)) {
			oldestFetched = s.LastFetchedAt
		}

		if s.Name != "" {
			if _, seen := nameMeta[s.Name]; !seen {
				names = append(names, s.Name)
			}
			nameMeta[s.Name] = sourceMeta{Type: typeStr, Category: cat}
		}

		if s.IsActive && (health == "stale" || health == "never_run") {
			ageKey := int64(1) << 62 // never_run sorts to the top
			if s.LastFetchedAt != nil {
				ageKey = now.Sub(*s.LastFetchedAt).Milliseconds()
			}
			attention = append(attention, attentionRow{src: s, health: health, ageKey: ageKey})
		}
	}

	if oldestFetched != nil {
		iso := oldestFetched.UTC().Format(time.RFC3339)
		resp.Freshness.OldestFetched = &iso
	}

	// Top sources by output — join content_items on source_name (the established
	// convention; content_items carry no source FK). When a category filter is
	// active we restrict to that category's source names so the join stays scoped.
	type outputRow struct {
		SourceName string
		Count      int64
		Ready      int64
		Failed     int64
		LastItemAt *time.Time
	}
	itemQuery := db.Model(&models.ContentItem{}).Where("tenant_id = ?", tenantID)
	skipTop := false
	if categoryFilter != "" {
		if len(names) == 0 {
			skipTop = true
		} else {
			itemQuery = itemQuery.Where("source_name IN ?", names)
		}
	}
	if !skipTop {
		var rows []outputRow
		if err := itemQuery.
			Select("COALESCE(NULLIF(source_name, ''), 'Unknown source') AS source_name, COUNT(*) AS count, COALESCE(SUM(CASE WHEN status = 'READY' THEN 1 ELSE 0 END), 0) AS ready, COALESCE(SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END), 0) AS failed, MAX(created_at) AS last_item_at").
			Group("COALESCE(NULLIF(source_name, ''), 'Unknown source')").
			Order("count DESC").
			Limit(10).
			Scan(&rows).Error; err != nil {
			return sourceStatsResponse{}, err
		}
		for _, r := range rows {
			stat := sourceOutputStat{
				Name:   r.SourceName,
				Items:  r.Count,
				Ready:  r.Ready,
				Failed: r.Failed,
			}
			if meta, ok := nameMeta[r.SourceName]; ok {
				stat.Type = meta.Type
				stat.Category = meta.Category
			}
			if r.LastItemAt != nil {
				iso := r.LastItemAt.UTC().Format(time.RFC3339)
				stat.LastItemAt = &iso
			}
			resp.TopSources = append(resp.TopSources, stat)
		}
	}

	// Recent failures / needs attention — oldest unfetched first, capped small.
	sort.Slice(attention, func(i, j int) bool { return attention[i].ageKey > attention[j].ageKey })
	for i, a := range attention {
		if i >= 6 {
			break
		}
		row := sourceAttention{
			ID:       a.src.PublicID.String(),
			Name:     a.src.Name,
			Type:     string(a.src.Type),
			Category: a.src.Category,
			Health:   a.health,
		}
		if a.src.LastFetchedAt != nil {
			iso := a.src.LastFetchedAt.UTC().Format(time.RFC3339)
			row.LastFetchedAt = &iso
		}
		resp.RecentFailures = append(resp.RecentFailures, row)
	}

	return resp, nil
}

// GetContentSource handles GET /admin/sources/:id
func GetContentSource(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}

	db := c.MustGet("db").(*gorm.DB)
	publicID := c.Param("id")
	id, err := uuid.Parse(publicID)
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "Invalid source ID",
			Code:    "INVALID_ID",
		})
		return
	}

	var source models.ContentSource
	if err := db.Where("public_id = ? AND tenant_id = ?", id, principal.TenantID).First(&source).Error; err != nil {
		c.JSON(http.StatusNotFound, authErrorResponse{
			Message: "Source not found",
			Code:    "NOT_FOUND",
		})
		return
	}

	c.JSON(http.StatusOK, mapContentSourceResponse(source))
}

// CreateContentSource handles POST /admin/sources
func CreateContentSource(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}

	db := c.MustGet("db").(*gorm.DB)
	var req createContentSourceRequest

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "Invalid request",
			Code:    "INVALID_REQUEST",
		})
		return
	}

	name := strings.TrimSpace(req.Name)
	if name == "" {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "Name is required",
			Code:    "NAME_REQUIRED",
		})
		return
	}

	sourceType := strings.TrimSpace(req.Type)
	if sourceType == "" {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "Type is required",
			Code:    "TYPE_REQUIRED",
		})
		return
	}

	apiConfig, err := mapToJSON(req.APIConfig)
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "Invalid api_config",
			Code:    "INVALID_API_CONFIG",
		})
		return
	}
	metadata, err := mapToJSON(req.Metadata)
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "Invalid metadata",
			Code:    "INVALID_METADATA",
		})
		return
	}

	isActive := true
	if req.IsActive != nil {
		isActive = *req.IsActive
	}

	fetchInterval := 60
	if req.FetchIntervalMinutes != nil {
		if *req.FetchIntervalMinutes <= 0 {
			c.JSON(http.StatusBadRequest, authErrorResponse{
				Message: "Fetch interval must be greater than zero",
				Code:    "INVALID_FETCH_INTERVAL",
			})
			return
		}
		fetchInterval = *req.FetchIntervalMinutes
	}

	resolvedType := models.SourceType(strings.ToUpper(sourceType))
	category := models.DefaultCategoryForType(resolvedType)
	if req.Category != nil && strings.TrimSpace(*req.Category) != "" {
		category = strings.ToLower(strings.TrimSpace(*req.Category))
	}

	source := models.ContentSource{
		TenantID:             principal.TenantID,
		Name:                 name,
		Type:                 resolvedType,
		Category:             category,
		FeedURL:              req.FeedURL,
		ImageURL:             sourceImageURL(req.ImageURL, req.FeedURL),
		APIConfig:            apiConfig,
		IsActive:             isActive,
		FetchIntervalMinutes: fetchInterval,
		Metadata:             metadata,
	}

	if err := db.Create(&source).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{
			Message: "Failed to create source",
			Code:    "CREATE_FAILED",
		})
		return
	}

	c.JSON(http.StatusCreated, mapContentSourceResponse(source))
}

// BulkCreateContentSources handles POST /admin/sources/bulk
func BulkCreateContentSources(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}

	db := c.MustGet("db").(*gorm.DB)
	var req bulkCreateContentSourcesRequest
	if err := c.ShouldBindJSON(&req); err != nil || len(req.Sources) == 0 {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "sources array is required",
			Code:    "INVALID_BULK_REQUEST",
		})
		return
	}

	created := make([]contentSourceResponse, 0, len(req.Sources))
	failed := make([]bulkCreateFailure, 0)

	for index, sourceReq := range req.Sources {
		name := strings.TrimSpace(sourceReq.Name)
		sourceType := strings.TrimSpace(sourceReq.Type)

		if name == "" {
			failed = append(failed, bulkCreateFailure{
				Index:   index,
				Name:    sourceReq.Name,
				Message: "Name is required",
			})
			continue
		}
		if sourceType == "" {
			failed = append(failed, bulkCreateFailure{
				Index:   index,
				Name:    name,
				Message: "Type is required",
			})
			continue
		}

		apiConfig, err := mapToJSON(sourceReq.APIConfig)
		if err != nil {
			failed = append(failed, bulkCreateFailure{
				Index:   index,
				Name:    name,
				Message: "Invalid api_config",
			})
			continue
		}

		metadata, err := mapToJSON(sourceReq.Metadata)
		if err != nil {
			failed = append(failed, bulkCreateFailure{
				Index:   index,
				Name:    name,
				Message: "Invalid metadata",
			})
			continue
		}

		isActive := true
		if sourceReq.IsActive != nil {
			isActive = *sourceReq.IsActive
		}

		fetchInterval := 60
		if sourceReq.FetchIntervalMinutes != nil {
			if *sourceReq.FetchIntervalMinutes <= 0 {
				failed = append(failed, bulkCreateFailure{
					Index:   index,
					Name:    name,
					Message: "Fetch interval must be greater than zero",
				})
				continue
			}
			fetchInterval = *sourceReq.FetchIntervalMinutes
		}

		bulkType := models.SourceType(strings.ToUpper(sourceType))
		bulkCategory := models.DefaultCategoryForType(bulkType)
		if sourceReq.Category != nil && strings.TrimSpace(*sourceReq.Category) != "" {
			bulkCategory = strings.ToLower(strings.TrimSpace(*sourceReq.Category))
		}

		source := models.ContentSource{
			TenantID:             principal.TenantID,
			Name:                 name,
			Type:                 bulkType,
			Category:             bulkCategory,
			FeedURL:              sourceReq.FeedURL,
			ImageURL:             sourceImageURL(sourceReq.ImageURL, sourceReq.FeedURL),
			APIConfig:            apiConfig,
			IsActive:             isActive,
			FetchIntervalMinutes: fetchInterval,
			Metadata:             metadata,
		}

		if err := db.Create(&source).Error; err != nil {
			failed = append(failed, bulkCreateFailure{
				Index:   index,
				Name:    name,
				Message: "Failed to create source",
			})
			continue
		}

		created = append(created, mapContentSourceResponse(source))
	}

	c.JSON(http.StatusOK, bulkCreateContentSourcesResponse{
		Created:  created,
		Failed:   failed,
		Total:    len(req.Sources),
		Accepted: len(created),
	})
}

// UpdateContentSource handles PUT /admin/sources/:id
func UpdateContentSource(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}

	db := c.MustGet("db").(*gorm.DB)
	publicID := c.Param("id")
	id, err := uuid.Parse(publicID)
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "Invalid source ID",
			Code:    "INVALID_ID",
		})
		return
	}

	var req updateContentSourceRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "Invalid request",
			Code:    "INVALID_REQUEST",
		})
		return
	}

	var source models.ContentSource
	if err := db.Where("public_id = ? AND tenant_id = ?", id, principal.TenantID).First(&source).Error; err != nil {
		c.JSON(http.StatusNotFound, authErrorResponse{
			Message: "Source not found",
			Code:    "NOT_FOUND",
		})
		return
	}

	if req.Name != nil {
		name := strings.TrimSpace(*req.Name)
		if name == "" {
			c.JSON(http.StatusBadRequest, authErrorResponse{
				Message: "Name cannot be empty",
				Code:    "NAME_REQUIRED",
			})
			return
		}
		source.Name = name
	}

	if req.Type != nil {
		sourceType := strings.TrimSpace(*req.Type)
		if sourceType == "" {
			c.JSON(http.StatusBadRequest, authErrorResponse{
				Message: "Type cannot be empty",
				Code:    "TYPE_REQUIRED",
			})
			return
		}
		source.Type = models.SourceType(strings.ToUpper(sourceType))
	}

	if req.Category != nil && strings.TrimSpace(*req.Category) != "" {
		source.Category = strings.ToLower(strings.TrimSpace(*req.Category))
	}

	if req.FeedURL != nil {
		source.FeedURL = req.FeedURL
	}

	if req.ImageURL != nil {
		source.ImageURL = req.ImageURL
	}
	if req.APIConfig != nil {
		apiConfig, err := mapToJSON(req.APIConfig)
		if err != nil {
			c.JSON(http.StatusBadRequest, authErrorResponse{
				Message: "Invalid api_config",
				Code:    "INVALID_API_CONFIG",
			})
			return
		}
		source.APIConfig = apiConfig
	}

	if req.Metadata != nil {
		metadata, err := mapToJSON(req.Metadata)
		if err != nil {
			c.JSON(http.StatusBadRequest, authErrorResponse{
				Message: "Invalid metadata",
				Code:    "INVALID_METADATA",
			})
			return
		}
		source.Metadata = metadata
	}

	if req.IsActive != nil {
		source.IsActive = *req.IsActive
	}

	if req.FetchIntervalMinutes != nil {
		if *req.FetchIntervalMinutes <= 0 {
			c.JSON(http.StatusBadRequest, authErrorResponse{
				Message: "Fetch interval must be greater than zero",
				Code:    "INVALID_FETCH_INTERVAL",
			})
			return
		}
		source.FetchIntervalMinutes = *req.FetchIntervalMinutes
	}

	if err := db.Save(&source).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{
			Message: "Failed to update source",
			Code:    "UPDATE_FAILED",
		})
		return
	}

	c.JSON(http.StatusOK, mapContentSourceResponse(source))
}

// DeleteContentSource handles DELETE /admin/sources/:id
func DeleteContentSource(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}

	db := c.MustGet("db").(*gorm.DB)
	publicID := c.Param("id")
	id, err := uuid.Parse(publicID)
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "Invalid source ID",
			Code:    "INVALID_ID",
		})
		return
	}

	var source models.ContentSource
	if err := db.Where("public_id = ? AND tenant_id = ?", id, principal.TenantID).First(&source).Error; err != nil {
		c.JSON(http.StatusNotFound, authErrorResponse{
			Message: "Source not found",
			Code:    "NOT_FOUND",
		})
		return
	}

	if err := db.Delete(&source).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{
			Message: "Failed to delete source",
			Code:    "DELETE_FAILED",
		})
		return
	}

	c.Status(http.StatusNoContent)
}

// RunContentSource handles POST /admin/sources/:id/run
func RunContentSource(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}

	db := c.MustGet("db").(*gorm.DB)
	publicID := c.Param("id")
	id, err := uuid.Parse(publicID)
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "Invalid source ID",
			Code:    "INVALID_ID",
		})
		return
	}

	var source models.ContentSource
	if err := db.Where("public_id = ? AND tenant_id = ?", id, principal.TenantID).First(&source).Error; err != nil {
		c.JSON(http.StatusNotFound, authErrorResponse{
			Message: "Source not found",
			Code:    "NOT_FOUND",
		})
		return
	}

	aggregationBaseURL := strings.TrimRight(strings.TrimSpace(os.Getenv("AGGREGATION_BASE_URL")), "/")
	if aggregationBaseURL == "" {
		c.JSON(http.StatusServiceUnavailable, authErrorResponse{
			Message: "Aggregation service URL is not configured",
			Code:    "AGGREGATION_NOT_CONFIGURED",
		})
		return
	}

	sourceURL, err := extractSourceRunURL(source)
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: err.Error(),
			Code:    "SOURCE_URL_REQUIRED",
		})
		return
	}

	settings, _ := parseSourceAPIConfig(source.APIConfig)
	triggerReq := aggregationTriggerRequest{
		SourceType: string(source.Type),
		URL:        sourceURL,
		Name:       source.Name,
		Settings:   settings,
		SourceID:   source.PublicID.String(),
	}

	triggerRes, err := triggerAggregationSourceRun(
		aggregationBaseURL,
		c.GetHeader("Authorization"),
		triggerReq,
	)
	if err != nil {
		c.JSON(http.StatusBadGateway, authErrorResponse{
			Message: "Failed to trigger aggregation run: " + err.Error(),
			Code:    "AGGREGATION_TRIGGER_FAILED",
		})
		return
	}

	now := time.Now().UTC()
	source.LastFetchedAt = &now
	_ = db.Save(&source).Error

	c.JSON(http.StatusOK, runSourceResponse{
		Message: triggerRes.Message,
		JobID:   triggerRes.JobID,
	})
}

// DiscoverSourceFeeds handles POST /admin/sources/discover
func DiscoverSourceFeeds(c *gin.Context) {
	_, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}

	var req discoverFeedsRequest
	if err := c.ShouldBindJSON(&req); err != nil || strings.TrimSpace(req.URL) == "" {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "url is required",
			Code:    "URL_REQUIRED",
		})
		return
	}

	aggregationBaseURL := strings.TrimRight(strings.TrimSpace(os.Getenv("AGGREGATION_BASE_URL")), "/")
	if aggregationBaseURL == "" {
		c.JSON(http.StatusServiceUnavailable, authErrorResponse{
			Message: "Aggregation service URL is not configured",
			Code:    "AGGREGATION_NOT_CONFIGURED",
		})
		return
	}

	responseBody, statusCode, err := proxyAggregationRequest(
		aggregationBaseURL,
		"/admin/discover",
		c.GetHeader("Authorization"),
		req,
	)
	if err != nil {
		c.JSON(http.StatusBadGateway, authErrorResponse{
			Message: "Failed to discover source feeds: " + err.Error(),
			Code:    "AGGREGATION_DISCOVERY_FAILED",
		})
		return
	}

	if statusCode < http.StatusOK || statusCode >= http.StatusMultipleChoices {
		var aggErr discoverFeedsResponse
		_ = json.Unmarshal(responseBody, &aggErr)
		msg := strings.TrimSpace(aggErr.Message)
		if msg == "" {
			msg = "Aggregation rejected discover request"
		}
		c.JSON(statusCode, authErrorResponse{
			Message: msg,
			Code:    "AGGREGATION_DISCOVERY_REJECTED",
		})
		return
	}

	var result discoverFeedsResponse
	if err := json.Unmarshal(responseBody, &result); err != nil {
		c.JSON(http.StatusBadGateway, authErrorResponse{
			Message: "Invalid response from aggregation service",
			Code:    "INVALID_AGGREGATION_RESPONSE",
		})
		return
	}

	c.JSON(http.StatusOK, result)
}

// PreviewSource handles POST /admin/sources/preview
func PreviewSource(c *gin.Context) {
	_, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}

	var req previewSourceRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "Invalid request body",
			Code:    "INVALID_REQUEST",
		})
		return
	}

	req.SourceType = strings.ToUpper(strings.TrimSpace(req.SourceType))
	req.URL = strings.TrimSpace(req.URL)
	if req.SourceType == "" || req.URL == "" {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "sourceType and url are required",
			Code:    "SOURCE_TYPE_AND_URL_REQUIRED",
		})
		return
	}

	aggregationBaseURL := strings.TrimRight(strings.TrimSpace(os.Getenv("AGGREGATION_BASE_URL")), "/")
	if aggregationBaseURL == "" {
		c.JSON(http.StatusServiceUnavailable, authErrorResponse{
			Message: "Aggregation service URL is not configured",
			Code:    "AGGREGATION_NOT_CONFIGURED",
		})
		return
	}

	responseBody, statusCode, err := proxyAggregationRequest(
		aggregationBaseURL,
		"/admin/preview",
		c.GetHeader("Authorization"),
		req,
	)
	if err != nil {
		c.JSON(http.StatusBadGateway, authErrorResponse{
			Message: "Failed to preview source: " + err.Error(),
			Code:    "AGGREGATION_PREVIEW_FAILED",
		})
		return
	}

	if statusCode < http.StatusOK || statusCode >= http.StatusMultipleChoices {
		var aggErr previewSourceResponse
		_ = json.Unmarshal(responseBody, &aggErr)
		msg := strings.TrimSpace(aggErr.Message)
		if msg == "" {
			msg = "Aggregation rejected preview request"
		}
		c.JSON(statusCode, authErrorResponse{
			Message: msg,
			Code:    "AGGREGATION_PREVIEW_REJECTED",
		})
		return
	}

	var result previewSourceResponse
	if err := json.Unmarshal(responseBody, &result); err != nil {
		c.JSON(http.StatusBadGateway, authErrorResponse{
			Message: "Invalid response from aggregation service",
			Code:    "INVALID_AGGREGATION_RESPONSE",
		})
		return
	}

	c.JSON(http.StatusOK, result)
}

// SearchPodcasts handles GET /admin/sources/podcast-search?term=&limit=&country=
// Proxies to the aggregation iTunes search so the "Add media source" wizard can
// let admins pick a podcast by name (returns feed URL + artwork).
func SearchPodcasts(c *gin.Context) {
	if _, ok := requireAdminPrincipal(c); !ok {
		return
	}

	term := strings.TrimSpace(c.Query("term"))
	if term == "" {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "term is required", Code: "TERM_REQUIRED"})
		return
	}

	q := url.Values{}
	q.Set("term", term)
	if l := strings.TrimSpace(c.Query("limit")); l != "" {
		q.Set("limit", l)
	}
	if co := strings.TrimSpace(c.Query("country")); co != "" {
		q.Set("country", co)
	}

	body, statusCode, err := proxyAggregationGet(c.GetHeader("Authorization"), "/admin/itunes/search?"+q.Encode())
	if err != nil {
		c.JSON(http.StatusBadGateway, authErrorResponse{Message: "Failed to search podcasts: " + err.Error(), Code: "ITUNES_SEARCH_FAILED"})
		return
	}
	if statusCode < http.StatusOK || statusCode >= http.StatusMultipleChoices {
		c.JSON(statusCode, authErrorResponse{Message: "Aggregation rejected podcast search", Code: "ITUNES_SEARCH_REJECTED"})
		return
	}

	var agg itunesAggResponse
	if err := json.Unmarshal(body, &agg); err != nil {
		c.JSON(http.StatusBadGateway, authErrorResponse{Message: "Invalid response from aggregation service", Code: "INVALID_AGGREGATION_RESPONSE"})
		return
	}

	resp := podcastSearchResponse{Results: []podcastSearchResult{}}
	for _, r := range agg.Results {
		if strings.TrimSpace(r.FeedURL) == "" {
			continue
		}
		resp.Results = append(resp.Results, podcastSearchResult{
			ID:       r.CollectionID,
			Name:     r.CollectionName,
			FeedURL:  r.FeedURL,
			ImageURL: r.ArtworkURL600,
		})
	}

	c.JSON(http.StatusOK, resp)
}

// ResolveYoutube handles GET /admin/sources/youtube-resolve?url=
// Proxies to the aggregation YouTube resolver so the wizard can confirm the
// channel (name + avatar) before saving.
func ResolveYoutube(c *gin.Context) {
	if _, ok := requireAdminPrincipal(c); !ok {
		return
	}

	target := strings.TrimSpace(c.Query("url"))
	if target == "" {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "url is required", Code: "URL_REQUIRED"})
		return
	}

	q := url.Values{}
	q.Set("url", target)

	body, statusCode, err := proxyAggregationGet(c.GetHeader("Authorization"), "/admin/youtube/resolve?"+q.Encode())
	if err != nil {
		c.JSON(http.StatusBadGateway, authErrorResponse{Message: "Failed to resolve channel: " + err.Error(), Code: "YOUTUBE_RESOLVE_FAILED"})
		return
	}
	if statusCode == http.StatusNotFound {
		c.JSON(http.StatusNotFound, authErrorResponse{Message: "Could not resolve a YouTube channel from that URL", Code: "YOUTUBE_NOT_RESOLVED"})
		return
	}
	if statusCode < http.StatusOK || statusCode >= http.StatusMultipleChoices {
		c.JSON(statusCode, authErrorResponse{Message: "Aggregation rejected channel resolve", Code: "YOUTUBE_RESOLVE_REJECTED"})
		return
	}

	var agg youtubeAggResponse
	if err := json.Unmarshal(body, &agg); err != nil {
		c.JSON(http.StatusBadGateway, authErrorResponse{Message: "Invalid response from aggregation service", Code: "INVALID_AGGREGATION_RESPONSE"})
		return
	}

	c.JSON(http.StatusOK, youtubeResolveResponse{
		ChannelID:       agg.ChannelID,
		Title:           agg.Title,
		Thumbnail:       agg.Thumbnail,
		SubscriberCount: agg.SubscriberCount,
	})
}

func mapContentSourceResponse(source models.ContentSource) contentSourceResponse {
	var lastFetched *string
	if source.LastFetchedAt != nil {
		formatted := source.LastFetchedAt.UTC().Format(time.RFC3339)
		lastFetched = &formatted
	}

	return contentSourceResponse{
		ID:                   source.PublicID.String(),
		Name:                 source.Name,
		Type:                 string(source.Type),
		Category:             source.Category,
		FeedURL:              source.FeedURL,
		ImageURL:             source.ImageURL,
		APIConfig:            json.RawMessage(source.APIConfig),
		IsActive:             source.IsActive,
		FetchIntervalMinutes: source.FetchIntervalMinutes,
		LastFetchedAt:        lastFetched,
		Metadata:             json.RawMessage(source.Metadata),
		CreatedAt:            source.CreatedAt.UTC().Format(time.RFC3339),
		UpdatedAt:            source.UpdatedAt.UTC().Format(time.RFC3339),
	}
}

func mapToJSON(value map[string]interface{}) (datatypes.JSON, error) {
	if value == nil {
		return nil, nil
	}
	bytes, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return datatypes.JSON(bytes), nil
}

func sourceImageURL(imageURL *string, feedURL *string) *string {
	// CMS stores an explicit operator-approved image URL but never fetches a
	// source/feed to discover one. Source retrieval belongs to Aggregation or
	// Enrichment, whose transports own SSRF and redirect containment.
	_ = feedURL
	if imageURL != nil && strings.TrimSpace(*imageURL) != "" {
		value := strings.TrimSpace(*imageURL)
		return &value
	}
	return nil
}

func parseSourceAPIConfig(raw datatypes.JSON) (map[string]interface{}, error) {
	if len(raw) == 0 {
		return map[string]interface{}{}, nil
	}

	var cfg map[string]interface{}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	if cfg == nil {
		cfg = map[string]interface{}{}
	}
	return cfg, nil
}

func extractSourceRunURL(source models.ContentSource) (string, error) {
	if source.FeedURL != nil {
		if value := strings.TrimSpace(*source.FeedURL); value != "" {
			return value, nil
		}
	}

	cfg, err := parseSourceAPIConfig(source.APIConfig)
	if err != nil {
		return "", fmt.Errorf("invalid source api_config")
	}

	candidateKeys := []string{
		"url", "feed_url", "feedUrl", "channel_url", "channelUrl",
		"channel_id", "channelId", "playlist_id", "playlistId", "subreddit",
	}
	for _, key := range candidateKeys {
		if raw, ok := cfg[key]; ok {
			if value, ok := raw.(string); ok && strings.TrimSpace(value) != "" {
				return strings.TrimSpace(value), nil
			}
		}
	}

	return "", fmt.Errorf("source URL is required for type %s", source.Type)
}

func triggerAggregationSourceRun(
	aggregationBaseURL string,
	authorizationHeader string,
	payload aggregationTriggerRequest,
) (aggregationTriggerResponse, error) {
	requestBody, statusCode, err := proxyAggregationRequest(
		aggregationBaseURL,
		"/admin/trigger",
		authorizationHeader,
		payload,
	)
	if err != nil {
		return aggregationTriggerResponse{}, err
	}

	var body aggregationTriggerResponse
	if err := json.Unmarshal(requestBody, &body); err != nil {
		return aggregationTriggerResponse{}, fmt.Errorf("invalid aggregation response")
	}

	if statusCode < http.StatusOK || statusCode >= http.StatusMultipleChoices {
		if strings.TrimSpace(body.Message) == "" {
			body.Message = fmt.Sprintf("aggregation responded with status %d", statusCode)
		}
		return aggregationTriggerResponse{}, fmt.Errorf("%s", body.Message)
	}

	if !body.Success {
		if strings.TrimSpace(body.Message) == "" {
			body.Message = "aggregation rejected trigger request"
		}
		return aggregationTriggerResponse{}, fmt.Errorf("%s", body.Message)
	}

	return body, nil
}

func proxyAggregationRequest(
	aggregationBaseURL string,
	path string,
	authorizationHeader string,
	payload interface{},
) ([]byte, int, error) {
	requestBody, err := json.Marshal(payload)
	if err != nil {
		return nil, 0, err
	}

	req, err := http.NewRequest(http.MethodPost, aggregationBaseURL+path, bytes.NewReader(requestBody))
	if err != nil {
		return nil, 0, err
	}

	req.Header.Set("Content-Type", "application/json")
	if strings.TrimSpace(authorizationHeader) != "" {
		req.Header.Set("Authorization", authorizationHeader)
	}

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, err
	}

	return body, resp.StatusCode, nil
}
