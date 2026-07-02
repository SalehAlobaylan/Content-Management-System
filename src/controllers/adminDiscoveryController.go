package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"encoding/json"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// ---------- Request / response types ----------

type discoveryProfileResponse struct {
	ID                   string   `json:"id"`
	Name                 string   `json:"name"`
	Description          string   `json:"description"`
	Keywords             []string `json:"keywords"`
	Languages            []string `json:"languages"`
	Category             string   `json:"category"`
	Enabled              bool     `json:"enabled"`
	MaxSuggestionsPerRun int      `json:"max_suggestions_per_run"`
	LastRunAt            *string  `json:"last_run_at,omitempty"`
	CreatedAt            string   `json:"created_at"`
	UpdatedAt            string   `json:"updated_at"`
}

type createDiscoveryProfileRequest struct {
	Name                 string   `json:"name"`
	Description          string   `json:"description"`
	Keywords             []string `json:"keywords"`
	Languages            []string `json:"languages"`
	Category             string   `json:"category"`
	Enabled              *bool    `json:"enabled"`
	MaxSuggestionsPerRun *int     `json:"max_suggestions_per_run"`
}

type updateDiscoveryProfileRequest struct {
	Name                 *string   `json:"name"`
	Description          *string   `json:"description"`
	Keywords             *[]string `json:"keywords"`
	Languages            *[]string `json:"languages"`
	Category             *string   `json:"category"`
	Enabled              *bool     `json:"enabled"`
	MaxSuggestionsPerRun *int      `json:"max_suggestions_per_run"`
}

type sourceSuggestionResponse struct {
	ID             string          `json:"id"`
	ProfileID      *string         `json:"profile_id,omitempty"`
	Name           string          `json:"name"`
	Type           string          `json:"type"`
	FeedURL        string          `json:"feed_url"`
	SiteURL        *string         `json:"site_url,omitempty"`
	ImageURL       *string         `json:"image_url,omitempty"`
	Language       *string         `json:"language,omitempty"`
	Confidence     float64         `json:"confidence"`
	RelevanceScore *float64        `json:"relevance_score,omitempty"`
	Health         json.RawMessage `json:"health,omitempty"`
	SampleItems    json.RawMessage `json:"sample_items,omitempty"`
	Evidence       json.RawMessage `json:"evidence,omitempty"`
	DiscoveredVia  string          `json:"discovered_via,omitempty"`
	Category       string          `json:"category"`
	Status         string          `json:"status"`
	RejectReason   *string         `json:"reject_reason,omitempty"`
	CreatedAt      string          `json:"created_at"`
	UpdatedAt      string          `json:"updated_at"`
}

type newsSourceResponse struct {
	contentSourceResponse
	DiscoveryProfileID *string `json:"discovery_profile_id,omitempty"`
	ItemsCount         int64   `json:"items_count"`
	Ready              int64   `json:"ready"`
	Failed             int64   `json:"failed"`
	LastItemAt         *string `json:"last_item_at,omitempty"`
	Engagement         int64   `json:"engagement"`
}

type aggregationDiscoveryProfile struct {
	ID                   string   `json:"id"`
	Name                 string   `json:"name"`
	Description          string   `json:"description,omitempty"`
	Keywords             []string `json:"keywords,omitempty"`
	Languages            []string `json:"languages,omitempty"`
	MaxSuggestionsPerRun int      `json:"maxSuggestionsPerRun"`
	// Category ('news' | 'media') routes the sweep to the right keyword provider —
	// media profiles discover podcasts via iTunes instead of the news web search.
	Category string `json:"category,omitempty"`
	// Config-driven overrides so manual runs honor the same tuning as scheduled
	// sweeps (recency window + which search provider).
	RecencyDays    int    `json:"recencyDays,omitempty"`
	SearchProvider string `json:"searchProvider,omitempty"`
}

type aggregationDiscoveryRequest struct {
	Profile aggregationDiscoveryProfile `json:"profile"`
}

type mediaSourcesContextRollups struct {
	Pending        int   `json:"pending"`
	Imported       int   `json:"imported"`
	AutoDiscovered int   `json:"auto_discovered"`
	Active         int   `json:"active"`
	Healthy        int64 `json:"healthy"`
	Stale          int64 `json:"stale"`
	NeverRun       int64 `json:"never_run"`
	Disabled       int64 `json:"disabled"`
	Failed         int   `json:"failed"`
	NoTranscript   int   `json:"no_transcript"`
	NeedsTrimming  int   `json:"needs_trimming"`
	NonAudioFirst  int   `json:"non_audio_first"`
}

type suggestionRelationshipResponse struct {
	Relationship      string   `json:"relationship"`
	MatchedSourceID   *string  `json:"matched_source_id,omitempty"`
	MatchedSourceName *string  `json:"matched_source_name,omitempty"`
	Reasons           []string `json:"reasons"`
}

type mediaSourceApprovalPreviewResponse struct {
	SourceType           string                 `json:"source_type"`
	Category             string                 `json:"category"`
	AttachedProfileID    *string                `json:"attached_profile_id,omitempty"`
	AttachedProfileName  *string                `json:"attached_profile_name,omitempty"`
	InitialEpisodeCap    int                    `json:"initial_episode_cap"`
	FetchIntervalMinutes int                    `json:"fetch_interval_minutes"`
	AtomizationDefaults  map[string]interface{} `json:"atomization_defaults"`
	FirstFetch           string                 `json:"first_fetch"`
}

type mediaSourceApprovalHandoffResponse struct {
	SuggestionID string  `json:"suggestion_id"`
	SourceID     *string `json:"source_id,omitempty"`
	SourceName   string  `json:"source_name"`
	ProfileID    *string `json:"profile_id,omitempty"`
	ProfileName  *string `json:"profile_name,omitempty"`
	Status       string  `json:"status"`
	ApprovedAt   string  `json:"approved_at"`
	ItemsCount   int64   `json:"items_count"`
	Ready        int64   `json:"ready"`
	Failed       int64   `json:"failed"`
}

type mediaSourceRecentItemResponse struct {
	ID               string  `json:"id"`
	Title            string  `json:"title"`
	Status           string  `json:"status"`
	PublishedAt      *string `json:"published_at,omitempty"`
	DurationSec      *int    `json:"duration_sec,omitempty"`
	CaptionState     *string `json:"caption_state,omitempty"`
	ChapteringStatus *string `json:"chaptering_status,omitempty"`
	FeedVisibility   string  `json:"feed_visibility"`
}

type mediaSourcesContextResponse struct {
	Profiles                  []discoveryProfileResponse                `json:"profiles"`
	Suggestions               []sourceSuggestionResponse                `json:"suggestions"`
	Sources                   []newsSourceResponse                      `json:"sources"`
	SourceStats               sourceStatsResponse                       `json:"source_stats"`
	Config                    models.DiscoveryConfig                    `json:"config"`
	Rollups                   mediaSourcesContextRollups                `json:"rollups"`
	SuggestionRelationships   map[string]suggestionRelationshipResponse `json:"suggestion_relationships"`
	RecentApprovals           []mediaSourceApprovalHandoffResponse      `json:"recent_approvals"`
	ApprovalPreview           *mediaSourceApprovalPreviewResponse       `json:"approval_preview,omitempty"`
	SelectedProfile           *discoveryProfileResponse                 `json:"selected_profile,omitempty"`
	SelectedSuggestion        *sourceSuggestionResponse                 `json:"selected_suggestion,omitempty"`
	SelectedSource            *newsSourceResponse                       `json:"selected_source,omitempty"`
	SelectedSourceRecentItems []mediaSourceRecentItemResponse           `json:"selected_source_recent_items,omitempty"`
	SchemaStatus              map[string]bool                           `json:"schema_status"`
}

// ---------- Profiles ----------

// ListDiscoveryProfiles handles GET /admin/discovery/profiles
func ListDiscoveryProfiles(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	var profiles []models.DiscoveryProfile
	if err := db.Where("tenant_id = ?", principal.TenantID).
		Order("created_at desc").Find(&profiles).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to fetch profiles", Code: "FETCH_FAILED"})
		return
	}

	data := make([]discoveryProfileResponse, 0, len(profiles))
	for _, p := range profiles {
		data = append(data, mapDiscoveryProfileResponse(p))
	}
	c.JSON(http.StatusOK, gin.H{"data": data})
}

// CreateDiscoveryProfile handles POST /admin/discovery/profiles
func CreateDiscoveryProfile(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	var req createDiscoveryProfileRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid request", Code: "INVALID_REQUEST"})
		return
	}
	name := strings.TrimSpace(req.Name)
	if name == "" {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Name is required", Code: "NAME_REQUIRED"})
		return
	}

	profile := models.DiscoveryProfile{
		TenantID:             principal.TenantID,
		Name:                 name,
		Description:          strings.TrimSpace(req.Description),
		Keywords:             pq.StringArray(req.Keywords),
		Languages:            pq.StringArray(req.Languages),
		Category:             normalizeDiscoveryCategory(req.Category),
		Enabled:              true,
		MaxSuggestionsPerRun: 10,
	}
	if req.Enabled != nil {
		profile.Enabled = *req.Enabled
	}
	if req.MaxSuggestionsPerRun != nil && *req.MaxSuggestionsPerRun > 0 {
		profile.MaxSuggestionsPerRun = *req.MaxSuggestionsPerRun
	}

	if err := db.Create(&profile).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to create profile", Code: "CREATE_FAILED"})
		return
	}
	c.JSON(http.StatusCreated, mapDiscoveryProfileResponse(profile))
}

// UpdateDiscoveryProfile handles PUT /admin/discovery/profiles/:id
func UpdateDiscoveryProfile(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid profile ID", Code: "INVALID_ID"})
		return
	}

	var profile models.DiscoveryProfile
	if err := db.Where("public_id = ? AND tenant_id = ?", id, principal.TenantID).First(&profile).Error; err != nil {
		c.JSON(http.StatusNotFound, authErrorResponse{Message: "Profile not found", Code: "NOT_FOUND"})
		return
	}

	var req updateDiscoveryProfileRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid request", Code: "INVALID_REQUEST"})
		return
	}
	if req.Name != nil {
		name := strings.TrimSpace(*req.Name)
		if name == "" {
			c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Name cannot be empty", Code: "NAME_REQUIRED"})
			return
		}
		profile.Name = name
	}
	if req.Description != nil {
		profile.Description = strings.TrimSpace(*req.Description)
	}
	if req.Keywords != nil {
		profile.Keywords = pq.StringArray(*req.Keywords)
	}
	if req.Languages != nil {
		profile.Languages = pq.StringArray(*req.Languages)
	}
	if req.Category != nil {
		profile.Category = normalizeDiscoveryCategory(*req.Category)
	}
	if req.Enabled != nil {
		profile.Enabled = *req.Enabled
	}
	if req.MaxSuggestionsPerRun != nil && *req.MaxSuggestionsPerRun > 0 {
		profile.MaxSuggestionsPerRun = *req.MaxSuggestionsPerRun
	}

	// Invalidate the cached embedding when its semantic inputs change so it
	// recomputes on the next scoring pass.
	if req.Name != nil || req.Description != nil || req.Keywords != nil {
		profile.Embedding = nil
	}

	if err := db.Save(&profile).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to update profile", Code: "UPDATE_FAILED"})
		return
	}
	c.JSON(http.StatusOK, mapDiscoveryProfileResponse(profile))
}

// DeleteDiscoveryProfile handles DELETE /admin/discovery/profiles/:id
func DeleteDiscoveryProfile(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid profile ID", Code: "INVALID_ID"})
		return
	}

	res := db.Where("public_id = ? AND tenant_id = ?", id, principal.TenantID).Delete(&models.DiscoveryProfile{})
	if res.Error != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to delete profile", Code: "DELETE_FAILED"})
		return
	}
	if res.RowsAffected == 0 {
		c.JSON(http.StatusNotFound, authErrorResponse{Message: "Profile not found", Code: "NOT_FOUND"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true})
}

// RunDiscoveryProfile handles POST /admin/discovery/profiles/:id/run — proxies
// a one-off discovery sweep to Aggregation.
func RunDiscoveryProfile(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid profile ID", Code: "INVALID_ID"})
		return
	}

	var profile models.DiscoveryProfile
	if err := db.Where("public_id = ? AND tenant_id = ?", id, principal.TenantID).First(&profile).Error; err != nil {
		c.JSON(http.StatusNotFound, authErrorResponse{Message: "Profile not found", Code: "NOT_FOUND"})
		return
	}

	aggregationBaseURL := strings.TrimRight(strings.TrimSpace(os.Getenv("AGGREGATION_BASE_URL")), "/")
	if aggregationBaseURL == "" {
		c.JSON(http.StatusServiceUnavailable, authErrorResponse{Message: "Aggregation service URL is not configured", Code: "AGGREGATION_NOT_CONFIGURED"})
		return
	}

	cfg := loadDiscoveryConfig(db, principal.TenantID)
	maxPerRun := profile.MaxSuggestionsPerRun
	if cfg.MaxCandidatesPerProfile > 0 && cfg.MaxCandidatesPerProfile < maxPerRun {
		maxPerRun = cfg.MaxCandidatesPerProfile
	}
	triggerRes, err := triggerAggregationDiscoveryRun(aggregationBaseURL, c.GetHeader("Authorization"), aggregationDiscoveryRequest{
		Profile: aggregationDiscoveryProfile{
			ID:                   profile.PublicID.String(),
			Name:                 profile.Name,
			Description:          profile.Description,
			Keywords:             []string(profile.Keywords),
			Languages:            []string(profile.Languages),
			MaxSuggestionsPerRun: maxPerRun,
			Category:             profile.Category,
			RecencyDays:          cfg.RecencyWindowDays,
			SearchProvider:       cfg.SearchProvider,
		},
	})
	if err != nil {
		writeDiscoveryAudit(db, principal, "discovery.run", profile.PublicID.String(), "failure", err.Error())
		c.JSON(http.StatusBadGateway, authErrorResponse{Message: "Failed to trigger discovery: " + err.Error(), Code: "DISCOVERY_TRIGGER_FAILED"})
		return
	}

	now := time.Now().UTC()
	profile.LastRunAt = &now
	_ = db.Save(&profile).Error
	writeDiscoveryAudit(db, principal, "discovery.run", profile.PublicID.String(), "success", "")

	c.JSON(http.StatusOK, runSourceResponse{Message: triggerRes.Message, JobID: triggerRes.JobID})
}

// ---------- Suggestions ----------

// ListSourceSuggestions handles GET /admin/discovery/suggestions
func ListSourceSuggestions(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	query := db.Model(&models.SourceSuggestion{}).Where("tenant_id = ?", principal.TenantID)

	status := strings.TrimSpace(c.Query("status"))
	if status == "" {
		status = models.SuggestionStatusPending
	}
	if !strings.EqualFold(status, "all") {
		query = query.Where("status = ?", strings.ToUpper(status))
	}
	if profileParam := strings.TrimSpace(c.Query("profile_id")); profileParam != "" {
		if internalID, found := resolveProfileInternalID(db, principal.TenantID, profileParam); found {
			query = query.Where("profile_id = ?", *internalID)
		}
	}
	// Category filter ('news' | 'media') so the For You hub fetches only media
	// suggestions instead of pulling a mixed page and dropping media client-side.
	if cat := strings.TrimSpace(c.Query("category")); cat != "" && !strings.EqualFold(cat, "all") {
		query = query.Where("category = ?", strings.ToLower(cat))
	}

	var total int64
	query.Count(&total)

	limit, page := paginationParams(c, 50, 200)
	var suggestions []models.SourceSuggestion
	if err := query.Order("relevance_score DESC NULLS LAST, confidence DESC, created_at DESC").
		Limit(limit).Offset((page - 1) * limit).Find(&suggestions).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to fetch suggestions", Code: "FETCH_FAILED"})
		return
	}

	profileIDs := loadProfilePublicIDs(db, principal.TenantID)
	data := make([]sourceSuggestionResponse, 0, len(suggestions))
	for _, s := range suggestions {
		data = append(data, mapSourceSuggestionResponse(s, profileIDs))
	}
	c.JSON(http.StatusOK, gin.H{"data": data, "total": total, "page": page, "limit": limit})
}

// ApproveSuggestion handles POST /admin/discovery/suggestions/:id/approve
func ApproveSuggestion(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid suggestion ID", Code: "INVALID_ID"})
		return
	}

	var suggestion models.SourceSuggestion
	if err := db.Where("public_id = ? AND tenant_id = ?", id, principal.TenantID).First(&suggestion).Error; err != nil {
		c.JSON(http.StatusNotFound, authErrorResponse{Message: "Suggestion not found", Code: "NOT_FOUND"})
		return
	}

	source, err := approveSuggestionTx(db, principal.TenantID, &suggestion)
	if err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to approve suggestion", Code: "APPROVE_FAILED"})
		return
	}
	writeDiscoveryAudit(db, principal, "discovery.approve", suggestion.PublicID.String(), "success", "")
	// Kick off the first ingestion so the new source starts producing items
	// immediately (best-effort — approval already succeeded regardless).
	triggerSourceFirstFetch(c.GetHeader("Authorization"), source)
	c.JSON(http.StatusOK, mapContentSourceResponse(*source))
}

// RejectSuggestion handles POST /admin/discovery/suggestions/:id/reject
func RejectSuggestion(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid suggestion ID", Code: "INVALID_ID"})
		return
	}

	var body struct {
		Reason string `json:"reason"`
	}
	_ = c.ShouldBindJSON(&body)

	var suggestion models.SourceSuggestion
	if err := db.Where("public_id = ? AND tenant_id = ?", id, principal.TenantID).First(&suggestion).Error; err != nil {
		c.JSON(http.StatusNotFound, authErrorResponse{Message: "Suggestion not found", Code: "NOT_FOUND"})
		return
	}

	suggestion.Status = models.SuggestionStatusRejected
	if reason := strings.TrimSpace(body.Reason); reason != "" {
		suggestion.RejectReason = &reason
	}
	if err := db.Save(&suggestion).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to reject suggestion", Code: "REJECT_FAILED"})
		return
	}
	writeDiscoveryAudit(db, principal, "discovery.reject", suggestion.PublicID.String(), "success", "")
	c.JSON(http.StatusOK, gin.H{"success": true})
}

type bulkSuggestionRequest struct {
	IDs []string `json:"ids"`
}

type bulkSuggestionResult struct {
	ID      string `json:"id"`
	Message string `json:"message,omitempty"`
}

type bulkSuggestionResponse struct {
	Succeeded []string               `json:"succeeded"`
	Failed    []bulkSuggestionResult `json:"failed"`
}

// BulkApproveSuggestions handles POST /admin/discovery/suggestions/bulk-approve
func BulkApproveSuggestions(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var req bulkSuggestionRequest
	if err := c.ShouldBindJSON(&req); err != nil || len(req.IDs) == 0 {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "ids are required", Code: "INVALID_REQUEST"})
		return
	}

	resp := bulkSuggestionResponse{Succeeded: []string{}, Failed: []bulkSuggestionResult{}}
	for _, raw := range req.IDs {
		id, err := uuid.Parse(strings.TrimSpace(raw))
		if err != nil {
			resp.Failed = append(resp.Failed, bulkSuggestionResult{ID: raw, Message: "invalid id"})
			continue
		}
		var suggestion models.SourceSuggestion
		if err := db.Where("public_id = ? AND tenant_id = ?", id, principal.TenantID).First(&suggestion).Error; err != nil {
			resp.Failed = append(resp.Failed, bulkSuggestionResult{ID: raw, Message: "not found"})
			continue
		}
		if _, err := approveSuggestionTx(db, principal.TenantID, &suggestion); err != nil {
			resp.Failed = append(resp.Failed, bulkSuggestionResult{ID: raw, Message: err.Error()})
			continue
		}
		resp.Succeeded = append(resp.Succeeded, raw)
	}
	writeDiscoveryAudit(db, principal, "discovery.bulk_approve", "", "success", "")
	c.JSON(http.StatusOK, resp)
}

// BulkRejectSuggestions handles POST /admin/discovery/suggestions/bulk-reject
func BulkRejectSuggestions(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var req bulkSuggestionRequest
	if err := c.ShouldBindJSON(&req); err != nil || len(req.IDs) == 0 {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "ids are required", Code: "INVALID_REQUEST"})
		return
	}

	res := db.Model(&models.SourceSuggestion{}).
		Where("tenant_id = ? AND public_id IN ?", principal.TenantID, req.IDs).
		Update("status", models.SuggestionStatusRejected)
	if res.Error != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to reject suggestions", Code: "REJECT_FAILED"})
		return
	}
	writeDiscoveryAudit(db, principal, "discovery.bulk_reject", "", "success", "")
	c.JSON(http.StatusOK, gin.H{"succeeded": res.RowsAffected})
}

// ---------- Active news sources (hub list) ----------

type newsSourceStatsRow struct {
	SourceName string     `gorm:"column:source_name"`
	Items      int64      `gorm:"column:items"`
	Ready      int64      `gorm:"column:ready"`
	Failed     int64      `gorm:"column:failed"`
	LastItemAt *time.Time `gorm:"column:last_item_at"`
	Engagement int64      `gorm:"column:engagement"`
}

// ListNewsSources handles GET /admin/discovery/sources — active news-type
// sources enriched with per-source ingestion stats, for the News Feeds hub.
func ListNewsSources(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	// Category-scoped: the News hub passes 'news' (default), the For You hub
	// passes 'media'. A source belongs to exactly one hub by its category.
	cat := strings.ToLower(strings.TrimSpace(c.Query("category")))
	if cat != models.SourceCategoryMedia {
		cat = models.SourceCategoryNews
	}
	query := db.Model(&models.ContentSource{}).
		Where("tenant_id = ? AND category = ?", principal.TenantID, cat)

	if typeParam := strings.ToUpper(strings.TrimSpace(c.Query("type"))); typeParam != "" {
		query = query.Where("type = ?", typeParam)
	}
	if profileParam := strings.TrimSpace(c.Query("profile_id")); profileParam != "" {
		if strings.EqualFold(profileParam, "ungrouped") {
			query = query.Where("discovery_profile_id IS NULL")
		} else if internalID, found := resolveProfileInternalID(db, principal.TenantID, profileParam); found {
			query = query.Where("discovery_profile_id = ?", *internalID)
		}
	}

	var sources []models.ContentSource
	if err := query.Order("created_at desc").Find(&sources).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to fetch sources", Code: "FETCH_FAILED"})
		return
	}

	// Per-source ingestion stats, grouped by source_name (denormalized on items).
	var statRows []newsSourceStatsRow
	db.Model(&models.ContentItem{}).
		Where("tenant_id = ? AND source_name IS NOT NULL AND source_name != ''", principal.TenantID).
		Select("source_name, COUNT(*) AS items, " +
			"COALESCE(SUM(CASE WHEN status = 'READY' THEN 1 ELSE 0 END), 0) AS ready, " +
			"COALESCE(SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END), 0) AS failed, " +
			"MAX(published_at) AS last_item_at, " +
			"COALESCE(SUM(like_count + comment_count + share_count), 0) AS engagement").
		Group("source_name").Scan(&statRows)
	// Key stats by the @-stripped, lowercased source name: content items
	// denormalize source_name to the bare handle (e.g. "spagov"), while a
	// manually-added X source may be named "@spagov" or differ in case.
	normName := func(s string) string { return strings.ToLower(strings.TrimPrefix(s, "@")) }
	statByName := make(map[string]newsSourceStatsRow, len(statRows))
	for _, r := range statRows {
		statByName[normName(r.SourceName)] = r
	}

	profileIDs := loadProfilePublicIDs(db, principal.TenantID)

	data := make([]newsSourceResponse, 0, len(sources))
	for _, s := range sources {
		row := newsSourceResponse{contentSourceResponse: mapContentSourceResponse(s)}
		if s.DiscoveryProfileID != nil {
			if pub, exists := profileIDs[*s.DiscoveryProfileID]; exists {
				row.DiscoveryProfileID = &pub
			}
		}
		if st, exists := statByName[normName(s.Name)]; exists {
			row.ItemsCount = st.Items
			row.Ready = st.Ready
			row.Failed = st.Failed
			row.Engagement = st.Engagement
			if st.LastItemAt != nil {
				formatted := st.LastItemAt.UTC().Format(time.RFC3339)
				row.LastItemAt = &formatted
			}
		}
		data = append(data, row)
	}
	c.JSON(http.StatusOK, gin.H{"data": data, "total": len(data)})
}

// GetMediaSourcesContext handles GET /admin/discovery/media-sources/context.
// It is a read model for the Console's unified Media Sources page: discovery
// profiles + pending suggestions + active media sources + fleet stats.
func GetMediaSourcesContext(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	var profiles []models.DiscoveryProfile
	if err := db.Where("tenant_id = ? AND category = ?", principal.TenantID, models.SourceCategoryMedia).
		Order("created_at desc").Find(&profiles).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to fetch media profiles", Code: "FETCH_FAILED"})
		return
	}
	profileData := make([]discoveryProfileResponse, 0, len(profiles))
	for _, p := range profiles {
		profileData = append(profileData, mapDiscoveryProfileResponse(p))
	}
	profileIDs := loadProfilePublicIDs(db, principal.TenantID)

	var suggestions []models.SourceSuggestion
	if err := db.Where("tenant_id = ? AND category = ? AND status = ?", principal.TenantID, models.SourceCategoryMedia, models.SuggestionStatusPending).
		Order("relevance_score DESC NULLS LAST, confidence DESC, created_at DESC").
		Limit(200).Find(&suggestions).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to fetch media suggestions", Code: "FETCH_FAILED"})
		return
	}
	suggestionData := make([]sourceSuggestionResponse, 0, len(suggestions))
	for _, s := range suggestions {
		suggestionData = append(suggestionData, mapSourceSuggestionResponse(s, profileIDs))
	}

	var recentApproved []models.SourceSuggestion
	if err := db.Where("tenant_id = ? AND category = ? AND status = ?", principal.TenantID, models.SourceCategoryMedia, models.SuggestionStatusApproved).
		Order("updated_at DESC").
		Limit(12).Find(&recentApproved).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to fetch recent media approvals", Code: "FETCH_FAILED"})
		return
	}
	approvedSuggestionData := make([]sourceSuggestionResponse, 0, len(recentApproved))
	for _, s := range recentApproved {
		approvedSuggestionData = append(approvedSuggestionData, mapSourceSuggestionResponse(s, profileIDs))
	}

	rawSources, err := listContentSourcesForDiscoveryCategory(db, principal.TenantID, models.SourceCategoryMedia)
	if err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to fetch media sources", Code: "FETCH_FAILED"})
		return
	}
	sourceData := mapContentSourcesToDiscoveryResponses(db, principal.TenantID, rawSources)
	stats, err := buildSourceStats(db, principal.TenantID, models.SourceCategoryMedia)
	if err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to fetch media source stats", Code: "STATS_FAILED"})
		return
	}

	cfg := loadDiscoveryConfig(db, principal.TenantID)
	allRelationshipSuggestions := make([]models.SourceSuggestion, 0, len(suggestions)+len(recentApproved))
	allRelationshipSuggestions = append(allRelationshipSuggestions, suggestions...)
	allRelationshipSuggestions = append(allRelationshipSuggestions, recentApproved...)
	rollups := buildMediaSourcesContextRollups(suggestionData, sourceData, stats)
	resp := mediaSourcesContextResponse{
		Profiles:                profileData,
		Suggestions:             suggestionData,
		Sources:                 sourceData,
		SourceStats:             stats,
		Config:                  cfg,
		Rollups:                 rollups,
		SuggestionRelationships: buildSuggestionRelationships(allRelationshipSuggestions, rawSources, sourceData),
		RecentApprovals:         buildMediaSourceApprovalHandoffs(recentApproved, sourceData, rawSources, profileData, profileIDs),
		SchemaStatus: map[string]bool{
			"profiles":    true,
			"suggestions": true,
			"sources":     true,
			"stats":       true,
			"config":      true,
		},
	}

	if selectedProfileID := strings.TrimSpace(c.Query("profile")); selectedProfileID != "" {
		for _, p := range profileData {
			if p.ID == selectedProfileID {
				selected := p
				resp.SelectedProfile = &selected
				break
			}
		}
	}
	if selectedSuggestionID := strings.TrimSpace(c.Query("suggestion")); selectedSuggestionID != "" {
		for _, s := range suggestionData {
			if s.ID == selectedSuggestionID {
				selected := s
				resp.SelectedSuggestion = &selected
				break
			}
		}
		if resp.SelectedSuggestion == nil {
			for _, s := range approvedSuggestionData {
				if s.ID == selectedSuggestionID {
					selected := s
					resp.SelectedSuggestion = &selected
					break
				}
			}
		}
		if resp.SelectedSuggestion != nil {
			resp.ApprovalPreview = buildMediaSourceApprovalPreview(*resp.SelectedSuggestion, profileData, cfg)
		}
	}
	if selectedSourceID := strings.TrimSpace(c.Query("source")); selectedSourceID != "" {
		for _, s := range sourceData {
			if s.ID == selectedSourceID {
				selected := s
				resp.SelectedSource = &selected
				resp.SelectedSourceRecentItems = listMediaSourceRecentItems(db, principal.TenantID, selected.Name)
				break
			}
		}
	}

	c.JSON(http.StatusOK, resp)
}

// ---------- Suggest profiles from topics ----------

type suggestedProfileDraft struct {
	Name         string   `json:"name"`
	Keywords     []string `json:"keywords"`
	Description  string   `json:"description"`
	ArticleCount int      `json:"article_count"`
}

// SuggestProfilesFromTopics handles POST /admin/discovery/suggest-profiles —
// proposes interest-profile drafts from the tenant's top trending topics so
// admins don't hand-create every profile. Read-only.
func SuggestProfilesFromTopics(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	var topics []models.Topic
	if err := db.Where("tenant_id = ? AND labeled = ?", principal.TenantID, true).
		Order("article_count DESC, last_member_at DESC NULLS LAST").
		Limit(12).Find(&topics).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to read topics", Code: "FETCH_FAILED"})
		return
	}

	// Skip topics that already have a same-named profile.
	existing := map[string]bool{}
	var profiles []models.DiscoveryProfile
	db.Where("tenant_id = ?", principal.TenantID).Find(&profiles)
	for _, p := range profiles {
		existing[strings.ToLower(strings.TrimSpace(p.Name))] = true
	}

	drafts := make([]suggestedProfileDraft, 0, len(topics))
	for _, t := range topics {
		label := strings.TrimSpace(t.Label)
		if label == "" || existing[strings.ToLower(label)] {
			continue
		}
		drafts = append(drafts, suggestedProfileDraft{
			Name:         label,
			Keywords:     keywordsFromLabel(label),
			Description:  "Auto-suggested from the \"" + label + "\" topic.",
			ArticleCount: t.ArticleCount,
		})
	}
	c.JSON(http.StatusOK, gin.H{"data": drafts})
}

func keywordsFromLabel(label string) []string {
	seen := map[string]bool{}
	out := []string{}
	for _, f := range strings.Fields(label) {
		w := strings.ToLower(strings.Trim(f, ".,:;!?\"'()[]"))
		if len([]rune(w)) < 3 || seen[w] {
			continue
		}
		seen[w] = true
		out = append(out, w)
		if len(out) >= 6 {
			break
		}
	}
	return out
}

// ---------- Discovery config (tuning + scheduling) ----------

// GetDiscoveryConfig handles GET /admin/discovery/config
func GetDiscoveryConfig(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	c.JSON(http.StatusOK, loadDiscoveryConfig(db, principal.TenantID))
}

// UpdateDiscoveryConfig handles PUT /admin/discovery/config — upserts the
// single-row config and best-effort re-syncs the Aggregation sweep schedule.
func UpdateDiscoveryConfig(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	var req models.DiscoveryConfig
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid request: " + err.Error(), Code: "INVALID_REQUEST"})
		return
	}
	for _, v := range []float64{req.MinConfidence, req.MinRelevance, req.DupThreshold, req.DupPenalty} {
		if v < 0 || v > 1 {
			c.JSON(http.StatusBadRequest, authErrorResponse{Message: "thresholds must be in [0,1]", Code: "INVALID_RANGE"})
			return
		}
	}
	if req.SweepIntervalHours < 1 {
		req.SweepIntervalHours = 1
	}
	if req.MaxCandidatesPerProfile < 1 {
		req.MaxCandidatesPerProfile = 1
	}
	if req.RecencyWindowDays < 1 {
		req.RecencyWindowDays = 1
	}
	switch req.SearchProvider {
	case "auto", "tavily", "crawl":
	default:
		req.SearchProvider = "auto"
	}
	req.TenantID = principal.TenantID

	var existing models.DiscoveryConfig
	if err := db.Where("tenant_id = ?", principal.TenantID).First(&existing).Error; err == nil {
		req.ID = existing.ID
		req.CreatedAt = existing.CreatedAt
		if err := db.Save(&req).Error; err != nil {
			c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to save config", Code: "SAVE_FAILED"})
			return
		}
	} else if err := db.Create(&req).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to create config", Code: "CREATE_FAILED"})
		return
	}

	// Best-effort: tell Aggregation to re-sync its repeatable sweep so interval
	// / automation-toggle changes take effect immediately.
	triggerAggregationResync(c.GetHeader("Authorization"))
	c.JSON(http.StatusOK, req)
}

// SweepNow handles POST /admin/discovery/sweep-now — proxy a manual sweep.
func SweepNow(c *gin.Context) {
	proxyAggregationSimple(c, "/admin/discovery/sweep-now")
}

// BuildGraph handles POST /admin/discovery/build-graph — proxy a manual graph build.
func BuildGraph(c *gin.Context) {
	proxyAggregationSimple(c, "/admin/discovery/build-graph-now")
}

// ImportYouTube handles POST /admin/discovery/import-youtube — the manual seed
// path. The admin pastes a youtubei/v1 payload (e.g. their personalized home
// feed); we proxy it to Aggregation, which parses the channels via guest
// InnerTube, enriches each, and posts them as suggestions for review. We attach
// the target profile's keywords + tenant so the suggestions score + file under
// the right interest. No credentials are stored — the admin stays the session.
func ImportYouTube(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	var req struct {
		Raw       json.RawMessage `json:"raw"`
		ProfileID string          `json:"profile_id"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid request body", Code: "INVALID_BODY"})
		return
	}
	if len(req.Raw) == 0 {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "raw youtubei/v1 payload is required", Code: "MISSING_RAW"})
		return
	}

	aggregationBaseURL := strings.TrimRight(strings.TrimSpace(os.Getenv("AGGREGATION_BASE_URL")), "/")
	if aggregationBaseURL == "" {
		c.JSON(http.StatusServiceUnavailable, authErrorResponse{Message: "Aggregation service URL is not configured", Code: "AGGREGATION_NOT_CONFIGURED"})
		return
	}

	// Resolve the target interest (optional) → keywords + canonical public id.
	var keywords []string
	profileID := ""
	if pid := strings.TrimSpace(req.ProfileID); pid != "" {
		if uid, err := uuid.Parse(pid); err == nil {
			var profile models.DiscoveryProfile
			if err := db.Where("public_id = ? AND tenant_id = ?", uid, principal.TenantID).First(&profile).Error; err == nil {
				keywords = []string(profile.Keywords)
				profileID = profile.PublicID.String()
			}
		}
	}

	payload := map[string]interface{}{
		"raw":       req.Raw,
		"profileId": profileID,
		"tenantId":  principal.TenantID,
		"keywords":  keywords,
	}
	body, status, err := proxyAggregationRequest(aggregationBaseURL, "/admin/discovery/import-youtube", c.GetHeader("Authorization"), payload)
	if err != nil {
		c.JSON(http.StatusBadGateway, authErrorResponse{Message: "Aggregation request failed: " + err.Error(), Code: "AGGREGATION_FAILED"})
		return
	}
	audit := "success"
	if status >= http.StatusBadRequest {
		audit = "failure"
	}
	writeDiscoveryAudit(db, principal, "discovery.import_youtube", profileID, audit, "")
	c.Data(status, "application/json", body)
}

// ImportYouTubeLinks handles POST /admin/discovery/import-youtube-links — the
// low-friction seed path. The admin pastes YouTube references (one per line: a
// @handle, channel URL, or any video/share link); we proxy them to Aggregation,
// which resolves each to its channel via guest InnerTube, enriches it, and posts
// suggestions for review. Same path as ImportYouTube, minus the 1 MB JSON paste.
func ImportYouTubeLinks(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	var req struct {
		Inputs    []string `json:"inputs"`
		ProfileID string   `json:"profile_id"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid request body", Code: "INVALID_BODY"})
		return
	}
	cleaned := make([]string, 0, len(req.Inputs))
	for _, s := range req.Inputs {
		if t := strings.TrimSpace(s); t != "" {
			cleaned = append(cleaned, t)
		}
	}
	if len(cleaned) == 0 {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "inputs (YouTube links/@handles) is required", Code: "MISSING_INPUTS"})
		return
	}

	aggregationBaseURL := strings.TrimRight(strings.TrimSpace(os.Getenv("AGGREGATION_BASE_URL")), "/")
	if aggregationBaseURL == "" {
		c.JSON(http.StatusServiceUnavailable, authErrorResponse{Message: "Aggregation service URL is not configured", Code: "AGGREGATION_NOT_CONFIGURED"})
		return
	}

	var keywords []string
	profileID := ""
	if pid := strings.TrimSpace(req.ProfileID); pid != "" {
		if uid, err := uuid.Parse(pid); err == nil {
			var profile models.DiscoveryProfile
			if err := db.Where("public_id = ? AND tenant_id = ?", uid, principal.TenantID).First(&profile).Error; err == nil {
				keywords = []string(profile.Keywords)
				profileID = profile.PublicID.String()
			}
		}
	}

	payload := map[string]interface{}{
		"inputs":    cleaned,
		"profileId": profileID,
		"tenantId":  principal.TenantID,
		"keywords":  keywords,
	}
	body, status, err := proxyAggregationRequest(aggregationBaseURL, "/admin/discovery/import-youtube-links", c.GetHeader("Authorization"), payload)
	if err != nil {
		c.JSON(http.StatusBadGateway, authErrorResponse{Message: "Aggregation request failed: " + err.Error(), Code: "AGGREGATION_FAILED"})
		return
	}
	audit := "success"
	if status >= http.StatusBadRequest {
		audit = "failure"
	}
	writeDiscoveryAudit(db, principal, "discovery.import_youtube_links", profileID, audit, "")
	c.Data(status, "application/json", body)
}

func proxyAggregationSimple(c *gin.Context, path string) {
	if _, ok := requireAdminPrincipal(c); !ok {
		return
	}
	aggregationBaseURL := strings.TrimRight(strings.TrimSpace(os.Getenv("AGGREGATION_BASE_URL")), "/")
	if aggregationBaseURL == "" {
		c.JSON(http.StatusServiceUnavailable, authErrorResponse{Message: "Aggregation service URL is not configured", Code: "AGGREGATION_NOT_CONFIGURED"})
		return
	}
	body, status, err := proxyAggregationRequest(aggregationBaseURL, path, c.GetHeader("Authorization"), map[string]interface{}{})
	if err != nil {
		c.JSON(http.StatusBadGateway, authErrorResponse{Message: "Aggregation request failed: " + err.Error(), Code: "AGGREGATION_FAILED"})
		return
	}
	c.Data(status, "application/json", body)
}

// GetAuthorities handles GET /admin/discovery/authorities — the top domains in
// the tenant's source-intelligence graph (the "your network" insight).
func GetAuthorities(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var cands []models.SourceCandidate
	q := db.Where("tenant_id = ?", principal.TenantID)
	// Optional per-platform filter (?kind=twitter|telegram|rss) for the
	// "Top voices in your X network" panel.
	if kind := strings.ToLower(strings.TrimSpace(c.Query("kind"))); kind != "" {
		q = q.Where("kind = ?", kind)
	}
	q.Order("authority_score desc, cocitation_count desc, citation_count desc").Limit(12).Find(&cands)
	out := make([]gin.H, 0, len(cands))
	for _, x := range cands {
		out = append(out, gin.H{
			"domain":           x.Domain,
			"kind":             x.Kind,
			"authority":        round2(x.AuthorityScore),
			"citation_count":   x.CitationCount,
			"cocitation_count": x.CocitationCount,
			"feed_valid":       x.FeedValid,
			"status":           x.Status,
		})
	}
	c.JSON(http.StatusOK, gin.H{"data": out})
}

func loadDiscoveryConfig(db *gorm.DB, tenantID string) models.DiscoveryConfig {
	var cfg models.DiscoveryConfig
	if err := db.Where("tenant_id = ?", tenantID).First(&cfg).Error; err != nil {
		return models.DefaultDiscoveryConfig(tenantID)
	}
	return cfg
}

func triggerAggregationResync(authHeader string) {
	aggregationBaseURL := strings.TrimRight(strings.TrimSpace(os.Getenv("AGGREGATION_BASE_URL")), "/")
	if aggregationBaseURL == "" {
		return
	}
	_, _, _ = proxyAggregationRequest(aggregationBaseURL, "/admin/discovery/resync-schedule", authHeader, map[string]interface{}{})
}

// ---------- helpers ----------

func mapDiscoveryProfileResponse(p models.DiscoveryProfile) discoveryProfileResponse {
	var lastRun *string
	if p.LastRunAt != nil {
		formatted := p.LastRunAt.UTC().Format(time.RFC3339)
		lastRun = &formatted
	}
	keywords := []string(p.Keywords)
	if keywords == nil {
		keywords = []string{}
	}
	languages := []string(p.Languages)
	if languages == nil {
		languages = []string{}
	}
	return discoveryProfileResponse{
		ID:                   p.PublicID.String(),
		Name:                 p.Name,
		Description:          p.Description,
		Keywords:             keywords,
		Languages:            languages,
		Category:             normalizeDiscoveryCategory(p.Category),
		Enabled:              p.Enabled,
		MaxSuggestionsPerRun: p.MaxSuggestionsPerRun,
		LastRunAt:            lastRun,
		CreatedAt:            p.CreatedAt.UTC().Format(time.RFC3339),
		UpdatedAt:            p.UpdatedAt.UTC().Format(time.RFC3339),
	}
}

// normalizeDiscoveryCategory clamps a free-text category to the two supported
// hubs, defaulting to news.
func normalizeDiscoveryCategory(raw string) string {
	if strings.EqualFold(strings.TrimSpace(raw), models.SourceCategoryMedia) {
		return models.SourceCategoryMedia
	}
	return models.SourceCategoryNews
}

func mapSourceSuggestionResponse(s models.SourceSuggestion, profileIDs map[uint]string) sourceSuggestionResponse {
	resp := sourceSuggestionResponse{
		ID:             s.PublicID.String(),
		Name:           s.Name,
		Type:           string(s.Type),
		FeedURL:        s.FeedURL,
		SiteURL:        s.SiteURL,
		ImageURL:       s.ImageURL,
		Language:       s.Language,
		Confidence:     s.Confidence,
		RelevanceScore: s.RelevanceScore,
		Health:         json.RawMessage(s.Health),
		SampleItems:    json.RawMessage(s.SampleItems),
		Evidence:       json.RawMessage(s.Evidence),
		DiscoveredVia:  s.DiscoveredVia,
		Category:       defaultStr(s.Category, models.DefaultCategoryForType(s.Type)),
		Status:         s.Status,
		RejectReason:   s.RejectReason,
		CreatedAt:      s.CreatedAt.UTC().Format(time.RFC3339),
		UpdatedAt:      s.UpdatedAt.UTC().Format(time.RFC3339),
	}
	if s.ProfileID != nil {
		if pub, ok := profileIDs[*s.ProfileID]; ok {
			resp.ProfileID = &pub
		}
	}
	return resp
}

func listDiscoverySourcesForCategory(db *gorm.DB, tenantID string, category string) ([]newsSourceResponse, error) {
	sources, err := listContentSourcesForDiscoveryCategory(db, tenantID, category)
	if err != nil {
		return nil, err
	}
	return mapContentSourcesToDiscoveryResponses(db, tenantID, sources), nil
}

func listContentSourcesForDiscoveryCategory(db *gorm.DB, tenantID string, category string) ([]models.ContentSource, error) {
	var sources []models.ContentSource
	query := db.Where("tenant_id = ?", tenantID)
	if category != models.SourceCategoryMedia {
		query = query.Where("category = ?", category)
	}
	if err := query.Order("created_at desc").Find(&sources).Error; err != nil {
		return nil, err
	}
	if category == models.SourceCategoryMedia {
		filtered := make([]models.ContentSource, 0, len(sources))
		for _, s := range sources {
			if isMediaConsoleSource(s) {
				filtered = append(filtered, s)
			}
		}
		sources = filtered
	}
	return sources, nil
}

func mapContentSourcesToDiscoveryResponses(db *gorm.DB, tenantID string, sources []models.ContentSource) []newsSourceResponse {
	var statRows []newsSourceStatsRow
	db.Model(&models.ContentItem{}).
		Where("tenant_id = ? AND source_name IS NOT NULL AND source_name != ''", tenantID).
		Select("source_name, COUNT(*) AS items, " +
			"COALESCE(SUM(CASE WHEN status = 'READY' THEN 1 ELSE 0 END), 0) AS ready, " +
			"COALESCE(SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END), 0) AS failed, " +
			"MAX(published_at) AS last_item_at, " +
			"COALESCE(SUM(like_count + comment_count + share_count), 0) AS engagement").
		Group("source_name").Scan(&statRows)

	normName := func(s string) string { return strings.ToLower(strings.TrimPrefix(s, "@")) }
	statByName := make(map[string]newsSourceStatsRow, len(statRows))
	for _, r := range statRows {
		statByName[normName(r.SourceName)] = r
	}

	profileIDs := loadProfilePublicIDs(db, tenantID)
	data := make([]newsSourceResponse, 0, len(sources))
	for _, s := range sources {
		row := newsSourceResponse{contentSourceResponse: mapContentSourceResponse(s)}
		if s.DiscoveryProfileID != nil {
			if pub, exists := profileIDs[*s.DiscoveryProfileID]; exists {
				row.DiscoveryProfileID = &pub
			}
		}
		if st, exists := statByName[normName(s.Name)]; exists {
			row.ItemsCount = st.Items
			row.Ready = st.Ready
			row.Failed = st.Failed
			row.Engagement = st.Engagement
			if st.LastItemAt != nil {
				formatted := st.LastItemAt.UTC().Format(time.RFC3339)
				row.LastItemAt = &formatted
			}
		}
		data = append(data, row)
	}
	return data
}

func buildMediaSourcesContextRollups(
	suggestions []sourceSuggestionResponse,
	sources []newsSourceResponse,
	stats sourceStatsResponse,
) mediaSourcesContextRollups {
	rollups := mediaSourcesContextRollups{
		Pending:        len(suggestions),
		Active:         len(sources),
		Healthy:        stats.ByHealth["healthy"],
		Stale:          stats.ByHealth["stale"],
		NeverRun:       stats.ByHealth["never_run"],
		Disabled:       stats.ByHealth["disabled"],
		AutoDiscovered: 0,
	}
	for _, s := range sources {
		if s.Failed > 0 {
			rollups.Failed++
		}
	}
	for _, s := range suggestions {
		if s.DiscoveredVia == "youtube-import" {
			rollups.Imported++
		} else {
			rollups.AutoDiscovered++
		}
		if rawJSONHasString(s.Evidence, "caption_state", "none") {
			rollups.NoTranscript++
		}
		if rawJSONBoolIs(s.Evidence, "needs_chaptering", true) {
			rollups.NeedsTrimming++
		}
		if rawJSONBoolIs(s.Health, "audio_first", false) {
			rollups.NonAudioFirst++
		}
	}
	return rollups
}

func rawJSONHasString(raw json.RawMessage, key string, value string) bool {
	if len(raw) == 0 {
		return false
	}
	var obj map[string]interface{}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return false
	}
	got, ok := obj[key].(string)
	return ok && got == value
}

func rawJSONBoolIs(raw json.RawMessage, key string, value bool) bool {
	if len(raw) == 0 {
		return false
	}
	var obj map[string]interface{}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return false
	}
	got, ok := obj[key].(bool)
	return ok && got == value
}

func rawJSONString(raw json.RawMessage, key string) string {
	if len(raw) == 0 {
		return ""
	}
	var obj map[string]interface{}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return ""
	}
	got, _ := obj[key].(string)
	return got
}

func rawJSONArrayLen(raw json.RawMessage) int {
	if len(raw) == 0 {
		return 0
	}
	var arr []interface{}
	if err := json.Unmarshal(raw, &arr); err != nil {
		return 0
	}
	return len(arr)
}

func buildSuggestionRelationships(
	suggestions []models.SourceSuggestion,
	sources []models.ContentSource,
	sourceData []newsSourceResponse,
) map[string]suggestionRelationshipResponse {
	sourceByID := make(map[uint]models.ContentSource, len(sources))
	for _, source := range sources {
		sourceByID[source.ID] = source
	}
	failedBySourceID := make(map[string]bool, len(sourceData))
	for _, source := range sourceData {
		failedBySourceID[source.ID] = source.Failed > 0
	}

	relationships := make(map[string]suggestionRelationshipResponse, len(suggestions))
	for _, suggestion := range suggestions {
		rel := suggestionRelationshipResponse{
			Relationship: "new",
			Reasons:      []string{},
		}

		if suggestion.ApprovedSourceID != nil {
			if source, ok := sourceByID[*suggestion.ApprovedSourceID]; ok {
				rel.Relationship = "already_approved"
				rel.Reasons = []string{"approved_source"}
				setRelationshipSource(&rel, source)
				relationships[suggestion.PublicID.String()] = rel
				continue
			}
		}

		suggestionFeed := normalizeDiscoveryURL(suggestion.FeedURL)
		suggestionSite := normalizeDiscoveryURL(discoveryPtrString(suggestion.SiteURL))
		suggestionName := normalizeDiscoveryName(suggestion.Name)

		var similar *suggestionRelationshipResponse
		for _, source := range sources {
			sourceFeed := normalizeDiscoveryURL(discoveryPtrString(source.FeedURL))
			sourceName := normalizeDiscoveryName(source.Name)

			if suggestionFeed != "" && sourceFeed != "" && suggestionFeed == sourceFeed {
				rel.Relationship = "duplicate"
				rel.Reasons = []string{"same_feed_url"}
				if suggestion.ProfileID != nil && source.DiscoveryProfileID != nil && *suggestion.ProfileID == *source.DiscoveryProfileID {
					rel.Reasons = append(rel.Reasons, "same_interest_profile")
				}
				setRelationshipSource(&rel, source)
				break
			}

			reasons := []string{}
			if suggestionName != "" && sourceName != "" && suggestionName == sourceName {
				reasons = append(reasons, "same_normalized_name")
			}
			if suggestionSite != "" && sourceFeed != "" && suggestionSite == sourceFeed {
				reasons = append(reasons, "same_site_url")
			}
			if len(reasons) > 0 && suggestion.ProfileID != nil && source.DiscoveryProfileID != nil && *suggestion.ProfileID == *source.DiscoveryProfileID {
				reasons = append(reasons, "same_interest_profile")
			}
			if len(reasons) > 0 && similar == nil {
				relationship := "similar"
				reasons = append(reasons, improvementReasonsForSuggestion(
					suggestion,
					source,
					failedBySourceID[source.PublicID.String()],
				)...)
				if hasImprovementReason(reasons) {
					relationship = "improves_existing"
				}
				candidate := suggestionRelationshipResponse{Relationship: relationship, Reasons: dedupeStrings(reasons)}
				setRelationshipSource(&candidate, source)
				similar = &candidate
			}
		}
		if rel.Relationship == "new" && similar != nil {
			rel = *similar
		}

		relationships[suggestion.PublicID.String()] = rel
	}
	return relationships
}

func improvementReasonsForSuggestion(suggestion models.SourceSuggestion, source models.ContentSource, sourceFailed bool) []string {
	reasons := []string{}
	sourceHealth := sourceMetadataObject(source.Metadata, "media_finding_health")
	sourceEvidence := sourceMetadataObject(source.Metadata, "media_finding_evidence")
	sourceSampleCount := sourceMetadataArrayLen(source.Metadata, "media_finding_sample_items")

	if rawJSONBoolIs(json.RawMessage(suggestion.Health), "audio_first", true) && !rawJSONBoolIs(sourceHealth, "audio_first", true) {
		reasons = append(reasons, "stronger_audio_evidence")
	}
	if captionState := rawJSONString(json.RawMessage(suggestion.Evidence), "caption_state"); captionState != "" && captionState != "none" {
		sourceCaptionState := rawJSONString(sourceEvidence, "caption_state")
		if sourceCaptionState == "" || sourceCaptionState == "none" {
			reasons = append(reasons, "transcript_available")
		}
	}
	if sampleCount := rawJSONArrayLen(json.RawMessage(suggestion.SampleItems)); sampleCount >= 3 && sampleCount > sourceSampleCount {
		reasons = append(reasons, "richer_sample_items")
	}
	if !source.IsActive {
		reasons = append(reasons, "existing_source_disabled")
	}
	if sourceFailed {
		reasons = append(reasons, "existing_source_failed")
	}
	if source.LastFetchedAt == nil {
		reasons = append(reasons, "existing_source_stale")
	} else if source.FetchIntervalMinutes > 0 {
		staleAfter := time.Duration(source.FetchIntervalMinutes*2) * time.Minute
		if time.Since(*source.LastFetchedAt) > staleAfter {
			reasons = append(reasons, "existing_source_stale")
		}
	}
	return reasons
}

func hasImprovementReason(reasons []string) bool {
	for _, reason := range reasons {
		switch reason {
		case "stronger_audio_evidence", "transcript_available", "richer_sample_items", "existing_source_disabled", "existing_source_failed", "existing_source_stale":
			return true
		}
	}
	return false
}

func sourceMetadataObject(metadata datatypes.JSON, key string) json.RawMessage {
	if len(metadata) == 0 {
		return nil
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(metadata, &obj); err != nil {
		return nil
	}
	return obj[key]
}

func sourceMetadataArrayLen(metadata datatypes.JSON, key string) int {
	return rawJSONArrayLen(sourceMetadataObject(metadata, key))
}

func dedupeStrings(values []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out
}

func setRelationshipSource(rel *suggestionRelationshipResponse, source models.ContentSource) {
	id := source.PublicID.String()
	name := source.Name
	rel.MatchedSourceID = &id
	rel.MatchedSourceName = &name
}

func normalizeDiscoveryName(value string) string {
	value = strings.ToLower(strings.TrimSpace(strings.TrimPrefix(value, "@")))
	replacer := strings.NewReplacer(" ", "", "-", "", "_", "", ".", "", ":", "", "/", "")
	return replacer.Replace(value)
}

func normalizeDiscoveryURL(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.TrimPrefix(value, "https://")
	value = strings.TrimPrefix(value, "http://")
	value = strings.TrimPrefix(value, "www.")
	if idx := strings.IndexAny(value, "?#"); idx >= 0 {
		value = value[:idx]
	}
	return strings.TrimRight(value, "/")
}

func discoveryPtrString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func buildMediaSourceApprovalPreview(
	suggestion sourceSuggestionResponse,
	profiles []discoveryProfileResponse,
	cfg models.DiscoveryConfig,
) *mediaSourceApprovalPreviewResponse {
	var profileName *string
	if suggestion.ProfileID != nil {
		for _, profile := range profiles {
			if profile.ID == *suggestion.ProfileID {
				name := profile.Name
				profileName = &name
				break
			}
		}
	}
	defaults := defaultMediaAtomizationPolicy()
	if cfg.MediaInitialMaxEpisodes > 0 {
		defaults["max_results"] = cfg.MediaInitialMaxEpisodes
		defaults["initial_atomization_limit"] = cfg.MediaInitialMaxEpisodes
	}
	return &mediaSourceApprovalPreviewResponse{
		SourceType:           suggestion.Type,
		Category:             defaultStr(suggestion.Category, models.SourceCategoryMedia),
		AttachedProfileID:    suggestion.ProfileID,
		AttachedProfileName:  profileName,
		InitialEpisodeCap:    cfg.MediaInitialMaxEpisodes,
		FetchIntervalMinutes: 60,
		AtomizationDefaults:  defaults,
		FirstFetch:           "queued_on_approve",
	}
}

func buildMediaSourceApprovalHandoffs(
	approved []models.SourceSuggestion,
	sourceData []newsSourceResponse,
	rawSources []models.ContentSource,
	profiles []discoveryProfileResponse,
	profileIDs map[uint]string,
) []mediaSourceApprovalHandoffResponse {
	sourceByInternalID := make(map[uint]models.ContentSource, len(rawSources))
	sourceByPublicID := make(map[string]newsSourceResponse, len(sourceData))
	for _, source := range rawSources {
		sourceByInternalID[source.ID] = source
	}
	for _, source := range sourceData {
		sourceByPublicID[source.ID] = source
	}
	profileNameByID := make(map[string]string, len(profiles))
	for _, profile := range profiles {
		profileNameByID[profile.ID] = profile.Name
	}

	out := make([]mediaSourceApprovalHandoffResponse, 0, len(approved))
	for _, suggestion := range approved {
		handoff := mediaSourceApprovalHandoffResponse{
			SuggestionID: suggestion.PublicID.String(),
			SourceName:   suggestion.Name,
			Status:       "approved",
			ApprovedAt:   suggestion.UpdatedAt.UTC().Format(time.RFC3339),
		}
		if suggestion.ProfileID != nil {
			if pub, ok := profileIDs[*suggestion.ProfileID]; ok {
				profileID := pub
				handoff.ProfileID = &profileID
				if name, exists := profileNameByID[pub]; exists {
					profileName := name
					handoff.ProfileName = &profileName
				}
			}
		}
		if suggestion.ApprovedSourceID != nil {
			if source, ok := sourceByInternalID[*suggestion.ApprovedSourceID]; ok {
				sourceID := source.PublicID.String()
				handoff.SourceID = &sourceID
				handoff.SourceName = source.Name
				if resp, exists := sourceByPublicID[sourceID]; exists {
					handoff.ItemsCount = resp.ItemsCount
					handoff.Ready = resp.Ready
					handoff.Failed = resp.Failed
					handoff.Status = mediaApprovalHandoffStatus(resp)
				}
			}
		}
		out = append(out, handoff)
	}
	return out
}

func mediaApprovalHandoffStatus(source newsSourceResponse) string {
	if !source.IsActive || source.Failed > 0 {
		return "needs_attention"
	}
	if source.ItemsCount > 0 || source.Ready > 0 {
		return "producing"
	}
	if source.LastFetchedAt != nil {
		return "waiting_for_items"
	}
	return "first_fetch_queued"
}

func listMediaSourceRecentItems(db *gorm.DB, tenantID string, sourceName string) []mediaSourceRecentItemResponse {
	normalized := strings.ToLower(strings.TrimPrefix(strings.TrimSpace(sourceName), "@"))
	if normalized == "" {
		return []mediaSourceRecentItemResponse{}
	}
	var items []models.ContentItem
	err := db.Where("tenant_id = ?", tenantID).
		Where("type IN ?", []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}).
		Where("source_name IS NOT NULL AND source_name <> ''").
		Where("LOWER(TRIM(LEADING '@' FROM source_name)) = ?", normalized).
		Order("COALESCE(published_at, created_at) DESC").
		Limit(5).
		Find(&items).Error
	if err != nil {
		return []mediaSourceRecentItemResponse{}
	}
	out := make([]mediaSourceRecentItemResponse, 0, len(items))
	for _, item := range items {
		out = append(out, mapMediaSourceRecentItemResponse(item))
	}
	return out
}

func mapMediaSourceRecentItemResponse(item models.ContentItem) mediaSourceRecentItemResponse {
	title := ""
	if item.Title != nil {
		title = *item.Title
	}
	var publishedAt *string
	if item.PublishedAt != nil {
		formatted := item.PublishedAt.UTC().Format(time.RFC3339)
		publishedAt = &formatted
	}
	return mediaSourceRecentItemResponse{
		ID:               item.PublicID.String(),
		Title:            title,
		Status:           string(item.Status),
		PublishedAt:      publishedAt,
		DurationSec:      item.DurationSec,
		CaptionState:     item.CaptionState,
		ChapteringStatus: item.ChapteringStatus,
		FeedVisibility:   item.FeedVisibility,
	}
}

// approveSuggestionTx creates (or links to an existing) ContentSource from a
// suggestion, idempotently, and marks the suggestion APPROVED.
func approveSuggestionTx(db *gorm.DB, tenantID string, suggestion *models.SourceSuggestion) (*models.ContentSource, error) {
	var result models.ContentSource
	err := db.Transaction(func(tx *gorm.DB) error {
		// Already approved → return the linked source if it still exists.
		if suggestion.ApprovedSourceID != nil {
			var existing models.ContentSource
			if err := tx.Where("id = ?", *suggestion.ApprovedSourceID).First(&existing).Error; err == nil {
				result = existing
				return nil
			}
		}
		// Dedupe against an existing source with the same feed URL.
		if strings.TrimSpace(suggestion.FeedURL) != "" {
			var existing models.ContentSource
			if err := tx.Where("tenant_id = ? AND feed_url = ?", tenantID, suggestion.FeedURL).First(&existing).Error; err == nil {
				suggestion.Status = models.SuggestionStatusApproved
				suggestion.ApprovedSourceID = &existing.ID
				if err := tx.Save(suggestion).Error; err != nil {
					return err
				}
				result = existing
				return nil
			}
		}
		feedURL := suggestion.FeedURL
		category := suggestion.Category
		if category == "" {
			category = models.DefaultCategoryForType(suggestion.Type)
		}
		// Storage guard + atomization policy: cap a newly-approved MEDIA source
		// to the N most-recent episodes/videos and mark it as a source whose
		// parent items should be atomized into chapter feed units.
		var apiConfig datatypes.JSON
		var sourceMetadata datatypes.JSON
		if category == models.SourceCategoryMedia {
			settings := defaultMediaAtomizationPolicy()
			if cfg := loadDiscoveryConfig(db, tenantID); cfg.MediaInitialMaxEpisodes > 0 {
				settings["max_results"] = cfg.MediaInitialMaxEpisodes
				settings["initial_atomization_limit"] = cfg.MediaInitialMaxEpisodes
			}
			if raw, err := json.Marshal(settings); err == nil {
				apiConfig = datatypes.JSON(raw)
			}
			meta := map[string]interface{}{}
			if len(suggestion.Health) > 0 {
				var health map[string]interface{}
				if json.Unmarshal(suggestion.Health, &health) == nil {
					meta["media_finding_health"] = health
				}
			}
			if len(suggestion.Evidence) > 0 {
				var evidence map[string]interface{}
				if json.Unmarshal(suggestion.Evidence, &evidence) == nil {
					meta["media_finding_evidence"] = evidence
				}
			}
			if len(suggestion.SampleItems) > 0 {
				var sampleItems []map[string]interface{}
				if json.Unmarshal(suggestion.SampleItems, &sampleItems) == nil {
					meta["media_finding_sample_items"] = sampleItems
				}
			}
			if len(meta) > 0 {
				if raw, err := json.Marshal(meta); err == nil {
					sourceMetadata = datatypes.JSON(raw)
				}
			}
		}
		source := models.ContentSource{
			TenantID:             tenantID,
			Name:                 suggestion.Name,
			Type:                 suggestion.Type,
			Category:             category,
			FeedURL:              &feedURL,
			ImageURL:             suggestion.ImageURL,
			APIConfig:            apiConfig,
			Metadata:             sourceMetadata,
			IsActive:             true,
			FetchIntervalMinutes: 60,
			DiscoveryProfileID:   suggestion.ProfileID,
		}
		if err := tx.Create(&source).Error; err != nil {
			return err
		}
		suggestion.Status = models.SuggestionStatusApproved
		suggestion.ApprovedSourceID = &source.ID
		if err := tx.Save(suggestion).Error; err != nil {
			return err
		}
		result = source
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func defaultMediaAtomizationPolicy() map[string]interface{} {
	return map[string]interface{}{
		"chaptering_enabled":             true,
		"auto_publish_high_confidence":   true,
		"parent_feed_visible":            false,
		"preserve_video":                 true,
		"remove_sponsor_segments":        true,
		"min_chapter_minutes":            5,
		"min_feed_unit_seconds":          forYouMinDurationSec,
		"soft_max_chapter_minutes":       30,
		"hard_max_chapter_minutes":       40,
		"atomization_min_parent_seconds": atomizationMinParentDurationSec,
		"max_chapters_per_parent":        5,
		"chaptering_mode":                "contextual",
		"high_confidence_threshold":      0.82,
		"preferred_playback_rendition":   "hls",
		"fallback_playback_rendition":    "mp4",
		"audio_only_allowed":             true,
	}
}

// triggerSourceFirstFetch best-effort kicks off the first ingestion for a
// newly-approved source so it starts producing items without a manual run.
func triggerSourceFirstFetch(authHeader string, source *models.ContentSource) {
	if source == nil {
		return
	}
	aggregationBaseURL := strings.TrimRight(strings.TrimSpace(os.Getenv("AGGREGATION_BASE_URL")), "/")
	if aggregationBaseURL == "" {
		return
	}
	sourceURL, err := extractSourceRunURL(*source)
	if err != nil {
		return
	}
	settings, _ := parseSourceAPIConfig(source.APIConfig)
	_, _ = triggerAggregationSourceRun(aggregationBaseURL, authHeader, aggregationTriggerRequest{
		SourceType: string(source.Type),
		URL:        sourceURL,
		Name:       source.Name,
		Settings:   settings,
		SourceID:   source.PublicID.String(),
	})
}

func triggerAggregationDiscoveryRun(aggregationBaseURL, authorizationHeader string, payload aggregationDiscoveryRequest) (aggregationTriggerResponse, error) {
	requestBody, statusCode, err := proxyAggregationRequest(aggregationBaseURL, "/admin/discovery/run", authorizationHeader, payload)
	if err != nil {
		return aggregationTriggerResponse{}, err
	}
	var body aggregationTriggerResponse
	if err := json.Unmarshal(requestBody, &body); err != nil {
		return aggregationTriggerResponse{}, err
	}
	if statusCode < http.StatusOK || statusCode >= http.StatusMultipleChoices || !body.Success {
		if strings.TrimSpace(body.Message) == "" {
			body.Message = "aggregation rejected discovery request"
		}
		return aggregationTriggerResponse{}, &discoveryError{msg: body.Message}
	}
	return body, nil
}

type discoveryError struct{ msg string }

func (e *discoveryError) Error() string { return e.msg }

func loadProfilePublicIDs(db *gorm.DB, tenantID string) map[uint]string {
	var profiles []models.DiscoveryProfile
	db.Where("tenant_id = ?", tenantID).Find(&profiles)
	m := make(map[uint]string, len(profiles))
	for _, p := range profiles {
		m[p.ID] = p.PublicID.String()
	}
	return m
}

func resolveProfileInternalID(db *gorm.DB, tenantID, publicID string) (*uint, bool) {
	id, err := uuid.Parse(strings.TrimSpace(publicID))
	if err != nil {
		return nil, false
	}
	var profile models.DiscoveryProfile
	if err := db.Where("public_id = ? AND tenant_id = ?", id, tenantID).First(&profile).Error; err != nil {
		return nil, false
	}
	return &profile.ID, true
}

func paginationParams(c *gin.Context, defaultLimit, maxLimit int) (limit, page int) {
	limit = defaultLimit
	page = 1
	if v := strings.TrimSpace(c.Query("limit")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	if limit > maxLimit {
		limit = maxLimit
	}
	if v := strings.TrimSpace(c.Query("page")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			page = n
		}
	}
	return limit, page
}

func writeDiscoveryAudit(db *gorm.DB, principal utils.AdminPrincipal, action, resource, status, errorMessage string) {
	entry := models.AuditLog{
		TenantID:       principal.TenantID,
		UserID:         principal.UserID,
		UserEmail:      principal.Email,
		Action:         action,
		TargetService:  "cms",
		TargetResource: resource,
		Status:         status,
		ErrorMessage:   errorMessage,
	}
	_ = db.Create(&entry).Error
}
