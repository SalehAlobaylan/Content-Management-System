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
