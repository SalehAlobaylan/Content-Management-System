package controllers

import (
	"content-management-system/src/models"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// =============================================================================
// Quality Profiles — CRUD
// =============================================================================

// ListQualityProfiles handles GET /admin/quality/profiles?scope=global|tenant
func ListQualityProfiles(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	scope := strings.ToLower(strings.TrimSpace(c.DefaultQuery("scope", "all")))

	q := db.Model(&models.QualityProfile{})
	switch scope {
	case "global":
		q = q.Where("tenant_id IS NULL")
	case "tenant":
		q = q.Where("tenant_id = ?", principal.TenantID)
	default: // "all" — global + this tenant's
		q = q.Where("tenant_id IS NULL OR tenant_id = ?", principal.TenantID)
	}

	var profiles []models.QualityProfile
	if err := q.Order("is_default DESC, name ASC").Find(&profiles).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to list profiles", Code: "LIST_FAILED"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": profiles})
}

type qualityProfileRequest struct {
	Scope            string `json:"scope"` // global | tenant (default: global)
	Name             string `json:"name"`
	Description      string `json:"description"`
	VideoCodec       string `json:"video_codec"`
	MaxHeight        int    `json:"max_height"`
	TargetBitrateKbps int   `json:"target_bitrate_kbps"`
	CRF              int    `json:"crf"`
	Preset           string `json:"preset"`
	AudioCodec       string `json:"audio_codec"`
	AudioBitrateKbps int    `json:"audio_bitrate_kbps"`
	IsDefault        *bool  `json:"is_default"`
	IsActive         *bool  `json:"is_active"`
}

func validateAndApplyProfile(p *models.QualityProfile, req qualityProfileRequest) error {
	if strings.TrimSpace(req.Name) == "" {
		return errors.New("name is required")
	}
	codec := strings.ToLower(strings.TrimSpace(req.VideoCodec))
	if codec == "" {
		codec = "h264"
	}
	if codec != "h264" && codec != "h265" && codec != "av1" {
		return errors.New("video_codec must be h264, h265, or av1")
	}
	audio := strings.ToLower(strings.TrimSpace(req.AudioCodec))
	if audio == "" {
		audio = "aac"
	}
	if audio != "aac" && audio != "opus" {
		return errors.New("audio_codec must be aac or opus")
	}
	preset := strings.ToLower(strings.TrimSpace(req.Preset))
	if preset == "" {
		preset = "fast"
	}
	if req.CRF < 0 || req.CRF > 51 {
		return errors.New("crf must be between 0 and 51")
	}
	if req.MaxHeight < 0 || req.MaxHeight > 4320 {
		return errors.New("max_height must be between 0 and 4320")
	}
	if req.TargetBitrateKbps < 0 || req.TargetBitrateKbps > 100_000 {
		return errors.New("target_bitrate_kbps out of range")
	}
	if req.AudioBitrateKbps < 0 || req.AudioBitrateKbps > 1024 {
		return errors.New("audio_bitrate_kbps out of range")
	}

	p.Name = strings.TrimSpace(req.Name)
	p.Description = req.Description
	p.VideoCodec = codec
	p.MaxHeight = req.MaxHeight
	p.TargetBitrateKbps = req.TargetBitrateKbps
	if req.CRF == 0 && req.TargetBitrateKbps == 0 {
		p.CRF = 23
	} else {
		p.CRF = req.CRF
	}
	p.Preset = preset
	p.AudioCodec = audio
	if req.AudioBitrateKbps == 0 {
		p.AudioBitrateKbps = 128
	} else {
		p.AudioBitrateKbps = req.AudioBitrateKbps
	}
	if req.IsDefault != nil {
		p.IsDefault = *req.IsDefault
	}
	if req.IsActive != nil {
		p.IsActive = *req.IsActive
	}
	return nil
}

// CreateQualityProfile handles POST /admin/quality/profiles
func CreateQualityProfile(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	var req qualityProfileRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid request", Code: "INVALID_REQUEST"})
		return
	}

	scope := strings.ToLower(strings.TrimSpace(req.Scope))
	if scope == "" {
		scope = "global"
	}

	p := models.QualityProfile{IsActive: true}
	if scope == "tenant" {
		t := principal.TenantID
		p.TenantID = &t
	}

	if err := validateAndApplyProfile(&p, req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: err.Error(), Code: "VALIDATION_FAILED"})
		return
	}

	// If marking IsDefault, clear the flag on all other profiles in the same scope.
	if p.IsDefault {
		clearDefaultFlag(db, p.TenantID)
	}

	if err := db.Create(&p).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to create profile: " + err.Error(), Code: "CREATE_FAILED"})
		return
	}
	c.JSON(http.StatusCreated, p)
}

// UpdateQualityProfile handles PUT /admin/quality/profiles/:id
func UpdateQualityProfile(c *gin.Context) {
	if _, ok := requireAdminPrincipal(c); !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid id", Code: "INVALID_ID"})
		return
	}

	var p models.QualityProfile
	if err := db.First(&p, id).Error; err != nil {
		c.JSON(http.StatusNotFound, authErrorResponse{Message: "Profile not found", Code: "NOT_FOUND"})
		return
	}

	var req qualityProfileRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid request", Code: "INVALID_REQUEST"})
		return
	}
	if err := validateAndApplyProfile(&p, req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: err.Error(), Code: "VALIDATION_FAILED"})
		return
	}
	if p.IsDefault {
		clearDefaultFlag(db, p.TenantID)
		p.IsDefault = true // re-set after the bulk update
	}
	if err := db.Save(&p).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to save", Code: "SAVE_FAILED"})
		return
	}
	c.JSON(http.StatusOK, p)
}

// DeleteQualityProfile handles DELETE /admin/quality/profiles/:id
// Refuses if the profile is referenced by any QualityRule.
func DeleteQualityProfile(c *gin.Context) {
	if _, ok := requireAdminPrincipal(c); !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid id", Code: "INVALID_ID"})
		return
	}
	var refCount int64
	db.Model(&models.QualityRule{}).Where("target_profile_id = ?", id).Count(&refCount)
	if refCount > 0 {
		c.JSON(http.StatusConflict, authErrorResponse{
			Message: fmt.Sprintf("Profile is used by %d rule(s); detach them first", refCount),
			Code:    "PROFILE_IN_USE",
		})
		return
	}
	if err := db.Delete(&models.QualityProfile{}, id).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to delete", Code: "DELETE_FAILED"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true})
}

func clearDefaultFlag(db *gorm.DB, tenantID *string) {
	q := db.Model(&models.QualityProfile{}).Where("is_default = TRUE")
	if tenantID == nil {
		q = q.Where("tenant_id IS NULL")
	} else {
		q = q.Where("tenant_id = ?", *tenantID)
	}
	q.Update("is_default", false)
}

// =============================================================================
// Quality Rules — CRUD
// =============================================================================

// ListQualityRules handles GET /admin/quality/rules
func ListQualityRules(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	var rules []models.QualityRule
	if err := db.Where("tenant_id IS NULL OR tenant_id = ?", principal.TenantID).
		Order("priority ASC, id ASC").Find(&rules).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to list", Code: "LIST_FAILED"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": rules})
}

type qualityRuleRequest struct {
	Scope             string   `json:"scope"`
	Name              string   `json:"name"`
	Enabled           *bool    `json:"enabled"`
	Priority          *int     `json:"priority"`
	MinAgeDays        *int     `json:"min_age_days"`
	MaxViewCount      *int     `json:"max_view_count"`
	MaxViewsPerDay    *float64 `json:"max_views_per_day"`
	ContentType       string   `json:"content_type"`
	SourceID          *uint    `json:"source_id"`
	OnlyIfHigherThan  *uint    `json:"only_if_higher_than"`
	TargetProfileID   *uint    `json:"target_profile_id"`
	SweepIntervalMins *int     `json:"sweep_interval_minutes"`
}

func validateAndApplyRule(r *models.QualityRule, req qualityRuleRequest, db *gorm.DB) error {
	if strings.TrimSpace(req.Name) == "" {
		return errors.New("name is required")
	}
	if req.TargetProfileID != nil {
		var p models.QualityProfile
		if err := db.First(&p, *req.TargetProfileID).Error; err != nil {
			return errors.New("target_profile_id does not reference an existing profile")
		}
		r.TargetProfileID = *req.TargetProfileID
	}
	if r.TargetProfileID == 0 {
		return errors.New("target_profile_id is required")
	}
	r.Name = strings.TrimSpace(req.Name)
	if req.Enabled != nil {
		r.Enabled = *req.Enabled
	}
	if req.Priority != nil {
		r.Priority = clampInt(*req.Priority, 0, 1000)
	}
	if req.MinAgeDays != nil {
		r.MinAgeDays = clampInt(*req.MinAgeDays, 0, 365*5)
	}
	if req.MaxViewCount != nil {
		v := *req.MaxViewCount
		r.MaxViewCount = &v
	}
	if req.MaxViewsPerDay != nil {
		v := *req.MaxViewsPerDay
		r.MaxViewsPerDay = &v
	}
	r.ContentType = strings.ToUpper(strings.TrimSpace(req.ContentType))
	if req.SourceID != nil {
		v := *req.SourceID
		r.SourceID = &v
	}
	if req.OnlyIfHigherThan != nil {
		v := *req.OnlyIfHigherThan
		r.OnlyIfHigherThan = &v
	}
	if req.SweepIntervalMins != nil {
		r.SweepIntervalMins = clampInt(*req.SweepIntervalMins, 5, 7*24*60)
	}
	if r.SweepIntervalMins == 0 {
		r.SweepIntervalMins = 1440
	}
	return nil
}

// CreateQualityRule handles POST /admin/quality/rules
func CreateQualityRule(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var req qualityRuleRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid request", Code: "INVALID_REQUEST"})
		return
	}
	r := models.QualityRule{Priority: 100}
	scope := strings.ToLower(strings.TrimSpace(req.Scope))
	if scope == "" {
		scope = "tenant"
	}
	if scope == "tenant" {
		t := principal.TenantID
		r.TenantID = &t
	}
	if err := validateAndApplyRule(&r, req, db); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: err.Error(), Code: "VALIDATION_FAILED"})
		return
	}
	if err := db.Create(&r).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to create rule", Code: "CREATE_FAILED"})
		return
	}
	notifyAggregationQualityRuleChanged(c.GetHeader("Authorization"))
	c.JSON(http.StatusCreated, r)
}

// UpdateQualityRule handles PUT /admin/quality/rules/:id
func UpdateQualityRule(c *gin.Context) {
	if _, ok := requireAdminPrincipal(c); !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid id", Code: "INVALID_ID"})
		return
	}
	var r models.QualityRule
	if err := db.First(&r, id).Error; err != nil {
		c.JSON(http.StatusNotFound, authErrorResponse{Message: "Rule not found", Code: "NOT_FOUND"})
		return
	}
	var req qualityRuleRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid request", Code: "INVALID_REQUEST"})
		return
	}
	if err := validateAndApplyRule(&r, req, db); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: err.Error(), Code: "VALIDATION_FAILED"})
		return
	}
	if err := db.Save(&r).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to save", Code: "SAVE_FAILED"})
		return
	}
	notifyAggregationQualityRuleChanged(c.GetHeader("Authorization"))
	c.JSON(http.StatusOK, r)
}

// DeleteQualityRule handles DELETE /admin/quality/rules/:id
func DeleteQualityRule(c *gin.Context) {
	if _, ok := requireAdminPrincipal(c); !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid id", Code: "INVALID_ID"})
		return
	}
	if err := db.Delete(&models.QualityRule{}, id).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to delete", Code: "DELETE_FAILED"})
		return
	}
	notifyAggregationQualityRuleChanged(c.GetHeader("Authorization"))
	c.JSON(http.StatusOK, gin.H{"success": true})
}

// =============================================================================
// Candidates — items eligible for a given rule (or ad-hoc filters)
// =============================================================================

type qualityCandidate struct {
	ID                 string  `json:"id"`
	Type               string  `json:"type"`
	Title              string  `json:"title"`
	ViewCount          int     `json:"view_count"`
	DurationSec        *int    `json:"duration_sec,omitempty"`
	FileSizeBytes      int64   `json:"file_size_bytes"`
	CurrentBitrateKbps *int    `json:"current_bitrate_kbps,omitempty"`
	CurrentProfileID   *uint   `json:"current_quality_profile_id,omitempty"`
	StorageTier        *string `json:"storage_tier,omitempty"`
	CreatedAt          string  `json:"created_at"`
	// Projected new size if re-encoded with the rule's target profile.
	ProjectedSizeBytes int64 `json:"projected_size_bytes"`
	ProjectedSavings   int64 `json:"projected_savings_bytes"`
}

type qualityCandidatesResponse struct {
	Data             []qualityCandidate `json:"data"`
	Total            int64              `json:"total"`
	Limit            int                `json:"limit"`
	TotalProjected   int64              `json:"total_projected_bytes"`
	TotalSavings     int64              `json:"total_savings_bytes"`
	TargetProfileID  uint               `json:"target_profile_id"`
}

// GetQualityCandidates handles GET /admin/quality/candidates
//
// Query: rule_id (preferred) OR profile_id + ad-hoc filters
//   - rule_id=N
//   - profile_id=N (alternative entry point: "show me what would change at this profile")
//   - min_age_days, max_view_count, content_type, limit
func GetQualityCandidates(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	var (
		targetProfileID  uint
		minAgeDays       int
		maxViewCount     *int
		contentType      string
		onlyIfHigherThan *uint
	)

	if raw := c.Query("rule_id"); raw != "" {
		ruleID, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid rule_id", Code: "INVALID"})
			return
		}
		var r models.QualityRule
		if err := db.First(&r, ruleID).Error; err != nil {
			c.JSON(http.StatusNotFound, authErrorResponse{Message: "Rule not found", Code: "NOT_FOUND"})
			return
		}
		targetProfileID = r.TargetProfileID
		minAgeDays = r.MinAgeDays
		if r.MaxViewCount != nil {
			v := *r.MaxViewCount
			maxViewCount = &v
		}
		contentType = r.ContentType
		onlyIfHigherThan = r.OnlyIfHigherThan
	} else if raw := c.Query("profile_id"); raw != "" {
		v, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid profile_id", Code: "INVALID"})
			return
		}
		targetProfileID = uint(v)
	} else {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "rule_id or profile_id is required", Code: "INVALID"})
		return
	}

	if mad := atoiDefault(c.Query("min_age_days"), -1); mad >= 0 {
		minAgeDays = mad
	}
	if mvc := atoiDefault(c.Query("max_view_count"), -1); mvc >= 0 {
		maxViewCount = &mvc
	}
	if ct := strings.ToUpper(strings.TrimSpace(c.Query("content_type"))); ct != "" {
		contentType = ct
	}
	limit := atoiDefault(c.Query("limit"), 100)
	if limit > 500 {
		limit = 500
	}

	var profile models.QualityProfile
	if err := db.First(&profile, targetProfileID).Error; err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "target profile not found", Code: "NOT_FOUND"})
		return
	}

	q := buildQualityCandidateQuery(db, qualityFilter{
		tenantID:         principal.TenantID,
		minAgeDays:       minAgeDays,
		maxViewCount:     maxViewCount,
		contentType:      contentType,
		targetProfileID:  targetProfileID,
		onlyIfHigherThan: onlyIfHigherThan,
	})

	var total int64
	q.Model(&models.ContentItem{}).Count(&total)

	var items []models.ContentItem
	if err := q.Order("view_count ASC, created_at ASC").Limit(limit).Find(&items).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to fetch candidates", Code: "LIST_FAILED"})
		return
	}

	resp := qualityCandidatesResponse{
		Data:            make([]qualityCandidate, 0, len(items)),
		Total:           total,
		Limit:           limit,
		TargetProfileID: targetProfileID,
	}
	for _, it := range items {
		cand := mapQualityCandidate(it, profile)
		resp.Data = append(resp.Data, cand)
		resp.TotalProjected += cand.ProjectedSizeBytes
		resp.TotalSavings += cand.ProjectedSavings
	}
	c.JSON(http.StatusOK, resp)
}

// qualityFilter centralises the eligibility predicate so the candidate, sweep,
// and re-encode endpoints share one definition.
type qualityFilter struct {
	tenantID         string
	minAgeDays       int
	maxViewCount     *int
	contentType      string
	targetProfileID  uint
	onlyIfHigherThan *uint
}

func buildQualityCandidateQuery(db *gorm.DB, f qualityFilter) *gorm.DB {
	cutoff := time.Now().UTC().AddDate(0, 0, -f.minAgeDays)
	q := db.Model(&models.ContentItem{}).
		Where("tenant_id = ?", f.tenantID).
		Where("status = ?", models.ContentStatusReady).
		Where("media_url IS NOT NULL").
		Where("(type = ? OR type = ?)", models.ContentTypeVideo, models.ContentTypePodcast)

	if f.minAgeDays > 0 {
		q = q.Where("created_at < ?", cutoff)
	}
	if f.maxViewCount != nil {
		q = q.Where("view_count <= ?", *f.maxViewCount)
	}
	if f.contentType != "" {
		q = q.Where("type = ?", f.contentType)
	}
	// Idempotency: skip items already at the target profile.
	if f.targetProfileID > 0 {
		q = q.Where("(current_quality_profile_id IS NULL OR current_quality_profile_id != ?)", f.targetProfileID)
	}
	// OnlyIfHigherThan = "skip items already at this profile id or smaller".
	// We treat smaller-id-as-newer as a heuristic; the real semantic is "if the
	// item is already at this profile, skip" which is the same idempotency check.
	if f.onlyIfHigherThan != nil {
		q = q.Where("(current_quality_profile_id IS NULL OR current_quality_profile_id != ?)", *f.onlyIfHigherThan)
	}
	return q
}

// projectSizeBytes estimates the post-re-encode size of an item under a given
// profile. The estimator uses (bitrate × duration) when both are known;
// otherwise it falls back to a per-codec heuristic.
func projectSizeBytes(it models.ContentItem, p models.QualityProfile) int64 {
	if it.DurationSec == nil || *it.DurationSec <= 0 {
		// Without a duration we cannot project; assume same size.
		return it.FileSizeBytes
	}
	dur := int64(*it.DurationSec)
	// Pick the projected video bitrate.
	var vKbps int
	if p.TargetBitrateKbps > 0 {
		vKbps = p.TargetBitrateKbps
	} else {
		// CRF mode — derive from the height cap. Rough table per H.264 norms.
		vKbps = bitrateForHeight(p.MaxHeight, p.CRF)
	}
	totalKbps := int64(vKbps + p.AudioBitrateKbps)
	// bytes = kbps × seconds × 1000 / 8
	return (totalKbps * dur * 1000) / 8
}

func bitrateForHeight(h, crf int) int {
	// Heuristic ladder — assumes CRF 23 gives the listed rate; bump up/down
	// roughly +/- 30% per CRF step away from 23.
	base := map[int]int{
		2160: 12000,
		1440: 8000,
		1080: 4500,
		720:  2000,
		480:  900,
		360:  500,
		0:    3500, // no cap → assume 1080p input
	}
	v, ok := base[h]
	if !ok {
		// Round to the nearest known height.
		switch {
		case h >= 2000:
			v = base[2160]
		case h >= 1300:
			v = base[1440]
		case h >= 900:
			v = base[1080]
		case h >= 600:
			v = base[720]
		case h >= 420:
			v = base[480]
		case h > 0:
			v = base[360]
		default:
			v = base[0]
		}
	}
	delta := crf - 23
	adjusted := float64(v) * pow(0.75, delta)
	return int(adjusted)
}

func pow(base float64, exp int) float64 {
	r := 1.0
	if exp >= 0 {
		for i := 0; i < exp; i++ {
			r *= base
		}
	} else {
		for i := 0; i < -exp; i++ {
			r /= base
		}
	}
	return r
}

func mapQualityCandidate(it models.ContentItem, p models.QualityProfile) qualityCandidate {
	title := ""
	if it.Title != nil {
		title = *it.Title
	}
	projected := projectSizeBytes(it, p)
	savings := it.FileSizeBytes - projected
	if savings < 0 {
		savings = 0
	}
	return qualityCandidate{
		ID:                 it.PublicID.String(),
		Type:               string(it.Type),
		Title:              title,
		ViewCount:          it.ViewCount,
		DurationSec:        it.DurationSec,
		FileSizeBytes:      it.FileSizeBytes,
		CurrentBitrateKbps: it.CurrentBitrateKbps,
		CurrentProfileID:   it.CurrentQualityProfileID,
		StorageTier:        it.StorageTier,
		CreatedAt:          it.CreatedAt.UTC().Format(time.RFC3339),
		ProjectedSizeBytes: projected,
		ProjectedSavings:   savings,
	}
}

// =============================================================================
// Re-encode — manual trigger
// =============================================================================

type reEncodeRequest struct {
	IDs       []string `json:"ids"`
	RuleID    *uint    `json:"rule_id"`
	ProfileID *uint    `json:"profile_id"`
	Trigger   string   `json:"trigger"`
	DryRun    bool     `json:"dry_run"`
}

type reEncodeItemRef struct {
	ContentItemID   string `json:"content_item_id"`
	TargetProfileID uint   `json:"target_profile_id"`
}

type reEncodeResponse struct {
	Enqueued       int   `json:"enqueued"`
	EstimatedFreed int64 `json:"estimated_freed_bytes"`
	DryRun         bool  `json:"dry_run"`
}

// TriggerReEncode handles POST /admin/quality/re-encode
func TriggerReEncode(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	var req reEncodeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid request", Code: "INVALID_REQUEST"})
		return
	}

	var profile models.QualityProfile
	var ruleID *uint

	if req.RuleID != nil {
		var r models.QualityRule
		if err := db.First(&r, *req.RuleID).Error; err != nil {
			c.JSON(http.StatusNotFound, authErrorResponse{Message: "Rule not found", Code: "NOT_FOUND"})
			return
		}
		if err := db.First(&profile, r.TargetProfileID).Error; err != nil {
			c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Rule references missing profile", Code: "PROFILE_MISSING"})
			return
		}
		v := r.ID
		ruleID = &v
	} else if req.ProfileID != nil {
		if err := db.First(&profile, *req.ProfileID).Error; err != nil {
			c.JSON(http.StatusNotFound, authErrorResponse{Message: "Profile not found", Code: "NOT_FOUND"})
			return
		}
	} else {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "rule_id or profile_id is required", Code: "INVALID"})
		return
	}

	// Resolve the candidate item set.
	var items []models.ContentItem
	if len(req.IDs) > 0 {
		ids := make([]uuid.UUID, 0, len(req.IDs))
		for _, raw := range req.IDs {
			id, err := uuid.Parse(raw)
			if err != nil {
				c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid id: " + raw, Code: "INVALID_ID"})
				return
			}
			ids = append(ids, id)
		}
		if err := db.Where("tenant_id = ? AND public_id IN ?", principal.TenantID, ids).Find(&items).Error; err != nil {
			c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to load items", Code: "LOAD_FAILED"})
			return
		}
	} else if req.RuleID != nil {
		// Pull from the rule's candidate query.
		var r models.QualityRule
		_ = db.First(&r, *req.RuleID).Error
		var maxViews *int
		if r.MaxViewCount != nil {
			v := *r.MaxViewCount
			maxViews = &v
		}
		q := buildQualityCandidateQuery(db, qualityFilter{
			tenantID:         principal.TenantID,
			minAgeDays:       r.MinAgeDays,
			maxViewCount:     maxViews,
			contentType:      r.ContentType,
			targetProfileID:  r.TargetProfileID,
			onlyIfHigherThan: r.OnlyIfHigherThan,
		})
		_ = q.Order("view_count ASC, created_at ASC").Limit(500).Find(&items).Error
	} else {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "ids or rule_id required", Code: "INVALID"})
		return
	}

	// Compute estimated savings.
	var freed int64
	for _, it := range items {
		freed += it.FileSizeBytes - projectSizeBytes(it, profile)
	}

	if req.DryRun {
		c.JSON(http.StatusOK, reEncodeResponse{
			Enqueued: len(items), EstimatedFreed: freed, DryRun: true,
		})
		return
	}

	// Build the payload Aggregation expects.
	refs := make([]reEncodeItemRef, 0, len(items))
	for _, it := range items {
		refs = append(refs, reEncodeItemRef{
			ContentItemID:   it.PublicID.String(),
			TargetProfileID: profile.ID,
		})
	}
	trigger := strings.TrimSpace(req.Trigger)
	if trigger == "" {
		trigger = "manual"
	}
	body := map[string]any{
		"items":   refs,
		"trigger": trigger,
		"rule_id": ruleID,
	}
	resp, status, err := proxyAggregationPost(c.GetHeader("Authorization"), "/admin/quality/re-encode", body)
	if err != nil {
		c.JSON(http.StatusBadGateway, authErrorResponse{Message: "Aggregation refused: " + err.Error(), Code: "AGGREGATION_FAILED"})
		return
	}
	if status >= 300 {
		c.JSON(http.StatusBadGateway, authErrorResponse{Message: "Aggregation responded " + strconv.Itoa(status) + ": " + string(resp), Code: "AGGREGATION_FAILED"})
		return
	}
	c.JSON(http.StatusOK, reEncodeResponse{
		Enqueued: len(items), EstimatedFreed: freed,
	})
}

// =============================================================================
// Probe — fresh ffprobe via Aggregation, plus per-profile projected savings
// =============================================================================

type probeResponse struct {
	ContentItemID    string                  `json:"content_item_id"`
	DurationSec      *int                    `json:"duration_sec,omitempty"`
	Width            *int                    `json:"width,omitempty"`
	Height           *int                    `json:"height,omitempty"`
	BitrateKbps      *int                    `json:"bitrate_kbps,omitempty"`
	VideoCodec       *string                 `json:"video_codec,omitempty"`
	AudioCodec       *string                 `json:"audio_codec,omitempty"`
	FileSizeBytes    int64                   `json:"file_size_bytes"`
	StorageTier      *string                 `json:"storage_tier,omitempty"`
	CurrentProfileID *uint                   `json:"current_quality_profile_id,omitempty"`
	Projections      []probeProjectionEntry  `json:"projections"`
}

type probeProjectionEntry struct {
	ProfileID          uint   `json:"profile_id"`
	ProfileName        string `json:"profile_name"`
	ProjectedSizeBytes int64  `json:"projected_size_bytes"`
	ProjectedSavings   int64  `json:"projected_savings_bytes"`
}

// ProbeContentItem handles POST /admin/quality/probe/:id
func ProbeContentItem(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid id", Code: "INVALID"})
		return
	}
	var it models.ContentItem
	if err := db.Where("public_id = ? AND tenant_id = ?", id, principal.TenantID).First(&it).Error; err != nil {
		c.JSON(http.StatusNotFound, authErrorResponse{Message: "Content not found", Code: "NOT_FOUND"})
		return
	}

	// Ask Aggregation to ffprobe the live S3 object.
	tier := "primary"
	if it.StorageTier != nil {
		tier = *it.StorageTier
	}
	body, status, err := proxyAggregationPost(c.GetHeader("Authorization"), "/admin/quality/probe", map[string]any{
		"content_item_id": it.PublicID.String(),
		"tier":            tier,
	})
	if err != nil || status >= 300 {
		// Fall back to DB-known values if Aggregation can't reach S3.
		resp := probeResponse{
			ContentItemID:    it.PublicID.String(),
			DurationSec:      it.DurationSec,
			BitrateKbps:      it.CurrentBitrateKbps,
			FileSizeBytes:    it.FileSizeBytes,
			StorageTier:      it.StorageTier,
			CurrentProfileID: it.CurrentQualityProfileID,
		}
		resp.Projections = computeProjections(db, principal.TenantID, it)
		c.JSON(http.StatusOK, resp)
		return
	}

	var probe struct {
		Duration    float64 `json:"duration"`
		Width       int     `json:"width"`
		Height      int     `json:"height"`
		BitrateKbps int     `json:"bitrate_kbps"`
		VideoCodec  string  `json:"video_codec"`
		AudioCodec  string  `json:"audio_codec"`
	}
	_ = json.Unmarshal(body, &probe)

	dur := int(probe.Duration)
	resp := probeResponse{
		ContentItemID:    it.PublicID.String(),
		DurationSec:      &dur,
		FileSizeBytes:    it.FileSizeBytes,
		StorageTier:      it.StorageTier,
		CurrentProfileID: it.CurrentQualityProfileID,
	}
	if probe.Width > 0 {
		resp.Width = &probe.Width
	}
	if probe.Height > 0 {
		resp.Height = &probe.Height
	}
	if probe.BitrateKbps > 0 {
		resp.BitrateKbps = &probe.BitrateKbps
	}
	if probe.VideoCodec != "" {
		v := probe.VideoCodec
		resp.VideoCodec = &v
	}
	if probe.AudioCodec != "" {
		v := probe.AudioCodec
		resp.AudioCodec = &v
	}
	resp.Projections = computeProjections(db, principal.TenantID, it)
	c.JSON(http.StatusOK, resp)
}

func computeProjections(db *gorm.DB, tenantID string, it models.ContentItem) []probeProjectionEntry {
	var profiles []models.QualityProfile
	db.Where("(tenant_id IS NULL OR tenant_id = ?) AND is_active = TRUE", tenantID).
		Order("name ASC").Find(&profiles)
	out := make([]probeProjectionEntry, 0, len(profiles))
	for _, p := range profiles {
		projected := projectSizeBytes(it, p)
		savings := it.FileSizeBytes - projected
		if savings < 0 {
			savings = 0
		}
		out = append(out, probeProjectionEntry{
			ProfileID:          p.ID,
			ProfileName:        p.Name,
			ProjectedSizeBytes: projected,
			ProjectedSavings:   savings,
		})
	}
	return out
}

// =============================================================================
// History + lifetime stats
// =============================================================================

type historyResponse struct {
	Data  []models.QualityHistory `json:"data"`
	Total int64                   `json:"total"`
}

// ListQualityHistory handles GET /admin/quality/history
func ListQualityHistory(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	limit := atoiDefault(c.Query("limit"), 50)
	if limit > 200 {
		limit = 200
	}
	var total int64
	db.Model(&models.QualityHistory{}).Where("tenant_id = ?", principal.TenantID).Count(&total)
	var rows []models.QualityHistory
	db.Where("tenant_id = ?", principal.TenantID).Order("created_at DESC").Limit(limit).Find(&rows)
	c.JSON(http.StatusOK, historyResponse{Data: rows, Total: total})
}

type qualityStatsResponse struct {
	TotalReencoded         int64                  `json:"total_reencoded"`
	TotalBytesSaved        int64                  `json:"total_bytes_saved"`
	EstimatedEgressSaved   int64                  `json:"estimated_egress_saved_bytes"`
	SavingsByProfile       []profileSavingsEntry  `json:"savings_by_profile"`
	LastReencodeAt         *string                `json:"last_reencode_at,omitempty"`
	ItemsAtNonDefault      int64                  `json:"items_at_non_default_profile"`
}

type profileSavingsEntry struct {
	ProfileID    uint   `json:"profile_id"`
	ProfileName  string `json:"profile_name"`
	ItemCount    int64  `json:"item_count"`
	BytesSaved   int64  `json:"bytes_saved"`
}

// GetQualityStats handles GET /admin/quality/stats
func GetQualityStats(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	resp := qualityStatsResponse{}

	// Lifetime aggregates from history.
	var totals struct {
		Total      int64
		BytesSaved int64
	}
	db.Model(&models.QualityHistory{}).
		Where("tenant_id = ? AND error = ''", principal.TenantID).
		Select("COUNT(*) as total, COALESCE(SUM(savings_bytes),0) as bytes_saved").
		Scan(&totals)
	resp.TotalReencoded = totals.Total
	resp.TotalBytesSaved = totals.BytesSaved

	// Estimated egress saved = SUM(savings × view_count) joined with content_items.
	type egressRow struct {
		Saved int64
	}
	var er egressRow
	db.Raw(`
        SELECT COALESCE(SUM(qh.savings_bytes * ci.view_count), 0) as saved
        FROM quality_history qh
        JOIN content_items ci ON ci.public_id = qh.content_item_id
        WHERE qh.tenant_id = ? AND qh.error = ''`, principal.TenantID).Scan(&er)
	resp.EstimatedEgressSaved = er.Saved

	// Per-profile breakdown.
	type byProfile struct {
		ProfileID  uint
		Name       string
		ItemCount  int64
		BytesSaved int64
	}
	var perProfile []byProfile
	db.Raw(`
        SELECT qh.to_profile_id as profile_id,
               qp.name,
               COUNT(*) as item_count,
               COALESCE(SUM(qh.savings_bytes), 0) as bytes_saved
        FROM quality_history qh
        LEFT JOIN quality_profiles qp ON qp.id = qh.to_profile_id
        WHERE qh.tenant_id = ? AND qh.error = ''
        GROUP BY qh.to_profile_id, qp.name
        ORDER BY bytes_saved DESC`, principal.TenantID).Scan(&perProfile)
	for _, r := range perProfile {
		resp.SavingsByProfile = append(resp.SavingsByProfile, profileSavingsEntry{
			ProfileID:   r.ProfileID,
			ProfileName: r.Name,
			ItemCount:   r.ItemCount,
			BytesSaved:  r.BytesSaved,
		})
	}

	var last models.QualityHistory
	if err := db.Where("tenant_id = ? AND error = ''", principal.TenantID).
		Order("created_at DESC").First(&last).Error; err == nil {
		s := last.CreatedAt.UTC().Format(time.RFC3339)
		resp.LastReencodeAt = &s
	}

	// Items still at the default (or unknown) profile — opportunity counter.
	db.Model(&models.ContentItem{}).
		Where("tenant_id = ? AND status = ? AND current_quality_profile_id IS NOT NULL AND file_size_bytes > 0",
			principal.TenantID, models.ContentStatusReady).
		Count(&resp.ItemsAtNonDefault)

	c.JSON(http.StatusOK, resp)
}

// =============================================================================
// Aggregation pings
// =============================================================================

func notifyAggregationQualityRuleChanged(authHeader string) {
	go func() {
		_, _, _ = proxyAggregationPost(authHeader, "/admin/quality/rule-changed", map[string]any{})
	}()
}
