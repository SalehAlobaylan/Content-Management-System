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
// Quality Profiles — CRUD + Resolve
//
// Phase 7 pivot: this controller now manages the platform's INGEST profiles
// (what encode params Aggregation uses for fresh content). Profiles are
// scoped by (tenant_id, source_type) and resolved most-specific-first.
// Re-encoding old content moved to the Storage system as
// archive_action='re_encode' — there's no rules / candidates / history surface
// here anymore.
// =============================================================================

// validSourceTypes mirrors models.SourceType. Kept as a small set for
// validation; admin can scope a profile to one of these or leave NULL for
// "any source".
var validSourceTypes = map[string]bool{
	"RSS": true, "WEBSITE": true, "TELEGRAM": true, "PODCAST": true,
	"YOUTUBE": true, "UPLOAD": true, "MANUAL": true,
}

// validOutputContainers — extension names accepted in the output_container
// column. HLS / DASH would need pipeline support and are deliberately omitted.
var validOutputContainers = map[string]bool{
	"mp4": true, "webm": true, "mov": true,
}

// -----------------------------------------------------------------------------
// List / Get
// -----------------------------------------------------------------------------

// ListQualityProfiles handles GET /admin/quality/profiles?scope=global|tenant|all
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
	default:
		q = q.Where("tenant_id IS NULL OR tenant_id = ?", principal.TenantID)
	}

	var profiles []models.QualityProfile
	if err := q.Order("tenant_id NULLS FIRST, source_type NULLS FIRST, name ASC").
		Find(&profiles).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to list profiles", Code: "LIST_FAILED"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": profiles})
}

// -----------------------------------------------------------------------------
// Create / Update / Delete
// -----------------------------------------------------------------------------

type qualityProfileRequest struct {
	Scope             string  `json:"scope"`        // global | tenant — only used at create time
	SourceType        *string `json:"source_type"`  // nullable: NULL = any source
	Name              string  `json:"name"`
	Description       string  `json:"description"`
	VideoCodec        string  `json:"video_codec"`
	MaxHeight         int     `json:"max_height"`
	TargetBitrateKbps *int    `json:"target_bitrate_kbps"`
	CRF               *int    `json:"crf"`
	Preset            string  `json:"preset"`
	AudioCodec        string  `json:"audio_codec"`
	AudioBitrateKbps  int     `json:"audio_bitrate_kbps"`

	OutputContainer        string `json:"output_container"`
	ThumbnailOffsetSeconds int    `json:"thumbnail_offset_seconds"`
	ThumbnailMaxHeight     int    `json:"thumbnail_max_height"`

	AllowedInputMimeTypes []string `json:"allowed_input_mime_types"`
	MaxInputSizeBytes     *int64   `json:"max_input_size_bytes"`
	MaxInputDurationSec   *int     `json:"max_input_duration_sec"`

	// PresetKey is descriptive — which Console preset spawned this profile.
	// Empty string for hand-built / custom profiles. No semantic validation
	// (Console picks from a known list); we just bound the length defensively.
	PresetKey *string `json:"preset_key"`

	IsActive *bool `json:"is_active"`
}

func validateAndApplyProfile(p *models.QualityProfile, req qualityProfileRequest) error {
	if strings.TrimSpace(req.Name) == "" {
		return errors.New("name is required")
	}

	// Source type — nullable, but if given must be a known value.
	if req.SourceType != nil {
		st := strings.ToUpper(strings.TrimSpace(*req.SourceType))
		if st == "" {
			p.SourceType = nil
		} else {
			if !validSourceTypes[st] {
				return fmt.Errorf("source_type must be one of RSS, WEBSITE, TELEGRAM, PODCAST, YOUTUBE, UPLOAD, MANUAL (got %q)", st)
			}
			p.SourceType = &st
		}
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
	if req.CRF != nil && (*req.CRF < 0 || *req.CRF > 51) {
		return errors.New("crf must be between 0 and 51")
	}
	if req.MaxHeight < 0 || req.MaxHeight > 4320 {
		return errors.New("max_height must be between 0 and 4320")
	}
	if req.TargetBitrateKbps != nil && (*req.TargetBitrateKbps < 0 || *req.TargetBitrateKbps > 100_000) {
		return errors.New("target_bitrate_kbps out of range")
	}
	if req.AudioBitrateKbps < 0 || req.AudioBitrateKbps > 1024 {
		return errors.New("audio_bitrate_kbps out of range")
	}

	container := strings.ToLower(strings.TrimSpace(req.OutputContainer))
	if container == "" {
		container = "mp4"
	}
	if !validOutputContainers[container] {
		return errors.New("output_container must be mp4, webm, or mov")
	}

	if req.ThumbnailOffsetSeconds < 0 || req.ThumbnailOffsetSeconds > 3600 {
		return errors.New("thumbnail_offset_seconds out of range")
	}
	if req.ThumbnailMaxHeight < 0 || req.ThumbnailMaxHeight > 4320 {
		return errors.New("thumbnail_max_height out of range")
	}
	if req.MaxInputSizeBytes != nil && *req.MaxInputSizeBytes < 0 {
		return errors.New("max_input_size_bytes must be non-negative")
	}
	if req.MaxInputDurationSec != nil && *req.MaxInputDurationSec < 0 {
		return errors.New("max_input_duration_sec must be non-negative")
	}

	p.Name = strings.TrimSpace(req.Name)
	p.Description = req.Description
	p.VideoCodec = codec
	p.MaxHeight = req.MaxHeight
	if req.TargetBitrateKbps != nil {
		p.TargetBitrateKbps = *req.TargetBitrateKbps
	}
	switch {
	case req.CRF != nil:
		p.CRF = *req.CRF
	case p.TargetBitrateKbps > 0:
		p.CRF = 0
	default:
		p.CRF = 23
	}
	p.Preset = preset
	p.AudioCodec = audio
	if req.AudioBitrateKbps == 0 {
		p.AudioBitrateKbps = 128
	} else {
		p.AudioBitrateKbps = req.AudioBitrateKbps
	}

	p.OutputContainer = container
	if req.ThumbnailOffsetSeconds > 0 {
		p.ThumbnailOffsetSeconds = req.ThumbnailOffsetSeconds
	} else if p.ThumbnailOffsetSeconds == 0 {
		p.ThumbnailOffsetSeconds = 2
	}
	if req.ThumbnailMaxHeight > 0 {
		p.ThumbnailMaxHeight = req.ThumbnailMaxHeight
	} else if p.ThumbnailMaxHeight == 0 {
		p.ThumbnailMaxHeight = 360
	}

	if req.AllowedInputMimeTypes != nil {
		// Normalize to lowercase, drop empties.
		cleaned := make([]string, 0, len(req.AllowedInputMimeTypes))
		for _, m := range req.AllowedInputMimeTypes {
			t := strings.ToLower(strings.TrimSpace(m))
			if t != "" {
				cleaned = append(cleaned, t)
			}
		}
		p.AllowedInputMimeTypes = cleaned
	}

	p.MaxInputSizeBytes = req.MaxInputSizeBytes
	p.MaxInputDurationSec = req.MaxInputDurationSec

	if req.PresetKey != nil {
		k := strings.TrimSpace(*req.PresetKey)
		if len(k) > 32 {
			return errors.New("preset_key must be 32 characters or fewer")
		}
		p.PresetKey = k
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
	if err := db.Save(&p).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to save", Code: "SAVE_FAILED"})
		return
	}
	c.JSON(http.StatusOK, p)
}

// DeleteQualityProfile handles DELETE /admin/quality/profiles/:id
//
// Refuses if any storage policy uses this profile as its
// re_encode_target_profile_id — detach the storage policy first.
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
	db.Model(&models.StoragePolicy{}).Where("re_encode_target_profile_id = ?", id).Count(&refCount)
	if refCount > 0 {
		c.JSON(http.StatusConflict, authErrorResponse{
			Message: fmt.Sprintf("Profile is referenced by %d storage policy/policies; clear the re-encode target first", refCount),
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

// =============================================================================
// Resolve — preview which profile applies for a (tenant, source_type) input
// =============================================================================

type resolveResult struct {
	Profile     *models.QualityProfile `json:"profile,omitempty"`
	MatchedOn   string                 `json:"matched_on"` // tenant+source | tenant | source | global | none
	UsedDefault bool                   `json:"used_default"`
}

// ResolveQualityProfile handles GET /admin/quality/profiles/resolve?tenant_id=X&source_type=Y
//
// Used by the Console "what would apply" preview AND (via the internal route)
// by Aggregation on every ingest job. Resolution order, most-specific first:
//
//	1. tenant_id=X AND source_type=Y
//	2. tenant_id=X AND source_type=NULL
//	3. tenant_id=NULL AND source_type=Y
//	4. tenant_id=NULL AND source_type=NULL  (the global default)
func ResolveQualityProfile(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	tenantID := strings.TrimSpace(c.DefaultQuery("tenant_id", principal.TenantID))
	sourceType := strings.ToUpper(strings.TrimSpace(c.Query("source_type")))

	profile, matched := resolveProfile(db, tenantID, sourceType)
	if profile == nil {
		c.JSON(http.StatusOK, resolveResult{MatchedOn: "none", UsedDefault: true})
		return
	}
	c.JSON(http.StatusOK, resolveResult{Profile: profile, MatchedOn: matched})
}

// resolveProfile is the shared implementation used by both the admin and
// internal endpoints. Returns the winning profile and a tag describing which
// rung of the resolution ladder matched.
//
// Strategy: fetch every candidate profile (any rung that could possibly
// match the inputs) in a single query, then pick the most-specific match
// in pure Go via pickMostSpecificProfile. One DB roundtrip + testable
// without DB mocking.
func resolveProfile(db *gorm.DB, tenantID, sourceType string) (*models.QualityProfile, string) {
	q := db.Model(&models.QualityProfile{}).Where("is_active = TRUE")
	if tenantID != "" {
		q = q.Where("tenant_id IS NULL OR tenant_id = ?", tenantID)
	} else {
		q = q.Where("tenant_id IS NULL")
	}
	if sourceType != "" {
		q = q.Where("source_type IS NULL OR source_type = ?", sourceType)
	} else {
		q = q.Where("source_type IS NULL")
	}

	var candidates []models.QualityProfile
	if err := q.Find(&candidates).Error; err != nil {
		return nil, "none"
	}
	return pickMostSpecificProfile(candidates, tenantID, sourceType)
}

// pickMostSpecificProfile is the pure-Go ranker. Given a candidate set
// (already filtered to "could possibly match"), it returns the most-specific
// row using the precedence:
//
//	tenant+source > tenant > source > global
//
// Within a tie (multiple rows at the same specificity, e.g. two global
// profiles both with source_type=NULL), the row with the lowest ID wins —
// deterministic and matches "earliest created".
func pickMostSpecificProfile(profiles []models.QualityProfile, tenantID, sourceType string) (*models.QualityProfile, string) {
	var (
		best      *models.QualityProfile
		bestScore int
		bestTag   string
	)
	for i := range profiles {
		p := &profiles[i]
		hasTenant := p.TenantID != nil && *p.TenantID == tenantID
		hasSource := p.SourceType != nil && *p.SourceType == sourceType
		// Sanity: skip rows that don't match the inputs at all. The SQL
		// pre-filter already drops most non-matches but we belt-and-suspenders
		// in case a tenant-specific row leaks through with a different tenant.
		if p.TenantID != nil && *p.TenantID != tenantID {
			continue
		}
		if p.SourceType != nil && *p.SourceType != sourceType {
			continue
		}
		score := 0
		tag := "global"
		switch {
		case hasTenant && hasSource:
			score = 3
			tag = "tenant+source"
		case hasTenant:
			score = 2
			tag = "tenant"
		case hasSource:
			score = 1
			tag = "source"
		}
		if best == nil || score > bestScore || (score == bestScore && p.ID < best.ID) {
			best = p
			bestScore = score
			bestTag = tag
		}
	}
	if best == nil {
		return nil, "none"
	}
	return best, bestTag
}

// =============================================================================
// Probe — read-only diagnostic, kept from Phase 3
// =============================================================================

type probeProjectionEntry struct {
	ProfileID          uint   `json:"profile_id"`
	ProfileName        string `json:"profile_name"`
	ProjectedSizeBytes int64  `json:"projected_size_bytes"`
	ProjectedSavings   int64  `json:"projected_savings_bytes"`
}

type probeResponse struct {
	ContentItemID string                 `json:"content_item_id"`
	DurationSec   *int                   `json:"duration_sec,omitempty"`
	Width         *int                   `json:"width,omitempty"`
	Height        *int                   `json:"height,omitempty"`
	BitrateKbps   *int                   `json:"bitrate_kbps,omitempty"`
	VideoCodec    *string                `json:"video_codec,omitempty"`
	AudioCodec    *string                `json:"audio_codec,omitempty"`
	FileSizeBytes int64                  `json:"file_size_bytes"`
	StorageTier   *string                `json:"storage_tier,omitempty"`
	Projections   []probeProjectionEntry `json:"projections"`
}

// ProbeContentItem handles POST /admin/quality/probe-item/:id
//
// Same semantic as the previous /admin/quality/probe/:id — renamed for
// clarity (the new Quality page is about ingest config, this is a tools
// helper rather than the page's primary purpose).
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
			ContentItemID: it.PublicID.String(),
			DurationSec:   it.DurationSec,
			BitrateKbps:   it.CurrentBitrateKbps,
			FileSizeBytes: it.FileSizeBytes,
			StorageTier:   it.StorageTier,
		}
		resp.Projections = computeProbeProjections(db, principal.TenantID, it)
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
		ContentItemID: it.PublicID.String(),
		DurationSec:   &dur,
		FileSizeBytes: it.FileSizeBytes,
		StorageTier:   it.StorageTier,
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
	resp.Projections = computeProbeProjections(db, principal.TenantID, it)
	c.JSON(http.StatusOK, resp)
}

func computeProbeProjections(db *gorm.DB, tenantID string, it models.ContentItem) []probeProjectionEntry {
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
// Projection helpers — also used by Storage when computing optimistic
// freed_bytes for an archive_action='re_encode' sweep.
// =============================================================================

// projectSizeBytes estimates the post-re-encode size of an item under a given
// profile. Heuristic: bitrate × duration when both are known; otherwise
// per-codec fallback. Same shape as the Phase 3 implementation.
func projectSizeBytes(it models.ContentItem, p models.QualityProfile) int64 {
	if it.DurationSec == nil || *it.DurationSec <= 0 {
		return it.FileSizeBytes
	}
	dur := int64(*it.DurationSec)
	var vKbps int
	if p.TargetBitrateKbps > 0 {
		vKbps = p.TargetBitrateKbps
	} else {
		vKbps = bitrateForHeight(p.MaxHeight, p.CRF)
	}
	totalKbps := int64(vKbps + p.AudioBitrateKbps)
	return (totalKbps * dur * 1000) / 8
}

// bitrateForHeight is a rough kbps lookup table per output resolution at
// CRF 23, with an exponential adjustment for non-23 CRF values.
func bitrateForHeight(h, crf int) int {
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

// =============================================================================
// Touch helper to silence the unused-import warning during the transition.
// =============================================================================

var _ = time.Now
