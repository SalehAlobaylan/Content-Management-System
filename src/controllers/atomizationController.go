package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

const (
	chapterStatusDraft             = "draft"
	chapterStatusReview            = "needs_review"
	chapterStatusPublished         = "published"
	chapterStatusRejected          = "rejected"
	feedVisibilityVisible          = "visible"
	feedVisibilityHidden           = "hidden"
	feedVisibilityReview           = "review"
	feedVisibilityEmbeddingPending = "embedding_pending"
	atomizationOverrideInherit     = "inherit"
	atomizationOverrideDisabled    = "disabled"
	atomizationOverrideEnabled     = "enabled"
)

type mediaAtomizationSchemaInfo struct {
	Ready   bool     `json:"ready"`
	Missing []string `json:"missing"`
	Message string   `json:"message"`
}

type atomizationPolicy struct {
	ChapteringEnabled           bool    `json:"chaptering_enabled"`
	AutoPublishHighConfidence   bool    `json:"auto_publish_high_confidence"`
	ParentFeedVisible           bool    `json:"parent_feed_visible"`
	PreserveVideo               bool    `json:"preserve_video"`
	RemoveSponsorSegments       bool    `json:"remove_sponsor_segments"`
	MinChapterMinutes           int     `json:"min_chapter_minutes"`
	MinFeedUnitSeconds          int     `json:"min_feed_unit_seconds"`
	SoftMaxChapterMinutes       int     `json:"soft_max_chapter_minutes"`
	HardMaxChapterMinutes       int     `json:"hard_max_chapter_minutes"`
	AtomizationMinParentSeconds int     `json:"atomization_min_parent_seconds"`
	MaxChaptersPerParent        int     `json:"max_chapters_per_parent"`
	ChapteringMode              string  `json:"chaptering_mode"`
	HighConfidenceThreshold     float64 `json:"high_confidence_threshold"`
	PreferredPlaybackRendition  string  `json:"preferred_playback_rendition"`
	FallbackPlaybackRendition   string  `json:"fallback_playback_rendition"`
	AudioOnlyAllowed            bool    `json:"audio_only_allowed"`
}

const (
	atomizationMinParentDurationSec = forYouHardMaxDurationSec
)

func defaultAtomizationPolicy() atomizationPolicy {
	return atomizationPolicy{
		ChapteringEnabled:           true,
		AutoPublishHighConfidence:   true,
		ParentFeedVisible:           false,
		PreserveVideo:               true,
		RemoveSponsorSegments:       true,
		MinChapterMinutes:           5,
		MinFeedUnitSeconds:          forYouMinDurationSec,
		SoftMaxChapterMinutes:       30,
		HardMaxChapterMinutes:       40,
		AtomizationMinParentSeconds: atomizationMinParentDurationSec,
		MaxChaptersPerParent:        5,
		ChapteringMode:              "contextual",
		HighConfidenceThreshold:     0.82,
		PreferredPlaybackRendition:  "hls",
		FallbackPlaybackRendition:   "mp4",
		AudioOnlyAllowed:            true,
	}
}

func policyFromModel(model models.MediaAtomizationPolicy) atomizationPolicy {
	return atomizationPolicy{
		ChapteringEnabled:           model.ChapteringEnabled,
		AutoPublishHighConfidence:   model.AutoPublishHighConfidence,
		ParentFeedVisible:           model.ParentFeedVisible,
		PreserveVideo:               model.PreserveVideo,
		RemoveSponsorSegments:       model.RemoveSponsorSegments,
		MinChapterMinutes:           model.MinChapterMinutes,
		MinFeedUnitSeconds:          model.MinFeedUnitSeconds,
		SoftMaxChapterMinutes:       model.SoftMaxChapterMinutes,
		HardMaxChapterMinutes:       model.HardMaxChapterMinutes,
		AtomizationMinParentSeconds: model.AtomizationMinParentSeconds,
		MaxChaptersPerParent:        model.MaxChaptersPerParent,
		ChapteringMode:              model.ChapteringMode,
		HighConfidenceThreshold:     model.HighConfidenceThreshold,
		PreferredPlaybackRendition:  model.PreferredPlaybackRendition,
		FallbackPlaybackRendition:   model.FallbackPlaybackRendition,
		AudioOnlyAllowed:            model.AudioOnlyAllowed,
	}
}

func getOrCreateMediaAtomizationPolicy(db *gorm.DB, tenantID string) models.MediaAtomizationPolicy {
	var policy models.MediaAtomizationPolicy
	if err := db.Where("tenant_id = ?", tenantID).First(&policy).Error; err == nil {
		return policy
	}
	policy = models.DefaultMediaAtomizationPolicy(tenantID)
	_ = db.Create(&policy).Error
	return policy
}

func mergeAtomizationPolicy(base atomizationPolicy, raw datatypes.JSON) atomizationPolicy {
	p := base
	cfg, _ := parseSourceAPIConfig(raw)
	if len(cfg) == 0 {
		return p
	}
	return applyAtomizationPolicyConfig(p, cfg)
}

func applyAtomizationPolicyConfig(p atomizationPolicy, cfg map[string]interface{}) atomizationPolicy {
	p.ChapteringEnabled = boolConfig(cfg, "chaptering_enabled", p.ChapteringEnabled)
	p.AutoPublishHighConfidence = boolConfig(cfg, "auto_publish_high_confidence", p.AutoPublishHighConfidence)
	p.ParentFeedVisible = boolConfig(cfg, "parent_feed_visible", p.ParentFeedVisible)
	p.PreserveVideo = boolConfig(cfg, "preserve_video", p.PreserveVideo)
	p.RemoveSponsorSegments = boolConfig(cfg, "remove_sponsor_segments", p.RemoveSponsorSegments)
	p.AudioOnlyAllowed = boolConfig(cfg, "audio_only_allowed", p.AudioOnlyAllowed)
	p.MinChapterMinutes = intConfig(cfg, "min_chapter_minutes", p.MinChapterMinutes)
	p.MinFeedUnitSeconds = intConfig(cfg, "min_feed_unit_seconds", p.MinFeedUnitSeconds)
	p.SoftMaxChapterMinutes = intConfig(cfg, "soft_max_chapter_minutes", p.SoftMaxChapterMinutes)
	p.HardMaxChapterMinutes = intConfig(cfg, "hard_max_chapter_minutes", p.HardMaxChapterMinutes)
	p.AtomizationMinParentSeconds = intConfig(cfg, "atomization_min_parent_seconds", p.AtomizationMinParentSeconds)
	p.MaxChaptersPerParent = intConfig(cfg, "max_chapters_per_parent", p.MaxChaptersPerParent)
	p.HighConfidenceThreshold = floatConfig(cfg, "high_confidence_threshold", p.HighConfidenceThreshold)
	p.ChapteringMode = stringConfig(cfg, "chaptering_mode", p.ChapteringMode)
	p.PreferredPlaybackRendition = stringConfig(cfg, "preferred_playback_rendition", p.PreferredPlaybackRendition)
	p.FallbackPlaybackRendition = stringConfig(cfg, "fallback_playback_rendition", p.FallbackPlaybackRendition)
	return p
}

func atomizationPolicyToMap(p atomizationPolicy) map[string]interface{} {
	return map[string]interface{}{
		"chaptering_enabled":             p.ChapteringEnabled,
		"auto_publish_high_confidence":   p.AutoPublishHighConfidence,
		"parent_feed_visible":            p.ParentFeedVisible,
		"preserve_video":                 p.PreserveVideo,
		"remove_sponsor_segments":        p.RemoveSponsorSegments,
		"min_chapter_minutes":            p.MinChapterMinutes,
		"min_feed_unit_seconds":          p.MinFeedUnitSeconds,
		"soft_max_chapter_minutes":       p.SoftMaxChapterMinutes,
		"hard_max_chapter_minutes":       p.HardMaxChapterMinutes,
		"atomization_min_parent_seconds": p.AtomizationMinParentSeconds,
		"max_chapters_per_parent":        p.MaxChaptersPerParent,
		"chaptering_mode":                p.ChapteringMode,
		"high_confidence_threshold":      p.HighConfidenceThreshold,
		"preferred_playback_rendition":   p.PreferredPlaybackRendition,
		"fallback_playback_rendition":    p.FallbackPlaybackRendition,
		"audio_only_allowed":             p.AudioOnlyAllowed,
	}
}

func validateAtomizationPolicy(p atomizationPolicy) atomizationPolicy {
	if p.MinFeedUnitSeconds < forYouMinDurationSec {
		p.MinFeedUnitSeconds = forYouMinDurationSec
	}
	if p.AtomizationMinParentSeconds < atomizationMinParentDurationSec {
		p.AtomizationMinParentSeconds = atomizationMinParentDurationSec
	}
	if p.HardMaxChapterMinutes <= 0 || p.HardMaxChapterMinutes*60 > forYouHardMaxDurationSec {
		p.HardMaxChapterMinutes = forYouHardMaxDurationSec / 60
	}
	if p.SoftMaxChapterMinutes <= 0 || p.SoftMaxChapterMinutes > p.HardMaxChapterMinutes {
		p.SoftMaxChapterMinutes = 30
	}
	if p.MinChapterMinutes <= 0 {
		p.MinChapterMinutes = 5
	}
	if p.MaxChaptersPerParent <= 0 {
		p.MaxChaptersPerParent = 5
	}
	if p.HighConfidenceThreshold <= 0 || p.HighConfidenceThreshold > 1 {
		p.HighConfidenceThreshold = 0.82
	}
	if strings.TrimSpace(p.ChapteringMode) == "" {
		p.ChapteringMode = "contextual"
	}
	if strings.TrimSpace(p.PreferredPlaybackRendition) == "" {
		p.PreferredPlaybackRendition = "hls"
	}
	if strings.TrimSpace(p.FallbackPlaybackRendition) == "" {
		p.FallbackPlaybackRendition = "mp4"
	}
	return p
}

func boolConfig(cfg map[string]interface{}, key string, fallback bool) bool {
	if v, ok := cfg[key].(bool); ok {
		return v
	}
	return fallback
}

func intConfig(cfg map[string]interface{}, key string, fallback int) int {
	switch v := cfg[key].(type) {
	case float64:
		if v > 0 {
			return int(v)
		}
	case int:
		if v > 0 {
			return v
		}
	}
	return fallback
}

func floatConfig(cfg map[string]interface{}, key string, fallback float64) float64 {
	if v, ok := cfg[key].(float64); ok && v > 0 {
		return v
	}
	return fallback
}

func stringConfig(cfg map[string]interface{}, key, fallback string) string {
	if v, ok := cfg[key].(string); ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	return fallback
}

func durationBucketLabel(ms int) string {
	minutes := int(math.Round(float64(ms) / 60000.0))
	buckets := []int{5, 10, 15, 20, 30, 40}
	best := buckets[0]
	bestDelta := math.Abs(float64(minutes - best))
	for _, b := range buckets[1:] {
		if d := math.Abs(float64(minutes - b)); d < bestDelta {
			best = b
			bestDelta = d
		}
	}
	return strconv.Itoa(best) + "m"
}

func minFeedUnitMs(policy atomizationPolicy) int {
	seconds := policy.MinFeedUnitSeconds
	if seconds <= 0 {
		seconds = forYouMinDurationSec
	}
	return seconds * 1000
}

func sourceForItem(db *gorm.DB, item *models.ContentItem) *models.ContentSource {
	if item.SourceFeedURL == nil || strings.TrimSpace(*item.SourceFeedURL) == "" {
		return nil
	}
	var source models.ContentSource
	if err := db.Where("tenant_id = ? AND feed_url = ?", item.TenantID, *item.SourceFeedURL).First(&source).Error; err != nil {
		return nil
	}
	return &source
}

type effectiveAtomizationPolicy struct {
	Policy         atomizationPolicy
	PolicySource   string
	DisabledReason *string
}

func atomizationPolicyForItem(db *gorm.DB, item *models.ContentItem) atomizationPolicy {
	return effectiveAtomizationPolicyForItem(db, item).Policy
}

func effectiveAtomizationPolicyForItem(db *gorm.DB, item *models.ContentItem) effectiveAtomizationPolicy {
	base := validateAtomizationPolicy(policyFromModel(getOrCreateMediaAtomizationPolicy(db, item.TenantID)))
	sourceName := "tenant"
	if source := sourceForItem(db, item); source != nil {
		cfg, _ := parseSourceAPIConfig(source.APIConfig)
		if len(cfg) > 0 {
			base = validateAtomizationPolicy(applyAtomizationPolicyConfig(base, cfg))
			sourceName = "source"
		}
	}
	override := atomizationOverrideInherit
	if item.AtomizationOverride != nil && strings.TrimSpace(*item.AtomizationOverride) != "" {
		override = strings.TrimSpace(*item.AtomizationOverride)
	}
	if override == atomizationOverrideDisabled {
		reason := "Episode atomization is disabled by admin override."
		if item.AtomizationOverrideReason != nil && strings.TrimSpace(*item.AtomizationOverrideReason) != "" {
			reason = strings.TrimSpace(*item.AtomizationOverrideReason)
		}
		base.ChapteringEnabled = false
		return effectiveAtomizationPolicy{Policy: base, PolicySource: "episode", DisabledReason: &reason}
	}
	if override == atomizationOverrideEnabled {
		base.ChapteringEnabled = true
		return effectiveAtomizationPolicy{Policy: base, PolicySource: "episode"}
	}
	if !base.ChapteringEnabled {
		reason := "Atomization is disabled by " + sourceName + " policy."
		return effectiveAtomizationPolicy{Policy: base, PolicySource: sourceName, DisabledReason: &reason}
	}
	return effectiveAtomizationPolicy{Policy: base, PolicySource: sourceName}
}

type atomizationInputResponse struct {
	Item             map[string]interface{} `json:"item"`
	Policy           atomizationPolicy      `json:"policy"`
	EffectivePolicy  atomizationPolicy      `json:"effective_policy"`
	PolicySource     string                 `json:"policy_source"`
	DisabledReason   *string                `json:"atomization_disabled_reason,omitempty"`
	ManualRequested  bool                   `json:"manual_requested"`
	Transcript       *studioTranscriptDTO   `json:"transcript,omitempty"`
	Segments         []segmentData          `json:"segments"`
	SponsorSegments  []sponsorSegment       `json:"sponsor_segments,omitempty"`
	ExistingChapters []studioChapterDTO     `json:"existing_chapters"`
}

func InternalListAtomizationCandidates(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	schema := getMediaAtomizationSchemaInfo(db)
	if !schema.Ready {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": schema.Message, "schema_status": schema})
		return
	}
	tenantID := strings.TrimSpace(c.DefaultQuery("tenant_id", "default"))
	limit := boundedLimit(c.Query("limit"), 25, 100)

	type candidateRow struct {
		ID                  string  `json:"id"`
		TenantID            string  `json:"tenant_id"`
		Type                string  `json:"type"`
		Title               *string `json:"title"`
		Excerpt             *string `json:"excerpt"`
		BodyText            *string `json:"body_text"`
		SourceName          *string `json:"source_name"`
		DurationSec         *int    `json:"duration_sec"`
		ChapteringStatus    *string `json:"chaptering_status"`
		TranscriptID        *string `json:"transcript_id"`
		ExistingChildCount  int64   `json:"existing_child_count"`
		MediaURL            *string `json:"media_url"`
		ThumbnailURL        *string `json:"thumbnail_url"`
		AtomizationOverride *string `json:"atomization_override"`
	}
	rows := []candidateRow{}
	if err := db.Raw(`
		SELECT p.public_id::text AS id, p.tenant_id, p.type, p.title, p.excerpt, p.body_text, p.source_name,
			p.duration_sec, p.chaptering_status, p.transcript_id::text AS transcript_id,
			COUNT(c.id) AS existing_child_count, p.media_url, p.thumbnail_url, p.atomization_override
		FROM content_items p
		LEFT JOIN content_sources s
			ON s.tenant_id = p.tenant_id
			AND s.feed_url = p.source_feed_url
		LEFT JOIN content_items c
			ON c.parent_content_item_id = p.public_id
			AND c.tenant_id = p.tenant_id
			AND c.status <> 'ARCHIVED'
			WHERE p.tenant_id = ?
				AND p.type IN ('VIDEO','PODCAST')
				AND p.parent_content_item_id IS NULL
				AND p.media_url IS NOT NULL AND p.media_url <> ''
				AND p.transcript_id IS NOT NULL
				AND p.duration_sec IS NOT NULL
				AND p.duration_sec > ?
				AND COALESCE(p.atomization_override, 'inherit') <> 'disabled'
				AND (
					COALESCE(s.api_config->>'chaptering_enabled', 'true') <> 'false'
					OR COALESCE(p.atomization_override, 'inherit') = 'enabled'
				)
				AND p.status IN ('READY','ARCHIVED')
				AND NOT EXISTS (
					SELECT 1 FROM transcription_jobs tj
					WHERE tj.content_item_id = p.public_id
						AND tj.status IN ('queued','running')
						AND tj.canceled = false
				)
				AND (
					COALESCE(p.chaptering_status, 'unstarted') NOT IN (
						'planning','cutting','renditions','children','embedding','embedding_pending',
						'completed','needs_review','published','archived'
					)
					OR (
						COALESCE(p.chaptering_status, 'unstarted') IN (
							'planning','cutting','renditions','children','embedding','embedding_pending'
						)
						AND p.updated_at < NOW() - INTERVAL '2 hours'
					)
				)
			GROUP BY p.public_id, p.tenant_id, p.type, p.title, p.excerpt, p.body_text, p.source_name, p.duration_sec, p.chaptering_status, p.transcript_id, p.media_url, p.thumbnail_url, p.atomization_override, p.updated_at
			HAVING COUNT(c.id) = 0
			ORDER BY
				CASE WHEN p.duration_sec IS NOT NULL AND p.duration_sec > ? THEN 0 ELSE 1 END,
				p.updated_at ASC
			LIMIT ?`, tenantID, atomizationMinParentDurationSec, atomizationMinParentDurationSec, limit).Scan(&rows).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to list atomization candidates: " + err.Error()})
		return
	}
	rows = filterAtomizationCandidateRows(db, rows)

	transcriptRows := []candidateRow{}
	if err := db.Raw(`
		SELECT p.public_id::text AS id, p.tenant_id, p.type, p.title, p.excerpt, p.body_text, p.source_name,
			p.duration_sec, p.chaptering_status, p.transcript_id::text AS transcript_id,
			COUNT(c.id) AS existing_child_count, p.media_url, p.thumbnail_url, p.atomization_override
		FROM content_items p
		LEFT JOIN content_sources s
			ON s.tenant_id = p.tenant_id
			AND s.feed_url = p.source_feed_url
		LEFT JOIN content_items c
			ON c.parent_content_item_id = p.public_id
			AND c.tenant_id = p.tenant_id
			AND c.status <> 'ARCHIVED'
		WHERE p.tenant_id = ?
			AND p.type IN ('VIDEO','PODCAST')
			AND p.parent_content_item_id IS NULL
			AND p.media_url IS NOT NULL AND p.media_url <> ''
			AND p.transcript_id IS NULL
			AND p.duration_sec IS NOT NULL
			AND p.duration_sec > ?
			AND COALESCE(p.atomization_override, 'inherit') <> 'disabled'
			AND (
				COALESCE(s.api_config->>'chaptering_enabled', 'true') <> 'false'
				OR COALESCE(p.atomization_override, 'inherit') = 'enabled'
			)
			AND p.status IN ('READY','ARCHIVED')
			AND NOT EXISTS (
				SELECT 1 FROM transcription_jobs tj
				WHERE tj.content_item_id = p.public_id
					AND tj.status IN ('queued','running')
					AND tj.canceled = false
			)
			AND COALESCE(p.chaptering_status, 'unstarted') NOT IN (
				'cutting','renditions','children','embedding','embedding_pending',
				'completed','needs_review','published','archived'
			)
		GROUP BY p.public_id, p.tenant_id, p.type, p.title, p.excerpt, p.body_text, p.source_name, p.duration_sec, p.chaptering_status, p.transcript_id, p.media_url, p.thumbnail_url, p.atomization_override, p.updated_at
		HAVING COUNT(c.id) = 0
		ORDER BY p.updated_at ASC
		LIMIT ?`, tenantID, atomizationMinParentDurationSec, limit).Scan(&transcriptRows).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to list atomization transcript candidates: " + err.Error()})
		return
	}
	transcriptRows = filterAtomizationCandidateRows(db, transcriptRows)

	c.JSON(http.StatusOK, gin.H{"items": rows, "transcript_candidates": transcriptRows})
}

func filterAtomizationCandidateRows[T interface{}](db *gorm.DB, rows []T) []T {
	filtered := make([]T, 0, len(rows))
	for _, row := range rows {
		raw, _ := json.Marshal(row)
		var probe struct {
			ID string `json:"id"`
		}
		if json.Unmarshal(raw, &probe) != nil || strings.TrimSpace(probe.ID) == "" {
			continue
		}
		id, err := uuid.Parse(probe.ID)
		if err != nil {
			continue
		}
		var item models.ContentItem
		if err := db.Where("public_id = ?", id).First(&item).Error; err != nil {
			continue
		}
		effective := effectiveAtomizationPolicyForItem(db, &item)
		if effective.Policy.ChapteringEnabled {
			filtered = append(filtered, row)
		}
	}
	return filtered
}

func InternalGetAtomizationInput(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid id"})
		return
	}
	var item models.ContentItem
	if err := db.Where("public_id = ?", id).First(&item).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Content not found"})
		return
	}
	if item.Type != models.ContentTypeVideo && item.Type != models.ContentTypePodcast {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Atomization only applies to VIDEO/PODCAST items"})
		return
	}
	if item.DurationSec == nil || *item.DurationSec <= atomizationMinParentDurationSec {
		c.JSON(http.StatusConflict, gin.H{"error": "Atomization only applies to media longer than 40 minutes"})
		return
	}
	effective := effectiveAtomizationPolicyForItem(db, &item)
	policy := effective.Policy
	var transcript *models.Transcript
	if item.TranscriptID != nil {
		var t models.Transcript
		if err := db.Where("public_id = ?", *item.TranscriptID).First(&t).Error; err == nil {
			transcript = &t
		}
	}
	segments := []segmentData{}
	var transcriptDTO *studioTranscriptDTO
	existing := []studioChapterDTO{}
	if transcript != nil {
		segments = extractSegments(transcript)
		dto := mapStudioTranscript(transcript)
		transcriptDTO = &dto
		existing = chaptersToDTO(loadOrSeedChapters(db, item.TenantID, transcript), durationMs(&item))
	}
	var meta struct {
		SponsorSegments []sponsorSegment `json:"sponsor_segments"`
	}
	if len(item.Metadata) > 0 {
		_ = json.Unmarshal(item.Metadata, &meta)
	}
	c.JSON(http.StatusOK, atomizationInputResponse{
		Item: map[string]interface{}{
			"id": item.PublicID.String(), "tenant_id": item.TenantID, "type": item.Type,
			"title": item.Title, "source": item.Source, "source_name": item.SourceName,
			"source_feed_url": item.SourceFeedURL, "media_url": item.MediaURL,
			"thumbnail_url": item.ThumbnailURL, "duration_sec": item.DurationSec,
			"original_url": item.OriginalURL, "published_at": item.PublishedAt,
			"has_video": item.HasVideo, "playback_url": item.PlaybackURL,
			"fallback_playback_url": item.FallbackPlaybackURL,
			"storage_tier":          item.StorageTier, "media_version": item.MediaVersion,
		},
		Policy: policy, EffectivePolicy: policy, PolicySource: effective.PolicySource,
		DisabledReason:  effective.DisabledReason,
		ManualRequested: item.ManualAtomizationRequestedAt != nil,
		Transcript:      transcriptDTO, Segments: segments,
		SponsorSegments: meta.SponsorSegments, ExistingChapters: existing,
	})
}

type atomizationChapterRequest struct {
	Title                string                   `json:"title"`
	Summary              *string                  `json:"summary"`
	StartMs              int                      `json:"start_ms"`
	EndMs                int                      `json:"end_ms"`
	Confidence           *float64                 `json:"confidence"`
	ContextLabel         *string                  `json:"context_label"`
	BoundaryReason       *string                  `json:"boundary_reason"`
	StandaloneScore      *float64                 `json:"standalone_score"`
	ContainsSponsorIntro bool                     `json:"contains_sponsor_intro"`
	NeedsReviewReason    *string                  `json:"needs_review_reason"`
	MediaURL             *string                  `json:"media_url"`
	ThumbnailURL         *string                  `json:"thumbnail_url"`
	PlaybackURL          *string                  `json:"playback_url"`
	PlaybackType         *string                  `json:"playback_type"`
	FallbackPlaybackURL  *string                  `json:"fallback_playback_url"`
	HasVideo             *bool                    `json:"has_video"`
	MediaRenditions      []map[string]interface{} `json:"media_renditions"`
	TranscriptSegments   []segmentData            `json:"transcript_segments"`
	TranscriptText       string                   `json:"transcript_text"`
}

type saveAtomizationPlanRequest struct {
	Chapters []atomizationChapterRequest `json:"chapters"`
}

func InternalSaveAtomizationPlan(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	item, transcript, ok := loadAtomizationParent(c, db)
	if !ok {
		return
	}
	var req saveAtomizationPlanRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}
	policy := atomizationPolicyForItem(db, item)
	rows := chaptersFromAtomizationRequest(item.TenantID, transcript.PublicID, req.Chapters, policy)
	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("transcript_id = ? AND tenant_id = ? AND source = ?", transcript.PublicID, item.TenantID, models.ChapterSourceDerived).
			Delete(&models.Chapter{}).Error; err != nil {
			return err
		}
		if len(rows) > 0 {
			return tx.Create(&rows).Error
		}
		return nil
	}); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save atomization plan"})
		return
	}
	status := "planned"
	item.ChapteringStatus = &status
	_ = db.Save(item).Error
	c.JSON(http.StatusOK, gin.H{"chapters": chaptersToDTO(rows, durationMs(item))})
}

func chaptersFromAtomizationRequest(tenantID string, transcriptID uuid.UUID, chapters []atomizationChapterRequest, policy atomizationPolicy) []models.Chapter {
	rows := make([]models.Chapter, 0, len(chapters))
	for _, ch := range chapters {
		title := strings.TrimSpace(ch.Title)
		if title == "" || ch.EndMs <= ch.StartMs {
			continue
		}
		conf := 0.0
		if ch.Confidence != nil {
			conf = *ch.Confidence
		}
		needsReviewReason := ch.NeedsReviewReason
		if needsReviewReason == nil && ch.EndMs-ch.StartMs < minFeedUnitMs(policy) {
			reason := "Chapter is below the 4:30 minimum feed duration."
			needsReviewReason = &reason
		}
		if needsReviewReason == nil && ch.EndMs-ch.StartMs > policy.HardMaxChapterMinutes*60_000 {
			reason := "Chapter exceeds hard maximum duration."
			needsReviewReason = &reason
		}
		status := chapterStatusReview
		if policy.AutoPublishHighConfidence && conf >= policy.HighConfidenceThreshold && needsReviewReason == nil {
			status = chapterStatusPublished
		}
		bucket := durationBucketLabel(ch.EndMs - ch.StartMs)
		end := ch.EndMs
		rows = append(rows, models.Chapter{
			TranscriptID: transcriptID, TenantID: tenantID, Title: title, Summary: ch.Summary,
			StartMs: ch.StartMs, EndMs: &end, Source: models.ChapterSourceDerived,
			Status: status, Confidence: ch.Confidence, ContextLabel: ch.ContextLabel,
			BoundaryReason: ch.BoundaryReason, StandaloneScore: ch.StandaloneScore,
			ContainsSponsorIntro: ch.ContainsSponsorIntro, NeedsReviewReason: needsReviewReason,
			DurationBucket: &bucket,
		})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].StartMs < rows[j].StartMs })
	return rows
}

type createAtomizedChildrenRequest struct {
	Chapters []atomizationChapterRequest `json:"chapters"`
}

type reportAtomizationRunRequest struct {
	RunID        *string `json:"run_id"`
	Status       string  `json:"status"`
	Phase        string  `json:"phase"`
	ChildCount   *int    `json:"child_count"`
	ReviewCount  *int    `json:"review_count"`
	ErrorMessage *string `json:"error_message"`
	Trigger      *string `json:"trigger"`
	RequestedBy  *string `json:"requested_by"`
}

func InternalCreateAtomizedChildren(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	parent, transcript, ok := loadAtomizationParent(c, db)
	if !ok {
		return
	}
	var req createAtomizedChildrenRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}
	policy := atomizationPolicyForItem(db, parent)
	children := []map[string]interface{}{}
	err := db.Transaction(func(tx *gorm.DB) error {
		if !policy.ParentFeedVisible {
			parent.IsFeedUnit = false
			parent.FeedVisibility = feedVisibilityHidden
		}
		status := "processing"
		parent.ChapteringStatus = &status
		if err := tx.Save(parent).Error; err != nil {
			return err
		}
		if err := tx.Model(&models.ContentItem{}).
			Where("tenant_id = ? AND parent_content_item_id = ?", parent.TenantID, parent.PublicID).
			Updates(map[string]interface{}{
				"status":            models.ContentStatusArchived,
				"feed_visibility":   feedVisibilityHidden,
				"is_feed_unit":      false,
				"chaptering_status": "archived",
			}).Error; err != nil {
			return err
		}
		for i, ch := range req.Chapters {
			child, err := upsertAtomizedChild(tx, parent, transcript, ch, i, policy)
			if err != nil {
				return err
			}
			children = append(children, map[string]interface{}{"id": child.PublicID.String(), "status": child.Status, "feed_visibility": child.FeedVisibility})
		}
		final := "completed"
		if hasReviewChapters(req.Chapters, policy) {
			final = "needs_review"
		}
		parent.ChapteringStatus = &final
		return tx.Save(parent).Error
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create atomized children: " + err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"children": children})
}

func InternalReportAtomizationRun(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	parentID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid parent id"})
		return
	}
	var parent models.ContentItem
	if err := db.Where("public_id = ?", parentID).First(&parent).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Parent content not found"})
		return
	}
	var req reportAtomizationRunRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}
	status := strings.TrimSpace(req.Status)
	phase := strings.TrimSpace(req.Phase)
	if status == "" || phase == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "status and phase are required"})
		return
	}
	now := time.Now().UTC()
	run := models.MediaAtomizationRun{}
	err = db.Transaction(func(tx *gorm.DB) error {
		if req.RunID != nil {
			if id, parseErr := uuid.Parse(*req.RunID); parseErr == nil {
				_ = tx.Where("public_id = ? AND parent_content_item_id = ?", id, parent.PublicID).First(&run).Error
			}
		}
		if run.ID == 0 {
			if err := supersedeActiveAtomizationRuns(tx, parent.TenantID, parent.PublicID, nil, now); err != nil {
				return err
			}
			run = models.MediaAtomizationRun{
				TenantID: parent.TenantID, ParentContentItemID: parent.PublicID,
				Status: status, Phase: phase, StartedAt: &now,
			}
		} else {
			run.Status = status
			run.Phase = phase
			if run.StartedAt == nil {
				run.StartedAt = &now
			}
		}
		if req.ChildCount != nil {
			run.ChildCount = *req.ChildCount
		}
		if req.ReviewCount != nil {
			run.ReviewCount = *req.ReviewCount
		}
		run.ErrorMessage = req.ErrorMessage
		if req.Trigger != nil {
			trigger := strings.TrimSpace(*req.Trigger)
			run.Trigger = &trigger
		}
		if req.RequestedBy != nil {
			if requestedBy, parseErr := uuid.Parse(*req.RequestedBy); parseErr == nil {
				run.RequestedBy = &requestedBy
			}
		}
		if status == "completed" || status == "needs_review" || status == "failed" {
			run.CompletedAt = &now
		}
		if err := tx.Save(&run).Error; err != nil {
			return err
		}
		if status == "completed" || status == "needs_review" || status == "failed" {
			if err := supersedeActiveAtomizationRuns(tx, parent.TenantID, parent.PublicID, &run.ID, now); err != nil {
				return err
			}
		}
		parentStatus := parentChapteringStatusFromRun(status, phase)
		parent.ChapteringStatus = &parentStatus
		return tx.Save(&parent).Error
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save atomization run"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"run_id": run.PublicID.String(), "status": run.Status, "phase": run.Phase})
}

func supersedeActiveAtomizationRuns(tx *gorm.DB, tenantID string, parentID uuid.UUID, exceptID *uint, completedAt time.Time) error {
	query := tx.Model(&models.MediaAtomizationRun{}).
		Where("tenant_id = ? AND parent_content_item_id = ? AND status IN ?", tenantID, parentID, []string{"queued", "running"})
	if exceptID != nil {
		query = query.Where("id <> ?", *exceptID)
	}
	return query.Updates(map[string]interface{}{
		"status":        "superseded",
		"completed_at":  completedAt,
		"error_message": nil,
	}).Error
}

func parentChapteringStatusFromRun(status, phase string) string {
	if status == "running" {
		return phase
	}
	return status
}

func hasReviewChapters(chapters []atomizationChapterRequest, policy atomizationPolicy) bool {
	for _, ch := range chapters {
		conf := 0.0
		if ch.Confidence != nil {
			conf = *ch.Confidence
		}
		if conf < policy.HighConfidenceThreshold ||
			ch.NeedsReviewReason != nil ||
			ch.EndMs-ch.StartMs < minFeedUnitMs(policy) ||
			ch.EndMs-ch.StartMs > policy.HardMaxChapterMinutes*60_000 {
			return true
		}
	}
	return false
}

func shouldPublishLinkedChapter(item models.ContentItem) bool {
	return item.ParentContentItemID != nil &&
		(item.FeedVisibility == feedVisibilityVisible ||
			(item.ChapteringStatus != nil && *item.ChapteringStatus == chapterStatusPublished))
}

func loadAtomizationParent(c *gin.Context, db *gorm.DB) (*models.ContentItem, *models.Transcript, bool) {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid id"})
		return nil, nil, false
	}
	var item models.ContentItem
	if err := db.Where("public_id = ?", id).First(&item).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Content not found"})
		return nil, nil, false
	}
	if item.TranscriptID == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Parent has no active transcript"})
		return nil, nil, false
	}
	var transcript models.Transcript
	if err := db.Where("public_id = ?", *item.TranscriptID).First(&transcript).Error; err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Parent transcript not found"})
		return nil, nil, false
	}
	return &item, &transcript, true
}

func upsertAtomizedChild(tx *gorm.DB, parent *models.ContentItem, parentTranscript *models.Transcript, ch atomizationChapterRequest, idx int, policy atomizationPolicy) (*models.ContentItem, error) {
	if strings.TrimSpace(ch.Title) == "" || ch.EndMs <= ch.StartMs {
		return nil, errors.New("invalid chapter")
	}
	conf := 0.0
	if ch.Confidence != nil {
		conf = *ch.Confidence
	}
	published := policy.AutoPublishHighConfidence &&
		conf >= policy.HighConfidenceThreshold &&
		ch.NeedsReviewReason == nil &&
		ch.EndMs-ch.StartMs >= minFeedUnitMs(policy) &&
		ch.EndMs-ch.StartMs <= policy.HardMaxChapterMinutes*60_000
	status := models.ContentStatusPending
	visibility := feedVisibilityReview
	chapteringStatus := chapterStatusReview
	if published {
		status = models.ContentStatusProcessing
		visibility = feedVisibilityEmbeddingPending
		chapteringStatus = feedVisibilityEmbeddingPending
	}
	durationSec := int(math.Round(float64(ch.EndMs-ch.StartMs) / 1000.0))
	bucket := durationBucketLabel(ch.EndMs - ch.StartMs)
	idempotency := normalizeIdempotencyKey("atomized:" + parent.PublicID.String() + ":" + strconv.Itoa(idx))
	renditionsJSON, _ := json.Marshal(ch.MediaRenditions)
	body := ch.TranscriptText
	if body == "" && len(ch.TranscriptSegments) > 0 {
		parts := make([]string, 0, len(ch.TranscriptSegments))
		for _, seg := range ch.TranscriptSegments {
			if t := strings.TrimSpace(seg.Text); t != "" {
				parts = append(parts, t)
			}
		}
		body = strings.Join(parts, " ")
	}
	title := strings.TrimSpace(ch.Title)
	item := models.ContentItem{}
	err := tx.Where("idempotency_key = ?", idempotency).First(&item).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}
	if errors.Is(err, gorm.ErrRecordNotFound) {
		item = models.ContentItem{
			TenantID: parent.TenantID, Type: parent.Type, Source: parent.Source, Status: status,
			IdempotencyKey: &idempotency, Title: &title, BodyText: &body, Excerpt: ch.Summary,
			Author: parent.Author, SourceName: parent.SourceName, SourceFeedURL: parent.SourceFeedURL,
			OriginalURL: parent.OriginalURL, MediaURL: ch.MediaURL, ThumbnailURL: ch.ThumbnailURL,
			DurationSec: &durationSec, TopicTags: parent.TopicTags, Metadata: parent.Metadata,
			PublishedAt: parent.PublishedAt, ParentContentItemID: &parent.PublicID,
			IsFeedUnit: true, FeedVisibility: visibility, ChapterIndex: &idx,
			ChapterStartMs: &ch.StartMs, ChapterEndMs: &ch.EndMs, ChapterConfidence: ch.Confidence,
			ChapteringStatus: &chapteringStatus,
			DurationBucket:   &bucket, PlaybackURL: ch.PlaybackURL, PlaybackType: ch.PlaybackType,
			FallbackPlaybackURL: ch.FallbackPlaybackURL, HasVideo: ch.HasVideo,
			MediaRenditions: datatypes.JSON(renditionsJSON),
		}
		if err := tx.Create(&item).Error; err != nil {
			return nil, err
		}
	} else {
		item.Status = status
		item.FeedVisibility = visibility
		item.IsFeedUnit = true
		item.Title = &title
		item.BodyText = &body
		item.Excerpt = ch.Summary
		item.MediaURL = ch.MediaURL
		item.ThumbnailURL = ch.ThumbnailURL
		item.DurationSec = &durationSec
		item.ChapterIndex = &idx
		item.ChapterStartMs = &ch.StartMs
		item.ChapterEndMs = &ch.EndMs
		item.ChapterConfidence = ch.Confidence
		item.ChapteringStatus = &chapteringStatus
		item.DurationBucket = &bucket
		item.PlaybackURL = ch.PlaybackURL
		item.PlaybackType = ch.PlaybackType
		item.FallbackPlaybackURL = ch.FallbackPlaybackURL
		item.HasVideo = ch.HasVideo
		item.MediaRenditions = datatypes.JSON(renditionsJSON)
		if err := tx.Save(&item).Error; err != nil {
			return nil, err
		}
	}
	if body != "" {
		segJSON, _ := json.Marshal(ch.TranscriptSegments)
		if item.TranscriptID != nil {
			var existingTranscript models.Transcript
			if err := tx.Where("public_id = ?", *item.TranscriptID).First(&existingTranscript).Error; err == nil {
				existingTranscript.FullText = body
				existingTranscript.Segments = datatypes.JSON(segJSON)
				_ = tx.Save(&existingTranscript).Error
			}
		} else {
			transcript := models.Transcript{
				ContentItemID: item.PublicID, FullText: body, Segments: datatypes.JSON(segJSON),
				Language: parentTranscript.Language, Source: parentTranscript.Source, Provider: parentTranscript.Provider,
			}
			if err := tx.Create(&transcript).Error; err == nil {
				item.TranscriptID = &transcript.PublicID
				_ = tx.Save(&item).Error
			}
		}
	}
	chapterStatus := chapterStatusReview
	if published {
		chapterStatus = feedVisibilityEmbeddingPending
	}
	tx.Model(&models.Chapter{}).
		Where("transcript_id = ? AND tenant_id = ? AND start_ms = ?", parentTranscript.PublicID, parent.TenantID, ch.StartMs).
		Updates(map[string]interface{}{"child_content_item_id": item.PublicID, "status": chapterStatus})
	return &item, nil
}

func AdminListAtomizationReview(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var items []models.ContentItem
	if err := db.Where("tenant_id = ? AND type IN ? AND chaptering_status = ?", principal.TenantID, []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}, "needs_review").
		Order("updated_at DESC").Limit(100).Find(&items).Error; err != nil {
		mediaAtomizationQueryError(c, err)
		return
	}
	c.JSON(http.StatusOK, utils.ResponseMessage{Code: http.StatusOK, Message: "Atomization review items fetched", Data: gin.H{"items": items}})
}

func AdminRepairMediaAtomizationLeaks(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	schema := getMediaAtomizationSchemaInfo(db)
	if !schema.Ready {
		c.JSON(http.StatusConflict, utils.HTTPError{
			Code:    http.StatusConflict,
			Message: schema.Message,
		})
		return
	}

	result, err := repairMediaAtomizationDurationLeaks(db, principal.TenantID)
	if err != nil {
		mediaAtomizationQueryError(c, err)
		return
	}
	c.JSON(http.StatusOK, utils.ResponseMessage{Code: http.StatusOK, Message: "Media atomization feed leaks repaired", Data: gin.H{
		"updated_count":                         result.UpdatedCount,
		"remaining_count":                       result.RemainingCount,
		"hidden_duration_violation_count":       result.HiddenDurationViolationCount,
		"archived_short_parent_child_count":     result.ArchivedShortParentChildCount,
		"restored_parent_count":                 result.RestoredParentCount,
		"restored_fuzzy_chapter_count":          result.RestoredFuzzyChapterCount,
		"remaining_visible_under_floor_count":   result.RemainingVisibleUnderFloorCount,
		"remaining_visible_over_hard_max_count": result.RemainingVisibleOverHardMaxCount,
		"schema_status":                         schema,
	}})
}

type atomizationPolicyPatchRequest struct {
	ChapteringEnabled           *bool    `json:"chaptering_enabled"`
	AutoPublishHighConfidence   *bool    `json:"auto_publish_high_confidence"`
	ParentFeedVisible           *bool    `json:"parent_feed_visible"`
	PreserveVideo               *bool    `json:"preserve_video"`
	RemoveSponsorSegments       *bool    `json:"remove_sponsor_segments"`
	MinChapterMinutes           *int     `json:"min_chapter_minutes"`
	MinFeedUnitSeconds          *int     `json:"min_feed_unit_seconds"`
	SoftMaxChapterMinutes       *int     `json:"soft_max_chapter_minutes"`
	HardMaxChapterMinutes       *int     `json:"hard_max_chapter_minutes"`
	AtomizationMinParentSeconds *int     `json:"atomization_min_parent_seconds"`
	MaxChaptersPerParent        *int     `json:"max_chapters_per_parent"`
	ChapteringMode              *string  `json:"chaptering_mode"`
	HighConfidenceThreshold     *float64 `json:"high_confidence_threshold"`
	PreferredPlaybackRendition  *string  `json:"preferred_playback_rendition"`
	FallbackPlaybackRendition   *string  `json:"fallback_playback_rendition"`
	AudioOnlyAllowed            *bool    `json:"audio_only_allowed"`
}

func applyAtomizationPolicyPatch(policy atomizationPolicy, req atomizationPolicyPatchRequest) atomizationPolicy {
	if req.ChapteringEnabled != nil {
		policy.ChapteringEnabled = *req.ChapteringEnabled
	}
	if req.AutoPublishHighConfidence != nil {
		policy.AutoPublishHighConfidence = *req.AutoPublishHighConfidence
	}
	if req.ParentFeedVisible != nil {
		policy.ParentFeedVisible = *req.ParentFeedVisible
	}
	if req.PreserveVideo != nil {
		policy.PreserveVideo = *req.PreserveVideo
	}
	if req.RemoveSponsorSegments != nil {
		policy.RemoveSponsorSegments = *req.RemoveSponsorSegments
	}
	if req.MinChapterMinutes != nil {
		policy.MinChapterMinutes = *req.MinChapterMinutes
	}
	if req.MinFeedUnitSeconds != nil {
		policy.MinFeedUnitSeconds = *req.MinFeedUnitSeconds
	}
	if req.SoftMaxChapterMinutes != nil {
		policy.SoftMaxChapterMinutes = *req.SoftMaxChapterMinutes
	}
	if req.HardMaxChapterMinutes != nil {
		policy.HardMaxChapterMinutes = *req.HardMaxChapterMinutes
	}
	if req.AtomizationMinParentSeconds != nil {
		policy.AtomizationMinParentSeconds = *req.AtomizationMinParentSeconds
	}
	if req.MaxChaptersPerParent != nil {
		policy.MaxChaptersPerParent = *req.MaxChaptersPerParent
	}
	if req.ChapteringMode != nil {
		policy.ChapteringMode = strings.TrimSpace(*req.ChapteringMode)
	}
	if req.HighConfidenceThreshold != nil {
		policy.HighConfidenceThreshold = *req.HighConfidenceThreshold
	}
	if req.PreferredPlaybackRendition != nil {
		policy.PreferredPlaybackRendition = strings.TrimSpace(*req.PreferredPlaybackRendition)
	}
	if req.FallbackPlaybackRendition != nil {
		policy.FallbackPlaybackRendition = strings.TrimSpace(*req.FallbackPlaybackRendition)
	}
	if req.AudioOnlyAllowed != nil {
		policy.AudioOnlyAllowed = *req.AudioOnlyAllowed
	}
	return validateAtomizationPolicy(policy)
}

func AdminGetMediaAtomizationPolicy(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	policy := validateAtomizationPolicy(policyFromModel(getOrCreateMediaAtomizationPolicy(db, principal.TenantID)))
	c.JSON(http.StatusOK, utils.ResponseMessage{Code: http.StatusOK, Message: "Media atomization policy fetched", Data: gin.H{"policy": policy}})
}

func AdminUpdateMediaAtomizationPolicy(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	var req atomizationPolicyPatchRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{Code: http.StatusBadRequest, Message: "Invalid request"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	model := getOrCreateMediaAtomizationPolicy(db, principal.TenantID)
	updated := applyAtomizationPolicyPatch(policyFromModel(model), req)
	model.ChapteringEnabled = updated.ChapteringEnabled
	model.AutoPublishHighConfidence = updated.AutoPublishHighConfidence
	model.ParentFeedVisible = updated.ParentFeedVisible
	model.PreserveVideo = updated.PreserveVideo
	model.RemoveSponsorSegments = updated.RemoveSponsorSegments
	model.MinChapterMinutes = updated.MinChapterMinutes
	model.MinFeedUnitSeconds = updated.MinFeedUnitSeconds
	model.SoftMaxChapterMinutes = updated.SoftMaxChapterMinutes
	model.HardMaxChapterMinutes = updated.HardMaxChapterMinutes
	model.AtomizationMinParentSeconds = updated.AtomizationMinParentSeconds
	model.MaxChaptersPerParent = updated.MaxChaptersPerParent
	model.ChapteringMode = updated.ChapteringMode
	model.HighConfidenceThreshold = updated.HighConfidenceThreshold
	model.PreferredPlaybackRendition = updated.PreferredPlaybackRendition
	model.FallbackPlaybackRendition = updated.FallbackPlaybackRendition
	model.AudioOnlyAllowed = updated.AudioOnlyAllowed
	if err := db.Save(&model).Error; err != nil {
		mediaAtomizationQueryError(c, err)
		return
	}
	c.JSON(http.StatusOK, utils.ResponseMessage{Code: http.StatusOK, Message: "Media atomization policy updated", Data: gin.H{"policy": updated}})
}

func AdminRunAtomizationSweepNow(c *gin.Context) {
	proxyAggregationSimple(c, "/admin/atomization/sweep-now")
}

func AdminListMediaAtomizationSources(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	base := validateAtomizationPolicy(policyFromModel(getOrCreateMediaAtomizationPolicy(db, principal.TenantID)))
	var sources []models.ContentSource
	q := db.Where("tenant_id = ? AND category = ?", principal.TenantID, models.SourceCategoryMedia)
	if search := strings.TrimSpace(c.Query("q")); search != "" {
		q = q.Where("(name ILIKE ? OR feed_url ILIKE ?)", "%"+search+"%", "%"+search+"%")
	}
	if err := q.Order("updated_at DESC").Limit(boundedLimit(c.Query("limit"), 80, 200)).Find(&sources).Error; err != nil {
		mediaAtomizationQueryError(c, err)
		return
	}
	rows := make([]gin.H, 0, len(sources))
	for _, source := range sources {
		cfg, _ := parseSourceAPIConfig(source.APIConfig)
		effective := validateAtomizationPolicy(applyAtomizationPolicyConfig(base, cfg))
		rows = append(rows, gin.H{
			"id": source.PublicID.String(), "name": source.Name, "type": source.Type,
			"feed_url": source.FeedURL, "is_active": source.IsActive,
			"policy": effective, "overrides": cfg, "chaptering_enabled": effective.ChapteringEnabled,
			"updated_at": source.UpdatedAt,
		})
	}
	c.JSON(http.StatusOK, utils.ResponseMessage{Code: http.StatusOK, Message: "Media atomization sources fetched", Data: gin.H{"items": rows}})
}

func AdminUpdateMediaAtomizationSourcePolicy(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{Code: http.StatusBadRequest, Message: "Invalid source id"})
		return
	}
	var req atomizationPolicyPatchRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{Code: http.StatusBadRequest, Message: "Invalid request"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var source models.ContentSource
	if err := db.Where("tenant_id = ? AND public_id = ?", principal.TenantID, id).First(&source).Error; err != nil {
		c.JSON(http.StatusNotFound, utils.HTTPError{Code: http.StatusNotFound, Message: "Source not found"})
		return
	}
	cfg, _ := parseSourceAPIConfig(source.APIConfig)
	applyAtomizationPatchToConfig(cfg, req)
	raw, _ := json.Marshal(cfg)
	source.APIConfig = datatypes.JSON(raw)
	if err := db.Save(&source).Error; err != nil {
		mediaAtomizationQueryError(c, err)
		return
	}
	base := policyFromModel(getOrCreateMediaAtomizationPolicy(db, principal.TenantID))
	effective := validateAtomizationPolicy(applyAtomizationPolicyConfig(base, cfg))
	c.JSON(http.StatusOK, utils.ResponseMessage{Code: http.StatusOK, Message: "Source atomization policy updated", Data: gin.H{"source_id": source.PublicID.String(), "policy": effective}})
}

func applyAtomizationPatchToConfig(cfg map[string]interface{}, req atomizationPolicyPatchRequest) {
	if req.ChapteringEnabled != nil {
		cfg["chaptering_enabled"] = *req.ChapteringEnabled
	}
	if req.AutoPublishHighConfidence != nil {
		cfg["auto_publish_high_confidence"] = *req.AutoPublishHighConfidence
	}
	if req.ParentFeedVisible != nil {
		cfg["parent_feed_visible"] = *req.ParentFeedVisible
	}
	if req.PreserveVideo != nil {
		cfg["preserve_video"] = *req.PreserveVideo
	}
	if req.RemoveSponsorSegments != nil {
		cfg["remove_sponsor_segments"] = *req.RemoveSponsorSegments
	}
	if req.MinChapterMinutes != nil {
		cfg["min_chapter_minutes"] = *req.MinChapterMinutes
	}
	if req.MinFeedUnitSeconds != nil {
		cfg["min_feed_unit_seconds"] = *req.MinFeedUnitSeconds
	}
	if req.SoftMaxChapterMinutes != nil {
		cfg["soft_max_chapter_minutes"] = *req.SoftMaxChapterMinutes
	}
	if req.HardMaxChapterMinutes != nil {
		cfg["hard_max_chapter_minutes"] = *req.HardMaxChapterMinutes
	}
	if req.AtomizationMinParentSeconds != nil {
		cfg["atomization_min_parent_seconds"] = *req.AtomizationMinParentSeconds
	}
	if req.MaxChaptersPerParent != nil {
		cfg["max_chapters_per_parent"] = *req.MaxChaptersPerParent
	}
	if req.ChapteringMode != nil {
		cfg["chaptering_mode"] = strings.TrimSpace(*req.ChapteringMode)
	}
	if req.HighConfidenceThreshold != nil {
		cfg["high_confidence_threshold"] = *req.HighConfidenceThreshold
	}
	if req.PreferredPlaybackRendition != nil {
		cfg["preferred_playback_rendition"] = strings.TrimSpace(*req.PreferredPlaybackRendition)
	}
	if req.FallbackPlaybackRendition != nil {
		cfg["fallback_playback_rendition"] = strings.TrimSpace(*req.FallbackPlaybackRendition)
	}
	if req.AudioOnlyAllowed != nil {
		cfg["audio_only_allowed"] = *req.AudioOnlyAllowed
	}
}

type atomizationOverrideRequest struct {
	Override string  `json:"override"`
	Reason   *string `json:"reason"`
}

func AdminUpdateMediaAtomizationParentOverride(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	parent, ok := loadAdminAtomizationParent(c, principal.TenantID)
	if !ok {
		return
	}
	var req atomizationOverrideRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{Code: http.StatusBadRequest, Message: "Invalid request"})
		return
	}
	override := strings.TrimSpace(req.Override)
	if override == "" {
		override = atomizationOverrideInherit
	}
	if override != atomizationOverrideInherit && override != atomizationOverrideDisabled && override != atomizationOverrideEnabled {
		c.JSON(http.StatusBadRequest, utils.HTTPError{Code: http.StatusBadRequest, Message: "override must be inherit, disabled, or enabled"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	now := time.Now().UTC()
	parent.AtomizationOverride = &override
	parent.AtomizationOverrideReason = req.Reason
	if principal.UserID != "" {
		if id, err := uuid.Parse(principal.UserID); err == nil {
			parent.AtomizationOverrideBy = &id
		}
	}
	parent.AtomizationOverrideAt = &now
	if override == atomizationOverrideInherit {
		parent.AtomizationOverrideReason = nil
		parent.AtomizationOverrideBy = nil
		parent.AtomizationOverrideAt = nil
	}
	if err := db.Save(parent).Error; err != nil {
		mediaAtomizationQueryError(c, err)
		return
	}
	c.JSON(http.StatusOK, utils.ResponseMessage{Code: http.StatusOK, Message: "Parent atomization override updated", Data: gin.H{"item": parent}})
}

func AdminAtomizeMediaParent(c *gin.Context) { adminQueueMediaParentAtomization(c, false) }

func AdminReatomizeMediaParent(c *gin.Context) { adminQueueMediaParentAtomization(c, true) }

func loadAdminAtomizationParent(c *gin.Context, tenantID string) (*models.ContentItem, bool) {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{Code: http.StatusBadRequest, Message: "Invalid parent id"})
		return nil, false
	}
	db := c.MustGet("db").(*gorm.DB)
	var parent models.ContentItem
	if err := db.Where("tenant_id = ? AND public_id = ? AND type IN ?", tenantID, id, []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}).First(&parent).Error; err != nil {
		c.JSON(http.StatusNotFound, utils.HTTPError{Code: http.StatusNotFound, Message: "Parent media not found"})
		return nil, false
	}
	return &parent, true
}

func adminQueueMediaParentAtomization(c *gin.Context, reatomize bool) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	parent, ok := loadAdminAtomizationParent(c, principal.TenantID)
	if !ok {
		return
	}
	if parent.DurationSec == nil || *parent.DurationSec <= atomizationMinParentDurationSec {
		c.JSON(http.StatusConflict, utils.HTTPError{Code: http.StatusConflict, Message: "Manual atomization only applies to media longer than 40 minutes"})
		return
	}
	if parent.MediaURL == nil || strings.TrimSpace(*parent.MediaURL) == "" {
		c.JSON(http.StatusConflict, utils.HTTPError{Code: http.StatusConflict, Message: "Parent has no media artifact to atomize"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	effective := effectiveAtomizationPolicyForItem(db, parent)
	if !effective.Policy.ChapteringEnabled {
		msg := "Atomization is disabled for this parent"
		if effective.DisabledReason != nil {
			msg = *effective.DisabledReason
		}
		c.JSON(http.StatusConflict, utils.HTTPError{Code: http.StatusConflict, Message: msg})
		return
	}
	now := time.Now().UTC()
	trigger := "manual"
	if reatomize {
		trigger = "reatomize"
	}
	parent.ManualAtomizationRequestedAt = &now
	status := "queued"
	parent.ChapteringStatus = &status
	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Save(parent).Error; err != nil {
			return err
		}
		run := models.MediaAtomizationRun{TenantID: parent.TenantID, ParentContentItemID: parent.PublicID, Status: "queued", Phase: "planning", StartedAt: &now, Trigger: &trigger}
		if principal.UserID != "" {
			if id, err := uuid.Parse(principal.UserID); err == nil {
				run.RequestedBy = &id
			}
		}
		return tx.Create(&run).Error
	}); err != nil {
		mediaAtomizationQueryError(c, err)
		return
	}
	aggregationBaseURL := strings.TrimRight(strings.TrimSpace(os.Getenv("AGGREGATION_BASE_URL")), "/")
	if aggregationBaseURL == "" {
		c.JSON(http.StatusServiceUnavailable, utils.HTTPError{Code: http.StatusServiceUnavailable, Message: "Aggregation service URL is not configured"})
		return
	}
	payload := gin.H{
		"contentItemId": parent.PublicID.String(), "reason": trigger, "hasTranscript": parent.TranscriptID != nil,
		"contentType": parent.Type, "mediaUrl": parent.MediaURL, "thumbnailUrl": parent.ThumbnailURL,
		"title": parent.Title, "excerpt": parent.Excerpt, "bodyText": parent.BodyText,
	}
	body, statusCode, err := proxyAggregationRequest(aggregationBaseURL, "/admin/atomization/parents/"+parent.PublicID.String()+"/atomize", c.GetHeader("Authorization"), payload)
	if err != nil {
		c.JSON(http.StatusBadGateway, utils.HTTPError{Code: http.StatusBadGateway, Message: "Aggregation request failed: " + err.Error()})
		return
	}
	c.Data(statusCode, "application/json", body)
}

func AdminGetMediaAtomizationOverview(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	schema := getMediaAtomizationSchemaInfo(db)
	if !schema.Ready {
		adminGetMediaAtomizationOverviewCompat(c, db, principal.TenantID, schema)
		return
	}
	type nameCount struct {
		Name  string `json:"name"`
		Count int64  `json:"count"`
	}
	type childStateCount struct {
		FeedVisibility string `json:"feed_visibility"`
		Status         string `json:"status"`
		Count          int64  `json:"count"`
	}
	type durationBucketCount struct {
		Bucket           string `json:"bucket"`
		Published        int64  `json:"published"`
		NeedsReview      int64  `json:"needs_review"`
		EmbeddingPending int64  `json:"embedding_pending"`
	}
	type sourcePerformance struct {
		SourceName       string  `json:"source_name"`
		SourceFeedURL    *string `json:"source_feed_url,omitempty"`
		ParentsProcessed int64   `json:"parents_processed"`
		ChildrenProduced int64   `json:"children_produced"`
		PublishedCount   int64   `json:"published_count"`
		ReviewCount      int64   `json:"review_count"`
		FailedCount      int64   `json:"failed_count"`
	}
	parentStatus := []nameCount{}
	childState := []childStateCount{}
	durationBuckets := []durationBucketCount{}
	sourcePerformanceRows := []sourcePerformance{}

	if err := db.Raw(`
		SELECT COALESCE(chaptering_status, 'unstarted') AS name, COUNT(*) AS count
		FROM content_items
		WHERE tenant_id = ? AND type IN ('VIDEO','PODCAST') AND parent_content_item_id IS NULL
		GROUP BY COALESCE(chaptering_status, 'unstarted')
		ORDER BY name`, principal.TenantID).Scan(&parentStatus).Error; err != nil {
		mediaAtomizationQueryError(c, err)
		return
	}

	if err := db.Raw(`
		SELECT feed_visibility, status, COUNT(*) AS count
		FROM content_items
		WHERE tenant_id = ? AND parent_content_item_id IS NOT NULL
		GROUP BY feed_visibility, status
		ORDER BY feed_visibility, status`, principal.TenantID).Scan(&childState).Error; err != nil {
		mediaAtomizationQueryError(c, err)
		return
	}

	var autoPublished, reviewNeeded, failedOrStuck, parentCount, childCount, durationViolationCount int64
	var disabledEpisodeCount, disabledSourceCount, manualRequestedCount int64
	var visibleUnderFloorCount, visibleOverHardMaxCount, shortParentActiveChildCount, shortChapterReviewCount int64
	if err := db.Model(&models.ContentItem{}).
		Where("tenant_id = ? AND parent_content_item_id IS NOT NULL AND feed_visibility = ? AND status = ?", principal.TenantID, feedVisibilityVisible, models.ContentStatusReady).
		Count(&autoPublished).Error; err != nil {
		mediaAtomizationQueryError(c, err)
		return
	}
	if err := db.Model(&models.ContentItem{}).
		Where("tenant_id = ? AND parent_content_item_id IS NOT NULL AND feed_visibility = ?", principal.TenantID, feedVisibilityReview).
		Count(&reviewNeeded).Error; err != nil {
		mediaAtomizationQueryError(c, err)
		return
	}
	if err := db.Model(&models.ContentItem{}).
		Where("tenant_id = ? AND type IN ? AND parent_content_item_id IS NULL AND (chaptering_status = ? OR status = ? OR (chaptering_status IN ? AND updated_at < ?))",
			principal.TenantID,
			[]models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast},
			"failed",
			models.ContentStatusFailed,
			[]string{"planning", "cutting", "renditions", "children", "embedding", "embedding_pending", "waiting_transcript"},
			time.Now().Add(-2*time.Hour),
		).Count(&failedOrStuck).Error; err != nil {
		mediaAtomizationQueryError(c, err)
		return
	}
	if err := db.Model(&models.ContentItem{}).
		Where("tenant_id = ? AND type IN ? AND parent_content_item_id IS NULL", principal.TenantID, []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}).
		Count(&parentCount).Error; err != nil {
		mediaAtomizationQueryError(c, err)
		return
	}
	if err := db.Model(&models.ContentItem{}).
		Where("tenant_id = ? AND parent_content_item_id IS NOT NULL", principal.TenantID).
		Count(&childCount).Error; err != nil {
		mediaAtomizationQueryError(c, err)
		return
	}
	if err := db.Model(&models.ContentItem{}).
		Where("tenant_id = ? AND type IN ? AND parent_content_item_id IS NULL AND atomization_override = ?", principal.TenantID, []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}, atomizationOverrideDisabled).
		Count(&disabledEpisodeCount).Error; err != nil {
		mediaAtomizationQueryError(c, err)
		return
	}
	if err := db.Model(&models.ContentSource{}).
		Where("tenant_id = ? AND category = ? AND api_config->>'chaptering_enabled' = 'false'", principal.TenantID, models.SourceCategoryMedia).
		Count(&disabledSourceCount).Error; err != nil {
		mediaAtomizationQueryError(c, err)
		return
	}
	if err := db.Model(&models.ContentItem{}).
		Where("tenant_id = ? AND type IN ? AND parent_content_item_id IS NULL AND manual_atomization_requested_at IS NOT NULL", principal.TenantID, []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}).
		Count(&manualRequestedCount).Error; err != nil {
		mediaAtomizationQueryError(c, err)
		return
	}
	if err := visibleFeedDurationViolationQuery(db, principal.TenantID).Count(&durationViolationCount).Error; err != nil {
		mediaAtomizationQueryError(c, err)
		return
	}
	if err := db.Model(&models.ContentItem{}).
		Where("tenant_id = ?", principal.TenantID).
		Where("type IN ?", []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}).
		Where("is_feed_unit = TRUE AND feed_visibility = ? AND status IN ?", feedVisibilityVisible, []models.ContentStatus{models.ContentStatusReady, models.ContentStatusArchived}).
		Where("(duration_sec IS NULL OR duration_sec < ?)", forYouMinDurationSec).
		Count(&visibleUnderFloorCount).Error; err != nil {
		mediaAtomizationQueryError(c, err)
		return
	}
	if err := db.Model(&models.ContentItem{}).
		Where("tenant_id = ?", principal.TenantID).
		Where("type IN ?", []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}).
		Where("is_feed_unit = TRUE AND feed_visibility = ? AND status IN ?", feedVisibilityVisible, []models.ContentStatus{models.ContentStatusReady, models.ContentStatusArchived}).
		Where("duration_sec > ?", forYouHardMaxDurationSec).
		Count(&visibleOverHardMaxCount).Error; err != nil {
		mediaAtomizationQueryError(c, err)
		return
	}
	if err := db.Raw(`
			SELECT COUNT(c.id)
			FROM content_items c
			JOIN content_items p ON p.public_id = c.parent_content_item_id AND p.tenant_id = c.tenant_id
			WHERE c.tenant_id = ?
				AND c.parent_content_item_id IS NOT NULL
				AND c.status <> 'ARCHIVED'
				AND c.feed_visibility <> 'hidden'
				AND COALESCE(p.duration_sec, 0) <= ?`, principal.TenantID, atomizationMinParentDurationSec).Scan(&shortParentActiveChildCount).Error; err != nil {
		mediaAtomizationQueryError(c, err)
		return
	}
	if err := db.Raw(`
			SELECT COUNT(ch.id)
			FROM chapters ch
			LEFT JOIN content_items c ON c.public_id = ch.child_content_item_id AND c.tenant_id = ch.tenant_id
			WHERE ch.tenant_id = ?
				AND COALESCE(ch.end_ms - ch.start_ms, COALESCE(c.duration_sec, 0) * 1000, 0) < ?
				AND (ch.status = 'needs_review' OR c.feed_visibility = 'review')`, principal.TenantID, forYouMinDurationSec*1000).Scan(&shortChapterReviewCount).Error; err != nil {
		mediaAtomizationQueryError(c, err)
		return
	}

	avgChaptersPerParent := 0.0
	if parentCount > 0 {
		avgChaptersPerParent = float64(childCount) / float64(parentCount)
	}
	var avgRow struct {
		AvgProcessingSeconds *float64 `gorm:"column:avg_processing_seconds"`
	}
	if err := db.Raw(`
		SELECT AVG(EXTRACT(EPOCH FROM (completed_at - started_at))) AS avg_processing_seconds
		FROM media_atomization_runs
		WHERE tenant_id = ? AND started_at IS NOT NULL AND completed_at IS NOT NULL`, principal.TenantID).Scan(&avgRow).Error; err != nil {
		mediaAtomizationQueryError(c, err)
		return
	}

	if err := db.Raw(`
		SELECT COALESCE(duration_bucket, 'unknown') AS bucket,
			SUM(CASE WHEN feed_visibility = 'visible' THEN 1 ELSE 0 END) AS published,
			SUM(CASE WHEN feed_visibility = 'review' THEN 1 ELSE 0 END) AS needs_review,
			SUM(CASE WHEN feed_visibility = 'embedding_pending' THEN 1 ELSE 0 END) AS embedding_pending
		FROM content_items
		WHERE tenant_id = ? AND parent_content_item_id IS NOT NULL
		GROUP BY COALESCE(duration_bucket, 'unknown')
		ORDER BY bucket`, principal.TenantID).Scan(&durationBuckets).Error; err != nil {
		mediaAtomizationQueryError(c, err)
		return
	}

	if err := db.Raw(`
		SELECT COALESCE(p.source_name, 'Unknown source') AS source_name,
			p.source_feed_url AS source_feed_url,
			COUNT(DISTINCT p.public_id) AS parents_processed,
			COUNT(c.id) AS children_produced,
			SUM(CASE WHEN c.feed_visibility = 'visible' THEN 1 ELSE 0 END) AS published_count,
			SUM(CASE WHEN c.feed_visibility = 'review' THEN 1 ELSE 0 END) AS review_count,
			SUM(CASE WHEN p.status = 'FAILED' OR p.chaptering_status = 'failed' THEN 1 ELSE 0 END) AS failed_count
		FROM content_items p
		LEFT JOIN content_items c ON c.parent_content_item_id = p.public_id AND c.tenant_id = p.tenant_id
		WHERE p.tenant_id = ? AND p.type IN ('VIDEO','PODCAST') AND p.parent_content_item_id IS NULL
		GROUP BY COALESCE(p.source_name, 'Unknown source'), p.source_feed_url
		ORDER BY children_produced DESC, parents_processed DESC
		LIMIT 12`, principal.TenantID).Scan(&sourcePerformanceRows).Error; err != nil {
		mediaAtomizationQueryError(c, err)
		return
	}

	c.JSON(http.StatusOK, utils.ResponseMessage{Code: http.StatusOK, Message: "Media atomization overview fetched", Data: gin.H{
		"parent_status_counts":            parentStatus,
		"child_state_counts":              childState,
		"auto_published_count":            autoPublished,
		"review_needed_count":             reviewNeeded,
		"failed_stuck_count":              failedOrStuck,
		"duration_violation_count":        durationViolationCount,
		"disabled_episode_count":          disabledEpisodeCount,
		"disabled_source_count":           disabledSourceCount,
		"manual_requested_count":          manualRequestedCount,
		"visible_under_floor_count":       visibleUnderFloorCount,
		"visible_over_hard_max_count":     visibleOverHardMaxCount,
		"short_parent_active_child_count": shortParentActiveChildCount,
		"short_chapter_review_count":      shortChapterReviewCount,
		"invariants": gin.H{
			"visible_under_floor_feed_units":   visibleUnderFloorCount,
			"visible_over_hard_max_feed_units": visibleOverHardMaxCount,
			"parents_under_40m_with_children":  shortParentActiveChildCount,
			"short_chapters_awaiting_review":   shortChapterReviewCount,
		},
		"policy": gin.H{
			"min_feed_unit_seconds":          forYouMinDurationSec,
			"atomization_min_parent_seconds": atomizationMinParentDurationSec,
			"hard_max_feed_unit_seconds":     forYouHardMaxDurationSec,
		},
		"average_chapters_per_parent": avgChaptersPerParent,
		"average_processing_seconds":  avgRow.AvgProcessingSeconds,
		"duration_distribution":       durationBuckets,
		"source_performance":          sourcePerformanceRows,
		"schema_status":               schema,
		"updated_at":                  time.Now().UTC().Format(time.RFC3339),
	}})
}

func AdminListMediaAtomizationParents(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	schema := getMediaAtomizationSchemaInfo(db)
	if !schema.Ready {
		adminListMediaAtomizationParentsCompat(c, db, principal.TenantID, schema)
		return
	}
	type parentRow struct {
		ID                           string     `json:"id"`
		Title                        *string    `json:"title"`
		Status                       string     `json:"status"`
		ChapteringStatus             *string    `json:"chaptering_status"`
		SourceName                   *string    `json:"source_name"`
		SourceFeedURL                *string    `json:"source_feed_url"`
		DurationSec                  *int       `json:"duration_sec"`
		TranscriptID                 *string    `json:"transcript_id"`
		ChildCount                   int64      `json:"child_count"`
		PublishedCount               int64      `json:"published_count"`
		ReviewCount                  int64      `json:"review_count"`
		EmbeddingPendingCount        int64      `json:"embedding_pending_count"`
		LatestError                  *string    `json:"latest_error"`
		AtomizationOverride          *string    `json:"atomization_override"`
		AtomizationOverrideReason    *string    `json:"atomization_override_reason"`
		ManualAtomizationRequestedAt *time.Time `json:"manual_atomization_requested_at"`
		UpdatedAt                    time.Time  `json:"updated_at"`
	}
	where := []string{"p.tenant_id = ?", "p.type IN ('VIDEO','PODCAST')", "p.parent_content_item_id IS NULL"}
	args := []interface{}{principal.TenantID}
	if status := strings.TrimSpace(c.Query("status")); status != "" {
		where = append(where, "COALESCE(p.chaptering_status, 'unstarted') = ?")
		args = append(args, status)
	}
	if source := strings.TrimSpace(c.Query("source")); source != "" {
		where = append(where, "(p.source_name ILIKE ? OR p.source_feed_url ILIKE ?)")
		args = append(args, "%"+source+"%", "%"+source+"%")
	}
	if q := strings.TrimSpace(c.Query("q")); q != "" {
		where = append(where, "(p.title ILIKE ? OR p.source_name ILIKE ?)")
		args = append(args, "%"+q+"%", "%"+q+"%")
	}
	if bucket := strings.TrimSpace(c.Query("bucket")); bucket != "" {
		where = append(where, "EXISTS (SELECT 1 FROM content_items bc WHERE bc.parent_content_item_id = p.public_id AND bc.duration_bucket = ?)")
		args = append(args, bucket)
	}
	if review := strings.TrimSpace(c.Query("review")); review == "true" || review == "needed" {
		where = append(where, "EXISTS (SELECT 1 FROM content_items rc WHERE rc.parent_content_item_id = p.public_id AND rc.feed_visibility = 'review')")
	}
	limit := boundedLimit(c.Query("limit"), 50, 200)
	args = append(args, limit)
	rows := []parentRow{}
	if err := db.Raw(`
		SELECT p.public_id::text AS id, p.title, p.status, p.chaptering_status, p.source_name, p.source_feed_url,
			p.duration_sec, p.transcript_id::text AS transcript_id,
			COUNT(c.id) AS child_count,
			SUM(CASE WHEN c.feed_visibility = 'visible' THEN 1 ELSE 0 END) AS published_count,
			SUM(CASE WHEN c.feed_visibility = 'review' THEN 1 ELSE 0 END) AS review_count,
			SUM(CASE WHEN c.feed_visibility = 'embedding_pending' THEN 1 ELSE 0 END) AS embedding_pending_count,
			CASE
				WHEN (SELECT r.status FROM media_atomization_runs r WHERE r.parent_content_item_id = p.public_id ORDER BY r.updated_at DESC LIMIT 1) = 'failed'
				THEN (SELECT r.error_message FROM media_atomization_runs r WHERE r.parent_content_item_id = p.public_id ORDER BY r.updated_at DESC LIMIT 1)
				ELSE NULL
			END AS latest_error,
			p.atomization_override,
			p.atomization_override_reason,
			p.manual_atomization_requested_at,
			p.updated_at
		FROM content_items p
		LEFT JOIN content_items c ON c.parent_content_item_id = p.public_id AND c.tenant_id = p.tenant_id AND c.status <> 'ARCHIVED' AND c.feed_visibility <> 'hidden'
		WHERE `+strings.Join(where, " AND ")+`
		GROUP BY p.public_id, p.title, p.status, p.chaptering_status, p.source_name, p.source_feed_url, p.duration_sec, p.transcript_id, p.atomization_override, p.atomization_override_reason, p.manual_atomization_requested_at, p.updated_at
		ORDER BY p.updated_at DESC
		LIMIT ?`, args...).Scan(&rows).Error; err != nil {
		mediaAtomizationQueryError(c, err)
		return
	}
	c.JSON(http.StatusOK, utils.ResponseMessage{Code: http.StatusOK, Message: "Media atomization parents fetched", Data: gin.H{"items": rows}})
}

func AdminListMediaAtomizationChapters(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	schema := getMediaAtomizationSchemaInfo(db)
	if !schema.Ready {
		c.JSON(http.StatusOK, utils.ResponseMessage{Code: http.StatusOK, Message: "Media atomization chapters unavailable until schema migration is applied", Data: gin.H{"items": []interface{}{}, "schema_status": schema}})
		return
	}
	type chapterRow struct {
		ID                  string    `json:"id"`
		Title               string    `json:"title"`
		Summary             *string   `json:"summary"`
		ParentID            string    `json:"parent_id"`
		ParentTitle         *string   `json:"parent_title"`
		ChildID             *string   `json:"child_id"`
		SourceName          *string   `json:"source_name"`
		Status              string    `json:"status"`
		FeedVisibility      *string   `json:"feed_visibility"`
		Confidence          *float64  `json:"confidence"`
		StartMs             int       `json:"start_ms"`
		EndMs               *int      `json:"end_ms"`
		DurationMs          int       `json:"duration_ms"`
		DurationBucket      *string   `json:"duration_bucket"`
		NeedsReviewReason   *string   `json:"needs_review_reason"`
		PlaybackURL         *string   `json:"playback_url"`
		PlaybackType        *string   `json:"playback_type"`
		FallbackPlaybackURL *string   `json:"fallback_playback_url"`
		HasVideo            *bool     `json:"has_video"`
		UpdatedAt           time.Time `json:"updated_at"`
	}
	where := []string{"ch.tenant_id = ?", "p.type IN ('VIDEO','PODCAST')"}
	args := []interface{}{principal.TenantID}
	switch strings.TrimSpace(c.Query("review")) {
	case "true", "needed":
		where = append(where, "(ch.status = 'needs_review' OR c.feed_visibility = 'review')")
	case "published":
		where = append(where, "(ch.status = 'published' OR c.feed_visibility = 'visible')")
	case "rejected":
		where = append(where, "(ch.status = 'rejected' OR c.status = 'ARCHIVED')")
	case "embedding_pending":
		where = append(where, "(ch.status = 'embedding_pending' OR c.feed_visibility = 'embedding_pending')")
	}
	if bucket := strings.TrimSpace(c.Query("bucket")); bucket != "" {
		where = append(where, "COALESCE(ch.duration_bucket, c.duration_bucket) = ?")
		args = append(args, bucket)
	}
	if q := strings.TrimSpace(c.Query("q")); q != "" {
		where = append(where, "(ch.title ILIKE ? OR p.title ILIKE ? OR p.source_name ILIKE ?)")
		args = append(args, "%"+q+"%", "%"+q+"%", "%"+q+"%")
	}
	limit := boundedLimit(c.Query("limit"), 50, 200)
	args = append(args, limit)
	rows := []chapterRow{}
	if err := db.Raw(`
		SELECT ch.public_id::text AS id, ch.title, ch.summary,
			p.public_id::text AS parent_id, p.title AS parent_title,
			ch.child_content_item_id::text AS child_id, p.source_name,
			ch.status, c.feed_visibility, ch.confidence,
			ch.start_ms, ch.end_ms,
			COALESCE(ch.end_ms - ch.start_ms, COALESCE(c.duration_sec, 0) * 1000, 0) AS duration_ms,
			COALESCE(ch.duration_bucket, c.duration_bucket) AS duration_bucket,
			ch.needs_review_reason,
			c.playback_url, c.playback_type, c.fallback_playback_url, c.has_video,
			ch.updated_at
		FROM chapters ch
		JOIN transcripts t ON t.public_id = ch.transcript_id
		JOIN content_items p ON p.transcript_id = t.public_id AND p.tenant_id = ch.tenant_id
		LEFT JOIN content_items c ON c.public_id = ch.child_content_item_id AND c.tenant_id = ch.tenant_id
		WHERE `+strings.Join(where, " AND ")+`
		ORDER BY ch.updated_at DESC
		LIMIT ?`, args...).Scan(&rows).Error; err != nil {
		mediaAtomizationQueryError(c, err)
		return
	}
	c.JSON(http.StatusOK, utils.ResponseMessage{Code: http.StatusOK, Message: "Media atomization chapters fetched", Data: gin.H{"items": rows}})
}

func AdminListMediaAtomizationRuns(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	schema := getMediaAtomizationSchemaInfo(db)
	if !schema.Ready {
		c.JSON(http.StatusOK, utils.ResponseMessage{Code: http.StatusOK, Message: "Media atomization runs unavailable until schema migration is applied", Data: gin.H{"items": []interface{}{}, "schema_status": schema}})
		return
	}
	query := db.Model(&models.MediaAtomizationRun{}).Where("tenant_id = ?", principal.TenantID)
	if status := strings.TrimSpace(c.Query("status")); status != "" {
		query = query.Where("status = ?", status)
	}
	if phase := strings.TrimSpace(c.Query("phase")); phase != "" {
		query = query.Where("phase = ?", phase)
	}
	if parent := strings.TrimSpace(c.Query("parent_id")); parent != "" {
		if id, err := uuid.Parse(parent); err == nil {
			query = query.Where("parent_content_item_id = ?", id)
		}
	}
	limit := boundedLimit(c.Query("limit"), 30, 100)
	var runs []models.MediaAtomizationRun
	if err := query.Order("updated_at DESC").Limit(limit).Find(&runs).Error; err != nil {
		mediaAtomizationQueryError(c, err)
		return
	}
	c.JSON(http.StatusOK, utils.ResponseMessage{Code: http.StatusOK, Message: "Media atomization runs fetched", Data: gin.H{"items": runs}})
}

type mediaAtomizationPipelineItem struct {
	ID                           string     `json:"id"`
	Title                        *string    `json:"title"`
	Status                       string     `json:"status"`
	ChapteringStatus             *string    `json:"chaptering_status"`
	SourceName                   *string    `json:"source_name"`
	DurationSec                  *int       `json:"duration_sec"`
	TranscriptID                 *string    `json:"transcript_id"`
	TranscriptState              string     `json:"transcript_state"`
	ChildCount                   int64      `json:"child_count"`
	PublishedCount               int64      `json:"published_count"`
	ReviewCount                  int64      `json:"review_count"`
	EmbeddingPendingCount        int64      `json:"embedding_pending_count"`
	LatestError                  *string    `json:"latest_error"`
	RunStatus                    *string    `json:"run_status"`
	RunPhase                     *string    `json:"run_phase"`
	AtomizationOverride          *string    `json:"atomization_override"`
	AtomizationOverrideReason    *string    `json:"atomization_override_reason"`
	ManualAtomizationRequestedAt *time.Time `json:"manual_atomization_requested_at"`
	UpdatedAt                    time.Time  `json:"updated_at"`
	AgeSeconds                   int64      `json:"age_seconds"`
	PrimaryAction                string     `json:"primary_action"`
	ActionHref                   string     `json:"action_href"`
}

type mediaAtomizationPipelineColumn struct {
	Key   string                         `json:"key"`
	Label string                         `json:"label"`
	Count int                            `json:"count"`
	Items []mediaAtomizationPipelineItem `json:"items"`
}

func AdminGetMediaAtomizationPipeline(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	schema := getMediaAtomizationSchemaInfo(db)
	if !schema.Ready {
		c.JSON(http.StatusOK, utils.ResponseMessage{Code: http.StatusOK, Message: "Media atomization pipeline unavailable until schema migration is applied", Data: gin.H{
			"columns":       defaultPipelineColumns(),
			"schema_status": schema,
			"updated_at":    time.Now().UTC().Format(time.RFC3339),
		}})
		return
	}

	where := []string{"p.tenant_id = ?", "p.type IN ('VIDEO','PODCAST')", "p.parent_content_item_id IS NULL"}
	args := []interface{}{principal.TenantID}
	if status := strings.TrimSpace(c.Query("status")); status != "" {
		where = append(where, "COALESCE(p.chaptering_status, 'unstarted') = ?")
		args = append(args, status)
	}
	if source := strings.TrimSpace(c.Query("source")); source != "" {
		where = append(where, "(p.source_name ILIKE ? OR p.source_feed_url ILIKE ?)")
		args = append(args, "%"+source+"%", "%"+source+"%")
	}
	if q := strings.TrimSpace(c.Query("q")); q != "" {
		where = append(where, "(p.title ILIKE ? OR p.source_name ILIKE ?)")
		args = append(args, "%"+q+"%", "%"+q+"%")
	}
	if bucket := strings.TrimSpace(c.Query("bucket")); bucket != "" {
		where = append(where, "EXISTS (SELECT 1 FROM content_items bc WHERE bc.parent_content_item_id = p.public_id AND bc.duration_bucket = ?)")
		args = append(args, bucket)
	}
	if review := strings.TrimSpace(c.Query("review")); review == "true" || review == "needed" {
		where = append(where, "EXISTS (SELECT 1 FROM content_items rc WHERE rc.parent_content_item_id = p.public_id AND rc.feed_visibility = 'review')")
	}
	args = append(args, boundedLimit(c.Query("limit"), 240, 500))

	rows := []mediaAtomizationPipelineItem{}
	if err := db.Raw(`
		SELECT p.public_id::text AS id, p.title, p.status, p.chaptering_status, p.source_name,
			p.duration_sec, p.transcript_id::text AS transcript_id,
			CASE WHEN p.transcript_id IS NULL THEN 'missing' ELSE 'ready' END AS transcript_state,
			COUNT(c.id) AS child_count,
			SUM(CASE WHEN c.feed_visibility = 'visible' THEN 1 ELSE 0 END) AS published_count,
			SUM(CASE WHEN c.feed_visibility = 'review' THEN 1 ELSE 0 END) AS review_count,
			SUM(CASE WHEN c.feed_visibility = 'embedding_pending' THEN 1 ELSE 0 END) AS embedding_pending_count,
			CASE
				WHEN (SELECT r.status FROM media_atomization_runs r WHERE r.parent_content_item_id = p.public_id ORDER BY r.updated_at DESC LIMIT 1) = 'failed'
				THEN (SELECT r.error_message FROM media_atomization_runs r WHERE r.parent_content_item_id = p.public_id ORDER BY r.updated_at DESC LIMIT 1)
				ELSE NULL
			END AS latest_error,
			(SELECT r.status FROM media_atomization_runs r WHERE r.parent_content_item_id = p.public_id ORDER BY r.updated_at DESC LIMIT 1) AS run_status,
			(SELECT r.phase FROM media_atomization_runs r WHERE r.parent_content_item_id = p.public_id ORDER BY r.updated_at DESC LIMIT 1) AS run_phase,
			p.atomization_override,
			p.atomization_override_reason,
			p.manual_atomization_requested_at,
			p.updated_at
		FROM content_items p
		LEFT JOIN content_items c ON c.parent_content_item_id = p.public_id AND c.tenant_id = p.tenant_id AND c.status <> 'ARCHIVED' AND c.feed_visibility <> 'hidden'
		WHERE `+strings.Join(where, " AND ")+`
		GROUP BY p.public_id, p.title, p.status, p.chaptering_status, p.source_name, p.duration_sec, p.transcript_id, p.atomization_override, p.atomization_override_reason, p.manual_atomization_requested_at, p.updated_at
		ORDER BY p.updated_at DESC
		LIMIT ?`, args...).Scan(&rows).Error; err != nil {
		mediaAtomizationQueryError(c, err)
		return
	}

	now := time.Now().UTC()
	columns := defaultPipelineColumns()
	index := map[string]int{}
	for i := range columns {
		index[columns[i].Key] = i
		columns[i].Items = []mediaAtomizationPipelineItem{}
	}
	for i := range rows {
		rows[i].AgeSeconds = int64(now.Sub(rows[i].UpdatedAt).Seconds())
		rows[i].ActionHref = "/platform/media-studio/" + rows[i].ID
		rows[i].PrimaryAction = pipelineActionForItem(rows[i])
		key := pipelineStageForItem(rows[i])
		col := index[key]
		columns[col].Count++
		if len(columns[col].Items) < 8 {
			columns[col].Items = append(columns[col].Items, rows[i])
		}
	}

	c.JSON(http.StatusOK, utils.ResponseMessage{Code: http.StatusOK, Message: "Media atomization pipeline fetched", Data: gin.H{
		"columns":       columns,
		"schema_status": schema,
		"updated_at":    now.Format(time.RFC3339),
	}})
}

func defaultPipelineColumns() []mediaAtomizationPipelineColumn {
	return []mediaAtomizationPipelineColumn{
		{Key: "ready", Label: "Ready"},
		{Key: "transcript", Label: "Transcript"},
		{Key: "planning", Label: "Planning"},
		{Key: "cutting", Label: "Cutting"},
		{Key: "embedding", Label: "Embedding"},
		{Key: "review", Label: "Review"},
		{Key: "published", Label: "Published"},
		{Key: "disabled", Label: "Disabled"},
		{Key: "failed", Label: "Failed"},
	}
}

func pipelineStageForItem(item mediaAtomizationPipelineItem) string {
	if item.AtomizationOverride != nil && *item.AtomizationOverride == atomizationOverrideDisabled {
		return "disabled"
	}
	status := "unstarted"
	if item.ChapteringStatus != nil && *item.ChapteringStatus != "" {
		status = *item.ChapteringStatus
	}
	switch status {
	case "failed":
		return "failed"
	case "needs_review":
		return "review"
	case "completed", "published":
		return "published"
	case "embedding", "embedding_pending":
		return "embedding"
	case "cutting", "renditions", "children":
		return "cutting"
	case "planning":
		return "planning"
	case "waiting_transcript", "transcript_ready":
		return "transcript"
	case "queued", "waiting_media", "media_ready", "unstarted":
		return "ready"
	default:
		if item.TranscriptID == nil {
			return "transcript"
		}
		return "ready"
	}
}

func pipelineActionForItem(item mediaAtomizationPipelineItem) string {
	switch pipelineStageForItem(item) {
	case "disabled":
		return "Disabled"
	case "review":
		return "Review"
	case "failed":
		return "Inspect"
	case "published":
		return "Open"
	default:
		return "Open"
	}
}

func getMediaAtomizationSchemaInfo(db *gorm.DB) mediaAtomizationSchemaInfo {
	missing := []string{}
	contentColumns := []string{
		"parent_content_item_id",
		"is_feed_unit",
		"feed_visibility",
		"chapter_index",
		"chapter_start_ms",
		"chapter_end_ms",
		"chapter_confidence",
		"chaptering_status",
		"duration_bucket",
		"playback_url",
		"playback_type",
		"fallback_playback_url",
		"has_video",
		"media_renditions",
		"atomization_override",
		"atomization_override_reason",
		"atomization_override_by",
		"atomization_override_at",
		"manual_atomization_requested_at",
	}
	for _, column := range contentColumns {
		if !db.Migrator().HasColumn(&models.ContentItem{}, column) {
			missing = append(missing, "content_items."+column)
		}
	}
	chapterColumns := []string{
		"end_ms",
		"status",
		"confidence",
		"context_label",
		"boundary_reason",
		"standalone_score",
		"contains_sponsor_intro",
		"needs_review_reason",
		"duration_bucket",
		"child_content_item_id",
	}
	for _, column := range chapterColumns {
		if !db.Migrator().HasColumn(&models.Chapter{}, column) {
			missing = append(missing, "chapters."+column)
		}
	}
	if !db.Migrator().HasTable(&models.MediaAtomizationRun{}) {
		missing = append(missing, "media_atomization_runs")
	}
	if !db.Migrator().HasTable(&models.MediaAtomizationPolicy{}) {
		missing = append(missing, "media_atomization_policies")
	}

	info := mediaAtomizationSchemaInfo{
		Ready:   len(missing) == 0,
		Missing: missing,
		Message: "Media atomization schema is ready.",
	}
	if !info.Ready {
		info.Message = "Media atomization schema is incomplete. Apply CMS migrations 20260627000000_media_atomization.sql, 20260627010000_media_atomization_operations.sql, 20260627020000_media_atomization_manual_controls.sql, and 20260627030000_media_atomization_unique_index_repair.sql, then restart CMS."
	}
	return info
}

func adminGetMediaAtomizationOverviewCompat(c *gin.Context, db *gorm.DB, tenantID string, schema mediaAtomizationSchemaInfo) {
	type nameCount struct {
		Name  string `json:"name"`
		Count int64  `json:"count"`
	}
	type sourcePerformance struct {
		SourceName       string  `json:"source_name"`
		SourceFeedURL    *string `json:"source_feed_url,omitempty"`
		ParentsProcessed int64   `json:"parents_processed"`
		ChildrenProduced int64   `json:"children_produced"`
		PublishedCount   int64   `json:"published_count"`
		ReviewCount      int64   `json:"review_count"`
		FailedCount      int64   `json:"failed_count"`
	}

	parentStatus := []nameCount{}
	sourcePerformanceRows := []sourcePerformance{}
	var parentCount, durationViolationCount int64

	if err := db.Model(&models.ContentItem{}).
		Where("tenant_id = ? AND type IN ?", tenantID, []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}).
		Count(&parentCount).Error; err != nil {
		mediaAtomizationQueryError(c, err)
		return
	}
	parentStatus = append(parentStatus, nameCount{Name: "schema_missing", Count: parentCount})

	if err := db.Model(&models.ContentItem{}).
		Where("tenant_id = ? AND type IN ? AND status IN ? AND (duration_sec < ? OR duration_sec > ?) AND media_url IS NOT NULL AND media_url != '' AND thumbnail_url IS NOT NULL AND thumbnail_url != ''",
			tenantID,
			[]models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast},
			[]models.ContentStatus{models.ContentStatusReady, models.ContentStatusArchived},
			forYouMinDurationSec,
			forYouHardMaxDurationSec,
		).Count(&durationViolationCount).Error; err != nil {
		mediaAtomizationQueryError(c, err)
		return
	}

	if err := db.Raw(`
		SELECT COALESCE(source_name, 'Unknown source') AS source_name,
			source_feed_url AS source_feed_url,
			COUNT(*) AS parents_processed,
			0 AS children_produced,
			0 AS published_count,
			0 AS review_count,
			SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_count
		FROM content_items
		WHERE tenant_id = ? AND type IN ('VIDEO','PODCAST')
		GROUP BY COALESCE(source_name, 'Unknown source'), source_feed_url
		ORDER BY parents_processed DESC
		LIMIT 12`, tenantID).Scan(&sourcePerformanceRows).Error; err != nil {
		mediaAtomizationQueryError(c, err)
		return
	}

	c.JSON(http.StatusOK, utils.ResponseMessage{Code: http.StatusOK, Message: "Media atomization overview fetched with degraded schema compatibility", Data: gin.H{
		"parent_status_counts":            parentStatus,
		"child_state_counts":              []interface{}{},
		"auto_published_count":            0,
		"review_needed_count":             0,
		"failed_stuck_count":              0,
		"duration_violation_count":        durationViolationCount,
		"visible_under_floor_count":       0,
		"visible_over_hard_max_count":     durationViolationCount,
		"short_parent_active_child_count": 0,
		"short_chapter_review_count":      0,
		"invariants": gin.H{
			"visible_under_floor_feed_units":   0,
			"visible_over_hard_max_feed_units": durationViolationCount,
			"parents_under_40m_with_children":  0,
			"short_chapters_awaiting_review":   0,
		},
		"policy": gin.H{
			"min_feed_unit_seconds":          forYouMinDurationSec,
			"atomization_min_parent_seconds": atomizationMinParentDurationSec,
			"hard_max_feed_unit_seconds":     forYouHardMaxDurationSec,
		},
		"average_chapters_per_parent": 0,
		"average_processing_seconds":  nil,
		"duration_distribution":       []interface{}{},
		"source_performance":          sourcePerformanceRows,
		"schema_status":               schema,
		"updated_at":                  time.Now().UTC().Format(time.RFC3339),
	}})
}

func adminListMediaAtomizationParentsCompat(c *gin.Context, db *gorm.DB, tenantID string, schema mediaAtomizationSchemaInfo) {
	type parentRow struct {
		ID                    string    `json:"id"`
		Title                 *string   `json:"title"`
		Status                string    `json:"status"`
		ChapteringStatus      *string   `json:"chaptering_status"`
		SourceName            *string   `json:"source_name"`
		SourceFeedURL         *string   `json:"source_feed_url"`
		DurationSec           *int      `json:"duration_sec"`
		TranscriptID          *string   `json:"transcript_id"`
		ChildCount            int64     `json:"child_count"`
		PublishedCount        int64     `json:"published_count"`
		ReviewCount           int64     `json:"review_count"`
		EmbeddingPendingCount int64     `json:"embedding_pending_count"`
		LatestError           *string   `json:"latest_error"`
		UpdatedAt             time.Time `json:"updated_at"`
	}
	where := []string{"tenant_id = ?", "type IN ('VIDEO','PODCAST')"}
	args := []interface{}{tenantID}
	if source := strings.TrimSpace(c.Query("source")); source != "" {
		where = append(where, "(source_name ILIKE ? OR source_feed_url ILIKE ?)")
		args = append(args, "%"+source+"%", "%"+source+"%")
	}
	if q := strings.TrimSpace(c.Query("q")); q != "" {
		where = append(where, "(title ILIKE ? OR source_name ILIKE ?)")
		args = append(args, "%"+q+"%", "%"+q+"%")
	}
	limit := boundedLimit(c.Query("limit"), 50, 200)
	args = append(args, limit)
	rows := []parentRow{}
	if err := db.Raw(`
		SELECT public_id::text AS id, title, status, NULL AS chaptering_status, source_name, source_feed_url,
			duration_sec, transcript_id::text AS transcript_id,
			0 AS child_count,
			0 AS published_count,
			0 AS review_count,
			0 AS embedding_pending_count,
			NULL AS latest_error,
			updated_at
		FROM content_items
		WHERE `+strings.Join(where, " AND ")+`
		ORDER BY updated_at DESC
		LIMIT ?`, args...).Scan(&rows).Error; err != nil {
		mediaAtomizationQueryError(c, err)
		return
	}

	schemaMessage := schema.Message
	longMessage := "Over the 40-minute feed ceiling. Suppressed from For You by the compatibility hard cap until atomized."
	for i := range rows {
		msg := schemaMessage
		if rows[i].DurationSec != nil && *rows[i].DurationSec > forYouHardMaxDurationSec {
			msg = longMessage + " " + schemaMessage
		}
		rows[i].LatestError = &msg
	}

	c.JSON(http.StatusOK, utils.ResponseMessage{Code: http.StatusOK, Message: "Media atomization parents fetched with degraded schema compatibility", Data: gin.H{"items": rows, "schema_status": schema}})
}

func visibleLongParentLeakQuery(db *gorm.DB, tenantID string) *gorm.DB {
	return db.Model(&models.ContentItem{}).
		Where("tenant_id = ?", tenantID).
		Where("type IN ?", []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}).
		Where("parent_content_item_id IS NULL").
		Where("is_feed_unit = TRUE").
		Where("feed_visibility = ?", feedVisibilityVisible).
		Where("status IN ?", []models.ContentStatus{models.ContentStatusReady, models.ContentStatusArchived}).
		Where("duration_sec > ?", forYouHardMaxDurationSec)
}

func visibleFeedDurationViolationQuery(db *gorm.DB, tenantID string) *gorm.DB {
	return db.Model(&models.ContentItem{}).
		Where("tenant_id = ?", tenantID).
		Where("type IN ?", []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}).
		Where("is_feed_unit = TRUE").
		Where("feed_visibility = ?", feedVisibilityVisible).
		Where("status IN ?", []models.ContentStatus{models.ContentStatusReady, models.ContentStatusArchived}).
		Where("(duration_sec IS NULL OR duration_sec < ? OR duration_sec > ?)", forYouMinDurationSec, forYouHardMaxDurationSec)
}

func validParentFeedUnitQuery(db *gorm.DB, tenantID string) *gorm.DB {
	return db.Model(&models.ContentItem{}).
		Where("tenant_id = ?", tenantID).
		Where("type IN ?", []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}).
		Where("parent_content_item_id IS NULL").
		Where("duration_sec BETWEEN ? AND ?", forYouMinDurationSec, atomizationMinParentDurationSec).
		Where("status = ?", models.ContentStatusReady)
}

type mediaAtomizationRepairResult struct {
	UpdatedCount                     int64
	RemainingCount                   int64
	HiddenDurationViolationCount     int64
	ArchivedShortParentChildCount    int64
	RestoredParentCount              int64
	RestoredFuzzyChapterCount        int64
	RemainingVisibleUnderFloorCount  int64
	RemainingVisibleOverHardMaxCount int64
}

func repairMediaAtomizationDurationLeaks(db *gorm.DB, tenantID string) (mediaAtomizationRepairResult, error) {
	out := mediaAtomizationRepairResult{}
	now := time.Now().UTC()

	shortParentChildren := db.Model(&models.ContentItem{}).
		Where("tenant_id = ?", tenantID).
		Where("parent_content_item_id IS NOT NULL").
		Where("status <> ?", models.ContentStatusArchived).
		Where("EXISTS (SELECT 1 FROM content_items p WHERE p.public_id = content_items.parent_content_item_id AND p.tenant_id = content_items.tenant_id AND COALESCE(p.duration_sec, 0) <= ?)", atomizationMinParentDurationSec)
	if result := shortParentChildren.Updates(map[string]interface{}{
		"status":            models.ContentStatusArchived,
		"is_feed_unit":      false,
		"feed_visibility":   feedVisibilityHidden,
		"chaptering_status": "archived",
		"updated_at":        now,
	}); result.Error != nil {
		return out, result.Error
	} else {
		out.ArchivedShortParentChildCount = result.RowsAffected
		out.UpdatedCount += result.RowsAffected
	}

	shortParentChapters := db.Model(&models.Chapter{}).
		Where("tenant_id = ?", tenantID).
		Where("child_content_item_id IS NOT NULL").
		Where("EXISTS (SELECT 1 FROM content_items c JOIN content_items p ON p.public_id = c.parent_content_item_id AND p.tenant_id = c.tenant_id WHERE c.public_id = chapters.child_content_item_id AND c.tenant_id = chapters.tenant_id AND COALESCE(p.duration_sec, 0) <= ?)", atomizationMinParentDurationSec)
	if err := shortParentChapters.Updates(map[string]interface{}{
		"status":     chapterStatusRejected,
		"updated_at": now,
	}).Error; err != nil {
		return out, err
	}

	violations := visibleFeedDurationViolationQuery(db, tenantID)
	if result := violations.Updates(map[string]interface{}{
		"is_feed_unit":    false,
		"feed_visibility": feedVisibilityHidden,
		"chaptering_status": gorm.Expr(
			"CASE WHEN duration_sec > ? AND transcript_id IS NULL THEN ? WHEN duration_sec > ? THEN ? WHEN duration_sec IS NULL THEN ? ELSE ? END",
			forYouHardMaxDurationSec,
			"waiting_transcript",
			forYouHardMaxDurationSec,
			"transcript_ready",
			"duration_missing",
			"duration_invalid",
		),
		"updated_at": now,
	}); result.Error != nil {
		return out, result.Error
	} else {
		out.HiddenDurationViolationCount = result.RowsAffected
		out.UpdatedCount += result.RowsAffected
	}

	validParents := validParentFeedUnitQuery(db, tenantID)
	if result := validParents.Updates(map[string]interface{}{
		"is_feed_unit":    true,
		"feed_visibility": feedVisibilityVisible,
		"updated_at":      now,
	}); result.Error != nil {
		return out, result.Error
	} else {
		out.RestoredParentCount = result.RowsAffected
		out.UpdatedCount += result.RowsAffected
	}

	fuzzyChapters := db.Model(&models.ContentItem{}).
		Where("tenant_id = ?", tenantID).
		Where("type IN ?", []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}).
		Where("parent_content_item_id IS NOT NULL").
		Where("duration_sec BETWEEN ? AND ?", forYouMinDurationSec, (5*60)-1).
		Where("status = ?", models.ContentStatusReady).
		Where("(chaptering_status = ? OR chaptering_status IS NULL)", chapterStatusPublished).
		Where("EXISTS (SELECT 1 FROM content_items p WHERE p.public_id = content_items.parent_content_item_id AND p.tenant_id = content_items.tenant_id AND p.duration_sec > ?)", atomizationMinParentDurationSec)
	if result := fuzzyChapters.Updates(map[string]interface{}{
		"is_feed_unit":    true,
		"feed_visibility": feedVisibilityVisible,
		"updated_at":      now,
	}); result.Error != nil {
		return out, result.Error
	} else {
		out.RestoredFuzzyChapterCount = result.RowsAffected
		out.UpdatedCount += result.RowsAffected
	}

	if err := visibleFeedDurationViolationQuery(db, tenantID).Count(&out.RemainingCount).Error; err != nil {
		return out, err
	}
	if err := db.Model(&models.ContentItem{}).
		Where("tenant_id = ?", tenantID).
		Where("type IN ?", []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}).
		Where("is_feed_unit = TRUE AND feed_visibility = ? AND status IN ?", feedVisibilityVisible, []models.ContentStatus{models.ContentStatusReady, models.ContentStatusArchived}).
		Where("(duration_sec IS NULL OR duration_sec < ?)", forYouMinDurationSec).
		Count(&out.RemainingVisibleUnderFloorCount).Error; err != nil {
		return out, err
	}
	if err := db.Model(&models.ContentItem{}).
		Where("tenant_id = ?", tenantID).
		Where("type IN ?", []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}).
		Where("is_feed_unit = TRUE AND feed_visibility = ? AND status IN ?", feedVisibilityVisible, []models.ContentStatus{models.ContentStatusReady, models.ContentStatusArchived}).
		Where("duration_sec > ?", forYouHardMaxDurationSec).
		Count(&out.RemainingVisibleOverHardMaxCount).Error; err != nil {
		return out, err
	}
	return out, nil
}

func mediaAtomizationQueryError(c *gin.Context, err error) {
	c.JSON(http.StatusInternalServerError, utils.HTTPError{
		Code:    http.StatusInternalServerError,
		Message: "Media atomization schema is not available. Apply the CMS migrations 20260627000000_media_atomization.sql, 20260627010000_media_atomization_operations.sql, 20260627020000_media_atomization_manual_controls.sql, and 20260627030000_media_atomization_unique_index_repair.sql, then restart CMS. Details: " + err.Error(),
	})
}

func boundedLimit(raw string, fallback, max int) int {
	limit, err := strconv.Atoi(raw)
	if err != nil || limit <= 0 {
		return fallback
	}
	if limit > max {
		return max
	}
	return limit
}

func AdminApproveAtomizedChapter(c *gin.Context) {
	updateAtomizedChapterReview(c, true)
}

func AdminRejectAtomizedChapter(c *gin.Context) {
	updateAtomizedChapterReview(c, false)
}

func updateAtomizedChapterReview(c *gin.Context, approve bool) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	chapterID, err := uuid.Parse(c.Param("chapter_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{Code: http.StatusBadRequest, Message: "Invalid chapter id"})
		return
	}
	var chapter models.Chapter
	if err := db.Where("public_id = ? AND tenant_id = ?", chapterID, principal.TenantID).First(&chapter).Error; err != nil {
		c.JSON(http.StatusNotFound, utils.HTTPError{Code: http.StatusNotFound, Message: "Chapter not found"})
		return
	}
	if chapter.ChildContentItemID == nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{Code: http.StatusBadRequest, Message: "Chapter has no atomized child item yet"})
		return
	}
	var child models.ContentItem
	if err := db.Where("public_id = ? AND tenant_id = ?", *chapter.ChildContentItemID, principal.TenantID).First(&child).Error; err != nil {
		c.JSON(http.StatusNotFound, utils.HTTPError{Code: http.StatusNotFound, Message: "Child item not found"})
		return
	}
	if approve {
		if child.DurationSec == nil || *child.DurationSec < forYouMinDurationSec {
			c.JSON(http.StatusConflict, utils.HTTPError{Code: http.StatusConflict, Message: "Cannot publish a chapter shorter than 4:30"})
			return
		}
		if *child.DurationSec > forYouHardMaxDurationSec {
			c.JSON(http.StatusConflict, utils.HTTPError{Code: http.StatusConflict, Message: "Cannot publish a chapter longer than 40 minutes"})
			return
		}
		chapter.Status = chapterStatusPublished
		child.Status = models.ContentStatusReady
		child.FeedVisibility = feedVisibilityVisible
		status := chapterStatusPublished
		child.ChapteringStatus = &status
	} else {
		chapter.Status = chapterStatusRejected
		child.FeedVisibility = feedVisibilityHidden
		child.Status = models.ContentStatusArchived
		status := chapterStatusRejected
		child.ChapteringStatus = &status
	}
	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Save(&chapter).Error; err != nil {
			return err
		}
		return tx.Save(&child).Error
	}); err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{Code: http.StatusInternalServerError, Message: "Failed to update chapter review state"})
		return
	}
	action := "rejected"
	if approve {
		action = "approved"
	}
	createStudioAudit(db, principal, "media_studio.atomized_chapter_"+action, child.PublicID.String(), "success", "", map[string]interface{}{
		"chapter_id": chapter.PublicID.String(),
		"child_id":   child.PublicID.String(),
	})
	c.JSON(http.StatusOK, utils.ResponseMessage{Code: http.StatusOK, Message: "Chapter " + action, Data: gin.H{"chapter": chapter, "child": child}})
}
