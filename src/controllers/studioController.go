package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"encoding/json"
	"math"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// Chapter-windowing constants. The transcript is chunked into time-windows
// before being sent to the LLM. minWindowSec is the floor (fine granularity for
// short clips); for long transcripts the window grows so the count stays under
// targetWindowCount — otherwise the LLM truncates the tail and the end of a long
// podcast never gets chaptered. Algorithm constants → code defaults.
const (
	minWindowSec      = 12.0
	targetWindowCount = 550
)

// minChapterGapMs is the minimum spacing between persisted chapter boundaries —
// a defensive guard so a buggy client can't store overlapping/zero-width chapters.
const minChapterGapMs = 1000

// ─── Shared shapes ──────────────────────────────────────────

type segmentData struct {
	Start float64 `json:"start"`
	End   float64 `json:"end"`
	Text  string  `json:"text"`
}

type jsonbChapter struct {
	Start  float64 `json:"start"`
	End    float64 `json:"end"`
	Title  string  `json:"title"`
	Source string  `json:"source"`
}

type heatmapPoint struct {
	Start float64 `json:"start"`
	End   float64 `json:"end"`
	Value float64 `json:"value"`
}

type sponsorSegment struct {
	Start    float64 `json:"start"`
	End      float64 `json:"end"`
	Category string  `json:"category"`
}

type studioContentDTO struct {
	ID            string  `json:"id"`
	Type          string  `json:"type"`
	Title         string  `json:"title"`
	Status        string  `json:"status"`
	MediaURL      *string `json:"media_url,omitempty"`
	ThumbnailURL  *string `json:"thumbnail_url,omitempty"`
	DurationSec   *int    `json:"duration_sec,omitempty"`
	FileSizeBytes int64   `json:"file_size_bytes"`
	StorageTier   *string `json:"storage_tier,omitempty"`
	CaptionState  *string `json:"caption_state,omitempty"`
	// Download-time engagement signals (from content_item.metadata).
	Heatmap         []heatmapPoint   `json:"heatmap,omitempty"`
	SponsorSegments []sponsorSegment `json:"sponsor_segments,omitempty"`
}

type studioTranscriptDTO struct {
	TranscriptID   string        `json:"transcript_id"`
	FullText       string        `json:"full_text"`
	Language       *string       `json:"language,omitempty"`
	Source         *string       `json:"source,omitempty"`
	Provider       *string       `json:"provider,omitempty"`
	ApprovedAt     *string       `json:"approved_at,omitempty"`
	ApprovedBy     *string       `json:"approved_by,omitempty"`
	ApprovalReason *string       `json:"approval_reason,omitempty"`
	Segments       []segmentData `json:"segments"`
}

type studioChapterDTO struct {
	ID                   string   `json:"id,omitempty"`
	Title                string   `json:"title"`
	Summary              *string  `json:"summary,omitempty"`
	StartMs              int      `json:"start_ms"`
	EndMs                int      `json:"end_ms"`
	Source               string   `json:"source"`
	Status               string   `json:"status,omitempty"`
	Confidence           *float64 `json:"confidence,omitempty"`
	ContextLabel         *string  `json:"context_label,omitempty"`
	BoundaryReason       *string  `json:"boundary_reason,omitempty"`
	StandaloneScore      *float64 `json:"standalone_score,omitempty"`
	ContainsSponsorIntro bool     `json:"contains_sponsor_intro,omitempty"`
	NeedsReviewReason    *string  `json:"needs_review_reason,omitempty"`
	DurationBucket       *string  `json:"duration_bucket,omitempty"`
	ChildContentItemID   *string  `json:"child_content_item_id,omitempty"`
}

type studioResponse struct {
	Content                studioContentDTO           `json:"content"`
	Transcript             *studioTranscriptDTO       `json:"transcript"`
	Chapters               []studioChapterDTO         `json:"chapters"`
	LatestTranscriptionJob *transcriptionJobResponse  `json:"latest_transcription_job,omitempty"`
	TranscriptQuality      *transcriptQualityResponse `json:"transcript_quality,omitempty"`
	TranscriptAudit        map[string]interface{}     `json:"transcript_audit,omitempty"`
}

// ─── Helpers ────────────────────────────────────────────────

// loadStudioItem fetches the tenant-scoped content item + its linked transcript.
// transcript is nil when the item has none yet.
func loadStudioItem(db *gorm.DB, tenantID, idParam string) (*models.ContentItem, *models.Transcript, error) {
	id, err := uuid.Parse(idParam)
	if err != nil {
		return nil, nil, errInvalidID
	}
	var item models.ContentItem
	if err := db.Where("public_id = ? AND tenant_id = ?", id, tenantID).First(&item).Error; err != nil {
		return nil, nil, errNotFound
	}
	if item.TranscriptID == nil {
		return &item, nil, nil
	}
	var transcript models.Transcript
	if err := db.Where("public_id = ?", *item.TranscriptID).First(&transcript).Error; err != nil {
		return &item, nil, nil
	}
	return &item, &transcript, nil
}

var (
	errInvalidID = &studioErr{"invalid content ID"}
	errNotFound  = &studioErr{"content not found"}
)

type studioErr struct{ msg string }

func (e *studioErr) Error() string { return e.msg }

// flexSegments parses a jsonb array of {start,end,text} (or word-level
// {start,end,word}) into segments. Tolerant of both shapes so it works for
// caption segments and legacy Whisper word_timestamps alike.
func flexSegments(raw datatypes.JSON) []segmentData {
	out := []segmentData{}
	if len(raw) == 0 {
		return out
	}
	var rows []map[string]any
	if err := json.Unmarshal(raw, &rows); err != nil {
		return out
	}
	for _, r := range rows {
		text := asString(r["text"])
		if text == "" {
			text = asString(r["word"])
		}
		if text == "" {
			continue
		}
		out = append(out, segmentData{Start: asFloat(r["start"]), End: asFloat(r["end"]), Text: text})
	}
	return out
}

// extractSegments prefers the segments column but falls back to word_timestamps,
// where older Media/Whisper write-backs stored the segment list before the
// dedicated segments column existed.
func extractSegments(t *models.Transcript) []segmentData {
	if segs := flexSegments(t.Segments); len(segs) > 0 {
		return segs
	}
	return flexSegments(t.WordTimestamps)
}

func asString(v any) string {
	if s, ok := v.(string); ok {
		return strings.TrimSpace(s)
	}
	return ""
}

func asFloat(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case int:
		return float64(n)
	case int64:
		return float64(n)
	}
	return 0
}

func durationMs(item *models.ContentItem) int {
	if item.DurationSec != nil && *item.DurationSec > 0 {
		return *item.DurationSec * 1000
	}
	return 0
}

// computeEnds fills missing EndMs from the next chapter's start (or the media
// duration for the last). Explicit atomization/editor ends are preserved.
func computeEnds(chapters []studioChapterDTO, durMs int) {
	for i := range chapters {
		if chapters[i].EndMs > chapters[i].StartMs {
			continue
		}
		if i+1 < len(chapters) {
			chapters[i].EndMs = chapters[i+1].StartMs
		} else if durMs > 0 {
			chapters[i].EndMs = durMs
		} else {
			chapters[i].EndMs = chapters[i].StartMs
		}
	}
}

// windowSizeFor picks the window span so a long transcript doesn't exceed the
// LLM's per-request window cap (which would silently drop the tail). Short
// transcripts keep the fine minWindowSec floor.
func windowSizeFor(segments []segmentData) float64 {
	if n := len(segments); n > 0 {
		totalSec := segments[n-1].End
		if scaled := totalSec / targetWindowCount; scaled > minWindowSec {
			return scaled
		}
	}
	return minWindowSec
}

func buildWindows(segments []segmentData, windowSec float64) []chapterWindowPayload {
	windows := []chapterWindowPayload{}
	if len(segments) == 0 {
		return windows
	}
	idx := 0
	curStart := segments[0].Start
	var sb strings.Builder
	for _, seg := range segments {
		if sb.Len() > 0 && seg.End-curStart >= windowSec {
			windows = append(windows, chapterWindowPayload{Index: idx, StartSec: curStart, Text: sb.String()})
			idx++
			curStart = seg.Start
			sb.Reset()
		}
		if sb.Len() > 0 {
			sb.WriteString(" ")
		}
		sb.WriteString(strings.TrimSpace(seg.Text))
	}
	if sb.Len() > 0 {
		windows = append(windows, chapterWindowPayload{Index: idx, StartSec: curStart, Text: sb.String()})
	}
	return windows
}

// ─── GET /admin/content/:id/studio ──────────────────────────

func GetStudio(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	item, transcript, err := loadStudioItem(db, principal.TenantID, c.Param("id"))
	if err != nil {
		status := http.StatusBadRequest
		if err == errNotFound {
			status = http.StatusNotFound
		}
		c.JSON(status, utils.HTTPError{Code: status, Message: err.Error()})
		return
	}

	resp := studioResponse{
		Content:  mapStudioContent(item),
		Chapters: []studioChapterDTO{},
	}

	if transcript != nil {
		resp.Transcript = &studioTranscriptDTO{
			TranscriptID:   transcript.PublicID.String(),
			FullText:       transcript.FullText,
			Language:       transcript.Language,
			Source:         transcript.Source,
			Provider:       transcript.Provider,
			ApprovedAt:     formatTimePtr(transcript.ApprovedAt),
			ApprovedBy:     transcript.ApprovedBy,
			ApprovalReason: transcript.ApprovalReason,
			Segments:       extractSegments(transcript),
		}
		chapters := loadOrSeedChapters(db, principal.TenantID, transcript)
		resp.Chapters = chaptersToDTO(chapters, durationMs(item))
		if q := latestTranscriptQuality(db, item.PublicID); q != nil {
			mapped := mapTranscriptQuality(*q)
			resp.TranscriptQuality = &mapped
		}
	}
	if job := latestTranscriptionJob(db, item.PublicID); job != nil {
		mapped := mapTranscriptionJob(*job)
		resp.LatestTranscriptionJob = &mapped
	}
	resp.TranscriptAudit = studioAuditSummary(db, principal.TenantID, item.PublicID.String())

	c.JSON(http.StatusOK, utils.ResponseMessage{
		Code:    http.StatusOK,
		Message: "Studio loaded",
		Data:    resp,
	})
}

func mapStudioContent(item *models.ContentItem) studioContentDTO {
	dto := studioContentDTO{
		ID:            item.PublicID.String(),
		Type:          string(item.Type),
		Status:        string(item.Status),
		MediaURL:      item.MediaURL,
		ThumbnailURL:  item.ThumbnailURL,
		DurationSec:   item.DurationSec,
		FileSizeBytes: item.FileSizeBytes,
		StorageTier:   item.StorageTier,
		CaptionState:  item.CaptionState,
	}
	if item.Title != nil {
		dto.Title = *item.Title
	}

	// Surface download-time engagement signals stored in metadata jsonb.
	if len(item.Metadata) > 0 {
		var meta struct {
			Heatmap         []heatmapPoint   `json:"heatmap"`
			SponsorSegments []sponsorSegment `json:"sponsor_segments"`
		}
		if json.Unmarshal(item.Metadata, &meta) == nil {
			dto.Heatmap = meta.Heatmap
			dto.SponsorSegments = meta.SponsorSegments
		}
	}
	return dto
}

// loadOrSeedChapters returns the transcript's chapter rows, lazily seeding them
// from the transcript's native Chapters jsonb (source=youtube) the first time.
func loadOrSeedChapters(db *gorm.DB, tenantID string, transcript *models.Transcript) []models.Chapter {
	var chapters []models.Chapter
	db.Where("transcript_id = ? AND tenant_id = ?", transcript.PublicID, tenantID).
		Order("start_ms ASC").Find(&chapters)
	if len(chapters) > 0 {
		return chapters
	}

	// Seed from native jsonb chapters, if any.
	var seed []jsonbChapter
	if len(transcript.Chapters) > 0 {
		_ = json.Unmarshal(transcript.Chapters, &seed)
	}
	if len(seed) == 0 {
		return chapters
	}
	rows := make([]models.Chapter, 0, len(seed))
	for _, ch := range seed {
		if strings.TrimSpace(ch.Title) == "" {
			continue
		}
		src := ch.Source
		if src == "" || src == "youtube" {
			src = models.ChapterSourceYouTube
		}
		rows = append(rows, models.Chapter{
			TranscriptID: transcript.PublicID,
			TenantID:     tenantID,
			Title:        ch.Title,
			StartMs:      int(math.Round(ch.Start * 1000)),
			Source:       src,
		})
	}
	if len(rows) > 0 {
		db.Create(&rows)
	}
	return rows
}

func chaptersToDTO(chapters []models.Chapter, durMs int) []studioChapterDTO {
	out := make([]studioChapterDTO, 0, len(chapters))
	for _, ch := range chapters {
		out = append(out, studioChapterDTO{
			ID:                   ch.PublicID.String(),
			Title:                ch.Title,
			Summary:              ch.Summary,
			StartMs:              ch.StartMs,
			Source:               ch.Source,
			Status:               ch.Status,
			Confidence:           ch.Confidence,
			ContextLabel:         ch.ContextLabel,
			BoundaryReason:       ch.BoundaryReason,
			StandaloneScore:      ch.StandaloneScore,
			ContainsSponsorIntro: ch.ContainsSponsorIntro,
			NeedsReviewReason:    ch.NeedsReviewReason,
			DurationBucket:       ch.DurationBucket,
		})
		if ch.EndMs != nil {
			out[len(out)-1].EndMs = *ch.EndMs
		}
		if ch.ChildContentItemID != nil {
			id := ch.ChildContentItemID.String()
			out[len(out)-1].ChildContentItemID = &id
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StartMs < out[j].StartMs })
	computeEnds(out, durMs)
	return out
}

// ─── POST /admin/content/:id/chapters/generate ──────────────
// Returns a PREVIEW (not persisted). The studio loads it into its working set.

type generateChaptersRequest struct {
	Mode              string `json:"mode"`
	TargetCount       *int   `json:"target_count"`
	TargetDurationSec *int   `json:"target_duration_sec"`
	MinSec            *int   `json:"min_sec"`
	MaxSec            *int   `json:"max_sec"`
	WithSummary       *bool  `json:"with_summary"`
}

func GenerateChapters(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	item, transcript, err := loadStudioItem(db, principal.TenantID, c.Param("id"))
	if err != nil {
		status := http.StatusBadRequest
		if err == errNotFound {
			status = http.StatusNotFound
		}
		c.JSON(status, utils.HTTPError{Code: status, Message: err.Error()})
		return
	}
	if transcript == nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code: http.StatusBadRequest, Message: "No transcript — generate a transcript first",
		})
		return
	}

	var req generateChaptersRequest
	_ = c.ShouldBindJSON(&req)
	if req.Mode == "" {
		req.Mode = "auto"
	}
	withSummary := true
	if req.WithSummary != nil {
		withSummary = *req.WithSummary
	}

	segments := extractSegments(transcript)
	windows := buildWindows(segments, windowSizeFor(segments))
	if len(windows) == 0 {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code: http.StatusBadRequest, Message: "Transcript has no segments to segment into chapters",
		})
		return
	}

	lang := ""
	if transcript.Language != nil {
		lang = *transcript.Language
	}

	generated, err := generateChaptersViaEnrichment(windows, chaptersGenOpts{
		Mode:              req.Mode,
		TargetCount:       req.TargetCount,
		TargetDurationSec: req.TargetDurationSec,
		MinSec:            req.MinSec,
		MaxSec:            req.MaxSec,
		WithSummary:       withSummary,
		Language:          lang,
	})
	if err != nil {
		c.JSON(http.StatusBadGateway, utils.HTTPError{
			Code: http.StatusBadGateway, Message: "Chapter generation failed: " + err.Error(),
		})
		return
	}

	// Map window indices → start_ms (snap to real window start times).
	preview := make([]studioChapterDTO, 0, len(generated))
	for _, gc := range generated {
		if gc.StartIndex < 0 || gc.StartIndex >= len(windows) {
			continue
		}
		preview = append(preview, studioChapterDTO{
			Title:                gc.Title,
			Summary:              gc.Summary,
			StartMs:              int(math.Round(windows[gc.StartIndex].StartSec * 1000)),
			Source:               models.ChapterSourceDerived,
			Status:               chapterStatusDraft,
			Confidence:           gc.Confidence,
			ContextLabel:         gc.ContextLabel,
			BoundaryReason:       gc.BoundaryReason,
			StandaloneScore:      gc.StandaloneScore,
			ContainsSponsorIntro: gc.ContainsSponsorOrIntro,
			NeedsReviewReason:    gc.NeedsReviewReason,
		})
	}
	sort.Slice(preview, func(i, j int) bool { return preview[i].StartMs < preview[j].StartMs })
	if len(preview) > 0 {
		preview[0].StartMs = 0 // always cover the start
	}
	computeEnds(preview, durationMs(item))

	c.JSON(http.StatusOK, utils.ResponseMessage{
		Code:    http.StatusOK,
		Message: "Chapters generated",
		Data:    gin.H{"chapters": preview},
	})
}

// ─── PUT /admin/content/:id/chapters ────────────────────────
// Transactional bulk-replace of the persisted chapter set (the studio's working
// set after generate + manual edits).

type saveChaptersRequest struct {
	Chapters []struct {
		Title                string   `json:"title"`
		Summary              *string  `json:"summary"`
		StartMs              int      `json:"start_ms"`
		EndMs                *int     `json:"end_ms"`
		Source               string   `json:"source"`
		Status               string   `json:"status"`
		Confidence           *float64 `json:"confidence"`
		ContextLabel         *string  `json:"context_label"`
		BoundaryReason       *string  `json:"boundary_reason"`
		StandaloneScore      *float64 `json:"standalone_score"`
		ContainsSponsorIntro bool     `json:"contains_sponsor_intro"`
		NeedsReviewReason    *string  `json:"needs_review_reason"`
	} `json:"chapters"`
}

func SaveChapters(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	item, transcript, err := loadStudioItem(db, principal.TenantID, c.Param("id"))
	if err != nil {
		status := http.StatusBadRequest
		if err == errNotFound {
			status = http.StatusNotFound
		}
		c.JSON(status, utils.HTTPError{Code: status, Message: err.Error()})
		return
	}
	if transcript == nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code: http.StatusBadRequest, Message: "No transcript to attach chapters to",
		})
		return
	}

	var req saveChaptersRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{Code: http.StatusBadRequest, Message: "Invalid request body"})
		return
	}

	durMs := durationMs(item)
	var existingChapters []models.Chapter
	db.Where("transcript_id = ? AND tenant_id = ?", transcript.PublicID, principal.TenantID).
		Order("start_ms ASC").Find(&existingChapters)
	rawExistingChapters, _ := json.Marshal(existingChapters)
	previousChecksum := checksumTranscriptText(string(rawExistingChapters), nil)

	rows := make([]models.Chapter, 0, len(req.Chapters))
	for _, ch := range req.Chapters {
		title := strings.TrimSpace(ch.Title)
		if title == "" {
			continue
		}
		start := ch.StartMs
		if start < 0 {
			start = 0
		}
		if durMs > 0 && start > durMs {
			start = durMs
		}
		src := ch.Source
		if src != models.ChapterSourceYouTube && src != models.ChapterSourceDerived {
			src = models.ChapterSourceManual
		}
		rows = append(rows, models.Chapter{
			TranscriptID:         transcript.PublicID,
			TenantID:             principal.TenantID,
			Title:                title,
			Summary:              ch.Summary,
			StartMs:              start,
			EndMs:                ch.EndMs,
			Source:               src,
			Status:               defaultStr(ch.Status, chapterStatusDraft),
			Confidence:           ch.Confidence,
			ContextLabel:         ch.ContextLabel,
			BoundaryReason:       ch.BoundaryReason,
			StandaloneScore:      ch.StandaloneScore,
			ContainsSponsorIntro: ch.ContainsSponsorIntro,
			NeedsReviewReason:    ch.NeedsReviewReason,
		})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].StartMs < rows[j].StartMs })
	if len(rows) > 0 {
		rows[0].StartMs = 0 // first chapter always starts at 0
	}

	// Drop boundaries closer than minChapterGapMs to the previous one so we never
	// persist overlapping / zero-width chapters (defensive — the client guards this).
	if len(rows) > 1 {
		filtered := rows[:1]
		for i := 1; i < len(rows); i++ {
			if rows[i].StartMs-filtered[len(filtered)-1].StartMs >= minChapterGapMs {
				filtered = append(filtered, rows[i])
			}
		}
		rows = filtered
	}

	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("transcript_id = ? AND tenant_id = ?", transcript.PublicID, principal.TenantID).
			Delete(&models.Chapter{}).Error; err != nil {
			return err
		}
		if len(rows) > 0 {
			if err := tx.Create(&rows).Error; err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{
			Code: http.StatusInternalServerError, Message: "Failed to save chapters",
		})
		return
	}
	sourceMix := map[string]int{}
	for _, row := range rows {
		sourceMix[row.Source]++
	}
	rawChapters, _ := json.Marshal(rows)
	createStudioAudit(db, principal, "media_studio.chapters_save", item.PublicID.String(), "success", "", map[string]interface{}{
		"chapter_count":     len(rows),
		"source_mix":        sourceMix,
		"previous_checksum": previousChecksum,
		"new_checksum":      checksumTranscriptText(string(rawChapters), nil),
	})

	c.JSON(http.StatusOK, utils.ResponseMessage{
		Code:    http.StatusOK,
		Message: "Chapters saved",
		Data:    gin.H{"chapters": chaptersToDTO(rows, durMs)},
	})
}

// ─── PUT /admin/content/:id/transcript ──────────────────────
// Light transcript editing: replace segments + recompute full_text, then
// re-trigger embedding (text changed).

type saveTranscriptRequest struct {
	Segments []segmentData `json:"segments"`
	Reason   string        `json:"reason"`
}

func SaveTranscript(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	item, transcript, err := loadStudioItem(db, principal.TenantID, c.Param("id"))
	if err != nil {
		status := http.StatusBadRequest
		if err == errNotFound {
			status = http.StatusNotFound
		}
		c.JSON(status, utils.HTTPError{Code: status, Message: err.Error()})
		return
	}
	if transcript == nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code: http.StatusBadRequest, Message: "No transcript to edit",
		})
		return
	}

	var req saveTranscriptRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{Code: http.StatusBadRequest, Message: "Invalid request body"})
		return
	}

	parts := make([]string, 0, len(req.Segments))
	for _, s := range req.Segments {
		if t := strings.TrimSpace(s.Text); t != "" {
			parts = append(parts, t)
		}
	}
	fullText := strings.Join(parts, " ")

	previousSegments := extractSegments(transcript)
	changedSegments := changedSegmentCount(previousSegments, req.Segments)
	prevChecksum := checksumTranscriptText(transcript.FullText, transcript.Segments)
	segJSON, _ := json.Marshal(req.Segments)
	transcript.Segments = datatypes.JSON(segJSON)
	transcript.FullText = fullText
	if err := db.Save(transcript).Error; err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{
			Code: http.StatusInternalServerError, Message: "Failed to save transcript",
		})
		return
	}
	newChecksum := checksumTranscriptText(transcript.FullText, transcript.Segments)
	createStudioAudit(db, principal, "media_studio.transcript_edit", item.PublicID.String(), "success", "", map[string]interface{}{
		"segment_count":          len(req.Segments),
		"previous_segment_count": len(previousSegments),
		"changed_segment_count":  changedSegments,
		"previous_checksum":      prevChecksum,
		"new_checksum":           newChecksum,
		"reason":                 strings.TrimSpace(req.Reason),
	})
	computeAndStoreTranscriptQuality(db, item, transcript, nil)

	// Best-effort re-embed: the transcript text changed. Model-agnostic — calls
	// whatever the embedding pipeline currently is. Fire-and-forget.
	itemCopy := *item
	go func() {
		if text := buildEmbeddingText(&itemCopy); text != "" {
			_ = triggerEmbedding(text, itemCopy.PublicID.String())
		}
	}()

	c.JSON(http.StatusOK, utils.ResponseMessage{
		Code:    http.StatusOK,
		Message: "Transcript saved",
		Data:    gin.H{"full_text": fullText, "segments": req.Segments},
	})
}

type approveTranscriptRequest struct {
	Reason string `json:"reason"`
}

func ApproveTranscript(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	item, transcript, err := loadStudioItem(db, principal.TenantID, c.Param("id"))
	if err != nil || transcript == nil {
		status := http.StatusBadRequest
		if err == errNotFound || transcript == nil {
			status = http.StatusNotFound
		}
		c.JSON(status, utils.HTTPError{Code: status, Message: "Transcript not found"})
		return
	}
	var req approveTranscriptRequest
	_ = c.ShouldBindJSON(&req)
	now := time.Now()
	reason := strings.TrimSpace(req.Reason)
	transcript.ApprovedAt = &now
	transcript.ApprovedBy = &principal.Email
	if reason != "" {
		transcript.ApprovalReason = &reason
	} else {
		transcript.ApprovalReason = nil
	}
	if err := db.Save(transcript).Error; err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{Code: http.StatusInternalServerError, Message: "Failed to approve transcript"})
		return
	}
	createStudioAudit(db, principal, "media_studio.transcript_approve", item.PublicID.String(), "success", "", map[string]interface{}{
		"transcript_id": transcript.PublicID.String(),
		"reason":        reason,
	})
	c.JSON(http.StatusOK, utils.ResponseMessage{Code: http.StatusOK, Message: "Transcript approved", Data: gin.H{"transcript": mapStudioTranscript(transcript)}})
}

func UnapproveTranscript(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	item, transcript, err := loadStudioItem(db, principal.TenantID, c.Param("id"))
	if err != nil || transcript == nil {
		status := http.StatusBadRequest
		if err == errNotFound || transcript == nil {
			status = http.StatusNotFound
		}
		c.JSON(status, utils.HTTPError{Code: status, Message: "Transcript not found"})
		return
	}
	transcript.ApprovedAt = nil
	transcript.ApprovedBy = nil
	transcript.ApprovalReason = nil
	if err := db.Save(transcript).Error; err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{Code: http.StatusInternalServerError, Message: "Failed to clear transcript approval"})
		return
	}
	createStudioAudit(db, principal, "media_studio.transcript_unapprove", item.PublicID.String(), "success", "", map[string]interface{}{
		"transcript_id": transcript.PublicID.String(),
	})
	c.JSON(http.StatusOK, utils.ResponseMessage{Code: http.StatusOK, Message: "Transcript approval cleared", Data: gin.H{"transcript": mapStudioTranscript(transcript)}})
}

type transcriptCompareCandidate struct {
	ID              string                     `json:"id"`
	Kind            string                     `json:"kind"`
	Source          *string                    `json:"source,omitempty"`
	Provider        *string                    `json:"provider,omitempty"`
	Language        *string                    `json:"language,omitempty"`
	CreatedAt       string                     `json:"created_at"`
	FullText        string                     `json:"full_text"`
	Segments        []segmentData              `json:"segments"`
	WordCount       int                        `json:"word_count"`
	SegmentCount    int                        `json:"segment_count"`
	Similarity      float64                    `json:"similarity"`
	DifferenceCount int                        `json:"difference_count"`
	Quality         *transcriptQualityResponse `json:"quality,omitempty"`
	ApprovedAt      *string                    `json:"approved_at,omitempty"`
	ApprovedBy      *string                    `json:"approved_by,omitempty"`
	ApprovalReason  *string                    `json:"approval_reason,omitempty"`
}

func mapStudioTranscript(t *models.Transcript) studioTranscriptDTO {
	return studioTranscriptDTO{
		TranscriptID:   t.PublicID.String(),
		FullText:       t.FullText,
		Language:       t.Language,
		Source:         t.Source,
		Provider:       t.Provider,
		ApprovedAt:     formatTimePtr(t.ApprovedAt),
		ApprovedBy:     t.ApprovedBy,
		ApprovalReason: t.ApprovalReason,
		Segments:       extractSegments(t),
	}
}

func wordSetSimilarity(a, b string) float64 {
	aw := strings.Fields(strings.ToLower(a))
	bw := strings.Fields(strings.ToLower(b))
	if len(aw) == 0 && len(bw) == 0 {
		return 1
	}
	if len(aw) == 0 || len(bw) == 0 {
		return 0
	}
	setA := map[string]bool{}
	setB := map[string]bool{}
	for _, w := range aw {
		setA[w] = true
	}
	for _, w := range bw {
		setB[w] = true
	}
	intersection := 0
	for w := range setA {
		if setB[w] {
			intersection++
		}
	}
	union := len(setA)
	for w := range setB {
		if !setA[w] {
			union++
		}
	}
	if union == 0 {
		return 0
	}
	return float64(intersection) / float64(union)
}

func compareCandidateQuality(db *gorm.DB, transcriptID uuid.UUID) *transcriptQualityResponse {
	if db == nil {
		return nil
	}
	var q models.TranscriptQuality
	if err := db.Where("transcript_id = ?", transcriptID).First(&q).Error; err != nil {
		return nil
	}
	mapped := mapTranscriptQuality(q)
	return &mapped
}

func makeTranscriptCandidate(db *gorm.DB, activeText string, kind string, t models.Transcript) transcriptCompareCandidate {
	segs := extractSegments(&t)
	sim := wordSetSimilarity(activeText, t.FullText)
	diff := int(math.Abs(float64(len(strings.Fields(activeText)) - len(strings.Fields(t.FullText)))))
	return transcriptCompareCandidate{
		ID:              t.PublicID.String(),
		Kind:            kind,
		Source:          t.Source,
		Provider:        t.Provider,
		Language:        t.Language,
		CreatedAt:       t.CreatedAt.UTC().Format(time.RFC3339),
		FullText:        t.FullText,
		Segments:        segs,
		WordCount:       len(strings.Fields(t.FullText)),
		SegmentCount:    len(segs),
		Similarity:      sim,
		DifferenceCount: diff,
		Quality:         compareCandidateQuality(db, t.PublicID),
		ApprovedAt:      formatTimePtr(t.ApprovedAt),
		ApprovedBy:      t.ApprovedBy,
		ApprovalReason:  t.ApprovalReason,
	}
}

func makeVersionCandidate(activeText string, v models.TranscriptVersion) transcriptCompareCandidate {
	t := models.Transcript{
		PublicID:       v.PublicID,
		FullText:       v.FullText,
		Segments:       v.Segments,
		WordTimestamps: v.WordTimestamps,
		Language:       v.Language,
		Source:         v.Source,
		Provider:       v.Provider,
		ApprovedAt:     v.ApprovedAt,
		ApprovedBy:     v.ApprovedBy,
		ApprovalReason: v.ApprovalReason,
		CreatedAt:      v.CreatedAt,
	}
	c := makeTranscriptCandidate(nil, activeText, "version", t)
	c.Quality = nil
	return c
}

func CompareTranscripts(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	item, transcript, err := loadStudioItem(db, principal.TenantID, c.Param("id"))
	if err != nil {
		status := http.StatusBadRequest
		if err == errNotFound {
			status = http.StatusNotFound
		}
		c.JSON(status, utils.HTTPError{Code: status, Message: err.Error()})
		return
	}
	if transcript == nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{Code: http.StatusBadRequest, Message: "No active transcript to compare"})
		return
	}
	candidates := []transcriptCompareCandidate{}
	var transcripts []models.Transcript
	db.Where("content_item_id = ? AND public_id <> ?", item.PublicID, transcript.PublicID).
		Order("created_at DESC").Limit(10).Find(&transcripts)
	for _, t := range transcripts {
		kind := "transcript"
		if t.Source != nil && strings.HasPrefix(*t.Source, "youtube_") {
			kind = "youtube_caption"
		} else if t.Source != nil && strings.HasPrefix(*t.Source, "stt_") {
			kind = "stt"
		}
		candidates = append(candidates, makeTranscriptCandidate(db, transcript.FullText, kind, t))
	}
	var versions []models.TranscriptVersion
	db.Where("content_item_id = ?", item.PublicID).Order("created_at DESC").Limit(10).Find(&versions)
	for _, v := range versions {
		candidates = append(candidates, makeVersionCandidate(transcript.FullText, v))
	}
	active := makeTranscriptCandidate(db, transcript.FullText, "active", *transcript)
	c.JSON(http.StatusOK, utils.ResponseMessage{
		Code:    http.StatusOK,
		Message: "Transcript comparison fetched",
		Data:    gin.H{"active": active, "candidates": candidates},
	})
}

func changedSegmentCount(prev, next []segmentData) int {
	maxLen := len(prev)
	if len(next) > maxLen {
		maxLen = len(next)
	}
	changed := 0
	for i := 0; i < maxLen; i++ {
		if i >= len(prev) || i >= len(next) {
			changed++
			continue
		}
		if prev[i].Start != next[i].Start || prev[i].End != next[i].End || strings.TrimSpace(prev[i].Text) != strings.TrimSpace(next[i].Text) {
			changed++
		}
	}
	return changed
}

func createStudioAudit(db *gorm.DB, principal utils.AdminPrincipal, action, resource, status, errorMessage string, payload map[string]interface{}) {
	raw, _ := json.Marshal(payload)
	entry := models.AuditLog{
		TenantID:       principal.TenantID,
		UserID:         principal.UserID,
		UserEmail:      principal.Email,
		Action:         action,
		TargetService:  "cms",
		TargetResource: resource,
		Status:         status,
		ErrorMessage:   errorMessage,
		Payload:        datatypes.JSON(raw),
	}
	_ = db.Create(&entry).Error
}

func studioAuditSummary(db *gorm.DB, tenantID, contentID string) map[string]interface{} {
	var last models.AuditLog
	if err := db.Where("tenant_id = ? AND target_resource = ? AND action IN ?", tenantID, contentID, []string{
		"media_studio.transcript_edit",
		"media_studio.chapters_save",
		"media_studio.transcript_approve",
		"media_studio.transcript_unapprove",
	}).Order("created_at DESC").First(&last).Error; err != nil {
		return nil
	}
	return map[string]interface{}{
		"last_action": last.Action,
		"last_status": last.Status,
		"last_at":     last.CreatedAt.UTC().Format(time.RFC3339),
		"user_email":  last.UserEmail,
	}
}
