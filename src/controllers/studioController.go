package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"encoding/json"
	"math"
	"net/http"
	"sort"
	"strings"

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
	ID           string  `json:"id"`
	Type         string  `json:"type"`
	Title        string  `json:"title"`
	Status       string  `json:"status"`
	MediaURL     *string `json:"media_url,omitempty"`
	ThumbnailURL *string `json:"thumbnail_url,omitempty"`
	DurationSec  *int    `json:"duration_sec,omitempty"`
	CaptionState *string `json:"caption_state,omitempty"`
	// Download-time engagement signals (from content_item.metadata).
	Heatmap         []heatmapPoint   `json:"heatmap,omitempty"`
	SponsorSegments []sponsorSegment `json:"sponsor_segments,omitempty"`
}

type studioTranscriptDTO struct {
	TranscriptID string        `json:"transcript_id"`
	FullText     string        `json:"full_text"`
	Language     *string       `json:"language,omitempty"`
	Segments     []segmentData `json:"segments"`
}

type studioChapterDTO struct {
	ID      string  `json:"id,omitempty"`
	Title   string  `json:"title"`
	Summary *string `json:"summary,omitempty"`
	StartMs int     `json:"start_ms"`
	EndMs   int     `json:"end_ms"`
	Source  string  `json:"source"`
}

type studioResponse struct {
	Content    studioContentDTO     `json:"content"`
	Transcript *studioTranscriptDTO `json:"transcript"`
	Chapters   []studioChapterDTO   `json:"chapters"`
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

// computeEnds fills EndMs from the next chapter's start (or the media duration
// for the last). Input must be sorted by StartMs.
func computeEnds(chapters []studioChapterDTO, durMs int) {
	for i := range chapters {
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
			TranscriptID: transcript.PublicID.String(),
			FullText:     transcript.FullText,
			Language:     transcript.Language,
			Segments:     extractSegments(transcript),
		}
		chapters := loadOrSeedChapters(db, principal.TenantID, transcript)
		resp.Chapters = chaptersToDTO(chapters, durationMs(item))
	}

	c.JSON(http.StatusOK, utils.ResponseMessage{
		Code:    http.StatusOK,
		Message: "Studio loaded",
		Data:    resp,
	})
}

func mapStudioContent(item *models.ContentItem) studioContentDTO {
	dto := studioContentDTO{
		ID:           item.PublicID.String(),
		Type:         string(item.Type),
		Status:       string(item.Status),
		MediaURL:     item.MediaURL,
		ThumbnailURL: item.ThumbnailURL,
		DurationSec:  item.DurationSec,
		CaptionState: item.CaptionState,
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
			ID:      ch.PublicID.String(),
			Title:   ch.Title,
			Summary: ch.Summary,
			StartMs: ch.StartMs,
			Source:  ch.Source,
		})
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
			Title:   gc.Title,
			Summary: gc.Summary,
			StartMs: int(math.Round(windows[gc.StartIndex].StartSec * 1000)),
			Source:  models.ChapterSourceDerived,
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
		Title   string  `json:"title"`
		Summary *string `json:"summary"`
		StartMs int     `json:"start_ms"`
		Source  string  `json:"source"`
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
			TranscriptID: transcript.PublicID,
			TenantID:     principal.TenantID,
			Title:        title,
			Summary:      ch.Summary,
			StartMs:      start,
			Source:       src,
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

	segJSON, _ := json.Marshal(req.Segments)
	transcript.Segments = datatypes.JSON(segJSON)
	transcript.FullText = fullText
	if err := db.Save(transcript).Error; err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{
			Code: http.StatusInternalServerError, Message: "Failed to save transcript",
		})
		return
	}

	// Best-effort re-embed: the transcript text changed. Model-agnostic — calls
	// whatever the embedding pipeline currently is. Fire-and-forget.
	itemCopy := *item
	go func() {
		if text := buildEmbeddingText(&itemCopy); text != "" {
			_ = triggerEmbedding(text, itemCopy.PublicID.String(), true)
		}
	}()

	c.JSON(http.StatusOK, utils.ResponseMessage{
		Code:    http.StatusOK,
		Message: "Transcript saved",
		Data:    gin.H{"full_text": fullText, "segments": req.Segments},
	})
}
