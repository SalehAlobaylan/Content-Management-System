package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// Embedding text cleanup: strip URLs + collapse whitespace so a noisy YouTube
// description (links/affiliate/social CTAs) doesn't dilute the vector. Safe for
// article bodies too.
var (
	embedURLRegex        = regexp.MustCompile(`https?://\S+`)
	embedWhitespaceRegex = regexp.MustCompile(`\s+`)
)

func cleanForEmbedding(text string) string {
	text = embedURLRegex.ReplaceAllString(text, " ")
	text = embedWhitespaceRegex.ReplaceAllString(text, " ")
	return strings.TrimSpace(text)
}

// ── Response types ──────────────────────────────────────────

type enrichmentStatsResponse struct {
	TotalMedia                  int64 `json:"total_media"`
	WithTranscript              int64 `json:"with_transcript"`
	MissingTranscript           int64 `json:"missing_transcript"`
	MissingTranscriptActionable int64 `json:"missing_transcript_actionable"`
	WithEmbedding               int64 `json:"with_embedding"`
	MissingEmbedding            int64 `json:"missing_embedding"`
	WithSparse                  int64 `json:"with_sparse"`
	MissingSparse               int64 `json:"missing_sparse"`
	WithImageEmbedding          int64 `json:"with_image_embedding"`
	MissingImageEmbedding       int64 `json:"missing_image_embedding"`
	TotalReady                  int64 `json:"total_ready"`
}

type missingEnrichmentCountsResponse struct {
	Transcript      int64 `json:"transcript"`
	Embedding       int64 `json:"embedding"`
	Sparse          int64 `json:"sparse"`
	Image           int64 `json:"image"`
	TranscriptImage int64 `json:"transcript_image"`
	EmbeddingSparse int64 `json:"embedding_sparse"`
}

type missingEnrichmentItem struct {
	ID                string `json:"id"`
	Title             string `json:"title"`
	Type              string `json:"type"`
	SourceName        string `json:"source_name"`
	Status            string `json:"status"`
	HasTranscript     bool   `json:"has_transcript"`
	HasEmbedding      bool   `json:"has_embedding"`
	HasSparse         bool   `json:"has_sparse"`
	HasImageEmbedding bool   `json:"has_image_embedding"`
	MediaURL          string `json:"media_url"`
	ThumbnailURL      string `json:"thumbnail_url"`
	CreatedAt         string `json:"created_at"`
}

type triggerEnrichmentRequest struct {
	Types []string `json:"types" binding:"required"`
	// Force bypasses the STT guard's toggle + state-machine checks (manual
	// "Enrich with STT" upgrade). The budget cap still applies.
	Force bool `json:"force"`
}

type triggerBatchRequest struct {
	ContentIDs []string `json:"content_ids" binding:"required"`
	Types      []string `json:"types" binding:"required"`
}

type triggerResultItem struct {
	ContentID string `json:"content_id"`
	Status    string `json:"status"`
	Error     string `json:"error,omitempty"`
}

// ── GET /admin/enrichment/stats ─────────────────────────────

func GetEnrichmentStats(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	stats, err := computeEnrichmentStats(db)
	if err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{
			Code:    http.StatusInternalServerError,
			Message: "Failed to fetch enrichment stats: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, utils.ResponseMessage{
		Code:    http.StatusOK,
		Message: "Enrichment stats fetched successfully",
		Data:    stats,
	})
}

// computeEnrichmentStats runs the coverage FILTER query and returns the struct.
// Extracted so the Enrichment Autopilot can capture before/after snapshots from
// the exact same read-model the admin sees on the page.
func computeEnrichmentStats(db *gorm.DB) (enrichmentStatsResponse, error) {
	var stats enrichmentStatsResponse

	// Single efficient query using PostgreSQL FILTER
	row := db.Raw(`
		SELECT
			COUNT(*) FILTER (WHERE type IN ('VIDEO','PODCAST')) as total_media,
			COUNT(*) FILTER (WHERE type IN ('VIDEO','PODCAST') AND transcript_id IS NOT NULL) as with_transcript,
				COUNT(*) FILTER (WHERE type IN ('VIDEO','PODCAST') AND transcript_id IS NULL AND status = 'READY') as missing_transcript,
				COUNT(*) FILTER (WHERE type IN ('VIDEO','PODCAST') AND transcript_id IS NULL AND status = 'READY' AND (duration_sec IS NULL OR duration_sec <= 2400)) as missing_transcript_actionable,
			COUNT(*) FILTER (WHERE embedding IS NOT NULL) as with_embedding,
			COUNT(*) FILTER (WHERE embedding IS NULL AND status = 'READY') as missing_embedding,
			COUNT(*) FILTER (WHERE embedding_sparse IS NOT NULL) as with_sparse,
			COUNT(*) FILTER (WHERE embedding IS NOT NULL AND embedding_sparse IS NULL AND status = 'READY') as missing_sparse,
			COUNT(*) FILTER (WHERE image_embedding IS NOT NULL) as with_image_embedding,
			COUNT(*) FILTER (WHERE thumbnail_url IS NOT NULL AND image_embedding IS NULL AND status = 'READY') as missing_image_embedding,
			COUNT(*) FILTER (WHERE status = 'READY') as total_ready
		FROM content_items
		WHERE status != 'ARCHIVED'
	`).Row()

	if err := row.Scan(
		&stats.TotalMedia,
		&stats.WithTranscript,
		&stats.MissingTranscript,
		&stats.MissingTranscriptActionable,
		&stats.WithEmbedding,
		&stats.MissingEmbedding,
		&stats.WithSparse,
		&stats.MissingSparse,
		&stats.WithImageEmbedding,
		&stats.MissingImageEmbedding,
		&stats.TotalReady,
	); err != nil {
		return stats, err
	}
	return stats, nil
}

// ── GET /admin/enrichment/missing-counts ───────────────────

func GetMissingEnrichmentCounts(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	contentType := c.Query("type")
	status := c.DefaultQuery("status", "READY")

	countFor := func(missingParam string) (int64, error) {
		var total int64
		err := buildMissingQuery(db, missingParam, contentType, status).Count(&total).Error
		return total, err
	}

	var counts missingEnrichmentCountsResponse
	var err error
	if counts.Transcript, err = countFor("transcript"); err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{Code: http.StatusInternalServerError, Message: "Failed to count transcript gaps: " + err.Error()})
		return
	}
	if counts.Embedding, err = countFor("embedding"); err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{Code: http.StatusInternalServerError, Message: "Failed to count embedding gaps: " + err.Error()})
		return
	}
	if counts.Sparse, err = countFor("sparse"); err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{Code: http.StatusInternalServerError, Message: "Failed to count sparse gaps: " + err.Error()})
		return
	}
	if counts.Image, err = countFor("image"); err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{Code: http.StatusInternalServerError, Message: "Failed to count image gaps: " + err.Error()})
		return
	}
	if counts.TranscriptImage, err = countFor("transcript,image"); err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{Code: http.StatusInternalServerError, Message: "Failed to count media enrichment gaps: " + err.Error()})
		return
	}
	if counts.EmbeddingSparse, err = countFor("embedding,sparse"); err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{Code: http.StatusInternalServerError, Message: "Failed to count news enrichment gaps: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, utils.ResponseMessage{
		Code:    http.StatusOK,
		Message: "Missing enrichment counts fetched successfully",
		Data:    counts,
	})
}

// ── GET /admin/enrichment/missing ───────────────────────────

func GetMissingEnrichments(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	// Parse query params
	missingParam := c.DefaultQuery("missing", "transcript")
	contentType := c.Query("type")
	status := c.DefaultQuery("status", "READY")
	limitStr := c.DefaultQuery("limit", "50")
	offsetStr := c.DefaultQuery("offset", "0")

	limit, _ := strconv.Atoi(limitStr)
	offset, _ := strconv.Atoi(offsetStr)
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}

	// Build the filtered query (shared with the bulk trigger endpoint).
	query := buildMissingQuery(db, missingParam, contentType, status)

	// Count total
	var total int64
	query.Count(&total)

	// Fetch items
	var items []models.ContentItem
	query.Order("created_at DESC").Limit(limit).Offset(offset).Find(&items)

	// Map to response
	responseItems := make([]missingEnrichmentItem, 0, len(items))
	for _, item := range items {
		resp := missingEnrichmentItem{
			ID:                item.PublicID.String(),
			Type:              string(item.Type),
			Status:            string(item.Status),
			HasTranscript:     item.TranscriptID != nil,
			HasEmbedding:      item.Embedding != nil,
			HasSparse:         item.EmbeddingSparse != nil,
			HasImageEmbedding: item.ImageEmbedding != nil,
			CreatedAt:         item.CreatedAt.Format("2006-01-02T15:04:05Z"),
		}
		if item.Title != nil {
			resp.Title = *item.Title
		}
		if item.SourceName != nil {
			resp.SourceName = *item.SourceName
		}
		if item.MediaURL != nil {
			resp.MediaURL = *item.MediaURL
		}
		if item.ThumbnailURL != nil {
			resp.ThumbnailURL = *item.ThumbnailURL
		}
		responseItems = append(responseItems, resp)
	}

	c.JSON(http.StatusOK, utils.ResponseMessage{
		Code:    http.StatusOK,
		Message: "Missing enrichments fetched successfully",
		Data: gin.H{
			"items":  responseItems,
			"total":  total,
			"limit":  limit,
			"offset": offset,
		},
	})
}

// ── POST /admin/enrichment/trigger/:id ──────────────────────

func TriggerEnrichment(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	contentIDStr := c.Param("id")
	contentID, err := uuid.Parse(contentIDStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code:    http.StatusBadRequest,
			Message: "Invalid content ID",
		})
		return
	}

	var req triggerEnrichmentRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code:    http.StatusBadRequest,
			Message: "Invalid request body: " + err.Error(),
		})
		return
	}

	// Look up content item
	var item models.ContentItem
	if err := db.Where("public_id = ?", contentID).First(&item).Error; err != nil {
		c.JSON(http.StatusNotFound, utils.HTTPError{
			Code:    http.StatusNotFound,
			Message: "Content item not found",
		})
		return
	}

	results, errors := triggerItemArtifacts(db, &item, req.Types, req.Force)

	c.JSON(http.StatusOK, utils.ResponseMessage{
		Code:    http.StatusOK,
		Message: "Enrichment trigger completed",
		Data: gin.H{
			"content_id": contentIDStr,
			"results":    results,
			"errors":     errors,
		},
	})
}

// ── POST /admin/enrichment/trigger-batch ────────────────────

func TriggerBatchEnrichment(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	var req triggerBatchRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code:    http.StatusBadRequest,
			Message: "Invalid request body: " + err.Error(),
		})
		return
	}

	// Cap at 10 items per batch to prevent overload
	if len(req.ContentIDs) > 10 {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code:    http.StatusBadRequest,
			Message: "Maximum 10 items per batch request",
		})
		return
	}

	results := make([]triggerResultItem, 0, len(req.ContentIDs))

	for _, idStr := range req.ContentIDs {
		contentID, err := uuid.Parse(idStr)
		if err != nil {
			results = append(results, triggerResultItem{ContentID: idStr, Status: "error", Error: "invalid ID"})
			continue
		}

		var item models.ContentItem
		if err := db.Where("public_id = ?", contentID).First(&item).Error; err != nil {
			results = append(results, triggerResultItem{ContentID: idStr, Status: "error", Error: "not found"})
			continue
		}

		_, itemErrors := triggerItemArtifacts(db, &item, req.Types, false)

		if len(itemErrors) > 0 {
			results = append(results, triggerResultItem{
				ContentID: idStr,
				Status:    "partial",
				Error:     strings.Join(itemErrors, "; "),
			})
		} else {
			results = append(results, triggerResultItem{ContentID: idStr, Status: "triggered"})
		}
	}

	c.JSON(http.StatusOK, utils.ResponseMessage{
		Code:    http.StatusOK,
		Message: "Batch enrichment completed",
		Data:    results,
	})
}

// ── GET /admin/enrichment/health ────────────────────────────
//
// After the Media-Service split, this endpoint reports the health of BOTH
// AI services so Platform-Console can render a unified card. The legacy
// top-level "status"/"models"/"dependencies" fields are preserved for the
// Enrichment-Service so older console builds keep working; the new
// "services" map is the canonical shape.

func GetEnrichmentServiceHealth(c *gin.Context) {
	type perServiceView struct {
		Status       string          `json:"status"`
		Error        string          `json:"error,omitempty"`
		Models       map[string]bool `json:"models"`
		Dependencies map[string]bool `json:"dependencies,omitempty"`
	}

	view := func(h *serviceHealthResponse, err error) perServiceView {
		if err != nil {
			return perServiceView{Status: "unreachable", Error: err.Error()}
		}
		return perServiceView{
			Status:       h.Status,
			Models:       h.Models,
			Dependencies: h.Dependencies,
		}
	}

	enrichment := view(checkEnrichmentHealth())
	media := view(checkMediaHealth())

	// Aggregate top-level status: ok only when both are ok.
	overallStatus := "ok"
	if enrichment.Status != "ok" || media.Status != "ok" {
		overallStatus = "not_ready"
	}

	// Legacy fields surface the Enrichment-Service so the existing Console
	// build keeps rendering even before the dual-service UI update lands.
	c.JSON(http.StatusOK, utils.ResponseMessage{
		Code:    http.StatusOK,
		Message: "AI services status",
		Data: gin.H{
			"status":       overallStatus,
			"models":       enrichment.Models,
			"dependencies": enrichment.Dependencies,
			"services": gin.H{
				"media":      media,
				"enrichment": enrichment,
			},
		},
	})
}

// ── Helpers ─────────────────────────────────────────────────

// buildEmbeddingText constructs a text string from content item fields for embedding.
func buildEmbeddingText(item *models.ContentItem) string {
	parts := []string{}
	if item.Title != nil && *item.Title != "" {
		parts = append(parts, *item.Title)
	}
	if item.Excerpt != nil && *item.Excerpt != "" {
		parts = append(parts, *item.Excerpt)
	}
	if item.BodyText != nil && *item.BodyText != "" {
		// Clean (strip URLs / collapse whitespace) then cap — the fuller body
		// contributes to the vector without its boilerplate/links. Capped for
		// UTF-8 safety; raised from 500 so the description meaningfully counts.
		body := cleanForEmbedding(*item.BodyText)
		runes := []rune(body)
		if len(runes) > 3000 {
			body = string(runes[:3000])
		}
		parts = append(parts, body)
	}
	return strings.Join(parts, " ")
}

// ── Shared query + trigger helpers ──────────────────────────

// missingEnrichmentClauses turns a comma-separated `missing` param into the
// SQL OR-clauses used to find content lacking each artifact. Shared by the
// missing-list endpoint and the bulk trigger so they stay in lock-step.
func missingEnrichmentClauses(missingParam string) []string {
	conditions := []string{}
	for _, mt := range strings.Split(missingParam, ",") {
		switch strings.TrimSpace(mt) {
		case "transcript":
			conditions = append(conditions, "transcript_id IS NULL AND type IN ('VIDEO','PODCAST')")
		case "embedding":
			conditions = append(conditions, "embedding IS NULL")
		case "sparse":
			// Has a dense vector but no sparse lexical weights.
			conditions = append(conditions, "embedding IS NOT NULL AND embedding_sparse IS NULL")
		case "image":
			// Has a thumbnail but no CLIP image embedding.
			conditions = append(conditions, "image_embedding IS NULL AND thumbnail_url IS NOT NULL")
		}
	}
	return conditions
}

// buildMissingQuery applies the shared status + type + missing-artifact filters.
// `contentType` accepts a comma-separated list (e.g. "VIDEO,PODCAST").
func buildMissingQuery(db *gorm.DB, missingParam, contentType, status string) *gorm.DB {
	query := db.Model(&models.ContentItem{}).Where("status != ?", "ARCHIVED")

	if status != "" {
		query = query.Where("status = ?", status)
	}

	if contentType != "" {
		types := strings.Split(contentType, ",")
		for i := range types {
			types[i] = strings.TrimSpace(types[i])
		}
		query = query.Where("type IN ?", types)
	}

	if conditions := missingEnrichmentClauses(missingParam); len(conditions) > 0 {
		query = query.Where("(" + strings.Join(conditions, " OR ") + ")")
	}

	return query
}

// Traced-outcome status vocabulary for a single (item × artifact) attempt.
const (
	artifactOutcomeTriggered = "triggered"
	artifactOutcomeAlready   = "already"
	artifactOutcomeSkipped   = "skipped"
	artifactOutcomeError     = "error"
)

// artifactOutcome is the structured result of one (item × artifact) attempt.
// The Enrichment Autopilot consumes this so it can ledger per-artifact and
// cross-link the created transcription job; the human trigger paths use the
// string wrapper below and are byte-for-byte unchanged.
type artifactOutcome struct {
	Artifact string
	Status   string // artifactOutcome* constant
	Reason   string
	JobID    string // transcript only, when a job was created
	SkipKind string // transcript only, when a typed guard declined it
}

// triggerItemArtifactsTraced runs the requested enrichment passes for one item
// and returns a structured outcome per artifact. `triggerSource` labels any
// transcription_jobs row it creates (empty = the historical ingest_auto/manual
// derivation). `force` only affects the transcript (STT) pass: it bypasses the
// guard's toggle + state-machine checks for a manual upgrade (budget cap still
// applies). This is the single place the per-artifact logic lives.
func triggerItemArtifactsTraced(db *gorm.DB, item *models.ContentItem, types []string, force bool, triggerSource string) []artifactOutcome {
	id := item.PublicID.String()
	out := make([]artifactOutcome, 0, len(types))
	for _, enrichType := range types {
		switch enrichType {
		case "transcript":
			o := artifactOutcome{Artifact: "transcript"}
			// When not forcing, an already-linked transcript is a skip. A forced
			// manual upgrade is allowed to re-run (e.g. youtube_auto → STT); the
			// guard's stt_done check still prevents wasteful re-billing.
			if item.TranscriptID != nil && !force {
				o.Status, o.Reason = artifactOutcomeAlready, "already exists"
			} else if item.Type != models.ContentTypeVideo && item.Type != models.ContentTypePodcast {
				o.Status, o.Reason = artifactOutcomeSkipped, "not VIDEO/PODCAST"
			} else if item.MediaURL == nil || *item.MediaURL == "" {
				o.Status, o.Reason = artifactOutcomeError, "no media_url available"
			} else if jobID, err := triggerTranscription(item, db, force, triggerSource); err != nil {
				if isSTTSkipped(err) {
					o.Status, o.Reason = artifactOutcomeSkipped, err.Error()
					o.SkipKind = string(sttSkipKindOf(err))
				} else {
					o.Status, o.Reason = artifactOutcomeError, err.Error()
				}
			} else {
				o.Status, o.JobID = artifactOutcomeTriggered, jobID
			}
			out = append(out, o)

		case "embedding":
			o := artifactOutcome{Artifact: "embedding"}
			if item.Embedding != nil {
				o.Status, o.Reason = artifactOutcomeAlready, "already exists"
			} else if text := buildEmbeddingText(item); text == "" {
				o.Status, o.Reason = artifactOutcomeError, "no text content available"
			} else if err := triggerEmbedding(text, id, true); err != nil {
				// extract_sparse=true → populates dense + sparse together.
				o.Status, o.Reason = artifactOutcomeError, err.Error()
			} else {
				o.Status = artifactOutcomeTriggered
			}
			out = append(out, o)

		case "sparse":
			o := artifactOutcome{Artifact: "sparse"}
			if item.EmbeddingSparse != nil {
				o.Status, o.Reason = artifactOutcomeAlready, "already exists"
			} else if text := buildEmbeddingText(item); text == "" {
				o.Status, o.Reason = artifactOutcomeError, "no text content available"
			} else if err := triggerEmbedding(text, id, true); err != nil {
				// Re-embed with sparse on — re-writes dense too (harmless, same value).
				o.Status, o.Reason = artifactOutcomeError, err.Error()
			} else {
				o.Status = artifactOutcomeTriggered
			}
			out = append(out, o)

		case "image":
			o := artifactOutcome{Artifact: "image"}
			if item.ImageEmbedding != nil {
				o.Status, o.Reason = artifactOutcomeAlready, "already exists"
			} else if item.ThumbnailURL == nil || *item.ThumbnailURL == "" {
				o.Status, o.Reason = artifactOutcomeError, "no thumbnail_url available"
			} else if err := triggerImageEmbedding(*item.ThumbnailURL, id); err != nil {
				o.Status, o.Reason = artifactOutcomeError, err.Error()
			} else {
				o.Status = artifactOutcomeTriggered
			}
			out = append(out, o)
		}
	}
	return out
}

// triggerItemArtifacts is the string-shaped wrapper kept for the single, batch,
// and bulk human trigger paths so their behaviour is identical. Already-present
// artifacts are reported as skips (in results), not errors.
func triggerItemArtifacts(db *gorm.DB, item *models.ContentItem, types []string, force bool) (results, errs []string) {
	for _, o := range triggerItemArtifactsTraced(db, item, types, force, "") {
		switch o.Status {
		case artifactOutcomeError:
			errs = append(errs, o.Artifact+": "+o.Reason)
		case artifactOutcomeTriggered:
			results = append(results, o.Artifact+": triggered")
		case artifactOutcomeAlready:
			results = append(results, o.Artifact+": already exists")
		default: // skipped
			results = append(results, o.Artifact+": skipped ("+o.Reason+")")
		}
	}
	return results, errs
}

// ── POST /admin/enrichment/trigger-all + GET /bulk-status ───
//
// "Enrich all missing" can span hundreds of items, each a synchronous model
// call — far too long for one HTTP request. So trigger-all kicks off a single
// background run and returns immediately; the UI polls bulk-status for a live
// progress bar. State is in-memory (one run at a time); a CMS restart mid-run
// just resets it — the reconcile sweep + re-trigger remain the backstop.

type bulkEnrichStatus struct {
	Running     bool       `json:"running"`
	Total       int        `json:"total"`
	Done        int        `json:"done"`
	Failed      int        `json:"failed"`
	Types       []string   `json:"types"`
	ContentType string     `json:"content_type,omitempty"`
	LastError   string     `json:"last_error,omitempty"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	FinishedAt  *time.Time `json:"finished_at,omitempty"`
}

var (
	bulkMu    sync.Mutex
	bulkState bulkEnrichStatus
)

// bulkMaxItems caps a single run so a misclick can't enqueue a runaway job.
const bulkMaxItems = 5000

func TriggerAllEnrichment(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	var req struct {
		Types []string `json:"types" binding:"required"`
		Type  string   `json:"type"`
		Max   int      `json:"max"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code:    http.StatusBadRequest,
			Message: "Invalid request body: " + err.Error(),
		})
		return
	}
	if len(req.Types) == 0 {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code:    http.StatusBadRequest,
			Message: "At least one artifact type is required",
		})
		return
	}

	limit := req.Max
	if limit <= 0 || limit > bulkMaxItems {
		limit = bulkMaxItems
	}

	if enrichmentAutopilotAnyRunInFlight() {
		c.JSON(http.StatusConflict, utils.HTTPError{Code: http.StatusConflict, Message: "an autopilot run is in flight; retry shortly"})
		return
	}
	bulkMu.Lock()
	if bulkState.Running {
		bulkMu.Unlock()
		c.JSON(http.StatusConflict, utils.HTTPError{
			Code:    http.StatusConflict,
			Message: "A bulk enrichment run is already in progress",
		})
		return
	}

	// The missing-filter is derived from the artifacts being triggered, so we
	// only load items that actually lack at least one of them.
	missingParam := strings.Join(req.Types, ",")
	var items []models.ContentItem
	buildMissingQuery(db, missingParam, req.Type, "READY").
		Order("created_at DESC").
		Limit(limit).
		Find(&items)

	if len(items) == 0 {
		bulkMu.Unlock()
		c.JSON(http.StatusOK, utils.ResponseMessage{
			Code:    http.StatusOK,
			Message: "Nothing missing to enrich",
			Data:    gin.H{"started": false, "total": 0},
		})
		return
	}

	now := time.Now()
	bulkState = bulkEnrichStatus{
		Running:     true,
		Total:       len(items),
		Types:       req.Types,
		ContentType: req.Type,
		StartedAt:   &now,
	}
	bulkMu.Unlock()

	go runBulkEnrich(db, items, req.Types)

	c.JSON(http.StatusAccepted, utils.ResponseMessage{
		Code:    http.StatusAccepted,
		Message: "Bulk enrichment started",
		Data:    gin.H{"started": true, "total": len(items)},
	})
}

// runBulkEnrich processes the captured items sequentially in the background.
// Sequential is intentional — it self-throttles the downstream AI services
// (one model call at a time) instead of stampeding them.
func runBulkEnrich(db *gorm.DB, items []models.ContentItem, types []string) {
	for i := range items {
		_, errs := triggerItemArtifacts(db, &items[i], types, false)
		bulkMu.Lock()
		bulkState.Done++
		if len(errs) > 0 {
			bulkState.Failed++
			bulkState.LastError = errs[len(errs)-1]
		}
		bulkMu.Unlock()
	}
	bulkMu.Lock()
	bulkState.Running = false
	fin := time.Now()
	bulkState.FinishedAt = &fin
	bulkMu.Unlock()
}

func GetBulkEnrichStatus(c *gin.Context) {
	bulkMu.Lock()
	snapshot := bulkState
	bulkMu.Unlock()

	c.JSON(http.StatusOK, utils.ResponseMessage{
		Code:    http.StatusOK,
		Message: "Bulk enrichment status",
		Data:    snapshot,
	})
}

// bulkEnrichRunning reports whether a manual "Enrich all" run is in flight. The
// Enrichment Autopilot checks this as a precondition so it never double-loads the
// CPU-constrained model services against a human bulk run (plan §9 concurrency).
func bulkEnrichRunning() bool {
	bulkMu.Lock()
	defer bulkMu.Unlock()
	return bulkState.Running
}

func bulkLaneBusy() bool {
	return bulkEnrichRunning() || enrichmentAutopilotAnyRunInFlight()
}
