package controllers

import (
	"bytes"
	"content-management-system/src/models"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"strings"
	"time"

	"gorm.io/gorm"
)

// ─── Service URL + token resolution ─────────────────────────

// enrichmentBaseURL returns the configured Enrichment Service URL (text
// embeddings, LLM-backed ops, retrieval). Transcription + image embedding
// now live in Media-Service — see mediaBaseURL.
func enrichmentBaseURL() string {
	return strings.TrimSpace(os.Getenv("ENRICHMENT_BASE_URL"))
}

// enrichmentServiceToken returns the bearer token for Enrichment Service auth.
// Falls back to the shared SERVICE_AUTH_TOKEN and finally CMS_SERVICE_TOKEN
// to match start.sh's resolution order.
func enrichmentServiceToken() string {
	if token := strings.TrimSpace(os.Getenv("ENRICHMENT_SERVICE_TOKEN")); token != "" {
		return token
	}
	if token := strings.TrimSpace(os.Getenv("SERVICE_AUTH_TOKEN")); token != "" {
		return token
	}
	return strings.TrimSpace(os.Getenv("CMS_SERVICE_TOKEN"))
}

// mediaBaseURL returns the configured Media-Service URL (Whisper
// transcription + CLIP image embedding). Default mirrors start.sh.
func mediaBaseURL() string {
	if url := strings.TrimSpace(os.Getenv("MEDIA_BASE_URL")); url != "" {
		return url
	}
	return "http://localhost:5051"
}

// mediaServiceToken returns the bearer token for Media-Service auth. Same
// fallback chain as Enrichment because they share one SERVICE_AUTH_TOKEN
// in the canonical dev setup.
func mediaServiceToken() string {
	if token := strings.TrimSpace(os.Getenv("MEDIA_SERVICE_TOKEN")); token != "" {
		return token
	}
	if token := strings.TrimSpace(os.Getenv("SERVICE_AUTH_TOKEN")); token != "" {
		return token
	}
	return strings.TrimSpace(os.Getenv("CMS_SERVICE_TOKEN"))
}

// ─── Health responses ───────────────────────────────────────

// serviceHealthResponse is the shape both Media-Service and
// Enrichment-Service return from /ready. Same struct works for both.
type serviceHealthResponse struct {
	Status       string          `json:"status"`
	Models       map[string]bool `json:"models"`
	Dependencies map[string]bool `json:"dependencies"`
}

// Kept as type alias so old callers compile unchanged.
type enrichmentHealthResponse = serviceHealthResponse

// checkServiceHealth calls GET /ready against an arbitrary AI service.
func checkServiceHealth(baseURL, serviceName string) (*serviceHealthResponse, error) {
	if baseURL == "" {
		return nil, fmt.Errorf("%s base URL is not configured", serviceName)
	}

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Get(baseURL + "/ready")
	if err != nil {
		return nil, fmt.Errorf("%s unreachable: %w", serviceName, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read %s health response: %w", serviceName, err)
	}

	var health serviceHealthResponse
	if err := json.Unmarshal(body, &health); err != nil {
		return nil, fmt.Errorf("invalid %s health response: %w", serviceName, err)
	}

	return &health, nil
}

// checkEnrichmentHealth calls GET /ready on the Enrichment Service.
func checkEnrichmentHealth() (*enrichmentHealthResponse, error) {
	return checkServiceHealth(enrichmentBaseURL(), "enrichment")
}

// checkMediaHealth calls GET /ready on the Media Service.
func checkMediaHealth() (*serviceHealthResponse, error) {
	return checkServiceHealth(mediaBaseURL(), "media")
}

// ─── Trigger helpers (called by admin retry / batch endpoints) ──

// triggerTranscription sends a transcription request to the Media-Service for
// the given item. It first runs the our-side STT guard (toggle + state machine +
// budget cap) — when the guard declines, it returns a *sttSkippedError that
// callers surface as "skipped" rather than a failure. On success it tracks the
// estimated spend. Media writes the transcript back to CMS via /internal
// endpoints (which set caption_state=stt_done + transcript_source).
//
// `force` bypasses the toggle + state-machine checks (manual admin upgrade) but
// still honors the budget cap.
func triggerTranscription(item *models.ContentItem, db *gorm.DB, force bool) error {
	if item.MediaURL == nil || *item.MediaURL == "" {
		return fmt.Errorf("no media_url available")
	}

	// Guard: may return *sttSkippedError (idempotent / disabled / over budget).
	if err := sttGuard(db, item, force); err != nil {
		return err
	}

	baseURL := mediaBaseURL()
	if baseURL == "" {
		return fmt.Errorf("MEDIA_BASE_URL is not configured")
	}

	token := mediaServiceToken()
	if token == "" {
		return fmt.Errorf("media service token is not configured")
	}

	// Build multipart form body (Media /v1/transcribe expects Form fields)
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	writer.WriteField("url", *item.MediaURL)
	writer.WriteField("content_id", item.PublicID.String())
	writer.WriteField("word_timestamps", "true")
	writer.Close()

	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/transcribe", &buf)
	if err != nil {
		return fmt.Errorf("failed to create transcription request: %w", err)
	}

	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+token)

	// Transcription can be slow — use a 120s timeout
	client := &http.Client{Timeout: 120 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("media transcription request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("media returned status %d: %s", resp.StatusCode, string(body))
	}

	// Track estimated spend against the monthly budget window.
	addTranscriptionSpend(db, item.TenantID, estimateSTTCostUSD(item.DurationSec))
	return nil
}

// triggerImageEmbedding sends a CLIP image-embedding request to Media-Service
// for the item's thumbnail/hero image. Same multipart shape as
// triggerTranscription; Media writes the 512-dim vector back to CMS.
func triggerImageEmbedding(imageURL string, contentID string) error {
	baseURL := mediaBaseURL()
	if baseURL == "" {
		return fmt.Errorf("MEDIA_BASE_URL is not configured")
	}

	token := mediaServiceToken()
	if token == "" {
		return fmt.Errorf("media service token is not configured")
	}

	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	writer.WriteField("url", imageURL)
	writer.WriteField("content_id", contentID)
	writer.Close()

	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/embed/image", &buf)
	if err != nil {
		return fmt.Errorf("failed to create image-embed request: %w", err)
	}

	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+token)

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("media image-embed request failed: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("media returned status %d: %s", resp.StatusCode, string(body))
	}

	return checkWriteBack(body)
}

// ─── Slice B: News-feed slide assembly ──────────────────────
//
// fetchNewsSlideViaEnrichment delegates News-feed related-item assembly to
// Enrichment-Service's /v1/feed/news/slide. The endpoint runs hybrid
// retrieval + reranker + ranking rules (freshness/diversity/quotas) and
// returns the final list ready for the feed. Callers (feedController) keep
// the legacy date-ordered query as a fallback when Enrichment is
// unreachable.

type enrichmentRelatedItem struct {
	ContentID   string   `json:"content_id"`
	Score       float64  `json:"score"`
	ContentType string   `json:"content_type"`
	Sources     []string `json:"sources"`
	RerankScore *float64 `json:"rerank_score"`
	PublishedAt *string  `json:"published_at"`
	SourceName  *string  `json:"source_name"`
}

type enrichmentNewsSlideResponse struct {
	Anchor  map[string]interface{}  `json:"anchor"`
	Related []enrichmentRelatedItem `json:"related"`
}

// fetchNewsSlideViaEnrichment calls POST /v1/feed/news/slide and returns
// the related items in display order. Returns (nil, err) on any failure;
// the caller (feedController.fetchRelatedItems) falls back to its
// date-ordered query when this fails — preserves availability on Enrichment
// outage.
func fetchNewsSlideViaEnrichment(anchorID string, limit int) ([]enrichmentRelatedItem, error) {
	baseURL := enrichmentBaseURL()
	if baseURL == "" {
		return nil, fmt.Errorf("ENRICHMENT_BASE_URL is not configured")
	}
	token := enrichmentServiceToken()
	if token == "" {
		return nil, fmt.Errorf("enrichment service token is not configured")
	}

	payload := map[string]interface{}{
		"anchor_content_id": anchorID,
		"k":                 limit,
		// Relate to ARTICLE as well as TWEET/COMMENT. The corpus today is
		// articles + videos (no tweets/comments yet), so restricting to
		// TWEET/COMMENT left the related slot empty and fell back to
		// date-ordered. Including ARTICLE makes "related" = topically-similar
		// news now, and tweets/comments fold in automatically once ingested.
		// The anchor itself is excluded by Enrichment's slide service.
		"types": []string{"ARTICLE", "TWEET", "COMMENT"},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal news-slide request: %w", err)
	}

	req, err := http.NewRequest(
		http.MethodPost,
		baseURL+"/v1/feed/news/slide",
		bytes.NewReader(body),
	)
	if err != nil {
		return nil, fmt.Errorf("build news-slide request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	// /v1/feed/news/slide does kNN + rerank + rules. Warm (slide-cache hit) it's
	// ~10ms, but a COLD cross-encoder rerank of the candidate pool is ~5s on CPU
	// — and several cold anchors can queue behind the single reranker process.
	// A 5s timeout made cold slides time out and silently fall back to
	// date-ordered "related" (the same recent items on every slide). 30s gives
	// the cold path comfortable headroom; the cache keeps the warm path fast.
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("enrichment news-slide request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf(
			"enrichment news-slide returned status %d: %s",
			resp.StatusCode, string(respBody),
		)
	}

	var decoded enrichmentNewsSlideResponse
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return nil, fmt.Errorf("decode news-slide response: %w", err)
	}
	return decoded.Related, nil
}

// ─── On-demand URL extraction (News "Add by URL") ───────────
//
// extractURLViaEnrichment delegates single-article extraction to
// Enrichment-Service's stealth web extraction endpoint (POST /v1/extract).
// Used by the admin News "Add by URL" tab to prefill the compose form — it
// only reads the page and returns the parsed fields; nothing is written.

type extractURLResult struct {
	Title       *string `json:"title"`
	Text        string  `json:"text"`
	Excerpt     *string `json:"excerpt"`
	Author      *string `json:"author"`
	PublishedAt *string `json:"published_at"`
	SiteName    *string `json:"site_name"`
	ImageURL    *string `json:"image_url"`
	WordCount   int     `json:"word_count"`
}

func extractURLViaEnrichment(url string) (*extractURLResult, error) {
	baseURL := enrichmentBaseURL()
	if baseURL == "" {
		return nil, fmt.Errorf("ENRICHMENT_BASE_URL is not configured")
	}

	token := enrichmentServiceToken()
	if token == "" {
		return nil, fmt.Errorf("enrichment service token is not configured")
	}

	payload := map[string]interface{}{"url": url}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal extract request: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/extract", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build extract request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	// Stealth fetch of an arbitrary page can be slow — allow 20s.
	client := &http.Client{Timeout: 20 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("enrichment extract request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("enrichment extract returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var decoded extractURLResult
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return nil, fmt.Errorf("decode extract response: %w", err)
	}
	return &decoded, nil
}

// ─── Whole-feed extraction (RSS/Atom import) ────────────────

type feedExtractItem struct {
	Title       string `json:"title"`
	Text        string `json:"text"`
	Excerpt     string `json:"excerpt"`
	URL         string `json:"url"`
	ImageURL    string `json:"image_url"`
	PublishedAt string `json:"published_at"`
	Author      string `json:"author"`
}

type feedExtractResult struct {
	IsFeed   bool              `json:"is_feed"`
	SiteName string            `json:"site_name"`
	Items    []feedExtractItem `json:"items"`
}

// extractFeedViaEnrichment asks Enrichment to extract EVERY item from an
// RSS/Atom feed (stealth fetch via Scrapling). Used by the News feed-import.
func extractFeedViaEnrichment(url string) (*feedExtractResult, error) {
	baseURL := enrichmentBaseURL()
	if baseURL == "" {
		return nil, fmt.Errorf("ENRICHMENT_BASE_URL is not configured")
	}
	token := enrichmentServiceToken()
	if token == "" {
		return nil, fmt.Errorf("enrichment service token is not configured")
	}

	payload := map[string]interface{}{"url": url}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal feed-extract request: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/extract/feed", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build feed-extract request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	// A feed can have many items behind a stealth fetch — allow 60s.
	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("enrichment feed-extract request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("enrichment feed-extract returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var decoded feedExtractResult
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return nil, fmt.Errorf("decode feed-extract response: %w", err)
	}
	return &decoded, nil
}

// ─── Topic labeling (first-class topics) ────────────────────
//
// generateTopicLabelViaEnrichment asks Enrichment's LLM to write ONE concise,
// meaningful topic title (in the content's language) for a cluster of article
// snippets. Used by the classifier when no existing topic is close enough and a
// new topic must be created.

type topicLabelResult struct {
	Label string `json:"label"`
}

func generateTopicLabelViaEnrichment(texts []string) (string, error) {
	baseURL := enrichmentBaseURL()
	if baseURL == "" {
		return "", fmt.Errorf("ENRICHMENT_BASE_URL is not configured")
	}
	token := enrichmentServiceToken()
	if token == "" {
		return "", fmt.Errorf("enrichment service token is not configured")
	}

	payload := map[string]interface{}{"texts": texts}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal topic-label request: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/topics/label", bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("build topic-label request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("enrichment topic-label request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("enrichment topic-label returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var decoded topicLabelResult
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return "", fmt.Errorf("decode topic-label response: %w", err)
	}
	return decoded.Label, nil
}

// ─── Media Studio: LLM chapter generation ───────────────────
//
// generateChaptersViaEnrichment delegates chapter segmentation to Enrichment's
// /v1/chapters/generate. CMS sends numbered transcript WINDOWS; the LLM returns
// boundary window indices (+ title/summary). Stateless — CMS maps indices back
// to timestamps and persists. Mirrors generateTopicLabelViaEnrichment.

type chapterWindowPayload struct {
	Index    int     `json:"index"`
	StartSec float64 `json:"start_sec"`
	Text     string  `json:"text"`
}

type chaptersGenOpts struct {
	Mode              string `json:"mode"`
	TargetCount       *int   `json:"target_count,omitempty"`
	TargetDurationSec *int   `json:"target_duration_sec,omitempty"`
	MinSec            *int   `json:"min_sec,omitempty"`
	MaxSec            *int   `json:"max_sec,omitempty"`
	WithSummary       bool   `json:"with_summary"`
	Language          string `json:"language,omitempty"`
}

type generatedChapter struct {
	StartIndex int     `json:"start_index"`
	Title      string  `json:"title"`
	Summary    *string `json:"summary"`
}

type chaptersGenerateResult struct {
	Chapters []generatedChapter `json:"chapters"`
}

func generateChaptersViaEnrichment(
	windows []chapterWindowPayload,
	opts chaptersGenOpts,
) ([]generatedChapter, error) {
	baseURL := enrichmentBaseURL()
	if baseURL == "" {
		return nil, fmt.Errorf("ENRICHMENT_BASE_URL is not configured")
	}
	token := enrichmentServiceToken()
	if token == "" {
		return nil, fmt.Errorf("enrichment service token is not configured")
	}

	payload := map[string]interface{}{
		"windows":             windows,
		"mode":                opts.Mode,
		"with_summary":        opts.WithSummary,
		"target_count":        opts.TargetCount,
		"target_duration_sec": opts.TargetDurationSec,
		"min_sec":             opts.MinSec,
		"max_sec":             opts.MaxSec,
	}
	if opts.Language != "" {
		payload["language"] = opts.Language
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal chapters request: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/chapters/generate", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build chapters request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	// LLM segmentation over a long transcript — allow generous time.
	client := &http.Client{Timeout: 90 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("enrichment chapters request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("enrichment chapters returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var decoded chaptersGenerateResult
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return nil, fmt.Errorf("decode chapters response: %w", err)
	}
	return decoded.Chapters, nil
}

// triggerEmbedding sends an embedding request to the Enrichment-Service.
// Enrichment writes the embedding back to CMS via /internal endpoints.
func triggerEmbedding(text string, contentID string, extractSparse bool) error {
	baseURL := enrichmentBaseURL()
	if baseURL == "" {
		return fmt.Errorf("ENRICHMENT_BASE_URL is not configured")
	}

	token := enrichmentServiceToken()
	if token == "" {
		return fmt.Errorf("enrichment service token is not configured")
	}

	// extract_sparse populates BGE-M3 lexical weights (hybrid retrieval) in the
	// same forward pass — free. extract_tags is deliberately OFF for admin
	// triggers: it hits the rate-limited LLM and would stall bulk re-embeds;
	// topic tags are populated by the normal ingest path instead.
	payload := map[string]interface{}{
		"texts":          []string{text},
		"content_ids":    []string{contentID},
		"extract_sparse": extractSparse,
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal embedding request: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/embed", bytes.NewReader(payloadBytes))
	if err != nil {
		return fmt.Errorf("failed to create embedding request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("enrichment embedding request failed: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("enrichment returned status %d: %s", resp.StatusCode, string(body))
	}

	// A 200 only means the vector was *computed*. Enrichment writes it back to
	// CMS separately and reports that outcome inline — surface a write-back
	// failure as an error so the trigger paths (single/batch/bulk) don't report
	// false success when nothing was persisted (e.g. a dimension mismatch).
	return checkWriteBack(body)
}

// checkWriteBack returns an error when an AI service computed a result but its
// write-back to CMS failed. Both /v1/embed and /v1/embed/image carry the same
// write_back_status / write_back_error fields.
func checkWriteBack(body []byte) error {
	var r struct {
		WriteBackStatus string `json:"write_back_status"`
		WriteBackError  string `json:"write_back_error"`
	}
	if err := json.Unmarshal(body, &r); err == nil && r.WriteBackStatus == "failed" {
		msg := r.WriteBackError
		if msg == "" {
			msg = "write-back to CMS failed"
		}
		return fmt.Errorf("write-back failed: %s", msg)
	}
	return nil
}
