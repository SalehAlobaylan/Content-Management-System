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

// mediaBaseURL returns the configured Media-Service URL (hosted STT +
// CLIP image embedding). Default mirrors start.sh.
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
	trigger := models.TranscriptionTriggerIngestAuto
	if force {
		trigger = models.TranscriptionTriggerManual
	}
	job, triggered, reason, err := createTranscriptionJobForItem(db, item, trigger, force)
	if err != nil {
		return err
	}
	if !triggered {
		return &sttSkippedError{reason: reason}
	}
	return submitTranscriptionJobToMedia(db, item, job.PublicID.String())
}

func triggerTranscriptionForJob(item *models.ContentItem, transcriptionJobID string) (string, error) {
	if item.MediaURL == nil || *item.MediaURL == "" {
		return "", fmt.Errorf("no media_url available")
	}

	baseURL := mediaBaseURL()
	if baseURL == "" {
		return "", fmt.Errorf("MEDIA_BASE_URL is not configured")
	}

	token := mediaServiceToken()
	if token == "" {
		return "", fmt.Errorf("media service token is not configured")
	}

	// Build multipart form body. CMS-originated jobs use Media's async route so
	// long videos/podcasts are not tied to a CMS goroutine HTTP timeout.
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	writer.WriteField("url", *item.MediaURL)
	writer.WriteField("content_id", item.PublicID.String())
	writer.WriteField("transcription_job_id", transcriptionJobID)
	writer.WriteField("word_timestamps", "true")
	writer.Close()

	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/transcribe/jobs", &buf)
	if err != nil {
		return "", fmt.Errorf("failed to create transcription request: %w", err)
	}

	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+token)

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("media transcription request failed: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("media returned status %d: %s", resp.StatusCode, string(body))
	}
	var decoded struct {
		JobID string `json:"job_id"`
	}
	_ = json.Unmarshal(body, &decoded)
	return decoded.JobID, nil
}

func cancelMediaTranscriptionJob(mediaJobID string) error {
	mediaJobID = strings.TrimSpace(mediaJobID)
	if mediaJobID == "" {
		return nil
	}
	baseURL := mediaBaseURL()
	if baseURL == "" {
		return fmt.Errorf("MEDIA_BASE_URL is not configured")
	}
	token := mediaServiceToken()
	if token == "" {
		return fmt.Errorf("media service token is not configured")
	}
	req, err := http.NewRequest(http.MethodDelete, baseURL+"/v1/transcribe/jobs/"+mediaJobID, nil)
	if err != nil {
		return fmt.Errorf("failed to create media cancel request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("media cancel request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("media cancel returned status %d: %s", resp.StatusCode, string(body))
	}
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

type storyDigestResult struct {
	Summary  string   `json:"summary"`
	Bullets  []string `json:"bullets"`
	Category string   `json:"category"`
}

// generateStorySummaryViaEnrichment digests a story's member texts into a
// source-grounded lede + bullets + one category slug via Enrichment's
// /v1/topics/digest. Mirrors generateTopicLabelViaEnrichment. Caller treats
// failure as best-effort.
func generateStorySummaryViaEnrichment(texts []string) (string, []string, string, error) {
	baseURL := enrichmentBaseURL()
	if baseURL == "" {
		return "", nil, "", fmt.Errorf("ENRICHMENT_BASE_URL is not configured")
	}
	token := enrichmentServiceToken()
	if token == "" {
		return "", nil, "", fmt.Errorf("enrichment service token is not configured")
	}

	payload := map[string]interface{}{"texts": texts, "max_bullets": 3}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", nil, "", fmt.Errorf("marshal story-digest request: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/topics/digest", bytes.NewReader(body))
	if err != nil {
		return "", nil, "", fmt.Errorf("build story-digest request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", nil, "", fmt.Errorf("enrichment story-digest request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return "", nil, "", fmt.Errorf("enrichment story-digest returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var decoded storyDigestResult
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return "", nil, "", fmt.Errorf("decode story-digest response: %w", err)
	}
	return decoded.Summary, decoded.Bullets, decoded.Category, nil
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

// embedQueryViaEnrichment embeds a single text synchronously and returns the
// 1024-dim L2-normalized dense vector (Qwen). Unlike triggerEmbedding it does
// NOT persist anything — used for on-the-fly relevance scoring of candidates.
func embedQueryViaEnrichment(text string) ([]float32, error) {
	baseURL := enrichmentBaseURL()
	if baseURL == "" {
		return nil, fmt.Errorf("ENRICHMENT_BASE_URL is not configured")
	}
	token := enrichmentServiceToken()
	if token == "" {
		return nil, fmt.Errorf("enrichment service token is not configured")
	}

	payload, err := json.Marshal(map[string]interface{}{"text": text})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/embed/query", bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("enrichment embed/query status %d: %s", resp.StatusCode, string(body))
	}
	var r struct {
		Embedding []float32 `json:"embedding"`
	}
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, err
	}
	if len(r.Embedding) == 0 {
		return nil, fmt.Errorf("enrichment returned empty embedding")
	}
	return r.Embedding, nil
}

// embedBatchViaEnrichment embeds multiple texts in one call and returns their
// vectors (no persistence — content_ids omitted). Used to score a candidate's
// sample items individually for sharper topic discrimination.
func embedBatchViaEnrichment(texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return nil, fmt.Errorf("no texts")
	}
	baseURL := enrichmentBaseURL()
	token := enrichmentServiceToken()
	if baseURL == "" || token == "" {
		return nil, fmt.Errorf("enrichment not configured")
	}
	payload, err := json.Marshal(map[string]interface{}{"texts": texts})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/embed", bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	client := &http.Client{Timeout: 20 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("enrichment embed status %d: %s", resp.StatusCode, string(body))
	}
	var r struct {
		Embeddings [][]float32 `json:"embeddings"`
	}
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, err
	}
	if len(r.Embeddings) == 0 {
		return nil, fmt.Errorf("empty embeddings")
	}
	return r.Embeddings, nil
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
