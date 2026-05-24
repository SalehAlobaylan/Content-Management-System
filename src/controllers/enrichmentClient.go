package controllers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"strings"
	"time"
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

// triggerTranscription sends a transcription request to the Media-Service.
// It uses multipart/form-data because Media's /v1/transcribe endpoint expects
// Form fields. Media writes the transcript back to CMS via /internal endpoints.
func triggerTranscription(mediaURL string, contentID string) error {
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
	writer.WriteField("url", mediaURL)
	writer.WriteField("content_id", contentID)
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

	return nil
}

// triggerEmbedding sends an embedding request to the Enrichment-Service.
// Enrichment writes the embedding back to CMS via /internal endpoints.
func triggerEmbedding(text string, contentID string) error {
	baseURL := enrichmentBaseURL()
	if baseURL == "" {
		return fmt.Errorf("ENRICHMENT_BASE_URL is not configured")
	}

	token := enrichmentServiceToken()
	if token == "" {
		return fmt.Errorf("enrichment service token is not configured")
	}

	payload := map[string]interface{}{
		"texts":       []string{text},
		"content_ids": []string{contentID},
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

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("enrichment returned status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}
