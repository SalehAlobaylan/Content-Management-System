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

// enrichmentBaseURL returns the configured Enrichment Service URL.
func enrichmentBaseURL() string {
	return strings.TrimSpace(os.Getenv("ENRICHMENT_BASE_URL"))
}

// enrichmentServiceToken returns the bearer token for Enrichment Service auth.
// Uses ENRICHMENT_SERVICE_TOKEN if set, falls back to CMS_SERVICE_TOKEN.
// The returned value must match the Enrichment Service's SERVICE_AUTH_TOKEN.
func enrichmentServiceToken() string {
	if token := strings.TrimSpace(os.Getenv("ENRICHMENT_SERVICE_TOKEN")); token != "" {
		return token
	}
	return strings.TrimSpace(os.Getenv("CMS_SERVICE_TOKEN"))
}

// enrichmentHealthResponse represents the Enrichment Service /ready response.
type enrichmentHealthResponse struct {
	Status       string                   `json:"status"`
	Models       map[string]bool          `json:"models"`
	Dependencies map[string]bool          `json:"dependencies"`
}

// checkEnrichmentHealth calls GET /ready on the Enrichment Service.
// Returns the health response and whether the service is reachable.
func checkEnrichmentHealth() (*enrichmentHealthResponse, error) {
	baseURL := enrichmentBaseURL()
	if baseURL == "" {
		return nil, fmt.Errorf("ENRICHMENT_BASE_URL is not configured")
	}

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Get(baseURL + "/ready")
	if err != nil {
		return nil, fmt.Errorf("enrichment service unreachable: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read enrichment health response: %w", err)
	}

	var health enrichmentHealthResponse
	if err := json.Unmarshal(body, &health); err != nil {
		return nil, fmt.Errorf("invalid enrichment health response: %w", err)
	}

	return &health, nil
}

// triggerTranscription sends a transcription request to the Enrichment Service.
// It uses multipart/form-data because the Enrichment /v1/transcribe endpoint expects Form fields.
// The Enrichment Service will write the transcript back to CMS via /internal endpoints.
func triggerTranscription(mediaURL string, contentID string) error {
	baseURL := enrichmentBaseURL()
	if baseURL == "" {
		return fmt.Errorf("ENRICHMENT_BASE_URL is not configured")
	}

	token := enrichmentServiceToken()
	if token == "" {
		return fmt.Errorf("CMS_SERVICE_TOKEN is not configured")
	}

	// Build multipart form body (Enrichment /v1/transcribe expects Form fields)
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
		return fmt.Errorf("enrichment transcription request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("enrichment returned status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// triggerEmbedding sends an embedding request to the Enrichment Service.
// The Enrichment Service will write the embedding back to CMS via /internal endpoints.
func triggerEmbedding(text string, contentID string) error {
	baseURL := enrichmentBaseURL()
	if baseURL == "" {
		return fmt.Errorf("ENRICHMENT_BASE_URL is not configured")
	}

	token := enrichmentServiceToken()
	if token == "" {
		return fmt.Errorf("CMS_SERVICE_TOKEN is not configured")
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
