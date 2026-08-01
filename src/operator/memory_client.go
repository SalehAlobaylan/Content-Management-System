package operator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type HTTPMemoryEmbedder struct {
	baseURL, token string
	client         *http.Client
}

func NewHTTPMemoryEmbedder(baseURL, token string, client *http.Client) (*HTTPMemoryEmbedder, error) {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if !strings.HasPrefix(baseURL, "http://") && !strings.HasPrefix(baseURL, "https://") || strings.TrimSpace(token) == "" {
		return nil, fmt.Errorf("%w: memory embedding capability unavailable", ErrInvalidContract)
	}
	if client == nil {
		client = &http.Client{Timeout: 20 * time.Second}
	}
	return &HTTPMemoryEmbedder{baseURL, token, client}, nil
}
func (h *HTTPMemoryEmbedder) post(ctx context.Context, path string, input, output any) error {
	body, err := json.Marshal(input)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+h.token)
	req.Header.Set("Content-Type", "application/json")
	res, err := h.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode > 299 {
		return fmt.Errorf("enrichment memory endpoint status %d", res.StatusCode)
	}
	return json.NewDecoder(http.MaxBytesReader(nil, res.Body, 256<<10)).Decode(output)
}
func (h *HTTPMemoryEmbedder) EmbedQuery(ctx context.Context, text string) ([]float32, string, error) {
	var out struct {
		Embedding  []float32 `json:"embedding"`
		SpaceID    string    `json:"space_id"`
		Dimensions int       `json:"dimensions"`
	}
	err := h.post(ctx, "/v1/embed/query", map[string]string{"text": text}, &out)
	if err != nil || len(out.Embedding) != 1024 || out.Dimensions != 1024 || strings.TrimSpace(out.SpaceID) == "" {
		return nil, "", fmt.Errorf("%w: invalid query embedding", ErrInvalidContract)
	}
	return out.Embedding, out.SpaceID, nil
}
func (h *HTTPMemoryEmbedder) Rerank(ctx context.Context, query string, candidates []string) ([]float64, error) {
	var out struct {
		Scores []float64 `json:"scores"`
	}
	if len(candidates) == 0 || len(candidates) > 8 {
		return nil, fmt.Errorf("%w: invalid rerank batch", ErrInvalidContract)
	}
	if err := h.post(ctx, "/v1/rerank", map[string]any{"query": query, "candidates": candidates}, &out); err != nil || len(out.Scores) != len(candidates) {
		return nil, fmt.Errorf("%w: invalid rerank response", ErrInvalidContract)
	}
	return out.Scores, nil
}
