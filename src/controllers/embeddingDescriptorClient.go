package controllers

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"content-management-system/src/spaceid"
)

// Embedding & Model Lifecycle System (stage 10) — expected vector-space source
// of truth. CMS learns "what does the current text/image space hash to" from the
// services actually running inference (Enrichment /v1/models embedder item,
// Media /v1/models clip item), rather than a second config that could drift from
// the model doing the work.
//
// Cached with observed_at + TTL. The AUDIT may display a last-known identity
// during an outage, but any MUTATING decision (campaign batch, write fencing)
// must use a FRESH identity — callers check Resolved()+freshness for those.

// modelDescriptor is the subset of a /v1/models ModelInfoItem this system needs.
type modelDescriptor struct {
	Name           string `json:"name"`
	Type           string `json:"type"`
	Loaded         bool   `json:"loaded"`
	Dimensions     int    `json:"dimensions"`
	Revision       string `json:"revision"`
	Normalized     bool   `json:"normalized"`
	Pooling        string `json:"pooling"`
	SpaceID        string `json:"space_id"`
	ProducerRecipe string `json:"producer_recipe"`
	ProducerID     string `json:"producer_id"`
}

type modelsResponse struct {
	Models []modelDescriptor `json:"models"`
}

// expectedSpace is the cached, resolved identity of one vector space.
type expectedSpace struct {
	Space      string // "text" | "image"
	SpaceID    string // "" when unresolved (service not ready / no revision)
	Dimensions int
	Model      string
	Revision   string
	ObservedAt time.Time
	Err        string // last fetch error, "" on success
}

// Fresh reports whether the descriptor was observed within ttl and carries a
// resolved space_id. Mutating work requires Fresh(); audit tolerates staleness.
func (e expectedSpace) Fresh(ttl time.Duration) bool {
	return e.SpaceID != "" && time.Since(e.ObservedAt) <= ttl
}

// ProducerFor computes the expected producer_id for a surface recipe in this
// space. CMS derives it independently (space_id + recipe) so it matches what a
// writer stamps without a second round-trip. "" when the space is unresolved.
func (e expectedSpace) ProducerFor(recipe string) string {
	return spaceid.ProducerID(e.SpaceID, recipe)
}

const descriptorCacheTTL = 10 * time.Minute

var (
	descriptorMu    sync.RWMutex
	descriptorCache = map[string]expectedSpace{} // space -> cached
)

// currentExpectedSpace returns the cached identity for a space, fetching if the
// cache is empty or stale. Errors are cached (so audit can report
// expected_space_unavailable) rather than thrown away.
func currentExpectedSpace(space string) expectedSpace {
	descriptorMu.RLock()
	cached, ok := descriptorCache[space]
	descriptorMu.RUnlock()
	if ok && time.Since(cached.ObservedAt) <= descriptorCacheTTL {
		return cached
	}
	fetched := fetchExpectedSpace(space)
	descriptorMu.Lock()
	descriptorCache[space] = fetched
	descriptorMu.Unlock()
	return fetched
}

// refreshExpectedSpace forces a fresh fetch (used before mutating work).
func refreshExpectedSpace(space string) expectedSpace {
	fetched := fetchExpectedSpace(space)
	descriptorMu.Lock()
	descriptorCache[space] = fetched
	descriptorMu.Unlock()
	return fetched
}

// fetchExpectedSpace calls the owning service's /v1/models and extracts the
// vector-space item. text ⇒ Enrichment embedder; image ⇒ Media clip.
func fetchExpectedSpace(space string) expectedSpace {
	now := time.Now().UTC()
	var baseURL, token, itemType string
	switch space {
	case EmbeddingSpaceText:
		baseURL, token, itemType = enrichmentBaseURL(), enrichmentServiceToken(), "embedder"
	case EmbeddingSpaceImage:
		baseURL, token, itemType = mediaBaseURL(), mediaServiceToken(), "clip"
	default:
		return expectedSpace{Space: space, ObservedAt: now, Err: "unknown space " + space}
	}
	if baseURL == "" || token == "" {
		return expectedSpace{Space: space, ObservedAt: now, Err: "service base URL/token not configured"}
	}

	desc, err := fetchModelDescriptor(baseURL, token, itemType)
	if err != nil {
		return expectedSpace{Space: space, ObservedAt: now, Err: err.Error()}
	}
	return expectedSpace{
		Space:      space,
		SpaceID:    desc.SpaceID, // "" if the service reports lifecycle-not-ready
		Dimensions: desc.Dimensions,
		Model:      desc.Name,
		Revision:   desc.Revision,
		ObservedAt: now,
	}
}

// textSurfaceStamp returns the (model, space_id, producer_id) a CMS-internal
// text-space writer should stamp for a given surface recipe, or all-nil when the
// text space is not currently resolved (leave the row unstamped debt rather than
// stamp a false-stable identity). Used by the story/topic/proposal/discovery
// writers so their new vectors carry provenance from day one.
func textSurfaceStamp(recipe string) (model, space, producer *string) {
	es := currentExpectedSpace(EmbeddingSpaceText)
	if !es.Fresh(descriptorCacheTTL) {
		return nil, nil, nil
	}
	m, s := es.Model, es.SpaceID
	p := es.ProducerFor(recipe)
	if p == "" {
		return nil, nil, nil
	}
	return &m, &s, &p
}

// currentTextSpaceIDForSimilarity is the fail-closed comparability gate used by
// CMS-owned query/in-memory consumers. Empty means semantic work must be held.
func currentTextSpaceIDForSimilarity() string {
	es := currentExpectedSpace(EmbeddingSpaceText)
	if !es.Fresh(descriptorCacheTTL) {
		return ""
	}
	return es.SpaceID
}

func textStampForObservedSpace(recipe, observedSpaceID string) (model, producer string, ok bool) {
	es := currentExpectedSpace(EmbeddingSpaceText)
	if !es.Fresh(descriptorCacheTTL) || es.SpaceID != observedSpaceID {
		return "", "", false
	}
	producer = es.ProducerFor(recipe)
	return es.Model, producer, producer != ""
}

func fetchModelDescriptor(baseURL, token, itemType string) (*modelDescriptor, error) {
	req, err := http.NewRequest(http.MethodGet, baseURL+"/v1/models", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("models fetch: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("models status %d: %s", resp.StatusCode, string(body))
	}
	var parsed modelsResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("models decode: %w", err)
	}
	for i := range parsed.Models {
		if parsed.Models[i].Type == itemType {
			return &parsed.Models[i], nil
		}
	}
	return nil, fmt.Errorf("no %s item in /v1/models", itemType)
}
