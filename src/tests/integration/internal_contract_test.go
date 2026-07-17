package integration

import (
	"bytes"
	"content-management-system/src/models"
	"content-management-system/src/spaceid"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
)

const (
	integrationAggregationToken = "cms-integration-aggregation-token"
	integrationEnrichmentToken  = "cms-integration-enrichment-token"
	integrationMediaToken       = "cms-integration-media-token"
)

func internalRequest(t *testing.T, method, path string, body any, token string) *httptest.ResponseRecorder {
	t.Helper()
	var reader *bytes.Reader
	if body == nil {
		reader = bytes.NewReader(nil)
	} else {
		encoded, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(encoded)
	}
	req := httptest.NewRequest(method, path, reader)
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	response := httptest.NewRecorder()
	router.ServeHTTP(response, req)
	return response
}

func TestInternalServiceAuthAndReplaySafeWritebackContract(t *testing.T) {
	clearWahbTables()

	if response := internalRequest(t, http.MethodPost, "/internal/content-items", map[string]string{}, ""); response.Code != http.StatusUnauthorized {
		t.Fatalf("missing internal token: want 401, got %d", response.Code)
	}
	if response := internalRequest(t, http.MethodPost, "/internal/content-items", map[string]string{}, "wrong"); response.Code != http.StatusUnauthorized {
		t.Fatalf("wrong internal token: want 401, got %d", response.Code)
	}

	payload := map[string]any{
		"idempotency_key": "integration-replay-key",
		"type":            "VIDEO",
		"source":          "PODCAST",
		"status":          "READY",
		"title":           "Internal contract fixture",
		"original_url":    "https://example.test/internal-contract",
		"source_name":     "Integration fixture",
	}
	first := internalRequest(t, http.MethodPost, "/internal/content-items", payload, integrationAggregationToken)
	if first.Code != http.StatusOK {
		t.Fatalf("create internal content: want 200, got %d: %s", first.Code, first.Body.String())
	}
	var created struct {
		ID      string `json:"id"`
		Created bool   `json:"created"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if !created.Created || created.ID == "" {
		t.Fatalf("first create was not recorded as new: %#v", created)
	}

	replay := internalRequest(t, http.MethodPost, "/internal/content-items", payload, integrationAggregationToken)
	if replay.Code != http.StatusOK {
		t.Fatalf("replay internal content: want 200, got %d: %s", replay.Code, replay.Body.String())
	}
	var replayed struct {
		ID      string `json:"id"`
		Created bool   `json:"created"`
	}
	if err := json.Unmarshal(replay.Body.Bytes(), &replayed); err != nil {
		t.Fatal(err)
	}
	if replayed.Created || replayed.ID != created.ID {
		t.Fatalf("idempotent replay changed result: first=%#v replay=%#v", created, replayed)
	}

	imageEmbedding := make([]float32, 512)
	imageSpaceID := "image-space-v1"
	if denied := internalRequest(t, http.MethodPatch, "/internal/content-items/"+created.ID+"/image-embedding", map[string]any{}, integrationAggregationToken); denied.Code != http.StatusForbidden {
		t.Fatalf("Aggregation must not write image vectors: want 403, got %d", denied.Code)
	}
	writeback := internalRequest(t, http.MethodPatch, "/internal/content-items/"+created.ID+"/image-embedding", map[string]any{
		"embedding":   imageEmbedding,
		"model":       "clip-ViT-B-32",
		"space_id":    imageSpaceID,
		"producer_id": spaceid.ProducerID(imageSpaceID, spaceid.RecipeContentImage),
	}, integrationMediaToken)
	if writeback.Code != http.StatusOK {
		t.Fatalf("image provenance writeback: want 200, got %d: %s", writeback.Code, writeback.Body.String())
	}

	id, err := uuid.Parse(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	var stored models.ContentItem
	if err := testDB.Where("public_id = ?", id).First(&stored).Error; err != nil {
		t.Fatal(err)
	}
	if stored.ImageEmbedding == nil || stored.ImageEmbeddingSpaceID == nil || *stored.ImageEmbeddingSpaceID == "" || stored.ImageEmbeddingProducerID == nil || *stored.ImageEmbeddingProducerID == "" {
		t.Fatalf("image provenance was not persisted: %#v", stored)
	}

	if denied := internalRequest(t, http.MethodPost, "/internal/transcripts", map[string]any{}, integrationEnrichmentToken); denied.Code != http.StatusForbidden {
		t.Fatalf("Enrichment must not write transcripts: want 403, got %d", denied.Code)
	}
	transcript := internalRequest(t, http.MethodPost, "/internal/transcripts", map[string]any{
		"content_item_id": created.ID,
		"full_text":       "A durable transcript contract fixture.",
		"language":        "en",
		"source":          "stt_deepgram",
		"provider":        "deepgram:nova-3",
	}, integrationMediaToken)
	if transcript.Code != http.StatusOK {
		t.Fatalf("transcript writeback: want 200, got %d: %s", transcript.Code, transcript.Body.String())
	}
	if err := testDB.Where("public_id = ?", id).First(&stored).Error; err != nil {
		t.Fatal(err)
	}
	if stored.TranscriptID == nil || stored.TranscriptSource == nil || *stored.TranscriptSource != "stt_deepgram" {
		t.Fatalf("transcript writeback was not linked to content: %#v", stored)
	}
}

func TestAdminPermissionBoundaryContract(t *testing.T) {
	clearTables()
	for _, route := range router.Routes() {
		if !strings.HasPrefix(route.Path, "/admin/") || route.Path == "/admin/me" {
			continue
		}
		route := route
		t.Run(route.Method+" "+route.Path, func(t *testing.T) {
			token := generateTestJWT(t, "550e8400-e29b-41d4-a716-446655440001", "viewer@me.test", "user", nil)
			path := adminRouteFixturePath(route.Path)
			req := httptest.NewRequest(route.Method, path, nil)
			req.Header.Set("Authorization", "Bearer "+token)
			response := httptest.NewRecorder()
			router.ServeHTTP(response, req)
			if response.Code != http.StatusForbidden {
				t.Fatalf("permissionless principal reached %s %s: want 403, got %d: %s", route.Method, route.Path, response.Code, response.Body.String())
			}
		})
	}
}

func adminRouteFixturePath(path string) string {
	parts := strings.Split(path, "/")
	for index, part := range parts {
		if strings.HasPrefix(part, ":") {
			parts[index] = "550e8400-e29b-41d4-a716-446655440099"
		}
	}
	return strings.Join(parts, "/")
}

func TestAdminSourceTenantIsolationContract(t *testing.T) {
	clearTables()
	feedA, feedB := "https://tenant-a.example.test/feed", "https://tenant-b.example.test/feed"
	sourceA := models.ContentSource{TenantID: "tenant-a", Name: "Tenant A source", Type: models.SourceTypeRSS, FeedURL: &feedA}
	sourceB := models.ContentSource{TenantID: "tenant-b", Name: "Tenant B source", Type: models.SourceTypeRSS, FeedURL: &feedB}
	if err := testDB.Create(&sourceA).Error; err != nil {
		t.Fatal(err)
	}
	if err := testDB.Create(&sourceB).Error; err != nil {
		t.Fatal(err)
	}
	token := generateTenantTestJWT(t, "550e8400-e29b-41d4-a716-446655440002", "tenant-a@me.test", "tenant-a", "user", []string{"source:read"})
	req := httptest.NewRequest(http.MethodGet, "/admin/sources/"+sourceB.PublicID.String(), nil)
	req.Header.Set("Authorization", "Bearer "+token)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, req)
	if response.Code != http.StatusNotFound {
		t.Fatalf("cross-tenant source lookup: want 404, got %d: %s", response.Code, response.Body.String())
	}
}
