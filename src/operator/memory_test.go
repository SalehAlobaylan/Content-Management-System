package operator

import (
	"content-management-system/src/models"
	"content-management-system/src/tests/testdb"
	"context"
	"encoding/json"
	"github.com/pgvector/pgvector-go"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestMemoryPermissionAndVectorGuards(t *testing.T) {
	access := AccessSnapshot{UserID: "a", TenantID: "t", Active: true, AccessVersion: "v", Permissions: []string{"feed:read"}}
	if !permissionsAllowed(access, []string{"feed:read"}) || permissionsAllowed(access, []string{"iam:read"}) {
		t.Fatal("memory permissions must remain CMS-authorized")
	}
	if got := cosine([]float32{1, 0}, pgvector.NewVector([]float32{1, 0})); got != 1 {
		t.Fatalf("cosine=%v", got)
	}
	if got := cosine([]float32{1}, pgvector.NewVector([]float32{1, 0})); got != -1 {
		t.Fatalf("dimension mismatch=%v", got)
	}
}

type fakeMemoryEmbedder struct{}

func (fakeMemoryEmbedder) EmbedQuery(context.Context, string) ([]float32, string, error) {
	return append([]float32{1}, make([]float32, 1023)...), "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", nil
}
func (fakeMemoryEmbedder) Rerank(context.Context, string, []string) ([]float64, error) {
	return []float64{0.9}, nil
}
func TestMemoryRetrievalHonorsReviewPermissionsAndValidity(t *testing.T) {
	db := testdb.Open(t)
	if err := db.AutoMigrate(&models.OperatorKnowledgeDocument{}, &models.OperatorKnowledgeChunk{}, &models.OperatorResolvedCase{}, &models.OperatorResolvedCaseChunk{}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	store := NewMemoryStore(db)
	store.now = func() time.Time { return now }
	space := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	doc := models.OperatorKnowledgeDocument{TenantID: "tenant-a", Key: "runbook", Version: 1, Title: "Reviewed runbook", Status: "reviewed", RequiredPermissions: []string{"feed:read"}, SpaceID: space, SourceVersion: "v1", ValidFrom: now.Add(-time.Hour), ReviewedBy: "admin", ReviewedAt: now}
	chunk := models.OperatorKnowledgeChunk{Ordinal: 0, Content: "historical runbook", Embedding: pgvector.NewVector(append([]float32{1}, make([]float32, 1023)...))}
	if err := store.CreateReviewedKnowledge(context.Background(), doc, []models.OperatorKnowledgeChunk{chunk}); err != nil {
		t.Fatal(err)
	}
	access := AccessSnapshot{UserID: "a", TenantID: "tenant-a", Active: true, AccessVersion: "v", Permissions: []string{"feed:read"}}
	hits, _, err := store.Retrieve(context.Background(), access, "why", fakeMemoryEmbedder{})
	if err != nil || len(hits) != 1 || hits[0].Kind != "knowledge" {
		t.Fatalf("hits=%#v err=%v", hits, err)
	}
	access.Permissions = []string{"iam:read"}
	hits, _, err = store.Retrieve(context.Background(), access, "why", fakeMemoryEmbedder{})
	if err != nil || len(hits) != 0 {
		t.Fatalf("unauthorized hits=%#v err=%v", hits, err)
	}
}

func TestHTTPMemoryEmbedderUsesOnlyServiceBearerAndBoundedSchemas(t *testing.T) {
	space := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer service-token" {
			t.Fatalf("missing service bearer")
		}
		if r.URL.Path == "/v1/embed/query" {
			_ = json.NewEncoder(w).Encode(map[string]any{"embedding": make([]float32, 1024), "dimensions": 1024, "space_id": space})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"scores": []float64{0.8}})
	}))
	defer server.Close()
	client, err := NewHTTPMemoryEmbedder(server.URL, "service-token", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, gotSpace, err := client.EmbedQuery(context.Background(), "why"); err != nil || gotSpace != space {
		t.Fatalf("embed=%v space=%q", err, gotSpace)
	}
	if scores, err := client.Rerank(context.Background(), "why", []string{"case"}); err != nil || scores[0] != 0.8 {
		t.Fatalf("rerank=%v scores=%v", err, scores)
	}
}

func TestRetrievedMemoryIsExplicitlyHistoricalEvidence(t *testing.T) {
	now := time.Now().UTC()
	packet := DecisionPacket{TenantID: "tenant-a"}
	AttachRetrievedMemory(&packet, []MemoryHit{{Kind: "knowledge", RecordID: "k1", Title: "Runbook", Content: "historical text", SourceVersion: "v1"}}, now)
	if packet.Evidence[0].Authority != EvidenceRetrieved || packet.Facts[0].Value.(map[string]any)["historical"] != true {
		t.Fatalf("retrieval was not labeled historical: %#v", packet)
	}
}
