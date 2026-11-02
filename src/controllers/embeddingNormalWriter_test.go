package controllers

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/spaceid"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pgvector/pgvector-go"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func normalWriterTestSpace(t *testing.T, space string) {
	t.Helper()
	descriptorMu.Lock()
	previous := descriptorCache
	descriptorCache = map[string]expectedSpace{EmbeddingSpaceText: {
		Space: EmbeddingSpaceText, SpaceID: space, Model: "model", Dimensions: 1024,
		ObservedAt: time.Now().UTC(),
	}}
	descriptorMu.Unlock()
	t.Cleanup(func() {
		descriptorMu.Lock()
		descriptorCache = previous
		descriptorMu.Unlock()
	})
}

func normalWriterTestInference(t *testing.T, observedSpace string) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/embed/query" {
			t.Errorf("unexpected inference path: %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"embedding": make([]float32, 1024), "space_id": observedSpace,
		})
	}))
	t.Cleanup(server.Close)
	t.Setenv("ENRICHMENT_BASE_URL", server.URL)
	t.Setenv("ENRICHMENT_SERVICE_TOKEN", "test-token")
}

func TestNormalEmbeddingWritersRejectInferenceSpaceMismatch(t *testing.T) {
	for _, observed := range []string{"", strings.Repeat("b", 64)} {
		t.Run(observed, func(t *testing.T) {
			space := strings.Repeat("a", 64)
			normalWriterTestSpace(t, space)
			normalWriterTestInference(t, observed)
			old := strings.Repeat("c", 64)
			oldProducer := spaceid.ProducerID(old, spaceid.RecipeDiscoveryProfile)
			original := pgvector.NewVector([]float32{1})
			profile := models.DiscoveryProfile{
				Name: "Profile", Embedding: &original,
				EmbeddingSpaceID: &old, EmbeddingProducerID: &oldProducer,
			}
			if vec, ok := ensureProfileEmbedding(nil, &profile); ok || vec != nil {
				t.Fatal("mismatched profile inference must not be returned or persisted")
			}
			if profile.Embedding != &original || *profile.EmbeddingSpaceID != old {
				t.Fatal("rejected inference changed the profile cache")
			}
			scorer := proposalScorer{spaceID: space, policy: models.PreferenceAutopilotPolicy{MaxEmbeddingCalls: 1}}
			if vec, ok := scorer.ensureEmbedding(&models.TopicProposal{SuggestedSlug: "topic"}, "موضوع", "Topic"); ok || vec != nil {
				t.Fatal("mismatched proposal inference must not be returned or persisted")
			}
		})
	}
}

func TestProposalCacheRequiresMatchingSpaceAndPinnedCatalog(t *testing.T) {
	space, other := strings.Repeat("a", 64), strings.Repeat("b", 64)
	normalWriterTestSpace(t, space)
	producer := spaceid.ProducerID(space, spaceid.RecipeTopicProposal)
	vec := pgvector.NewVector([]float32{1})
	proposal := models.TopicProposal{
		SuggestedSlug: "topic", Embedding: &vec,
		EmbeddingInputHash: hashProposalEmbeddingInput("topic", "موضوع", "Topic", ""),
		EmbeddingSpaceID:   &space, EmbeddingProducerID: &producer,
	}
	scorer := proposalScorer{spaceID: space}
	if _, ok := scorer.ensureEmbedding(&proposal, "موضوع", "Topic"); !ok {
		t.Fatal("matching cached proposal should remain usable")
	}
	proposal.EmbeddingSpaceID = &other
	if _, ok := scorer.ensureEmbedding(&proposal, "موضوع", "Topic"); ok {
		t.Fatal("producer alone cannot make a different-space cache usable")
	}
	proposal.EmbeddingSpaceID = &space
	scorer.spaceID = other
	if _, ok := scorer.ensureEmbedding(&proposal, "موضوع", "Topic"); ok {
		t.Fatal("a scorer cannot cross spaces after loading its topic catalog")
	}
}

func TestNormalProfileRefreshPersistsVectorAndMatchingStampsTogether(t *testing.T) {
	space := strings.Repeat("a", 64)
	normalWriterTestSpace(t, space)
	normalWriterTestInference(t, space)
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	// Optional workload evidence being unavailable must not skip native stamps.
	mock.ExpectBegin().WillReturnError(errors.New("optional workload store unavailable"))
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE "discovery_profiles" SET "embedding"=$1,"embedding_model"=$2,"embedding_producer_id"=$3,"embedding_space_id"=$4,"updated_at"=$5 WHERE tenant_id = $6 AND id = $7`)).
		WithArgs(sqlmock.AnyArg(), "model", spaceid.ProducerID(space, spaceid.RecipeDiscoveryProfile), space, sqlmock.AnyArg(), "tenant", 7).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	profile := models.DiscoveryProfile{ID: 7, TenantID: "tenant", Name: "Profile"}
	if _, ok := ensureProfileEmbedding(db, &profile); !ok {
		t.Fatal("matching inference and atomic write should succeed")
	}
	if profile.EmbeddingSpaceID == nil || *profile.EmbeddingSpaceID != space {
		t.Fatal("successful profile refresh lost its provenance")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestDiscoveryNoveltyRejectsUntypedOrDifferentSpaceVector(t *testing.T) {
	space := strings.Repeat("a", 64)
	normalWriterTestSpace(t, space)
	for _, observed := range []string{"", strings.Repeat("b", 64)} {
		if got := noveltyFactor(nil, "tenant", []float32{1}, 0.8, 0.2, observed); got != 1 {
			t.Fatalf("uncomparable novelty query must skip semantic scoring, got %v", got)
		}
	}
}
