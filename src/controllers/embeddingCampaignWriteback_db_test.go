package controllers

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"content-management-system/src/contentstage"
	"content-management-system/src/models"
	"content-management-system/src/spaceid"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// Real handler/transaction coverage: a CMS-issued lifecycle action remains an
// authorized write path after the ordinary pipeline requires durable receipts.
func TestEmbeddingCampaignWritebackUnderDurableRequired(t *testing.T) {
	for _, surfaceKey := range []string{"content_text", "content_image"} {
		t.Run(surfaceKey, func(t *testing.T) {
			db := embeddingLifecycleTestDB(t)
			if err := db.AutoMigrate(
				&models.ContentItem{}, &models.ContentStageRequest{}, &models.ContentStageAttempt{},
				&models.ContentStageReceipt{}, &models.ContentStageEvent{}, &models.ContentStageCutover{},
			); err != nil {
				t.Fatal(err)
			}
			contentstage.ResetSchemaAvailabilityCache()
			t.Cleanup(contentstage.ResetSchemaAvailabilityCache)
			surface, _ := EmbeddingSurfaceByKey(surfaceKey)
			modelType, model, pooling := "embedder", "Qwen/Qwen3-Embedding-0.6B", "sentence-transformers-config"
			lane, kind := models.ContentStageLaneNews, models.ContentTypeNews
			if surfaceKey == "content_image" {
				modelType, model, pooling = "clip", "clip-ViT-B-32", "clip-visual-proj"
				lane, kind = models.ContentStageLanePods, models.ContentTypeVideo
			}
			revision := strings.Repeat("a", 40)
			space := (spaceid.Basis{Model: model, Revision: revision, Dimensions: surface.Dim, Normalized: true, Pooling: pooling}).SpaceID()
			producer := spaceid.ProducerID(space, surface.Recipe)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/models" {
					t.Errorf("unexpected outbound request: %s", r.URL.Path)
					w.WriteHeader(404)
					return
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"models": []map[string]any{{
					"name": model, "type": modelType, "loaded": true, "revision": revision,
					"dimensions": surface.Dim, "normalized": true, "pooling": pooling,
					"space_id": space, "producer_id": producer, "producer_recipe": surface.Recipe,
				}}})
			}))
			t.Cleanup(server.Close)
			t.Setenv("ENRICHMENT_BASE_URL", server.URL)
			t.Setenv("MEDIA_BASE_URL", server.URL)
			t.Setenv("ENRICHMENT_SERVICE_TOKEN", "test-token")
			t.Setenv("MEDIA_SERVICE_TOKEN", "test-token")
			cutover := models.ContentStageCutover{TenantID: "tenant", Lane: lane, Mode: models.ContentStageCutoverDurableRequired}
			if err := db.Create(&cutover).Error; err != nil {
				t.Fatal(err)
			}
			if mode, err := contentstage.CutoverMode(db, "tenant", lane); err != nil || mode != models.ContentStageCutoverDurableRequired {
				t.Fatalf("fixture did not enable durable_required: %s %v", mode, err)
			}
			title, thumbnail := "Canonical input", "https://images.test/current.png"
			item := models.ContentItem{PublicID: uuid.New(), TenantID: "tenant", Type: kind, Source: models.SourceTypeManual, Title: &title, ThumbnailURL: &thumbnail}
			if err := db.Create(&item).Error; err != nil {
				t.Fatal(err)
			}
			campaign := models.EmbeddingCampaign{TenantID: embeddingLifecycleTenant, Space: surface.Space, State: models.EmbeddingCampaignRunning, TargetSpaceID: space}
			if err := db.Create(&campaign).Error; err != nil {
				t.Fatal(err)
			}
			fingerprint, err := campaignInputFingerprint(db, &item, surfaceKey)
			if err != nil {
				t.Fatal(err)
			}
			action := models.EmbeddingCampaignAction{
				CampaignID: campaign.ID, TenantID: item.TenantID, SurfaceKey: surfaceKey,
				TargetID: item.PublicID.String(), ExpectedProducerID: producer,
				Status: models.EmbeddingActionAttempted, RequestID: uuid.NewString(), Tool: toolForSurface(surface),
				Evidence: jsonEvidence(map[string]any{"input_fingerprint": fingerprint}),
			}
			if err := db.Create(&action).Error; err != nil {
				t.Fatal(err)
			}
			vector := make([]float32, surface.Dim)
			vector[0] = 1
			payload := map[string]any{"embedding": vector, "model": model, "space_id": space, "producer_id": producer}
			callback := func() *httptest.ResponseRecorder {
				body, err := json.Marshal(payload)
				if err != nil {
					t.Fatal(err)
				}
				response := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(response)
				c.Set("db", db)
				c.Params = gin.Params{{Key: "id", Value: item.PublicID.String()}}
				c.Request = httptest.NewRequest(http.MethodPatch, "/internal/content-items/"+item.PublicID.String()+"/embedding", bytes.NewReader(body))
				c.Request.Header.Set("Content-Type", "application/json")
				if surfaceKey == "content_text" {
					InternalUpdateContentEmbedding(c)
				} else {
					InternalUpdateContentImageEmbedding(c)
				}
				return response
			}
			if response := callback(); response.Code == http.StatusOK {
				t.Fatal("durable_required accepted an ordinary uncorrelated write")
			}
			payload["embedding_campaign"] = embeddingCampaignCorrelation{CampaignID: campaign.ID, ActionID: action.ID, RequestID: action.RequestID}
			if response := callback(); response.Code != http.StatusOK {
				t.Fatalf("valid campaign callback under durable_required: %d %s", response.Code, response.Body.String())
			}
			if err := ownerReadback(db, surface, item.PublicID.String(), space, producer); err != nil {
				t.Fatalf("callback did not persist current vector identity: %v", err)
			}
			inputColumn := "title"
			if surfaceKey == "content_image" {
				inputColumn = "thumbnail_url"
			}
			if err := db.Model(&item).Update(inputColumn, "changed-after-claim").Error; err != nil {
				t.Fatal(err)
			}
			vector[0] = 2
			if response := callback(); response.Code != http.StatusConflict {
				t.Fatalf("edited target accepted stale callback: %d %s", response.Code, response.Body.String())
			}
			var stored models.ContentItem
			if err := db.Where("public_id=?", item.PublicID).First(&stored).Error; err != nil {
				t.Fatal(err)
			}
			storedVector := stored.Embedding
			if surfaceKey == "content_image" {
				storedVector = stored.ImageEmbedding
			}
			if storedVector == nil || storedVector.Slice()[0] != 1 {
				t.Fatal("rejected stale callback changed the stored vector")
			}
		})
	}
}
