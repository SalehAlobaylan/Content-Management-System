package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/spaceid"
	"encoding/json"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Real PostgreSQL exercises the complete campaign state loop with a deterministic
// owner HTTP double. It never loads models or connects to an application database.
func TestEmbeddingCampaignRuntimePauseOutageAndCompletion(t *testing.T) {
	db, c := embeddingSafetyFixture(t)
	ddl := []string{
		`CREATE TABLE content_items(id bigserial,public_id uuid PRIMARY KEY,tenant_id text,type text,status text,title text,excerpt text,body_text text,transcript_id uuid,story_id uuid,news_retention_state text,created_at timestamptz DEFAULT now(),updated_at timestamptz DEFAULT now(),embedding vector(1024),embedding_space_id text,embedding_producer_id text)`,
		`CREATE TABLE stories(id bigserial,public_id uuid,tenant_id text,embedding vector(1024),embedding_space_id text,embedding_producer_id text,last_member_at timestamptz,related_ids jsonb)`,
		`CREATE TABLE topics(id bigserial,public_id uuid,tenant_id text,centroid vector(1024),centroid_space_id text,centroid_producer_id text,active boolean,needs_remap boolean)`,
		`CREATE TABLE topic_proposals(id bigserial,tenant_id text,embedding vector(1024),embedding_space_id text,embedding_producer_id text)`,
		`CREATE TABLE discovery_profiles(id bigserial,public_id uuid,tenant_id text,embedding vector(1024),embedding_space_id text,embedding_producer_id text)`,
		`CREATE TABLE content_item_topics(content_item_id uuid,topic_id uuid)`,
	}
	for _, sql := range ddl {
		if err := db.Exec(sql).Error; err != nil {
			t.Fatal(err)
		}
	}
	basis := spaceid.Basis{Model: "test-model", Revision: strings.Repeat("a", 40), Dimensions: 1024, Normalized: true, Pooling: "mean"}
	c.TargetSpaceID, c.TargetModel = basis.SpaceID(), basis.Model
	if err := db.Save(&c).Error; err != nil {
		t.Fatal(err)
	}
	id := uuid.New()
	if err := db.Exec(`INSERT INTO content_items(public_id,tenant_id,type,status,title,embedding) VALUES (?,'tenant','VIDEO','READY','Title',array_fill(0.1::real,ARRAY[1024])::vector)`, id).Error; err != nil {
		t.Fatal(err)
	}
	secondID := uuid.New()
	if err := db.Exec(`INSERT INTO content_items(public_id,tenant_id,type,status,title,embedding) VALUES (?,'second-tenant','VIDEO','READY','Other title',array_fill(0.1::real,ARRAY[1024])::vector)`, secondID).Error; err != nil {
		t.Fatal(err)
	}
	var outage atomic.Bool
	var calls atomic.Int32
	owner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if outage.Load() {
			w.WriteHeader(503)
			return
		}
		if r.URL.Path == "/v1/models" {
			_ = json.NewEncoder(w).Encode(map[string]any{"models": []map[string]any{{"name": basis.Model, "type": "embedder", "loaded": true, "revision": basis.Revision, "dimensions": 1024, "space_id": basis.SpaceID()}}})
			return
		}
		if r.URL.Path != "/v1/embed" {
			w.WriteHeader(404)
			return
		}
		calls.Add(1)
		var body struct {
			ContentIDs  []string                      `json:"content_ids"`
			Correlation *embeddingCampaignCorrelation `json:"embedding_campaign"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Correlation == nil {
			t.Error("campaign correlation omitted")
			w.WriteHeader(400)
			return
		}
		targetID, parseErr := uuid.Parse(body.ContentIDs[0])
		if parseErr != nil {
			t.Error(parseErr)
			w.WriteHeader(400)
			return
		}
		producer := spaceid.ProducerID(basis.SpaceID(), spaceid.RecipeContentText)
		err := db.Transaction(func(tx *gorm.DB) error {
			var item models.ContentItem
			if err := tx.Where("public_id=?", targetID).First(&item).Error; err != nil {
				return err
			}
			if err := authorizeEmbeddingCampaignWrite(tx, item, "content_text", body.Correlation, basis.SpaceID(), producer); err != nil {
				return err
			}
			if err := validateCampaignEmbeddingInput(tx, &item, "content_text", body.Correlation); err != nil {
				return err
			}
			return tx.Exec(`UPDATE content_items SET embedding_space_id=?,embedding_producer_id=? WHERE public_id=?`, basis.SpaceID(), producer, targetID).Error
		})
		if err != nil {
			t.Error(err)
			w.WriteHeader(409)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"write_back_status": "ok"})
	}))
	defer owner.Close()
	t.Setenv("ENRICHMENT_BASE_URL", owner.URL)
	t.Setenv("ENRICHMENT_SERVICE_TOKEN", "test-owner")
	if _, err := transitionCampaign(db, c.ID, "pause"); err != nil {
		t.Fatal(err)
	}
	if n, err := executeCampaignBatch(db, &c); err != nil || n != 0 || calls.Load() != 0 {
		t.Fatal("pause admitted model work", err)
	}
	if _, err := transitionCampaign(db, c.ID, "resume"); err != nil {
		t.Fatal(err)
	}
	outage.Store(true)
	if _, err := executeCampaignBatch(db, &c); err == nil || c.State != models.EmbeddingCampaignBlocked {
		t.Fatal("outage did not block campaign")
	}
	outage.Store(false)
	if _, err := transitionCampaign(db, c.ID, "resume"); err != nil {
		t.Fatal(err)
	}
	if _, blocked := fenceEmbeddingWrite(db, "text", spaceid.RecipeContentText, "old-space", "old-producer"); !blocked {
		t.Fatal("old writer accepted")
	}
	if n, err := executeCampaignBatch(db, &c); err != nil || n != 2 || c.State != models.EmbeddingCampaignVerifying {
		t.Fatalf("migration n=%d state=%s err=%v", n, c.State, err)
	}
	advanceCampaignVerification(db, &c)
	deadline := time.Now().Add(time.Second)
	for classificationBackfillRunning.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if c.State != models.EmbeddingCampaignCompleted || calls.Load() != 2 {
		t.Fatalf("state=%s calls=%d", c.State, calls.Load())
	}
	var actions []models.EmbeddingCampaignAction
	db.Where("campaign_id=?", c.ID).Find(&actions)
	tenants := map[string]bool{}
	for _, a := range actions {
		tenants[a.TenantID] = true
	}
	if !tenants["tenant"] || !tenants["second-tenant"] {
		t.Fatal("target tenant attribution lost", tenants)
	}
	if len(actions) != 2 || actions[0].Status != models.EmbeddingActionCompleted || actions[1].Status != models.EmbeddingActionCompleted {
		t.Fatalf("invalid durable readback ledger: %#v", actions)
	}
}
