package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

func podsPipelineTestRouter(db *gorm.DB, tenant string) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Set("db", db)
		c.Set(utils.AdminPrincipalContextKey, utils.AdminPrincipal{TenantID: tenant, Email: "operator@example.test", Permissions: []string{"aggregation:read", "content:read", "content:write"}})
	})
	r.GET("/activity", GetPodsPipelineActivity)
	r.PATCH("/settings", UpdateMediaAcquisitionConfig)
	return r
}

func TestPodsPipelineActivityTenantPaginationAndOrigin(t *testing.T) {
	db := opsTestDB(t)
	at := time.Now().UTC().Truncate(time.Microsecond)
	source := models.ContentSource{PublicID: uuid.New(), TenantID: "tenant-a", Name: "Recorded podcast", Type: models.SourceTypeRSS, Category: "media"}
	if err := db.Create(&source).Error; err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 23; i++ {
		tenant, lane, trigger, actor := "tenant-a", "media", "schedule", ""
		if i == 1 {
			trigger, actor = "manual", "operator@example.test"
		}
		if i == 21 {
			tenant = "tenant-b"
		}
		if i == 22 {
			lane = "news"
		}
		if err := db.Exec(`INSERT INTO source_run_requests (tenant_id,content_source_id,lane,requested_by,requested_by_actor_id,state,correlation_id,idempotency_key,requested_at) VALUES (?,?,?,?,?,'succeeded',?,?,?)`, tenant, source.PublicID, lane, trigger, actor, fmt.Sprint(i), fmt.Sprint(i), at).Error; err != nil {
			t.Fatal(err)
		}
	}
	publication := uuid.New()
	if err := db.Exec(`INSERT INTO atomization_generations(public_id,tenant_id,parent_content_item_id,work_request_id,generation_number,transcript_digest,policy_digest,input_digest,plan_digest,state,activation_at,expected_units) VALUES (?,'tenant-a',?,?,1,'','','','','active',?,3)`, publication, uuid.New(), uuid.New(), at).Error; err != nil {
		t.Fatal(err)
	}
	r := podsPipelineTestRouter(db, "tenant-a")
	cursor := ""
	seen := map[uuid.UUID]bool{}
	manual, scheduled, published := false, false, false
	for page := 0; page < 3; page++ {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest("GET", "/activity?cursor="+cursor, nil))
		if w.Code != 200 {
			t.Fatalf("activity: %d %s", w.Code, w.Body.String())
		}
		var result struct {
			Items []podsPipelineActivityEvent `json:"items"`
			Next  string                      `json:"next_cursor"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		for _, item := range result.Items {
			if seen[item.ID] {
				t.Fatal("cursor repeated an event")
			}
			seen[item.ID] = true
			if item.Kind == "publication" {
				published = true
				if item.Trigger != "unrecorded" || item.Actor != "" || item.Units != 3 {
					t.Fatalf("invented publication origin: %+v", item)
				}
			}
			if item.Trigger == "manual" {
				manual = item.Actor == "operator@example.test"
			}
			if item.Trigger == "schedule" {
				scheduled = true
			}
		}
		cursor = result.Next
		if cursor == "" {
			break
		}
	}
	if len(seen) != 22 || !manual || !scheduled || !published {
		t.Fatalf("scoped activity mismatch: events=%d manual=%v schedule=%v publication=%v", len(seen), manual, scheduled, published)
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest("GET", "/activity?cursor=invalid", nil))
	if w.Code != 400 {
		t.Fatalf("invalid cursor: %d", w.Code)
	}
}

func TestPodsPipelineCountsAndFleetProjection(t *testing.T) {
	db := opsTestDB(t)
	fixtures := []struct {
		tenant, lane, state, status string
		generation                  int64
	}{
		{"tenant-a", "pods", "running", "PENDING", 2}, {"tenant-a", "pods", "claimed", "PENDING", 2},
		{"tenant-a", "pods", "verifying", "PENDING", 2}, {"tenant-a", "pods", "uncertain", "PENDING", 2},
		{"tenant-a", "pods", "queued", "PENDING", 2}, {"tenant-a", "pods", "running", "PENDING", 1},
		{"tenant-a", "pods", "running", "ARCHIVED", 2}, {"tenant-b", "pods", "running", "PENDING", 2},
		{"tenant-a", "news", "running", "PENDING", 2},
	}
	for i, f := range fixtures {
		item := models.ContentItem{PublicID: uuid.New(), TenantID: f.tenant, Type: models.ContentTypePodcast, Source: models.SourceTypeRSS, Status: models.ContentStatus(f.status), ProcessingGeneration: 2}
		if err := db.Create(&item).Error; err != nil {
			t.Fatal(err)
		}
		stage, owner := "pods_media_artifacts", "aggregation_pods"
		if f.lane == "news" {
			stage, owner = "news_text_embedding", "enrichment"
		}
		if err := db.Exec(`INSERT INTO content_stage_requests(tenant_id,content_item_id,processing_generation,lane,stage,owner,blocking_scope,state,input_fingerprint,idempotency_key) VALUES (?,?,?,?,?,?,'content_ready',?,'fixture',?)`, f.tenant, item.PublicID, f.generation, f.lane, stage, owner, f.state, fmt.Sprint(i)).Error; err != nil {
			t.Fatal(err)
		}
	}
	for i, state := range []string{"running", "accepted", "verification_required", "requested"} {
		if err := db.Exec(`INSERT INTO source_run_requests(tenant_id,content_source_id,lane,requested_by,state,correlation_id,idempotency_key) VALUES ('tenant-a',?,'media','manual',?,?,?)`, uuid.New(), state, fmt.Sprint(i), fmt.Sprint(i)).Error; err != nil {
			t.Fatal(err)
		}
	}
	counts, err := readPodsPipelineActivityCounts(db, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if counts.RunningSteps != 1 || counts.ClaimedSteps != 1 || counts.VerifyingSteps != 1 || counts.UnresolvedSteps != 1 || counts.QueuedSteps != 1 || counts.RunningSourceRuns != 1 || counts.AdmittedSourceRuns != 1 || counts.VerifyingSourceRuns != 1 || counts.QueuedSourceRuns != 1 {
		t.Fatalf("incorrect current work: %+v", counts)
	}
	control := models.ContentStageControl{TenantID: "tenant-a", Lane: "pods", SchedulingEnabled: false, ExecutionEnabled: false}
	if err := db.Create(&control).Error; err != nil {
		t.Fatal(err)
	}
	// GORM defaults true; pin the paused owner values explicitly.
	if err := db.Model(&control).Updates(map[string]any{"scheduling_enabled": false, "execution_enabled": false}).Error; err != nil {
		t.Fatal(err)
	}
	member, lane, ok := opsFindLane("pods_pipeline", "processing")
	if !ok || lane.PauseColumn != "" {
		t.Fatal("pipeline must have its own control, outside expiring fleet commands")
	}
	statuses := opsStatusForLane(db, member, lane, time.Now())
	if len(statuses) != 1 || statuses[0].State != "paused" || statuses[0].Pausable || !strings.Contains(statuses[0].Evidence, "Running steps/source checks: 2") {
		t.Fatalf("fleet lost pause or continuing work: %+v", statuses)
	}
	if err := db.Model(&control).Updates(map[string]any{"scheduling_enabled": true, "execution_enabled": true}).Error; err != nil {
		t.Fatal(err)
	}
	statuses = opsStatusForLane(db, member, lane, time.Now())
	if len(statuses) != 1 || statuses[0].State != "attention" {
		t.Fatalf("unresolved outcomes must remain visible: %+v", statuses)
	}
	if !strings.Contains(statuses[0].Evidence, "processing needs setup") {
		t.Fatalf("unprovisioned pipeline must not look ready: %+v", statuses)
	}
}

func TestPodsProcessingSettingsRevisionConflict(t *testing.T) {
	db := opsTestDB(t)
	saved, err := getMediaAcquisitionConfig(db, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	r := podsPipelineTestRouter(db, "tenant-a")
	apply := func(at time.Time, limit int) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		body := fmt.Sprintf(`{"pods_source_run_item_limit":%d,"expected_updated_at":%q}`, limit, at.Format(time.RFC3339Nano))
		req := httptest.NewRequest("PATCH", "/settings", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		r.ServeHTTP(w, req)
		return w
	}
	first := apply(saved.UpdatedAt, 12)
	if first.Code != 200 {
		t.Fatalf("first save: %d %s", first.Code, first.Body.String())
	}
	stale := apply(saved.UpdatedAt, 15)
	if stale.Code != 409 {
		t.Fatalf("stale save: %d %s", stale.Code, stale.Body.String())
	}
	var result models.MediaAcquisitionConfig
	if err := db.Where("tenant_id=?", "tenant-a").First(&result).Error; err != nil {
		t.Fatal(err)
	}
	if result.PodsSourceRunItemLimit != 12 {
		t.Fatal("stale editor overwrote saved settings")
	}
}
