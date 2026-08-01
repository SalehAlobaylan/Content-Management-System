package controllers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"content-management-system/src/models"
	operatorpkg "content-management-system/src/operator"
	"content-management-system/src/tests/testdb"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

func openSourceLineageTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db := testdb.Open(t)
	if err := db.AutoMigrate(&models.ContentSource{}, &models.SourceRunRequest{}, &models.ContentProcessingEvent{}, &models.OperatorActionPlan{}, &models.OperatorActionStep{}); err != nil {
		t.Fatalf("migrate source lineage test schema: %v", err)
	}
	clear := func() {
		_ = db.Exec("DELETE FROM content_processing_events").Error
		_ = db.Exec("DELETE FROM source_run_requests").Error
		_ = db.Exec("DELETE FROM content_sources").Error
	}
	clear()
	t.Cleanup(clear)
	return db
}

func TestOperatorSourceRunUsesOnlyCMSInternalHandoffAndPersistsCorrelation(t *testing.T) {
	db := openSourceLineageTestDB(t)
	feedURL := "https://example.test/feed.xml"
	source := models.ContentSource{TenantID: "tenant-a", Name: "Source", Type: models.SourceTypeRSS, Category: models.SourceCategoryNews, IsActive: true, FeedURL: &feedURL}
	if err := db.Create(&source).Error; err != nil {
		t.Fatal(err)
	}
	plan := models.OperatorActionPlan{TenantID: "tenant-a", ActorID: "admin-a", IdempotencyKey: uuid.NewString()}
	step := models.OperatorActionStep{TenantID: "tenant-a"}
	if err := db.Create(&plan).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&step).Error; err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/operator/source-runs" || r.Header.Get("Authorization") != "Bearer test-service-token" {
			http.Error(w, "wrong internal handoff", http.StatusForbidden)
			return
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body["source_id"] != source.PublicID.String() || body["operator_plan_id"] != plan.PublicID.String() || body["operator_step_id"] != step.PublicID.String() || body["idempotency_key"] != plan.IdempotencyKey {
			http.Error(w, "missing typed correlation", http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"job_id": "operator-fetch-42"}})
	}))
	t.Cleanup(server.Close)
	t.Setenv("AGGREGATION_BASE_URL", server.URL)
	t.Setenv("AGGREGATION_CMS_SERVICE_TOKEN", "test-service-token")
	canonical := operatorpkg.CanonicalPlan{TargetIDs: []string{source.PublicID.String()}}
	result, before, err := runOperatorSourceOnce(db, "tenant-a", "sources.run_once", plan, step, canonical)
	if err != nil {
		t.Fatal(err)
	}
	if result.JobID != "operator-fetch-42" || before["source_run_request_id"] != result.RequestID {
		t.Fatalf("unexpected registered handoff result: result=%+v before=%#v", result, before)
	}
	var request models.SourceRunRequest
	if err := db.Where("public_id=?", result.RequestID).First(&request).Error; err != nil {
		t.Fatal(err)
	}
	if request.State != models.SourceRunAccepted || request.OperatorPlanID == nil || *request.OperatorPlanID != plan.PublicID || request.OperatorStepID == nil || *request.OperatorStepID != step.PublicID || request.IdempotencyKey != plan.IdempotencyKey {
		t.Fatalf("operator source handoff lost correlation: %+v", request)
	}
}

func TestSourceRunRequestRecordsRequestAndAcceptanceEvents(t *testing.T) {
	db := openSourceLineageTestDB(t)
	source := models.ContentSource{TenantID: "tenant-a", Name: "Arabic podcast", Type: models.SourceTypePodcast, Category: models.SourceCategoryMedia}
	if err := db.Create(&source).Error; err != nil {
		t.Fatal(err)
	}
	request, err := createSourceRunRequest(db, source, "approval_handoff", "admin-a", nil)
	if err != nil {
		t.Fatal(err)
	}
	if request.State != models.SourceRunRequested || request.CorrelationID == "" {
		t.Fatalf("unexpected requested lineage: %+v", request)
	}
	if err := markSourceRunAccepted(db, request.PublicID, "bullmq-job-42"); err != nil {
		t.Fatal(err)
	}
	var stored models.SourceRunRequest
	if err := db.First(&stored, request.ID).Error; err != nil {
		t.Fatal(err)
	}
	if stored.State != models.SourceRunAccepted || stored.AggregationJobID != "bullmq-job-42" || stored.AcceptedAt == nil {
		t.Fatalf("unexpected accepted lineage: %+v", stored)
	}
	var events []models.ContentProcessingEvent
	if err := db.Where("source_run_request_id=?", request.ID).Order("created_at ASC").Find(&events).Error; err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].EventClass != "source_run_requested" || events[1].EventClass != "aggregation_job_accepted" {
		t.Fatalf("unexpected source-run events: %+v", events)
	}
}

func TestSourceRunRequestRejectsUnknownRequester(t *testing.T) {
	db := openSourceLineageTestDB(t)
	source := models.ContentSource{TenantID: "tenant-a", Name: "Source", Type: models.SourceTypeRSS, Category: models.SourceCategoryNews}
	if err := db.Create(&source).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := createSourceRunRequest(db, source, "queue_guess", "admin-a", nil); err == nil {
		t.Fatal("unregistered requester must be rejected")
	}
}

func TestSourceRunRequestPreservesOptionalOperatorPlanCorrelation(t *testing.T) {
	db := openSourceLineageTestDB(t)
	source := models.ContentSource{TenantID: "tenant-a", Name: "Source", Type: models.SourceTypeRSS, Category: models.SourceCategoryNews}
	if err := db.Create(&source).Error; err != nil {
		t.Fatal(err)
	}
	planID, stepID := uuid.New(), uuid.New()
	request, err := createSourceRunRequestWithCorrelation(db, source, "system", "", nil, SourceRunCorrelation{OperatorPlanID: &planID, OperatorStepID: &stepID, IdempotencyKey: "operator-plan:source-run:1"})
	if err != nil {
		t.Fatal(err)
	}
	if request.OperatorPlanID == nil || *request.OperatorPlanID != planID || request.OperatorStepID == nil || *request.OperatorStepID != stepID || request.IdempotencyKey != "operator-plan:source-run:1" {
		t.Fatalf("operator correlation was not persisted: %+v", request)
	}
}
