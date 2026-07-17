package controllers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/tests/testdb"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// Pipeline DB tests are opt-in: testdb.Open creates a random database only
// after its disposable-environment guard has accepted the operator settings.
func pipelineAutopilotTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	if os.Getenv("CMS_TEST_ADMIN_URL") == "" && os.Getenv("CMS_TEST_DATABASE_URL") == "" {
		t.Skip("set guarded CMS_TEST_ADMIN_URL or CMS_TEST_DATABASE_URL to run Pipeline Autopilot DB tests")
	}
	db := testdb.Open(t)
	if err := db.AutoMigrate(
		&models.ContentItem{},
		&models.PipelineAutopilotPolicy{},
		&models.PipelineAutopilotRun{},
		&models.PipelineAutopilotAction{},
		&models.AuditLog{},
	); err != nil {
		t.Fatalf("migrate pipeline autopilot test schema: %v", err)
	}
	clear := func() {
		for _, table := range []string{"pipeline_autopilot_actions", "pipeline_autopilot_runs", "pipeline_autopilot_policies", "content_items", "audit_logs"} {
			_ = db.Exec("DELETE FROM " + table).Error
		}
	}
	clear()
	t.Cleanup(clear)
	return db
}

type pipelineRetryRequest struct {
	IDs   []string `json:"ids"`
	Limit int      `json:"limit"`
}

type pipelineAggregationTestStub struct {
	server   *httptest.Server
	mu       sync.Mutex
	retries  []pipelineRetryRequest
	queues   []autopilotQueueStat
	errors   []string
	requeued *int
}

func (s *pipelineAggregationTestStub) snapshotRetries() []pipelineRetryRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]pipelineRetryRequest(nil), s.retries...)
}

func (s *pipelineAggregationTestStub) setQueues(queues []autopilotQueueStat) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queues = append([]autopilotQueueStat(nil), queues...)
}

func (s *pipelineAggregationTestStub) setRetryResponse(requeued int, errors []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requeued = &requeued
	s.errors = append([]string(nil), errors...)
}

func pipelineAggregationStub(t *testing.T) *pipelineAggregationTestStub {
	t.Helper()
	stub := &pipelineAggregationTestStub{}
	stub.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer pipeline-test-token" {
			http.Error(w, "missing test token", http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/internal/queues":
			stub.mu.Lock()
			queues := append([]autopilotQueueStat(nil), stub.queues...)
			stub.mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"data": queues})
		case "/internal/retry-pending", "/internal/retry-failed":
			var retry pipelineRetryRequest
			if err := json.NewDecoder(r.Body).Decode(&retry); err != nil || len(retry.IDs) == 0 || retry.Limit != len(retry.IDs) {
				http.Error(w, "explicit ids and matching limit required", http.StatusBadRequest)
				return
			}
			stub.mu.Lock()
			stub.retries = append(stub.retries, retry)
			requeued, errors := len(retry.IDs), append([]string(nil), stub.errors...)
			if stub.requeued != nil {
				requeued = *stub.requeued
			}
			stub.mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "requeued": requeued, "total": len(retry.IDs), "errors": errors})
		default:
			http.NotFound(w, r)
		}
	}))
	stub.queues = []autopilotQueueStat{{Queue: "media-queue"}, {Queue: "ai-queue"}, {Queue: "aggregation-dlq"}}
	t.Cleanup(stub.server.Close)
	t.Setenv("AGGREGATION_BASE_URL", stub.server.URL)
	t.Setenv("AGGREGATION_CMS_SERVICE_TOKEN", "pipeline-test-token")
	return stub
}

func TestPipelineAutopilotDB_QueueDepthBlocksOnlySaturatedTarget(t *testing.T) {
	db := pipelineAutopilotTestDB(t)
	stub := pipelineAggregationStub(t)
	stub.setQueues([]autopilotQueueStat{{Queue: "media-queue"}, {Queue: "ai-queue", Waiting: 500}, {Queue: "aggregation-dlq"}})
	policy := models.DefaultPipelineAutopilotPolicy("default")
	policy.Enabled = true
	policy.Mode = models.PipelineAutopilotModeSafeAuto
	if err := db.Create(&policy).Error; err != nil {
		t.Fatalf("create pipeline policy: %v", err)
	}
	newsID, videoID := uuid.New(), uuid.New()
	for _, item := range []models.ContentItem{
		{PublicID: newsID, TenantID: "default", Type: models.ContentTypeNews, Source: models.SourceTypeRSS, Status: models.ContentStatusPending},
		{PublicID: videoID, TenantID: "default", Type: models.ContentTypeVideo, Source: models.SourceTypeYouTube, Status: models.ContentStatusPending},
	} {
		if err := db.Create(&item).Error; err != nil {
			t.Fatalf("seed pending item: %v", err)
		}
		if err := db.Model(&item).Update("created_at", time.Now().Add(-time.Hour)).Error; err != nil {
			t.Fatalf("age pending item: %v", err)
		}
	}
	_, actions, err := runPipelineAutopilot(db, "default", pipelineAutopilotRunOptions{Trigger: "test"})
	if err != nil {
		t.Fatalf("run queue-depth autopilot: %v", err)
	}
	retries := stub.snapshotRetries()
	if len(retries) != 1 || len(retries[0].IDs) != 1 || retries[0].IDs[0] != videoID.String() {
		t.Fatalf("only shallow media target may dispatch, got %+v", retries)
	}
	found := false
	for _, action := range actions {
		if action.TargetQueue == "ai-queue" && action.Guardrail == models.PipelineAutopilotGuardQueueDepth {
			found = true
		}
	}
	if !found {
		t.Fatalf("saturated AI target must write queue-depth guardrail: %+v", actions)
	}
}

func TestPipelineAutopilotDB_ObserveRunPersistsWithoutRetry(t *testing.T) {
	db := pipelineAutopilotTestDB(t)
	pipelineAggregationStub(t)
	policy := models.DefaultPipelineAutopilotPolicy("default")
	policy.Enabled = true
	policy.Mode = models.PipelineAutopilotModeObserve
	if err := db.Create(&policy).Error; err != nil {
		t.Fatalf("create pipeline policy: %v", err)
	}

	run, actions, err := runPipelineAutopilot(db, "default", pipelineAutopilotRunOptions{Trigger: "test"})
	if err != nil {
		t.Fatalf("run observe pipeline autopilot: %v", err)
	}
	if run.ID == 0 || run.Status != models.PipelineAutopilotRunStatusCompleted {
		t.Fatalf("unexpected persisted run: %+v", run)
	}
	if len(actions) != 0 {
		t.Fatalf("empty candidate set must not create actions: %+v", actions)
	}
	var persisted int64
	if err := db.Model(&models.PipelineAutopilotRun{}).Where("id = ?", run.ID).Count(&persisted).Error; err != nil || persisted != 1 {
		t.Fatalf("run was not persisted: count=%d err=%v", persisted, err)
	}
}

func TestPipelineAutopilotDB_SafeAutoRetriesOnlyExplicitIDs(t *testing.T) {
	db := pipelineAutopilotTestDB(t)
	stub := pipelineAggregationStub(t)
	policy := models.DefaultPipelineAutopilotPolicy("default")
	policy.Enabled = true
	policy.Mode = models.PipelineAutopilotModeSafeAuto
	if err := db.Create(&policy).Error; err != nil {
		t.Fatalf("create pipeline policy: %v", err)
	}
	old := time.Now().Add(-time.Hour)
	ids := []uuid.UUID{uuid.New(), uuid.New()}
	for _, id := range ids {
		item := models.ContentItem{PublicID: id, TenantID: "default", Type: models.ContentTypePodcast, Source: models.SourceTypePodcast, Status: models.ContentStatusFailed}
		if err := db.Create(&item).Error; err != nil {
			t.Fatalf("seed failed item: %v", err)
		}
		if err := db.Model(&item).Update("updated_at", old).Error; err != nil {
			t.Fatalf("age failed item: %v", err)
		}
	}

	_, _, err := runPipelineAutopilot(db, "default", pipelineAutopilotRunOptions{Trigger: "test"})
	if err != nil {
		t.Fatalf("run safe-auto pipeline autopilot: %v", err)
	}
	retries := stub.snapshotRetries()
	if len(retries) != 1 || len(retries[0].IDs) != len(ids) || retries[0].Limit != len(ids) {
		t.Fatalf("expected one explicit retry request for seeded ids, got %+v", retries)
	}
	want := map[string]bool{ids[0].String(): true, ids[1].String(): true}
	for _, id := range retries[0].IDs {
		if !want[id] {
			t.Fatalf("retry included unseeded id %q", id)
		}
		delete(want, id)
	}
	if len(want) != 0 {
		t.Fatalf("retry omitted seeded ids: %+v", want)
	}
}

func TestPipelineAutopilotDB_PendingAgeFloorUsesCreatedAt(t *testing.T) {
	db := pipelineAutopilotTestDB(t)
	pipelineAggregationStub(t)
	policy := models.DefaultPipelineAutopilotPolicy("default")
	policy.Enabled = true
	policy.Mode = models.PipelineAutopilotModeObserve
	if err := db.Create(&policy).Error; err != nil {
		t.Fatalf("create pipeline policy: %v", err)
	}
	freshID, overdueID := uuid.New(), uuid.New()
	for _, seed := range []struct {
		id  uuid.UUID
		age time.Duration
	}{{freshID, 5 * time.Minute}, {overdueID, 45 * time.Minute}} {
		item := models.ContentItem{PublicID: seed.id, TenantID: "default", Type: models.ContentTypePodcast, Source: models.SourceTypePodcast, Status: models.ContentStatusPending}
		if err := db.Create(&item).Error; err != nil {
			t.Fatalf("seed pending item: %v", err)
		}
		if err := db.Model(&item).Update("created_at", time.Now().Add(-seed.age)).Error; err != nil {
			t.Fatalf("age pending item: %v", err)
		}
	}

	_, actions, err := runPipelineAutopilot(db, "default", pipelineAutopilotRunOptions{Trigger: "test"})
	if err != nil {
		t.Fatalf("run observe pipeline autopilot: %v", err)
	}
	if len(actions) != 1 || actions[0].ContentItemID == nil || *actions[0].ContentItemID != overdueID || actions[0].Status != models.PipelineAutopilotActionStatusWouldExecute {
		t.Fatalf("age floor must select only overdue pending item, got %+v", actions)
	}
}

func TestPipelineAutopilotDB_RetryBackoffSelectsOnlyExpiredAttempt(t *testing.T) {
	db := pipelineAutopilotTestDB(t)
	policy := models.DefaultPipelineAutopilotPolicy("default")
	policy.Enabled = true
	now := time.Now().UTC()
	recentID, expiredID := uuid.New(), uuid.New()
	for _, id := range []uuid.UUID{recentID, expiredID} {
		item := models.ContentItem{PublicID: id, TenantID: "default", Type: models.ContentTypePodcast, Source: models.SourceTypePodcast, Status: models.ContentStatusFailed}
		if err := db.Create(&item).Error; err != nil {
			t.Fatalf("seed failed item: %v", err)
		}
		if err := db.Model(&item).Update("updated_at", now.Add(-time.Hour)).Error; err != nil {
			t.Fatalf("age failed item: %v", err)
		}
	}
	for id, startedAt := range map[uuid.UUID]time.Time{recentID: now.Add(-time.Hour), expiredID: now.Add(-13 * time.Hour)} {
		action := models.PipelineAutopilotAction{RunID: 99, TenantID: "default", Lane: models.PipelineLaneFailedRetryable, Verdict: models.PipelineVerdictRetryFailed, ContentItemID: &id, Status: models.PipelineAutopilotActionStatusSuccess, StartedAt: startedAt}
		if err := db.Create(&action).Error; err != nil {
			t.Fatalf("seed prior attempt: %v", err)
		}
	}
	runner := pipelineAutopilotRunner{db: db, run: &models.PipelineAutopilotRun{TenantID: "default"}, policy: policy}
	candidates := runner.selectCandidates()
	if len(candidates) != 1 || candidates[0].Item.PublicID != expiredID {
		t.Fatalf("backoff must retain only expired attempt, got %+v", candidates)
	}
}

func TestPipelineAutopilotDB_AttemptCapStaysOutOfRetryAndCreatesAttention(t *testing.T) {
	db := pipelineAutopilotTestDB(t)
	stub := pipelineAggregationStub(t)
	policy := models.DefaultPipelineAutopilotPolicy("default")
	policy.Enabled = true
	policy.Mode = models.PipelineAutopilotModeSafeAuto
	if err := db.Create(&policy).Error; err != nil {
		t.Fatalf("create pipeline policy: %v", err)
	}
	id := uuid.New()
	item := models.ContentItem{PublicID: id, TenantID: "default", Type: models.ContentTypePodcast, Source: models.SourceTypePodcast, Status: models.ContentStatusFailed}
	if err := db.Create(&item).Error; err != nil {
		t.Fatalf("seed failed item: %v", err)
	}
	if err := db.Model(&item).Update("updated_at", time.Now().Add(-time.Hour)).Error; err != nil {
		t.Fatalf("age failed item: %v", err)
	}
	for range policy.MaxAttempts {
		now := time.Now().Add(-48 * time.Hour)
		action := models.PipelineAutopilotAction{RunID: 1, TenantID: "default", Lane: models.PipelineLaneFailedRetryable, Verdict: models.PipelineVerdictRetryFailed, ContentItemID: &id, Status: models.PipelineAutopilotActionStatusSuccess, StartedAt: now, FinishedAt: &now}
		if err := db.Create(&action).Error; err != nil {
			t.Fatalf("seed prior attempt: %v", err)
		}
	}

	_, actions, err := runPipelineAutopilot(db, "default", pipelineAutopilotRunOptions{Trigger: "test"})
	if err != nil {
		t.Fatalf("run safe-auto pipeline autopilot: %v", err)
	}
	if got := stub.snapshotRetries(); len(got) != 0 {
		t.Fatalf("attempt-capped item must not reach retry endpoint: %+v", got)
	}
	found := false
	for _, action := range actions {
		if action.Lane == models.PipelineLaneFailedExhausted && action.Status == models.PipelineAutopilotActionStatusAttention && action.Guardrail == models.PipelineAutopilotGuardAttemptCap {
			found = true
		}
	}
	if !found {
		t.Fatalf("attempt cap must create human-attention action: %+v", actions)
	}
}

func TestPipelineAutopilotDB_ObserveDoesNotConsumeRetryMemory(t *testing.T) {
	db := pipelineAutopilotTestDB(t)
	stub := pipelineAggregationStub(t)
	policy := models.DefaultPipelineAutopilotPolicy("default")
	policy.Enabled = true
	policy.Mode = models.PipelineAutopilotModeObserve
	if err := db.Create(&policy).Error; err != nil {
		t.Fatalf("create pipeline policy: %v", err)
	}
	id := uuid.New()
	item := models.ContentItem{PublicID: id, TenantID: "default", Type: models.ContentTypePodcast, Source: models.SourceTypePodcast, Status: models.ContentStatusFailed}
	if err := db.Create(&item).Error; err != nil {
		t.Fatalf("seed failed item: %v", err)
	}
	if err := db.Model(&item).Update("updated_at", time.Now().Add(-time.Hour)).Error; err != nil {
		t.Fatalf("age failed item: %v", err)
	}

	_, observeActions, err := runPipelineAutopilot(db, "default", pipelineAutopilotRunOptions{Trigger: "test"})
	if err != nil {
		t.Fatalf("run observe pipeline autopilot: %v", err)
	}
	if len(observeActions) != 1 || observeActions[0].Status != models.PipelineAutopilotActionStatusWouldExecute {
		t.Fatalf("observe run must record one would-execute action: %+v", observeActions)
	}
	if got := stub.snapshotRetries(); len(got) != 0 {
		t.Fatalf("observe run must not call retry endpoint: %+v", got)
	}
	if err := db.Model(&models.PipelineAutopilotPolicy{}).Where("tenant_id = ?", "default").Update("mode", models.PipelineAutopilotModeSafeAuto).Error; err != nil {
		t.Fatalf("enable safe auto: %v", err)
	}
	_, _, err = runPipelineAutopilot(db, "default", pipelineAutopilotRunOptions{Trigger: "test"})
	if err != nil {
		t.Fatalf("run safe-auto pipeline autopilot: %v", err)
	}
	retries := stub.snapshotRetries()
	if len(retries) != 1 || len(retries[0].IDs) != 1 || retries[0].IDs[0] != id.String() {
		t.Fatalf("safe-auto run must retain eligibility after observe, got %+v", retries)
	}
}

func TestPipelineAutopilotDB_ResolvesRetryOutcomesFromCurrentItemState(t *testing.T) {
	db := pipelineAutopilotTestDB(t)
	pipelineAggregationStub(t)
	policy := models.DefaultPipelineAutopilotPolicy("default")
	policy.Enabled = true
	policy.Mode = models.PipelineAutopilotModeObserve
	if err := db.Create(&policy).Error; err != nil {
		t.Fatalf("create pipeline policy: %v", err)
	}
	priorRun := models.PipelineAutopilotRun{TenantID: "default", Trigger: "test", Mode: models.PipelineAutopilotModeSafeAuto, Status: models.PipelineAutopilotRunStatusCompleted, StartedAt: time.Now().Add(-72 * time.Hour)}
	if err := db.Create(&priorRun).Error; err != nil {
		t.Fatalf("create prior run: %v", err)
	}
	ids := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	statuses := []models.ContentStatus{models.ContentStatusReady, models.ContentStatusFailed, models.ContentStatusPending}
	for index, id := range ids {
		item := models.ContentItem{PublicID: id, TenantID: "default", Type: models.ContentTypePodcast, Source: models.SourceTypePodcast, Status: statuses[index]}
		if err := db.Create(&item).Error; err != nil {
			t.Fatalf("seed outcome item: %v", err)
		}
		startedAt := time.Now().Add(-72 * time.Hour)
		action := models.PipelineAutopilotAction{RunID: priorRun.ID, TenantID: "default", Lane: models.PipelineLaneFailedRetryable, Verdict: models.PipelineVerdictRetryFailed, ContentItemID: &id, Status: models.PipelineAutopilotActionStatusSuccess, Outcome: models.PipelineAutopilotOutcomePending, StartedAt: startedAt}
		if err := db.Create(&action).Error; err != nil {
			t.Fatalf("seed pending outcome: %v", err)
		}
	}
	if _, _, err := runPipelineAutopilot(db, "default", pipelineAutopilotRunOptions{Trigger: "test"}); err != nil {
		t.Fatalf("run outcome resolver: %v", err)
	}
	var actions []models.PipelineAutopilotAction
	if err := db.Where("run_id = ?", priorRun.ID).Order("id ASC").Find(&actions).Error; err != nil {
		t.Fatalf("load resolved actions: %v", err)
	}
	want := []string{models.PipelineAutopilotOutcomeRecovered, models.PipelineAutopilotOutcomeFailedAgain, models.PipelineAutopilotOutcomeUnresolved}
	if len(actions) != len(want) {
		t.Fatalf("expected %d resolved actions, got %+v", len(want), actions)
	}
	for index, action := range actions {
		if action.Outcome != want[index] {
			t.Fatalf("action %d outcome=%q want %q", index, action.Outcome, want[index])
		}
	}
}

func TestPipelineAutopilotDB_TrustDemotionBlocksFailedRetryLane(t *testing.T) {
	db := pipelineAutopilotTestDB(t)
	stub := pipelineAggregationStub(t)
	policy := models.DefaultPipelineAutopilotPolicy("default")
	policy.Enabled = true
	policy.Mode = models.PipelineAutopilotModeSafeAuto
	if err := db.Create(&policy).Error; err != nil {
		t.Fatalf("create pipeline policy: %v", err)
	}
	priorRun := models.PipelineAutopilotRun{TenantID: "default", Trigger: "test", Mode: models.PipelineAutopilotModeSafeAuto, Status: models.PipelineAutopilotRunStatusCompleted, StartedAt: time.Now().Add(-72 * time.Hour)}
	if err := db.Create(&priorRun).Error; err != nil {
		t.Fatalf("create prior run: %v", err)
	}
	for range policy.TrustMinOutcomes {
		startedAt := time.Now().Add(-72 * time.Hour)
		action := models.PipelineAutopilotAction{RunID: priorRun.ID, TenantID: "default", Lane: models.PipelineLaneFailedRetryable, Verdict: models.PipelineVerdictRetryFailed, Status: models.PipelineAutopilotActionStatusSuccess, Outcome: models.PipelineAutopilotOutcomeFailedAgain, StartedAt: startedAt}
		if err := db.Create(&action).Error; err != nil {
			t.Fatalf("seed failed trust outcome: %v", err)
		}
	}
	id := uuid.New()
	item := models.ContentItem{PublicID: id, TenantID: "default", Type: models.ContentTypePodcast, Source: models.SourceTypePodcast, Status: models.ContentStatusFailed}
	if err := db.Create(&item).Error; err != nil {
		t.Fatalf("seed eligible failed item: %v", err)
	}
	if err := db.Model(&item).Update("updated_at", time.Now().Add(-time.Hour)).Error; err != nil {
		t.Fatalf("age eligible failed item: %v", err)
	}
	_, actions, err := runPipelineAutopilot(db, "default", pipelineAutopilotRunOptions{Trigger: "test"})
	if err != nil {
		t.Fatalf("run trust-demoted autopilot: %v", err)
	}
	if got := stub.snapshotRetries(); len(got) != 0 {
		t.Fatalf("demoted lane must not retry: %+v", got)
	}
	found := false
	for _, action := range actions {
		if action.Lane == models.PipelineLaneFailedRetryable && action.Guardrail == models.PipelineAutopilotGuardTrustGate {
			found = true
		}
	}
	if !found {
		t.Fatalf("demoted lane must write trust-gate action: %+v", actions)
	}
}

func TestPipelineAutopilotDB_SourceCeilingBlocksOnlyCappedSource(t *testing.T) {
	db := pipelineAutopilotTestDB(t)
	stub := pipelineAggregationStub(t)
	policy := models.DefaultPipelineAutopilotPolicy("default")
	policy.Enabled = true
	policy.Mode = models.PipelineAutopilotModeSafeAuto
	if err := db.Create(&policy).Error; err != nil {
		t.Fatalf("create pipeline policy: %v", err)
	}
	priorRun := models.PipelineAutopilotRun{TenantID: "default", Trigger: "test", Mode: models.PipelineAutopilotModeSafeAuto, Status: models.PipelineAutopilotRunStatusCompleted, StartedAt: time.Now().Add(-time.Hour)}
	if err := db.Create(&priorRun).Error; err != nil {
		t.Fatalf("create prior run: %v", err)
	}
	for range policy.PerSourceDailyRetries {
		startedAt := time.Now().Add(-time.Hour)
		action := models.PipelineAutopilotAction{RunID: priorRun.ID, TenantID: "default", Lane: models.PipelineLaneFailedRetryable, Verdict: models.PipelineVerdictRetryFailed, SourceFilter: "capped-source", Status: models.PipelineAutopilotActionStatusSuccess, StartedAt: startedAt}
		if err := db.Create(&action).Error; err != nil {
			t.Fatalf("seed source retry history: %v", err)
		}
	}
	cappedName, openName := "capped-source", "open-source"
	cappedID, openID := uuid.New(), uuid.New()
	for _, item := range []models.ContentItem{
		{PublicID: cappedID, TenantID: "default", Type: models.ContentTypePodcast, Source: models.SourceTypePodcast, SourceName: &cappedName, Status: models.ContentStatusFailed},
		{PublicID: openID, TenantID: "default", Type: models.ContentTypePodcast, Source: models.SourceTypePodcast, SourceName: &openName, Status: models.ContentStatusFailed},
	} {
		if err := db.Create(&item).Error; err != nil {
			t.Fatalf("seed source item: %v", err)
		}
		if err := db.Model(&item).Update("updated_at", time.Now().Add(-time.Hour)).Error; err != nil {
			t.Fatalf("age source item: %v", err)
		}
	}
	_, actions, err := runPipelineAutopilot(db, "default", pipelineAutopilotRunOptions{Trigger: "test"})
	if err != nil {
		t.Fatalf("run source-ceiling autopilot: %v", err)
	}
	retries := stub.snapshotRetries()
	if len(retries) != 1 || len(retries[0].IDs) != 1 || retries[0].IDs[0] != openID.String() {
		t.Fatalf("only uncapped source may retry, got %+v", retries)
	}
	found := false
	for _, action := range actions {
		if action.SourceFilter == cappedName && action.Guardrail == models.PipelineAutopilotGuardSourceCeiling {
			found = true
		}
	}
	if !found {
		t.Fatalf("capped source must write source-ceiling guardrail: %+v", actions)
	}
}

func TestPipelineAutopilotDB_RecoveryCooldownBlocksRetryAfterHealthFailure(t *testing.T) {
	db := pipelineAutopilotTestDB(t)
	stub := pipelineAggregationStub(t)
	policy := models.DefaultPipelineAutopilotPolicy("default")
	policy.Enabled = true
	policy.Mode = models.PipelineAutopilotModeSafeAuto
	if err := db.Create(&policy).Error; err != nil {
		t.Fatalf("create pipeline policy: %v", err)
	}
	failedRun := models.PipelineAutopilotRun{TenantID: "default", Trigger: "test", Mode: models.PipelineAutopilotModeSafeAuto, Status: models.PipelineAutopilotRunStatusFailed, ErrorClass: models.PipelineAutopilotErrorClassAggregationUnreachable, StartedAt: time.Now().Add(-time.Hour)}
	if err := db.Create(&failedRun).Error; err != nil {
		t.Fatalf("create health-failed run: %v", err)
	}
	id := uuid.New()
	item := models.ContentItem{PublicID: id, TenantID: "default", Type: models.ContentTypePodcast, Source: models.SourceTypePodcast, Status: models.ContentStatusFailed}
	if err := db.Create(&item).Error; err != nil {
		t.Fatalf("seed failed item: %v", err)
	}
	if err := db.Model(&item).Update("updated_at", time.Now().Add(-time.Hour)).Error; err != nil {
		t.Fatalf("age failed item: %v", err)
	}
	_, actions, err := runPipelineAutopilot(db, "default", pipelineAutopilotRunOptions{Trigger: "test"})
	if err != nil {
		t.Fatalf("run recovery cooldown autopilot: %v", err)
	}
	if got := stub.snapshotRetries(); len(got) != 0 {
		t.Fatalf("recovery cooldown must not retry: %+v", got)
	}
	found := false
	for _, action := range actions {
		if action.Guardrail == models.PipelineAutopilotGuardRecoveryCooldown {
			found = true
		}
	}
	if !found {
		t.Fatalf("recovery cooldown must write guardrail action: %+v", actions)
	}
	var stored models.PipelineAutopilotPolicy
	if err := db.Where("tenant_id = ?", "default").First(&stored).Error; err != nil || stored.LastHealthOKAt == nil {
		t.Fatalf("recovery run must stamp last health ok: policy=%+v err=%v", stored, err)
	}
}

func TestPipelineAutopilotDB_PartialBatchFailureIsolatedPerID(t *testing.T) {
	db := pipelineAutopilotTestDB(t)
	stub := pipelineAggregationStub(t)
	policy := models.DefaultPipelineAutopilotPolicy("default")
	policy.Enabled, policy.Mode = true, models.PipelineAutopilotModeSafeAuto
	if err := db.Create(&policy).Error; err != nil {
		t.Fatalf("create pipeline policy: %v", err)
	}
	source := "same-source"
	failingID, succeedingID := uuid.New(), uuid.New()
	for _, id := range []uuid.UUID{failingID, succeedingID} {
		item := models.ContentItem{PublicID: id, TenantID: "default", Type: models.ContentTypePodcast, Source: models.SourceTypePodcast, SourceName: &source, Status: models.ContentStatusFailed}
		if err := db.Create(&item).Error; err != nil {
			t.Fatalf("seed batch item: %v", err)
		}
		if err := db.Model(&item).Update("updated_at", time.Now().Add(-time.Hour)).Error; err != nil {
			t.Fatalf("age batch item: %v", err)
		}
	}
	stub.setRetryResponse(1, []string{failingID.String() + ": boom"})
	run, actions, err := runPipelineAutopilot(db, "default", pipelineAutopilotRunOptions{Trigger: "test"})
	if err != nil {
		t.Fatalf("run partial batch: %v", err)
	}
	if run.Status != models.PipelineAutopilotRunStatusPartial {
		t.Fatalf("partial per-id failure must make run partial: %+v", run)
	}
	states := map[uuid.UUID]string{}
	for _, action := range actions {
		if action.ContentItemID != nil {
			states[*action.ContentItemID] = action.Status
		}
	}
	if states[failingID] != models.PipelineAutopilotActionStatusError || states[succeedingID] != models.PipelineAutopilotActionStatusSuccess {
		t.Fatalf("per-id response must isolate error and success: %+v", states)
	}
}
