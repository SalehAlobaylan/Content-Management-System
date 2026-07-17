package controllers

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/tests/testdb"
	"content-management-system/src/utils"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// Lifecycle tests use the shared disposable PostgreSQL fixture.
func systemHealthAutopilotTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	if os.Getenv("CMS_TEST_ADMIN_URL") == "" && os.Getenv("CMS_TEST_DATABASE_URL") == "" {
		t.Skip("set guarded CMS_TEST_ADMIN_URL or CMS_TEST_DATABASE_URL to run System Health DB tests")
	}
	db := testdb.Open(t)
	if err := db.AutoMigrate(
		&models.SystemAutopilotPolicy{}, &models.SystemIncidentEpisode{}, &models.SystemAutopilotRun{}, &models.SystemAutopilotAction{}, &models.AuditLog{},
		&models.PipelineAutopilotPolicy{}, &models.EnrichmentAutopilotPolicy{}, &models.NewsCirculationPolicy{},
		&models.MediaCirculationPolicy{}, &models.MediaStudioAutopilotPolicy{}, &models.EmbeddingLifecyclePolicy{}, &models.RedundancyPolicy{},
	); err != nil {
		t.Fatalf("migrate system health fixture: %v", err)
	}
	clear := func() {
		for _, table := range []string{
			"system_autopilot_actions", "system_autopilot_runs", "system_incident_episodes", "system_autopilot_policies", "audit_logs",
			"pipeline_autopilot_policies", "enrichment_autopilot_policies", "news_circulation_policies", "media_circulation_policies",
			"media_studio_autopilot_policies", "embedding_lifecycle_policies", "redundancy_policies",
		} {
			_ = db.Exec("DELETE FROM " + table).Error
		}
	}
	clear()
	t.Cleanup(clear)
	return db
}

func systemHealthSnapshotAt(at time.Time, anomalies ...systemAnomaly) (systemHealthSnapshot, []systemAnomaly) {
	overall := "healthy"
	if len(anomalies) > 0 {
		overall = "degraded"
	}
	return systemHealthSnapshot{Timestamp: at.UTC().Format(time.RFC3339), Overall: overall}, anomalies
}

func healthySystemSnapshotAt(at time.Time) (systemHealthSnapshot, []systemAnomaly) {
	services := []systemProbeResult{
		{Name: "cms", Status: "healthy"}, {Name: "iam", Status: "healthy"}, {Name: "aggregation", Status: "healthy"},
		{Name: "enrichment", Status: "healthy"}, {Name: "media", Status: "healthy"}, {Name: "platform", Status: "healthy"},
	}
	return systemHealthSnapshot{Timestamp: at.UTC().Format(time.RFC3339), Overall: "healthy", Services: services}, nil
}

func systemHealthFailure(at time.Time) (systemHealthSnapshot, []systemAnomaly) {
	return systemHealthSnapshotAt(at, systemAnomaly{Key: "aggregation:service_down", Service: "aggregation", Verdict: models.SystemVerdictServiceDown, Severity: "critical", Summary: "Aggregation unavailable"})
}

func runSystemHealthFixture(t *testing.T, db *gorm.DB, at time.Time, snapshot systemHealthSnapshot, anomalies []systemAnomaly) (models.SystemAutopilotRun, []models.SystemAutopilotAction) {
	t.Helper()
	run, actions, err := runSystemHealthAutopilotWithDeps(db, systemAutopilotRunOptions{Trigger: "test"}, systemAutopilotDeps{
		now:     func() time.Time { return at },
		collect: func(*gorm.DB) (systemHealthSnapshot, []systemAnomaly) { return snapshot, anomalies },
	})
	if err != nil {
		t.Fatal(err)
	}
	return run, actions
}

func seedSystemHealthPolicy(t *testing.T, db *gorm.DB, mode string, mutate func(*models.SystemAutopilotPolicy)) models.SystemAutopilotPolicy {
	t.Helper()
	p := models.DefaultSystemAutopilotPolicy()
	p.Enabled, p.Mode = true, mode
	if mutate != nil {
		mutate(&p)
	}
	if err := db.Create(&p).Error; err != nil {
		t.Fatal(err)
	}
	return p
}

func TestSystemHealthDB_SingleBlipDoesNotOpenEpisode(t *testing.T) {
	db := systemHealthAutopilotTestDB(t)
	p := models.DefaultSystemAutopilotPolicy()
	p.Enabled, p.Mode, p.ConfirmProbes = true, models.SystemAutopilotModeObserve, 2
	if err := db.Create(&p).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	deps := systemAutopilotDeps{now: func() time.Time { return now }, collect: func(*gorm.DB) (systemHealthSnapshot, []systemAnomaly) {
		return systemHealthSnapshotAt(now, systemAnomaly{Key: "aggregation:service_down", Service: "aggregation", Verdict: models.SystemVerdictServiceDown, Severity: "critical", Summary: "Aggregation unavailable"})
	}}
	if _, _, err := runSystemHealthAutopilotWithDeps(db, systemAutopilotRunOptions{Trigger: "test"}, deps); err != nil {
		t.Fatal(err)
	}
	var episodes int64
	if err := db.Model(&models.SystemIncidentEpisode{}).Count(&episodes).Error; err != nil {
		t.Fatal(err)
	}
	if episodes != 0 {
		t.Fatalf("one unconfirmed blip opened %d episodes", episodes)
	}
}

func TestSystemHealthDB_HarnessSmoke(t *testing.T) {
	db := systemHealthAutopilotTestDB(t)
	p := models.DefaultSystemAutopilotPolicy()
	if err := db.Create(&p).Error; err != nil {
		t.Fatal(err)
	}
	var stored models.SystemAutopilotPolicy
	if err := db.Where("scope = ?", systemAutopilotScope).First(&stored).Error; err != nil {
		t.Fatal(err)
	}
}

func TestSystemHealthDB_ConfirmNOpensOneEpisode(t *testing.T) {
	db := systemHealthAutopilotTestDB(t)
	seedSystemHealthPolicy(t, db, models.SystemAutopilotModeObserve, func(p *models.SystemAutopilotPolicy) { p.ConfirmProbes = 2 })
	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	snapshot, anomalies := systemHealthFailure(now)
	runSystemHealthFixture(t, db, now, snapshot, anomalies)
	_, actions := runSystemHealthFixture(t, db, now.Add(time.Minute), snapshot, anomalies)
	var episodes int64
	if err := db.Model(&models.SystemIncidentEpisode{}).Count(&episodes).Error; err != nil {
		t.Fatal(err)
	}
	if episodes != 1 {
		t.Fatalf("episodes = %d, want 1", episodes)
	}
	if len(actions) == 0 {
		t.Fatal("confirmed incident must write an action ledger")
	}
}

func TestSystemHealthDB_ObserveNeverWritesSiblingPause(t *testing.T) {
	db := systemHealthAutopilotTestDB(t)
	seedSystemHealthPolicy(t, db, models.SystemAutopilotModeObserve, func(p *models.SystemAutopilotPolicy) { p.ConfirmProbes = 1 })
	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	snapshot, anomalies := systemHealthFailure(now)
	_, actions := runSystemHealthFixture(t, db, now, snapshot, anomalies)
	if len(actions) == 0 {
		t.Fatal("observe containment must record a would-pause action")
	}
	var policy models.PipelineAutopilotPolicy
	if err := db.Where("tenant_id = ?", defaultCirculationTenant).First(&policy).Error; err == nil && policy.PausedUntil != nil {
		t.Fatalf("observe mode wrote pipeline pause %v", policy.PausedUntil)
	}
}

func TestSystemHealthDB_ContainmentPauseStillObserves(t *testing.T) {
	db := systemHealthAutopilotTestDB(t)
	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	seedSystemHealthPolicy(t, db, models.SystemAutopilotModeSafeAuto, func(p *models.SystemAutopilotPolicy) {
		p.ConfirmProbes = 1
		until := now.Add(time.Hour)
		p.ContainmentPausedUntil = &until
	})
	snapshot, anomalies := systemHealthFailure(now)
	_, actions := runSystemHealthFixture(t, db, now, snapshot, anomalies)
	var episodes int64
	_ = db.Model(&models.SystemIncidentEpisode{}).Count(&episodes).Error
	if episodes != 1 {
		t.Fatalf("containment pause must not stop observation, episodes=%d", episodes)
	}
	foundWouldPause := false
	for _, action := range actions {
		foundWouldPause = foundWouldPause || action.Action == models.SystemAutopilotActionWouldPause
	}
	if !foundWouldPause {
		t.Fatalf("expected would_pause while containment is paused, got %+v", actions)
	}
}

func TestSystemHealthDB_ResolveMHealthyClosesEpisode(t *testing.T) {
	db := systemHealthAutopilotTestDB(t)
	seedSystemHealthPolicy(t, db, models.SystemAutopilotModeObserve, func(p *models.SystemAutopilotPolicy) { p.ConfirmProbes, p.ResolveProbes = 1, 3 })
	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	failure, anomalies := systemHealthFailure(now)
	runSystemHealthFixture(t, db, now, failure, anomalies)
	for i := 1; i <= 3; i++ {
		healthy, none := healthySystemSnapshotAt(now.Add(time.Duration(i) * time.Minute))
		runSystemHealthFixture(t, db, now.Add(time.Duration(i)*time.Minute), healthy, none)
	}
	var episode models.SystemIncidentEpisode
	if err := db.First(&episode).Error; err != nil {
		t.Fatal(err)
	}
	if episode.Status != models.SystemIncidentStatusResolved {
		t.Fatalf("status = %q, want resolved", episode.Status)
	}
}

func TestSystemHealthDB_UnknownProbeBreaksRecoveryStreak(t *testing.T) {
	db := systemHealthAutopilotTestDB(t)
	seedSystemHealthPolicy(t, db, models.SystemAutopilotModeObserve, func(p *models.SystemAutopilotPolicy) { p.ConfirmProbes, p.ResolveProbes = 1, 3 })
	now := time.Date(2026, 7, 13, 13, 0, 0, 0, time.UTC)
	failure, anomalies := systemHealthFailure(now)
	runSystemHealthFixture(t, db, now, failure, anomalies)
	healthy, none := healthySystemSnapshotAt(now.Add(time.Minute))
	runSystemHealthFixture(t, db, now.Add(time.Minute), healthy, none)
	unknown := healthy
	unknown.Timestamp = now.Add(2 * time.Minute).UTC().Format(time.RFC3339)
	for i := range unknown.Services {
		if unknown.Services[i].Name == "aggregation" {
			unknown.Services[i].Status = "unknown"
		}
	}
	unknown.Overall = "degraded"
	runSystemHealthFixture(t, db, now.Add(2*time.Minute), unknown, none)
	finalHealthy, finalNone := healthySystemSnapshotAt(now.Add(3 * time.Minute))
	runSystemHealthFixture(t, db, now.Add(3*time.Minute), finalHealthy, finalNone)
	var episode models.SystemIncidentEpisode
	if err := db.First(&episode).Error; err != nil {
		t.Fatal(err)
	}
	if episode.Status != models.SystemIncidentStatusRecovering {
		t.Fatalf("status = %q, want recovering after unknown probe broke the streak", episode.Status)
	}
}

func TestSystemHealthDB_FirstRelapseImmediatelyReopensEpisode(t *testing.T) {
	db := systemHealthAutopilotTestDB(t)
	seedSystemHealthPolicy(t, db, models.SystemAutopilotModeObserve, func(p *models.SystemAutopilotPolicy) { p.ConfirmProbes, p.ResolveProbes = 2, 3 })
	now := time.Date(2026, 7, 13, 14, 0, 0, 0, time.UTC)
	failure, anomalies := systemHealthFailure(now)
	runSystemHealthFixture(t, db, now, failure, anomalies)
	runSystemHealthFixture(t, db, now.Add(time.Minute), failure, anomalies)
	healthy, none := healthySystemSnapshotAt(now.Add(2 * time.Minute))
	runSystemHealthFixture(t, db, now.Add(2*time.Minute), healthy, none)
	// This relapse has no preceding matching failure, so it is not a fresh
	// confirmed anomaly. A recovering episode must still reopen immediately.
	runSystemHealthFixture(t, db, now.Add(3*time.Minute), failure, anomalies)
	var episode models.SystemIncidentEpisode
	if err := db.First(&episode).Error; err != nil {
		t.Fatal(err)
	}
	if episode.Status != models.SystemIncidentStatusOpen || episode.RecoveringSince != nil {
		t.Fatalf("relapse must immediately reopen the episode, got %+v", episode)
	}
	var timeline []map[string]interface{}
	if err := json.Unmarshal(episode.Timeline, &timeline); err != nil {
		t.Fatal(err)
	}
	if len(timeline) == 0 || timeline[len(timeline)-1]["transition"] != "relapsed" {
		t.Fatalf("expected a relapsed transition, got %+v", timeline)
	}
}

func TestSystemHealthDB_UnchangedIncidentDoesNotGrowTimeline(t *testing.T) {
	db := systemHealthAutopilotTestDB(t)
	seedSystemHealthPolicy(t, db, models.SystemAutopilotModeObserve, func(p *models.SystemAutopilotPolicy) { p.ConfirmProbes = 1 })
	now := time.Date(2026, 7, 13, 15, 0, 0, 0, time.UTC)
	failure, anomalies := systemHealthFailure(now)
	for i := 0; i < 500; i++ {
		at := now.Add(time.Duration(i) * time.Minute)
		failure.Timestamp = at.UTC().Format(time.RFC3339)
		runSystemHealthFixture(t, db, at, failure, anomalies)
	}
	var episode models.SystemIncidentEpisode
	if err := db.First(&episode).Error; err != nil {
		t.Fatal(err)
	}
	var timeline []map[string]interface{}
	if err := json.Unmarshal(episode.Timeline, &timeline); err != nil {
		t.Fatal(err)
	}
	if len(timeline) != 1 || timeline[0]["transition"] != "opened" {
		t.Fatalf("unchanged incident must retain only its opened transition, got %+v", timeline)
	}
}

func TestSystemHealthDB_FlapGuardCountsResolvedCyclesNotHumanCloses(t *testing.T) {
	db := systemHealthAutopilotTestDB(t)
	now := time.Date(2026, 7, 13, 16, 0, 0, 0, time.UTC)
	for i := 0; i < 3; i++ {
		if err := db.Create(&models.SystemIncidentEpisode{
			RootService: "aggregation", Verdict: models.SystemVerdictServiceDown, Status: models.SystemIncidentStatusClosedByHuman,
			Severity: "critical", FirstDetectedAt: now, LastSeenAt: now, ResolvedAt: &now,
		}).Error; err != nil {
			t.Fatal(err)
		}
	}
	anomaly := systemAnomaly{Key: "aggregation:service_down", Service: "aggregation", Verdict: models.SystemVerdictServiceDown, Severity: "critical"}
	if got := applySystemFlapGuard(db, []systemAnomaly{anomaly}, 3, now, func(models.SystemAutopilotAction) {}); len(got) != 1 {
		t.Fatalf("human closes must not count as flaps, got %+v", got)
	}
	for i := 0; i < 3; i++ {
		if err := db.Create(&models.SystemIncidentEpisode{
			RootService: "aggregation", Verdict: models.SystemVerdictServiceDown, Status: models.SystemIncidentStatusResolved,
			Severity: "critical", FirstDetectedAt: now, LastSeenAt: now, ResolvedAt: &now,
		}).Error; err != nil {
			t.Fatal(err)
		}
	}
	attention := 0
	if got := applySystemFlapGuard(db, []systemAnomaly{anomaly}, 3, now, func(action models.SystemAutopilotAction) {
		if action.Guardrail == models.SystemAutopilotGuardFlapping {
			attention++
		}
	}); len(got) != 0 || attention != 1 {
		t.Fatalf("three resolved cycles must freeze a fresh opening, got=%+v attention=%d", got, attention)
	}
}

func TestSystemHealthDB_HumanCloseWritesEpisodeAction(t *testing.T) {
	db := systemHealthAutopilotTestDB(t)
	now := time.Date(2026, 7, 13, 17, 0, 0, 0, time.UTC)
	ep := models.SystemIncidentEpisode{
		RootService: "aggregation", Verdict: models.SystemVerdictServiceDown, Status: models.SystemIncidentStatusOpen,
		Severity: "critical", FirstDetectedAt: now, LastSeenAt: now,
	}
	if err := db.Create(&ep).Error; err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(`{"reason":"operator verified recovery"}`))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Params = gin.Params{{Key: "id", Value: ep.PublicID.String()}}
	c.Set("db", db)
	c.Set(utils.AdminPrincipalContextKey, utils.AdminPrincipal{UserID: "admin", Email: "admin@example.test", TenantID: "default"})
	CloseSystemIncidentEpisode(c)
	if w.Code != http.StatusOK {
		t.Fatalf("close status = %d, body=%s", w.Code, w.Body.String())
	}
	var actions []models.SystemAutopilotAction
	if err := db.Where("episode_id = ? AND action = ?", ep.ID, models.SystemAutopilotActionCloseEpisode).Find(&actions).Error; err != nil {
		t.Fatal(err)
	}
	if len(actions) != 1 || actions[0].Reason != "operator verified recovery" {
		t.Fatalf("missing close episode ledger action: %+v", actions)
	}
}

func TestSystemHealthDB_AdvisoryLockExcludesSecondConnection(t *testing.T) {
	db := systemHealthAutopilotTestDB(t)
	releaseFirst, acquiredFirst := tryAcquireSystemAutopilotAdvisoryLock(db)
	if !acquiredFirst {
		t.Fatal("first advisory-lock acquisition failed")
	}
	defer releaseFirst()
	if releaseSecond, acquiredSecond := tryAcquireSystemAutopilotAdvisoryLock(db); acquiredSecond {
		releaseSecond()
		t.Fatal("second connection acquired the same System Health advisory lock")
	}
	releaseFirst()
	if releaseThird, acquiredThird := tryAcquireSystemAutopilotAdvisoryLock(db); !acquiredThird {
		t.Fatal("advisory lock did not release after the first run")
	} else {
		releaseThird()
	}
}

func TestSystemHealthDB_ContainmentUsesPerTenantExactOwnership(t *testing.T) {
	db := systemHealthAutopilotTestDB(t)
	now := time.Date(2026, 7, 13, 18, 0, 0, 0, time.UTC)
	ep := models.SystemIncidentEpisode{
		RootService: "aggregation", Verdict: models.SystemVerdictServiceDown, Status: models.SystemIncidentStatusOpen,
		Severity: "critical", FirstDetectedAt: now, LastSeenAt: now,
	}
	if err := db.Create(&ep).Error; err != nil {
		t.Fatal(err)
	}
	policy := models.DefaultSystemAutopilotPolicy()
	policy.Mode, policy.ContainmentTTLMinutes = models.SystemAutopilotModeSafeAuto, 60
	store := func(tx *gorm.DB, action models.SystemAutopilotAction) (models.SystemAutopilotAction, error) {
		action.RunID, action.StartedAt = 0, now
		action.FinishedAt = &now
		if err := tx.Create(&action).Error; err != nil {
			return action, err
		}
		return action, nil
	}
	anomaly := systemAnomaly{Key: "aggregation:service_down", Service: "aggregation", Verdict: models.SystemVerdictServiceDown, Severity: "critical"}
	applied, writeErrors := handleSystemContainment(db, policy, anomaly, &ep, now, store, func(models.SystemAutopilotAction) {})
	if !applied || writeErrors != 0 {
		t.Fatalf("containment result applied=%t errors=%d", applied, writeErrors)
	}
	var pipeline models.PipelineAutopilotPolicy
	if err := db.Where("tenant_id = ?", defaultCirculationTenant).First(&pipeline).Error; err != nil {
		t.Fatal(err)
	}
	if pipeline.PausedUntil == nil {
		t.Fatal("Pipeline policy was not paused")
	}
	ledger, legacy := readSystemContainmentLedger(ep.Containment)
	if legacy {
		t.Fatal("new containment must use per-tenant ownership ledger")
	}
	owned, ok := containmentEntry(ledger, "pipeline", defaultCirculationTenant)
	if !ok || owned.WrittenUntil == "" || owned.Outcome != "paused" {
		t.Fatalf("missing Pipeline tenant ownership: %+v", ledger)
	}
	resumeActions := []models.SystemAutopilotAction{}
	if errors := resumeRecoveredSystemContainment(db, policy, []models.SystemIncidentEpisode{ep}, now.Add(time.Minute), store, func(action models.SystemAutopilotAction) { resumeActions = append(resumeActions, action) }); errors != 0 {
		t.Fatalf("resume errors = %d", errors)
	}
	var resumedPipeline models.PipelineAutopilotPolicy
	if err := db.Where("tenant_id = ?", defaultCirculationTenant).First(&resumedPipeline).Error; err != nil {
		t.Fatal(err)
	}
	if resumedPipeline.PausedUntil != nil {
		t.Fatalf("exactly owned pause was not resumed: %v; episode=%d actions=%+v", resumedPipeline.PausedUntil, ep.ID, resumeActions)
	}
}

func TestSystemHealthDB_QueueBacklogIsAttentionOnly(t *testing.T) {
	db := systemHealthAutopilotTestDB(t)
	seedSystemHealthPolicy(t, db, models.SystemAutopilotModeObserve, nil)
	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	anomaly := systemAnomaly{
		Key: "aggregation:queue_backlog", Service: "aggregation", Verdict: models.SystemVerdictQueueBacklog,
		Severity: "warning", Summary: "Aggregation queue is backed up",
	}
	for i := 0; i < 3; i++ {
		at := now.Add(time.Duration(i) * time.Minute)
		snapshot, anomalies := systemHealthSnapshotAt(at, anomaly)
		runSystemHealthFixture(t, db, at, snapshot, anomalies)
	}
	var episodes int64
	if err := db.Model(&models.SystemIncidentEpisode{}).Count(&episodes).Error; err != nil {
		t.Fatal(err)
	}
	if episodes != 0 {
		t.Fatalf("queue backlog opened %d incident episodes", episodes)
	}
	var attention int64
	if err := db.Model(&models.SystemAutopilotAction{}).
		Where("guardrail = ? AND status = ?", models.SystemAutopilotGuardQueueBacklogNoIncident, "attention").
		Count(&attention).Error; err != nil {
		t.Fatal(err)
	}
	if attention != 1 {
		t.Fatalf("queue backlog attention actions = %d, want 1", attention)
	}
}

func TestSystemHealthDB_EmptyRunHistoryDoesNotConfirm(t *testing.T) {
	db := systemHealthAutopilotTestDB(t)
	seedSystemHealthPolicy(t, db, models.SystemAutopilotModeObserve, func(p *models.SystemAutopilotPolicy) { p.ConfirmProbes = 2 })
	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	finished := now.Add(-time.Minute)
	if err := db.Create(&models.SystemAutopilotRun{
		Trigger: "test", Mode: models.SystemAutopilotModeObserve, Status: models.SystemAutopilotRunStatusCompleted,
		Headline: models.SystemAutopilotHeadlineWatching, StartedAt: finished, FinishedAt: &finished,
		// JSONB rejects malformed payloads at the database boundary. An empty
		// object is the persisted equivalent: it has no run_snapshot and must
		// not contribute to a confirmation streak.
		ProbeResults: []byte(`{}`), ErrorClass: models.SystemAutopilotErrorClassNone,
	}).Error; err != nil {
		t.Fatal(err)
	}
	snapshot, anomalies := systemHealthFailure(now)
	runSystemHealthFixture(t, db, now, snapshot, anomalies)
	var episodes int64
	if err := db.Model(&models.SystemIncidentEpisode{}).Count(&episodes).Error; err != nil {
		t.Fatal(err)
	}
	if episodes != 0 {
		t.Fatalf("empty history confirmed %d incidents", episodes)
	}
}

func TestSystemHealthDB_WorkerStallNeverPausesSiblings(t *testing.T) {
	db := systemHealthAutopilotTestDB(t)
	seedSystemHealthPolicy(t, db, models.SystemAutopilotModeSafeAuto, func(p *models.SystemAutopilotPolicy) { p.ConfirmProbes = 1 })
	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	anomaly := systemAnomaly{
		Key: "media:worker_stalled", Service: "media", Verdict: models.SystemVerdictWorkerStalled,
		Severity: "critical", Summary: "Media worker is stalled",
	}
	snapshot, anomalies := systemHealthSnapshotAt(now, anomaly)
	_, actions := runSystemHealthFixture(t, db, now, snapshot, anomalies)
	var episodes int64
	if err := db.Model(&models.SystemIncidentEpisode{}).Count(&episodes).Error; err != nil {
		t.Fatal(err)
	}
	if episodes != 1 {
		t.Fatalf("worker stall episodes = %d, want 1", episodes)
	}
	for _, action := range actions {
		if action.Action == models.SystemAutopilotActionPauseSibling {
			t.Fatalf("worker stall must not pause sibling autopilots: %+v", actions)
		}
	}
}
