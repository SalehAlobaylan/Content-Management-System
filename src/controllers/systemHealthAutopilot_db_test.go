package controllers

import (
	"os"
	"strings"
	"testing"
	"time"

	"content-management-system/src/models"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Opt-in lifecycle tests use a disposable Postgres only. Never point this at a
// shared database: the fixture migrates and clears the listed tables.
func systemHealthAutopilotTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := strings.TrimSpace(os.Getenv("SYSTEM_HEALTH_TEST_DATABASE_URL"))
	if dsn == "" {
		t.Skip("set SYSTEM_HEALTH_TEST_DATABASE_URL to run System Health PostgreSQL tests")
	}
	if !strings.Contains(strings.ToLower(dsn), "test") {
		t.Fatal("SYSTEM_HEALTH_TEST_DATABASE_URL must name a disposable test database")
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	if err := db.Exec("CREATE EXTENSION IF NOT EXISTS pgcrypto").Error; err != nil {
		t.Fatal(err)
	}
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
