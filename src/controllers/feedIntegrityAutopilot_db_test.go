package controllers

import (
	"encoding/json"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"content-management-system/src/models"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// These tests exercise PostgreSQL row claims and persisted ledger behavior. They
// are intentionally opt-in because they migrate and clear a disposable test DB.
// Run with FEED_INTEGRITY_TEST_DATABASE_URL pointing at a disposable test DB.
func feedIntegrityTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := strings.TrimSpace(os.Getenv("FEED_INTEGRITY_TEST_DATABASE_URL"))
	if dsn == "" {
		t.Skip("set FEED_INTEGRITY_TEST_DATABASE_URL to run PostgreSQL Feed Integrity integration tests")
	}
	if !strings.Contains(strings.ToLower(dsn), "test") {
		t.Fatal("FEED_INTEGRITY_TEST_DATABASE_URL must name a disposable test database")
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	if err := db.Exec("CREATE EXTENSION IF NOT EXISTS pgcrypto").Error; err != nil {
		t.Fatalf("enable pgcrypto in test database: %v", err)
	}
	if err := db.AutoMigrate(&models.FeedIntegrityPolicy{}, &models.FeedIntegrityRun{}, &models.FeedIntegrityFinding{}, &models.FeedIntegrityEpisode{}, &models.FeedIntegritySuppression{}, &models.FeedIntegrityAction{}, &models.NewsSnapshot{}, &models.AuditLog{}); err != nil {
		t.Fatalf("migrate feed integrity test schema: %v", err)
	}
	clear := func() {
		_ = db.Exec("DELETE FROM feed_integrity_actions").Error
		_ = db.Exec("DELETE FROM feed_integrity_suppressions").Error
		_ = db.Exec("DELETE FROM feed_integrity_findings").Error
		_ = db.Exec("DELETE FROM feed_integrity_episodes").Error
		_ = db.Exec("DELETE FROM feed_integrity_runs").Error
		_ = db.Exec("DELETE FROM feed_integrity_policies").Error
		_ = db.Exec("DELETE FROM news_snapshots").Error
		_ = db.Exec("DELETE FROM audit_logs").Error
	}
	clear()
	t.Cleanup(clear)
	return db
}

func seedFeedIntegrityAction(t *testing.T, db *gorm.DB, tenant string, outcome string) (models.FeedIntegrityRun, models.FeedIntegrityEpisode, models.FeedIntegrityAction) {
	t.Helper()
	now := time.Now().UTC()
	policy := models.DefaultFeedIntegrityPolicy(tenant)
	policy.ScheduledEnabled, policy.AutopilotEnabled, policy.AutopilotMode = true, true, models.FeedIntegrityAutopilotModeAssist
	if err := db.Create(&policy).Error; err != nil {
		t.Fatalf("create policy: %v", err)
	}
	run := models.FeedIntegrityRun{TenantID: tenant, Trigger: "test", Tier: models.FeedIntegrityTierLight, Status: models.FeedIntegrityRunCompleted, Headline: "all_clear", StartedAt: now.Add(-time.Minute), FinishedAt: &now, ErrorClass: "none", AutopilotErrorClass: "none"}
	if err := db.Create(&run).Error; err != nil {
		t.Fatalf("create run: %v", err)
	}
	episode := models.FeedIntegrityEpisode{TenantID: tenant, CheckKey: "edge_news_cache_stale", Axis: models.FeedIntegrityAxisConsumer, Feed: "news", Variant: "window:today", Scope: "today", Status: models.FeedIntegrityEpisodeOpen, Severity: "major", Summary: "test", FirstDetectedAt: now, LastSeenAt: now}
	if err := db.Create(&episode).Error; err != nil {
		t.Fatalf("create episode: %v", err)
	}
	action := models.FeedIntegrityAction{TenantID: tenant, RunID: run.ID, EpisodeID: episode.ID, ActionClass: models.FeedIntegrityActionRefreshWindow, OwnerSystem: "news-circulation", TargetScope: "today", Mode: models.FeedIntegrityAutopilotModeAssist, Outcome: outcome, Decision: models.FeedIntegrityDecisionReady, IdempotencyKey: tenant + "-claim-" + outcome, EvidenceFingerprint: "fingerprint-" + tenant, RegistryVersion: feedIntegrityRegistryVersion}
	if err := db.Create(&action).Error; err != nil {
		t.Fatalf("create action: %v", err)
	}
	return run, episode, action
}

func TestFeedIntegrityDBConcurrentClaimHasOneWinner(t *testing.T) {
	db := feedIntegrityTestDB(t)
	_, _, action := seedFeedIntegrityAction(t, db, "tenant-a", models.FeedIntegrityActionReady)
	var wg sync.WaitGroup
	results := make(chan bool, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := claimFeedIntegrityAction(db, "tenant-a", action.ID)
			results <- err == nil
		}()
	}
	wg.Wait()
	close(results)
	successes := 0
	for ok := range results {
		if ok {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("expected one successful claim, got %d", successes)
	}
	var stored models.FeedIntegrityAction
	if err := db.First(&stored, action.ID).Error; err != nil {
		t.Fatal(err)
	}
	if stored.Outcome != models.FeedIntegrityActionClaimed || stored.ClaimToken == "" {
		t.Fatalf("claim was not persisted: %+v", stored)
	}
}

func TestFeedIntegrityDBRejectCannotClaimAndTenantCannotCrossClaim(t *testing.T) {
	db := feedIntegrityTestDB(t)
	_, _, rejected := seedFeedIntegrityAction(t, db, "tenant-a", models.FeedIntegrityActionRejected)
	if _, err := claimFeedIntegrityAction(db, "tenant-a", rejected.ID); err == nil {
		t.Fatal("rejected action must not claim")
	}
	_, _, ready := seedFeedIntegrityAction(t, db, "tenant-b", models.FeedIntegrityActionReady)
	if _, err := claimFeedIntegrityAction(db, "tenant-a", ready.ID); err == nil {
		t.Fatal("tenant A must not claim tenant B action")
	}
}

func TestFeedIntegrityDBExpiredClaimRecoversForOneLaterWorker(t *testing.T) {
	db := feedIntegrityTestDB(t)
	_, _, action := seedFeedIntegrityAction(t, db, "tenant-a", models.FeedIntegrityActionReady)
	claimed, err := claimFeedIntegrityAction(db, "tenant-a", action.ID)
	if err != nil {
		t.Fatalf("initial claim: %v", err)
	}
	expired := time.Now().UTC().Add(-time.Second)
	if err := db.Model(&claimed).Updates(map[string]interface{}{"claim_expires_at": expired}).Error; err != nil {
		t.Fatalf("expire crashed-worker lease: %v", err)
	}
	recoverExpiredFeedIntegrityClaims(db, time.Now().UTC())

	var recovered models.FeedIntegrityAction
	if err := db.First(&recovered, action.ID).Error; err != nil {
		t.Fatal(err)
	}
	if recovered.Outcome != models.FeedIntegrityActionReady || recovered.ClaimToken != "" || recovered.ClaimExpiresAt != nil || recovered.Guardrail != "stale_claim_recovered" {
		t.Fatalf("expired claim was not safely recovered: %+v", recovered)
	}
	if _, err := claimFeedIntegrityAction(db, "tenant-a", action.ID); err != nil {
		t.Fatalf("later worker should reclaim recovered action: %v", err)
	}
	if _, err := claimFeedIntegrityAction(db, "tenant-a", action.ID); err == nil {
		t.Fatal("only one later worker may reclaim the recovered action")
	}
}

func TestFeedIntegrityDBDualEvidenceAndObserveEvaluationDoNotMutateSnapshot(t *testing.T) {
	db := feedIntegrityTestDB(t)
	now := time.Now().UTC()
	policy := models.DefaultFeedIntegrityPolicy("tenant-a")
	policy.ScheduledEnabled, policy.AutopilotEnabled, policy.AutopilotMode = true, true, models.FeedIntegrityAutopilotModeObserve
	if err := db.Create(&policy).Error; err != nil {
		t.Fatal(err)
	}
	run := models.FeedIntegrityRun{TenantID: "tenant-a", Trigger: "test", Tier: models.FeedIntegrityTierLight, Status: models.FeedIntegrityRunCompleted, Headline: "watching", StartedAt: now.Add(-time.Minute), FinishedAt: &now, ErrorClass: "none", AutopilotErrorClass: "none"}
	if err := db.Create(&run).Error; err != nil {
		t.Fatal(err)
	}
	snapshot := models.NewsSnapshot{TenantID: "tenant-a", Window: "today", SlideCount: 1, BuiltAt: now.Add(-time.Minute), Dirty: true}
	if err := db.Create(&snapshot).Error; err != nil {
		t.Fatal(err)
	}
	findings := []models.FeedIntegrityFinding{
		{RunID: run.ID, TenantID: "tenant-a", Lane: "inventory", CheckKey: "inv_news_cache_rebuild_debt", Axis: models.FeedIntegrityAxisReadiness, Feed: "news", Variant: "today", TargetType: "snapshot", TargetRef: "today", CandidateCount: 1, Status: "violation", Severity: "minor"},
		{RunID: run.ID, TenantID: "tenant-a", Lane: "edge", CheckKey: "edge_news_cache_stale", Axis: models.FeedIntegrityAxisConsumer, Feed: "news", Variant: "window:today", TargetType: "snapshot", TargetRef: "today", CandidateCount: 1, Status: "violation", Severity: "major"},
	}
	if err := db.Create(&findings).Error; err != nil {
		t.Fatal(err)
	}
	if !feedIntegritySnapshotDualEvidence(db, run.ID, "window:today") {
		t.Fatal("matching inventory and edge evidence must satisfy dual evidence")
	}
	episode := models.FeedIntegrityEpisode{TenantID: "tenant-a", CheckKey: "edge_news_cache_stale", Axis: models.FeedIntegrityAxisConsumer, Feed: "news", Variant: "window:today", Scope: "today", Status: models.FeedIntegrityEpisodeOpen, Severity: "major", Summary: "stale", FirstDetectedAt: now, LastSeenAt: now}
	if err := db.Create(&episode).Error; err != nil {
		t.Fatal(err)
	}
	if err := evaluateFeedIntegrityAutopilot(db, run.ID); err != nil {
		t.Fatalf("evaluate observe run: %v", err)
	}
	var after models.NewsSnapshot
	if err := db.Where("tenant_id=? AND \"window\"=?", "tenant-a", "today").First(&after).Error; err != nil {
		t.Fatal(err)
	}
	if !after.BuiltAt.Equal(snapshot.BuiltAt) || !after.Dirty {
		t.Fatal("Observe evaluation must not refresh or alter snapshots")
	}
	var action models.FeedIntegrityAction
	if err := db.Where("run_id=?", run.ID).First(&action).Error; err != nil {
		t.Fatalf("observe evaluation must write its ledger action: %v", err)
	}
	if action.Outcome != models.FeedIntegrityActionWouldExecute {
		t.Fatalf("observe evaluation must not execute, got %q", action.Outcome)
	}
}

func TestFeedIntegrityDBOverflowCreatesAggregateBacklogEpisode(t *testing.T) {
	db := feedIntegrityTestDB(t)
	now := time.Now().UTC()
	policy := models.DefaultFeedIntegrityPolicy("tenant-a")
	policy.ConfirmRuns = 1
	if err := db.Create(&policy).Error; err != nil {
		t.Fatal(err)
	}
	run := models.FeedIntegrityRun{TenantID: "tenant-a", Trigger: "test", Tier: models.FeedIntegrityTierLight, Status: models.FeedIntegrityRunCompleted, Headline: "watching", StartedAt: now.Add(-time.Minute), FinishedAt: &now, ErrorClass: "none", AutopilotErrorClass: "none"}
	if err := db.Create(&run).Error; err != nil {
		t.Fatal(err)
	}
	findings := make([]models.FeedIntegrityFinding, 0, 25)
	for i := 0; i < 25; i++ {
		findings = append(findings, models.FeedIntegrityFinding{RunID: run.ID, TenantID: "tenant-a", Lane: "edge", CheckKey: "edge_fy_bounds_served", Axis: models.FeedIntegrityAxisConsumer, Feed: "foryou", Variant: "default", TargetType: "content_item", TargetRef: "item-" + string(rune('a'+i)), CandidateCount: 1, Status: "violation", Severity: "major"})
	}
	if err := db.Create(&findings).Error; err != nil {
		t.Fatal(err)
	}
	updateFeedIntegrityEpisodes(db, "tenant-a", policy, run, findings)
	var regular int64
	db.Model(&models.FeedIntegrityEpisode{}).Where("tenant_id=? AND check_key=? AND scope NOT LIKE ?", "tenant-a", "edge_fy_bounds_served", "aggregate:%").Count(&regular)
	if regular != 20 {
		t.Fatalf("expected 20 individual episodes, got %d", regular)
	}
	var aggregate models.FeedIntegrityEpisode
	if err := db.Where("tenant_id=? AND check_key=? AND scope LIKE ?", "tenant-a", "edge_fy_bounds_served", "aggregate:%").First(&aggregate).Error; err != nil {
		t.Fatalf("expected aggregate overflow episode: %v", err)
	}
	var evidence struct {
		AffectedCount int  `json:"affected_count"`
		Overflow      bool `json:"overflow"`
	}
	if err := json.Unmarshal(aggregate.Evidence, &evidence); err != nil {
		t.Fatal(err)
	}
	if !evidence.Overflow || evidence.AffectedCount != 5 {
		t.Fatalf("unexpected overflow evidence: %+v", evidence)
	}
}
