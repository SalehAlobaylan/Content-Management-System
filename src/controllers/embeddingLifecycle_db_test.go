package controllers

import (
	"os"
	"strings"
	"testing"

	"content-management-system/src/models"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Opt-in PostgreSQL integration tests for the Embedding & Model Lifecycle System
// (stage 10 Slice 6). These exercise the DB-enforced invariants that unit tests
// cannot: one-non-terminal-campaign-per-space, unique action ownership (the
// no-double-claim guarantee), and write fencing. Run with
// EMBEDDING_LIFECYCLE_TEST_DATABASE_URL pointing at a DISPOSABLE test DB.
func embeddingLifecycleTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := strings.TrimSpace(os.Getenv("EMBEDDING_LIFECYCLE_TEST_DATABASE_URL"))
	if dsn == "" {
		t.Skip("set EMBEDDING_LIFECYCLE_TEST_DATABASE_URL to run PostgreSQL lifecycle integration tests")
	}
	if !strings.Contains(strings.ToLower(dsn), "test") {
		t.Fatal("EMBEDDING_LIFECYCLE_TEST_DATABASE_URL must name a disposable test database")
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	if err := db.AutoMigrate(
		&models.EmbeddingLifecyclePolicy{}, &models.EmbeddingLifecycleRun{}, &models.EmbeddingLifecycleFinding{},
		&models.EmbeddingCampaign{}, &models.EmbeddingCampaignAction{}, &models.EmbeddingCampaignException{},
	); err != nil {
		t.Fatalf("migrate lifecycle schema: %v", err)
	}
	// GORM cannot express a PARTIAL unique index via struct tags, so the
	// one-non-terminal-campaign-per-space constraint lives only in the deliberate
	// SQL migration (20260711050000_embedding_campaigns.sql). Apply the identical
	// DDL here so the harness verifies the same invariant production enforces.
	if err := db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_embedding_campaign_active_space
		ON embedding_campaigns (space)
		WHERE state IN ('draft','running','paused','blocked','verifying')`).Error; err != nil {
		t.Fatalf("apply partial unique index: %v", err)
	}
	clear := func() {
		for _, tbl := range []string{
			"embedding_campaign_actions", "embedding_campaign_exceptions", "embedding_campaigns",
			"embedding_lifecycle_findings", "embedding_lifecycle_runs", "embedding_lifecycle_policies",
		} {
			_ = db.Exec("DELETE FROM " + tbl).Error
		}
	}
	clear()
	t.Cleanup(clear)
	return db
}

// TestOneNonTerminalCampaignPerSpace proves the partial-unique index forbids a
// second active campaign for the same space.
func TestOneNonTerminalCampaignPerSpace(t *testing.T) {
	db := embeddingLifecycleTestDB(t)
	first := models.EmbeddingCampaign{TenantID: "default", Space: "text", State: models.EmbeddingCampaignRunning, TargetSpaceID: "aaa"}
	if err := db.Create(&first).Error; err != nil {
		t.Fatalf("first campaign create: %v", err)
	}
	second := models.EmbeddingCampaign{TenantID: "default", Space: "text", State: models.EmbeddingCampaignDraft, TargetSpaceID: "bbb"}
	if err := db.Create(&second).Error; err == nil {
		t.Fatal("expected unique-violation creating a second non-terminal campaign for the same space")
	}
	// A terminal campaign for the same space is allowed (does not collide).
	done := models.EmbeddingCampaign{TenantID: "default", Space: "text", State: models.EmbeddingCampaignCompleted, TargetSpaceID: "ccc"}
	if err := db.Create(&done).Error; err != nil {
		t.Fatalf("terminal campaign for same space should be allowed: %v", err)
	}
}

// TestUniqueActionOwnership proves two runners cannot both claim the same target
// for the same (campaign, surface, producer).
func TestUniqueActionOwnership(t *testing.T) {
	db := embeddingLifecycleTestDB(t)
	camp := models.EmbeddingCampaign{TenantID: "default", Space: "text", State: models.EmbeddingCampaignRunning, TargetSpaceID: "aaa"}
	if err := db.Create(&camp).Error; err != nil {
		t.Fatalf("campaign: %v", err)
	}
	claim := models.EmbeddingCampaignAction{
		CampaignID: camp.ID, TenantID: "default", SurfaceKey: "content_text",
		TargetID: "item-1", ExpectedProducerID: "prod-x", Status: models.EmbeddingActionAttempted,
	}
	if err := db.Create(&claim).Error; err != nil {
		t.Fatalf("first claim should succeed: %v", err)
	}
	dup := claim
	dup.ID = 0
	if err := db.Create(&dup).Error; err == nil {
		t.Fatal("expected unique-violation on a duplicate ownership claim (double-claim must be impossible)")
	}
	// A different producer (a re-run to a new target) is a distinct ownership.
	other := claim
	other.ID = 0
	other.ExpectedProducerID = "prod-y"
	if err := db.Create(&other).Error; err != nil {
		t.Fatalf("distinct-producer claim should be allowed: %v", err)
	}
	// A later retry to the same producer is distinct, while concurrent runners
	// still collide on the same retry number.
	retry := claim
	retry.ID = 0
	retry.RetryNumber = 1
	if err := db.Create(&retry).Error; err != nil {
		t.Fatalf("next retry attempt should be allowed: %v", err)
	}
}

// TestFenceEmbeddingWrite proves the write fence blocks stale/missing producers
// only while a campaign is running, and never otherwise.
func TestFenceEmbeddingWrite(t *testing.T) {
	db := embeddingLifecycleTestDB(t)
	// No campaign → never blocked.
	if _, blocked := fenceEmbeddingWrite(db, "text", "content-title-excerpt-body:v1", "", ""); blocked {
		t.Fatal("no active campaign must never block a write")
	}
	// Running campaign → fence active.
	camp := models.EmbeddingCampaign{TenantID: "default", Space: "text", State: models.EmbeddingCampaignRunning, TargetSpaceID: "space-1"}
	if err := db.Create(&camp).Error; err != nil {
		t.Fatalf("campaign: %v", err)
	}
	if _, blocked := fenceEmbeddingWrite(db, "text", "content-title-excerpt-body:v1", "", ""); !blocked {
		t.Fatal("running campaign must block a write with no producer")
	}
	if _, blocked := fenceEmbeddingWrite(db, "text", "content-title-excerpt-body:v1", "space-1", "wrong-producer"); !blocked {
		t.Fatal("running campaign must block a write with a stale producer")
	}
}
