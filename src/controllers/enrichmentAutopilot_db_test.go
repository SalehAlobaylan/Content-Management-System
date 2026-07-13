package controllers

import (
	"os"
	"strings"
	"testing"
	"time"

	"content-management-system/src/models"

	"github.com/google/uuid"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// These opt-in tests cover persisted Enrichment Autopilot invariants. They
// migrate and clear only a disposable Postgres database; set
// ENRICHMENT_AUTOPILOT_TEST_DATABASE_URL to a DSN containing "test" to run.
func enrichmentAutopilotTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := strings.TrimSpace(os.Getenv("ENRICHMENT_AUTOPILOT_TEST_DATABASE_URL"))
	if dsn == "" {
		t.Skip("set ENRICHMENT_AUTOPILOT_TEST_DATABASE_URL to run Enrichment Autopilot PostgreSQL tests")
	}
	if !strings.Contains(strings.ToLower(dsn), "test") {
		t.Fatal("ENRICHMENT_AUTOPILOT_TEST_DATABASE_URL must name a disposable test database")
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	if err := db.Exec("CREATE EXTENSION IF NOT EXISTS vector").Error; err != nil {
		t.Fatalf("enable vector extension: %v", err)
	}
	if err := db.Exec("CREATE EXTENSION IF NOT EXISTS pgcrypto").Error; err != nil {
		t.Fatalf("enable pgcrypto extension: %v", err)
	}
	if err := db.AutoMigrate(&models.ContentItem{}, &models.TranscriptionConfig{}, &models.TranscriptionJob{}, &models.EnrichmentAutopilotPolicy{}, &models.EnrichmentAutopilotRun{}, &models.EnrichmentAutopilotAction{}, &models.AuditLog{}); err != nil {
		t.Fatalf("migrate autopilot test schema: %v", err)
	}
	clear := func() {
		for _, table := range []string{"enrichment_autopilot_actions", "enrichment_autopilot_runs", "enrichment_autopilot_policies", "transcription_jobs", "transcription_configs", "content_items", "audit_logs"} {
			_ = db.Exec("DELETE FROM " + table).Error
		}
	}
	clear()
	t.Cleanup(clear)
	return db
}

func seedEnrichmentPolicy(t *testing.T, db *gorm.DB, tenant, mode string) models.EnrichmentAutopilotPolicy {
	t.Helper()
	p := models.DefaultEnrichmentAutopilotPolicy(tenant)
	p.Enabled, p.Mode = true, mode
	if err := db.Create(&p).Error; err != nil {
		t.Fatalf("create policy: %v", err)
	}
	return p
}

func seedEnrichmentContent(t *testing.T, db *gorm.DB, duration *int) models.ContentItem {
	t.Helper()
	title, media := "test media", "https://example.test/media.mp3"
	item := models.ContentItem{PublicID: uuid.New(), TenantID: "default", Type: models.ContentTypePodcast, Source: models.SourceTypePodcast, Status: models.ContentStatusReady, Title: &title, MediaURL: &media, DurationSec: duration, CreatedAt: time.Now().Add(-time.Hour)}
	if err := db.Create(&item).Error; err != nil {
		t.Fatalf("create content: %v", err)
	}
	return item
}

func TestEnrichmentAutopilotDB_InFlightSTTDedup(t *testing.T) {
	db := enrichmentAutopilotTestDB(t)
	item := seedEnrichmentContent(t, db, nil)
	job := models.TranscriptionJob{PublicID: uuid.New(), TenantID: item.TenantID, ContentItemID: item.PublicID, TriggerSource: models.TranscriptionTriggerEnrichmentAutopilot, Status: models.TranscriptionJobStatusQueued}
	if err := db.Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	if !hasActiveTranscriptionJob(db, item.PublicID) {
		t.Fatal("queued job must suppress another autopilot STT trigger")
	}
}

func TestEnrichmentAutopilotDB_TranscriptScopeAndTrustReset(t *testing.T) {
	db := enrichmentAutopilotTestDB(t)
	seedEnrichmentPolicy(t, db, "default", models.EnrichmentAutopilotModeObserve)
	long := 3601
	seedEnrichmentContent(t, db, &long)
	run := models.EnrichmentAutopilotRun{TenantID: "default", Trigger: "test", Mode: models.EnrichmentAutopilotModeObserve, Status: models.EnrichmentAutopilotRunStatusCompleted, StartedAt: time.Now()}
	if err := db.Create(&run).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&models.EnrichmentAutopilotAction{RunID: run.ID, TenantID: "default", Artifact: models.EnrichmentArtifactTranscript, Status: models.EnrichmentAutopilotActionStatusError, StartedAt: time.Now()}).Error; err != nil {
		t.Fatal(err)
	}
	var count int64
	if err := buildMissingQuery(db, models.EnrichmentArtifactTranscript, "VIDEO,PODCAST", "READY").Where("(duration_sec IS NULL OR duration_sec <= 2400)").Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("long parents must be excluded from transcript candidates, got %d", count)
	}
	if err := db.Model(&models.EnrichmentAutopilotAction{}).Where("run_id = ?", run.ID).Update("status", models.EnrichmentAutopilotActionStatusErrorAcknowledged).Error; err != nil {
		t.Fatal(err)
	}
	trust := computeEnrichmentTrust(db, "default", models.DefaultEnrichmentAutopilotPolicy("default"))
	if trust[models.EnrichmentArtifactTranscript].Attempts != 0 {
		t.Fatal("acknowledged failures must leave the trust window")
	}
}
