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

// Opt-in PostgreSQL integration tests for the RUX rollup + retention path. Run
// with EXPERIENCE_TEST_DATABASE_URL pointing at a disposable test DB.
func experienceTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := strings.TrimSpace(os.Getenv("EXPERIENCE_TEST_DATABASE_URL"))
	if dsn == "" {
		t.Skip("set EXPERIENCE_TEST_DATABASE_URL to run RUX PostgreSQL integration tests")
	}
	if !strings.Contains(strings.ToLower(dsn), "test") {
		t.Fatal("EXPERIENCE_TEST_DATABASE_URL must name a disposable test database")
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	if err := db.Exec("CREATE EXTENSION IF NOT EXISTS pgcrypto").Error; err != nil {
		t.Fatalf("enable pgcrypto: %v", err)
	}
	if err := db.AutoMigrate(&models.ExperienceEvent{}, &models.ExperienceMetricRollup{}, &models.ExperiencePolicy{},
		&models.ExperienceEvaluationRun{}, &models.ExperienceIncident{}, &models.ExperienceAction{}, &models.ExperienceSuppression{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	for _, tbl := range []string{"experience_events", "experience_metric_rollups", "experience_incidents", "experience_actions"} {
		_ = db.Exec("DELETE FROM " + tbl).Error
	}
	return db
}

func insertEvent(t *testing.T, db *gorm.DB, receivedAt time.Time, eventType, surface, release string, failureClass *string, dur *int) {
	t.Helper()
	ev := models.ExperienceEvent{
		EventID: uuid.New(), TenantID: "default", SchemaVersion: 1, EventType: eventType, Surface: surface,
		OccurredAt: receivedAt, ReceivedAt: receivedAt, SessionID: "s-" + uuid.NewString()[:8],
		PageLoadID: uuid.New(), Release: release, BrowserFamily: "safari", BrowserMajor: 18,
		DeviceClass: "mobile", NetworkClass: "unknown", FailureClass: failureClass, DurationMS: dur,
	}
	if err := db.Create(&ev).Error; err != nil {
		t.Fatalf("insert event: %v", err)
	}
}

func loadRollupRow(t *testing.T, db *gorm.DB, metric, surface string, bucket time.Time) models.ExperienceMetricRollup {
	t.Helper()
	var row models.ExperienceMetricRollup
	err := db.Where("tenant_id = ? AND bucket_start = ? AND metric_key = ? AND surface = ? AND cohort_dim = ? AND cohort_val = ?",
		"default", bucket, metric, surface, "global", "all").First(&row).Error
	if err != nil {
		t.Fatalf("load rollup %s/%s: %v", metric, surface, err)
	}
	return row
}

func TestRollup_TerminalOutcomeCounting_And_Idempotency(t *testing.T) {
	db := experienceTestDB(t)
	bucket := floorBucket(time.Now().UTC().Add(-2 * time.Hour))
	within := bucket.Add(10 * time.Minute)
	fatal := "media_error"
	autoplay := "autoplay_blocked"
	dur := 800

	for i := 0; i < 60; i++ {
		insertEvent(t, db, within, "playback_started", "foryou", "r1", nil, &dur)
	}
	for i := 0; i < 40; i++ {
		insertEvent(t, db, within, "playback_failed", "foryou", "r1", &fatal, nil)
	}
	for i := 0; i < 10; i++ {
		insertEvent(t, db, within, "playback_failed", "foryou", "r1", &autoplay, nil)
	}

	kept := map[string]bool{"r1": true}
	if _, err := rollupBucket(db, "default", bucket, kept, 100000); err != nil {
		t.Fatalf("rollup: %v", err)
	}

	start := loadRollupRow(t, db, mPlaybackStartSuccess, "foryou", bucket)
	if start.Numerator != 60 || start.Denominator != 100 {
		t.Errorf("start success: want 60/100, got %d/%d", start.Numerator, start.Denominator)
	}
	fatalRow := loadRollupRow(t, db, mPlaybackFatalRate, "foryou", bucket)
	if fatalRow.Numerator != 40 || fatalRow.Denominator != 100 {
		t.Errorf("fatal rate: want 40/100, got %d/%d", fatalRow.Numerator, fatalRow.Denominator)
	}
	autoRow := loadRollupRow(t, db, mAutoplayBlockedRate, "foryou", bucket)
	if autoRow.Numerator != 10 || autoRow.Denominator != 110 {
		t.Errorf("autoplay rate: want 10/110, got %d/%d", autoRow.Numerator, autoRow.Denominator)
	}

	// Re-run the same bucket — idempotent upsert must NOT double the counts.
	if _, err := rollupBucket(db, "default", bucket, kept, 100000); err != nil {
		t.Fatalf("rollup rerun: %v", err)
	}
	start2 := loadRollupRow(t, db, mPlaybackStartSuccess, "foryou", bucket)
	if start2.Numerator != 60 || start2.Denominator != 100 {
		t.Errorf("idempotency broken: want 60/100, got %d/%d", start2.Numerator, start2.Denominator)
	}
}

func TestRetention_DeletesRawKeepsRollups(t *testing.T) {
	db := experienceTestDB(t)
	db.Create(&models.ExperiencePolicy{TenantID: "default", RawRetentionDays: 7, MinuteRollupRetentionHours: 48, HourRollupRetentionDays: 400, MaxReleaseCohorts: 4})

	oldBucket := floorBucket(time.Now().UTC().AddDate(0, 0, -10))
	old := oldBucket.Add(5 * time.Minute)
	dur := 500
	for i := 0; i < 55; i++ {
		insertEvent(t, db, old, "feed_rendered", "foryou", "r1", nil, &dur)
	}
	if _, err := rollupBucket(db, "default", oldBucket, map[string]bool{"r1": true}, 100000); err != nil {
		t.Fatalf("rollup: %v", err)
	}

	SweepExperienceRetention(db)

	var rawCount int64
	db.Model(&models.ExperienceEvent{}).Where("received_at < ?", time.Now().AddDate(0, 0, -7)).Count(&rawCount)
	if rawCount != 0 {
		t.Errorf("expected old raw events swept, %d remain", rawCount)
	}
	// The rollup for that bucket must survive (aggregate history is durable).
	row := loadRollupRow(t, db, mFeedRenderSuccess, "foryou", oldBucket)
	if row.Numerator != 55 {
		t.Errorf("rollup must survive retention, got numerator %d", row.Numerator)
	}
}
