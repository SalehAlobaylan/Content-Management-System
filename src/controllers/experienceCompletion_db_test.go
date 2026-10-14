package controllers

import (
	"content-management-system/src/experiencequalification"
	"content-management-system/src/models"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func seedExperienceTerminal(t *testing.T, db *gorm.DB, tenant, phase, outcome string, at time.Time, session string) models.ExperienceEvent {
	t.Helper()
	ev := terminalFixture(phase, outcome)
	ev.TenantID = tenant
	ev.SessionID = session
	ev.ReceivedAt = at
	ev.OccurredAt = at
	ev.Measurements = jsonBytes(map[string]any{"duration_ms": *ev.DurationMS, "visible": true})
	if _, err := persistExperienceEvent(db, &ev); err != nil {
		t.Fatal(err)
	}
	return ev
}
func experienceDatabaseCases(t *testing.T, db *gorm.DB) {
	t.Run("duplicate_and_conflicting_terminals", func(t *testing.T) {
		tenant := "rux-" + uuid.NewString()
		ev := seedExperienceTerminal(t, db, tenant, "startup", "success", time.Now().Add(-2*time.Hour), "tab")
		ev.ID = 0
		ev.EventID = uuid.New()
		disposition, err := persistExperienceEvent(db, &ev)
		if err != nil || disposition != "duplicate" {
			t.Fatalf("retry %s %v", disposition, err)
		}
		ev.ID = 0
		ev.EventID = uuid.New()
		ev.Outcome = "failed"
		disposition, err = persistExperienceEvent(db, &ev)
		if err != nil || disposition != "conflict" {
			t.Fatalf("conflict %s %v", disposition, err)
		}
		valid, err := bucketIntegrity(db, tenant, "pods", floorBucket(ev.ReceivedAt))
		if err != nil || valid {
			t.Fatal("conflict did not withhold telemetry")
		}
	})
	t.Run("common_hourly_population", func(t *testing.T) {
		tenant := "rux-" + uuid.NewString()
		bucket := floorBucket(time.Now().Add(-2 * time.Hour))
		at := bucket.Add(time.Minute)
		seedExperienceTerminal(t, db, tenant, "feed", "success", at, "long-lived")
		seedExperienceTerminal(t, db, tenant, "feed", "success", at, "second-tab")
		ev := terminalFixture("feed", "failed")
		ev.TenantID = tenant
		ev.EventType = "client_failure"
		ev.JourneyID = nil
		ev.Phase = ""
		ev.Outcome = ""
		ev.SessionID = "long-lived"
		ev.ReceivedAt = at.Add(time.Minute)
		if err := db.Create(&ev).Error; err != nil {
			t.Fatal(err)
		}
		if _, err := rollupBucket(db, tenant, bucket, map[string]bool{"r2": true}, 1); err != nil {
			t.Fatal(err)
		}
		var row models.ExperienceMetricRollup
		if err := db.Where("tenant_id=? AND metric_key=? AND cohort_dim='global'", tenant, mErrorFreeSessions).First(&row).Error; err != nil {
			t.Fatal(err)
		}
		if row.Numerator != 1 || row.Denominator != 2 || row.AffectedSessions != 1 {
			t.Fatalf("population %d/%d affected%d", row.Numerator, row.Denominator, row.AffectedSessions)
		}
	})
	t.Run("checkpoint_rollback_and_replay", func(t *testing.T) {
		tenant := "rux-" + uuid.NewString()
		policy := models.DefaultExperiencePolicy(tenant)
		policy.EnabledSurfaces = "pods"
		if err := db.Create(&policy).Error; err != nil {
			t.Fatal(err)
		}
		bucket := floorBucket(time.Now().Add(-time.Hour))
		for i := 0; i < 60; i++ {
			seedExperienceTerminal(t, db, tenant, "startup", "failed", bucket.Add(time.Minute), "tab-"+uuid.NewString())
		}
		injected := errors.New("injected checkpoint failure")
		callback := "rux_checkpoint_injection_" + uuid.NewString()
		db.Callback().Update().Before("gorm:update").Register(callback, func(tx *gorm.DB) {
			if tx.Statement.Table == "experience_policies" && strings.Contains(tx.Statement.SQL.String(), "last_evaluated_bucket") {
				tx.AddError(injected)
			}
		})
		// GORM callbacks before SQL construction inspect the update map instead.
		db.Callback().Update().Remove(callback)
		db.Callback().Update().Before("gorm:update").Register(callback, func(tx *gorm.DB) {
			if tx.Statement.Table == "experience_policies" {
				if values, ok := tx.Statement.Dest.(map[string]interface{}); ok {
					if _, yes := values["last_evaluated_bucket"]; yes {
						tx.AddError(injected)
					}
				}
			}
		})
		failedRun, err := RunExperienceEvaluation(db, tenant, "fixture")
		db.Callback().Update().Remove(callback)
		if err == nil {
			t.Fatal("injection was not reached")
		}
		if failedRun.Status != models.RuxRunFailed || failedRun.BucketsProcessed != 0 || failedRun.WindowStart != nil {
			t.Fatal("failed run claimed a rolled-back decision window")
		}
		for _, table := range []string{"experience_metric_rollups", "experience_actions"} {
			var count int64
			if err := db.Table(table).Where("tenant_id=?", tenant).Count(&count).Error; err != nil || count != 0 {
				t.Fatalf("rolled-back %s persisted %d rows: %v", table, count, err)
			}
		}
		if err := db.Where("tenant_id=?", tenant).First(&policy).Error; err != nil || policy.LastEvaluatedBucket != nil {
			t.Fatal("failed window advanced its checkpoint")
		}
		var count int64
		if err := db.Model(&models.ExperienceIncident{}).Where("tenant_id=?", tenant).Count(&count).Error; err != nil || count != 0 {
			t.Fatalf("failed window committed %d incidents %v", count, err)
		}
		if _, err := RunExperienceEvaluation(db, tenant, "fixture"); err != nil {
			t.Fatal(err)
		}
		var first []models.ExperienceIncident
		if err := db.Where("tenant_id=?", tenant).Find(&first).Error; err != nil {
			t.Fatal(err)
		}
		if len(first) == 0 {
			t.Fatal("successful replay never evaluated the seeded failure")
		}
		for _, incident := range first {
			if incident.Status != "confirming" || incident.ViolationStreak != 1 {
				t.Fatal("one replayed bad hour did not remain watching")
			}
		}
		if _, err := RunExperienceEvaluation(db, tenant, "fixture"); err != nil {
			t.Fatal(err)
		}
		var second []models.ExperienceIncident
		if err := db.Where("tenant_id=?", tenant).Find(&second).Error; err != nil {
			t.Fatal(err)
		}
		if len(first) != len(second) {
			t.Fatal("replay changed incident population")
		}
		for i := range first {
			if first[i].ViolationStreak != second[i].ViolationStreak {
				t.Fatal("replay advanced streak")
			}
		}
	})
}
func TestExperienceDatabaseCompletion(t *testing.T) { experienceDatabaseCases(t, experienceTestDB(t)) }
func TestExperienceDatabaseConcurrentRunners(t *testing.T) {
	db := experienceTestDB(t)
	tenant := "rux-" + uuid.NewString()
	p := models.DefaultExperiencePolicy(tenant)
	p.EnabledSurfaces = "pods"
	if err := db.Create(&p).Error; err != nil {
		t.Fatal(err)
	}
	at := floorBucket(time.Now()).Add(-time.Hour + time.Minute)
	for i := 0; i < 60; i++ {
		seedExperienceTerminal(t, db, tenant, "startup", "failed", at, "tab-"+uuid.NewString())
	}
	var wg sync.WaitGroup
	failures := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := RunExperienceEvaluation(db, tenant, "concurrent-fixture")
			failures <- err
		}()
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		if err != nil {
			t.Fatal(err)
		}
	}
	var incidents []models.ExperienceIncident
	if err := db.Where("tenant_id=?", tenant).Find(&incidents).Error; err != nil || len(incidents) == 0 {
		t.Fatalf("concurrent fixture produced no evaluated incidents: %v", err)
	}
	for _, inc := range incidents {
		if inc.ViolationStreak != 1 || inc.ConfirmedAt != nil {
			t.Fatal("concurrent runners confirmed one window twice")
		}
	}
}
func TestExperienceCanonicalQualification(t *testing.T) {
	id := os.Getenv("RUX_CANONICAL_ENV")
	if id == "" {
		t.Skip("explicit disposable Neon qualification is run by scripts/real-experience-smoke.sh")
	}
	root, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	values, err := experiencequalification.LiteralProfile(root, id)
	if err != nil {
		t.Fatal(err)
	}
	if err := experiencequalification.Disposable(root, id, values); err != nil {
		t.Fatal(err)
	}
	dsn, err := experiencequalification.CMSURL(values)
	if err != nil {
		t.Fatal(err)
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal("canonical fixture database unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	tx := db.WithContext(ctx).Begin()
	if tx.Error != nil {
		t.Fatal("canonical fixture transaction unavailable")
	}
	defer tx.Rollback()
	for _, version := range []string{"20261013010000_real_experience_completion.sql", "20261013020000_real_experience_native.sql"} {
		canonical, err := os.ReadFile(filepath.Join(root, "Content-Management-System", "migrations", version))
		if err != nil {
			t.Fatal("canonical fixture SQL unavailable")
		}
		var recorded string
		if err := tx.Raw("SELECT checksum_sha256 FROM cms_schema_migrations WHERE version=?", version).Scan(&recorded).Error; err != nil || recorded != fmt.Sprintf("%x", sha256.Sum256(canonical)) {
			t.Fatal("canonical migration ledger does not match exact fixture SQL")
		}
	}
	if !tx.Migrator().HasTable(&models.ExperienceTerminalReceipt{}) || !tx.Migrator().HasIndex(&models.ExperienceIncident{}, "experience_incidents_active_fingerprint") {
		t.Fatal("canonical RUX postconditions absent")
	}
	experienceDatabaseCases(t, tx)
	nativeDatabasePopulationCases(t, tx)
}

func TestExperienceDatabaseLargeWindowResumes(t *testing.T) {
	db := experienceTestDB(t)
	tenant := "rux-" + uuid.NewString()
	policy := models.DefaultExperiencePolicy(tenant)
	policy.EnabledSurfaces = "pods"
	if err := db.Create(&policy).Error; err != nil {
		t.Fatal(err)
	}
	bucket := floorBucket(time.Now()).Add(-time.Hour)
	err := db.Exec(`INSERT INTO experience_events(event_id,tenant_id,schema_version,event_type,surface,occurred_at,received_at,session_id,page_load_id,sequence,release,browser_family,browser_major,device_class,network_class,duration_ms,visible,phase,outcome,terminal_status,journey_id)
 SELECT gen_random_uuid(),?,2,'journey_terminal','pods',?,?,('tab-' || i::text),gen_random_uuid(),i,'r2','safari',18,'mobile','unknown',1000,true,'feed','success','accepted',gen_random_uuid() FROM generate_series(1,200001) AS i`, tenant, bucket.Add(time.Minute), bucket.Add(time.Minute)).Error
	if err != nil {
		t.Fatal(err)
	}
	first, err := RunExperienceEvaluation(db, tenant, "large-fixture")
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != "partial" || first.BucketsProcessed != 0 {
		t.Fatalf("large window silently completed: %+v", first)
	}
	var progress models.ExperienceRollupProgress
	if err := db.Where("tenant_id=? AND resolution='hour'", tenant).First(&progress).Error; err != nil {
		t.Fatal(err)
	}
	if progress.EventsProcessed != 200000 {
		t.Fatal(progress.EventsProcessed)
	}
	if _, err := RunExperienceEvaluation(db, tenant, "large-fixture"); err != nil {
		t.Fatal(err)
	}
	var row models.ExperienceMetricRollup
	if err := db.Where("tenant_id=? AND metric_key=? AND cohort_dim='global' AND resolution='hour'", tenant, mFeedRenderSuccess).First(&row).Error; err != nil {
		t.Fatal(err)
	}
	if row.Denominator != 200001 || !row.Completed {
		t.Fatalf("resume missing evidence: %d complete%v", row.Denominator, row.Completed)
	}
}
