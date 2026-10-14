package controllers

import (
	"content-management-system/src/models"
	"errors"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"strings"
	"testing"
	"time"
)

func TestExperienceBulkRollup_PageUsesThreeStatements(t *testing.T) {
	db := experienceTestDB(t)
	tenant := "rux-bulk-" + uuid.NewString()
	start := floorBucket(time.Now()).Add(-time.Hour)
	kept := map[string]bool{"r2": true, "browser:safari-18": true}
	for index := 0; index < 20; index++ {
		seedExperienceTerminal(t, db, tenant, "startup", "success", start.Add(time.Minute), "tab-"+uuid.NewString())
	}
	var progress models.ExperienceRollupProgress
	if err := db.Transaction(func(tx *gorm.DB) error {
		var err error
		progress, err = beginRollupProgress(tx, tenant, "hour", start)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	counter := &experienceStatementCounter{}
	err := db.Transaction(func(tx *gorm.DB) error {
		done, err := accumulatePage(tx.Session(&gorm.Session{Logger: counter}), tenant, "hour", start, start.Add(time.Hour), kept, &progress)
		if err != nil {
			return err
		}
		if !done {
			t.Fatal("short page not complete")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if counter.statements.Load() != 3 {
		t.Fatalf("page incurred%d statements, want event read/delta insert/cursor save", counter.statements.Load())
	}
	var row models.ExperienceMetricRollup
	if err := db.Where("tenant_id=? AND metric_key=? AND cohort_dim='global'", tenant, mPlaybackStartSuccess).First(&row).Error; err != nil {
		t.Fatal(err)
	}
	if row.Denominator != 20 || row.SampleCount != 20 || row.OutcomeCount != 20 || row.Completed {
		t.Fatalf("partial aggregate incorrectly finalized: %+v", row)
	}
}
func TestExperienceBulkRollup_HistogramAndCountsAddExactly(t *testing.T) {
	db := experienceTestDB(t)
	tenant := "rux-bulk-" + uuid.NewString()
	start := floorBucket(time.Now()).Add(-time.Hour)
	key := rollupKey{mFeedRenderSuccess, "pods", "global", "all", "web"}
	first := &rollupAgg{num: 1, denom: 1, samples: 1, latencyMS: 2000, outcomes: 1}
	first.latBuckets[latencyBucketIndex(2000)] = 1
	second := &rollupAgg{num: 2, denom: 3, samples: 2, latencyMS: 5000, outcomes: 3}
	second.latBuckets[latencyBucketIndex(2500)] = 2
	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := upsertExperienceRollupDeltas(tx, tenant, "hour", start, map[rollupKey]*rollupAgg{key: first}); err != nil {
			return err
		}
		return upsertExperienceRollupDeltas(tx, tenant, "hour", start, map[rollupKey]*rollupAgg{key: second})
	}); err != nil {
		t.Fatal(err)
	}
	var row models.ExperienceMetricRollup
	if err := db.Where("tenant_id=?", tenant).First(&row).Error; err != nil {
		t.Fatal(err)
	}
	buckets := parseLatBuckets(row.LatencyBuckets)
	if row.Numerator != 3 || row.Denominator != 4 || row.SampleCount != 3 || row.LatencySum != 7000 || row.OutcomeCount != 4 || buckets[latencyBucketIndex(2000)] != 1 || buckets[latencyBucketIndex(2500)] != 2 {
		t.Fatalf("wrong accumulated evidence: %+v buckets%v", row, buckets)
	}
}
func TestExperienceBulkRollup_CursorFailureRollsBackDelta(t *testing.T) {
	db := experienceTestDB(t)
	tenant := "rux-bulk-" + uuid.NewString()
	start := floorBucket(time.Now()).Add(-time.Hour)
	seedExperienceTerminal(t, db, tenant, "startup", "success", start.Add(time.Minute), "tab")
	var progress models.ExperienceRollupProgress
	if err := db.Transaction(func(tx *gorm.DB) error {
		var err error
		progress, err = beginRollupProgress(tx, tenant, "hour", start)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	callback := "rux-bulk-cursor-failure-" + uuid.NewString()
	if err := db.Callback().Update().Before("gorm:update").Register(callback, func(tx *gorm.DB) {
		if tx.Statement.Table == "experience_rollup_progress" {
			tx.AddError(errors.New("injected page cursor failure"))
		}
	}); err != nil {
		t.Fatal(err)
	}
	err := db.Transaction(func(tx *gorm.DB) error {
		_, err := accumulatePage(tx, tenant, "hour", start, start.Add(time.Hour), map[string]bool{"r2": true}, &progress)
		return err
	})
	db.Callback().Update().Remove(callback)
	if err == nil {
		t.Fatal("failure injection not reached")
	}
	var rows int64
	if err := db.Model(&models.ExperienceMetricRollup{}).Where("tenant_id=?", tenant).Count(&rows).Error; err != nil || rows != 0 {
		t.Fatalf("failed page leaked%d aggregate rows: %v", rows, err)
	}
	if err := db.Where("tenant_id=?", tenant).First(&progress).Error; err != nil {
		t.Fatal(err)
	}
	if progress.Cursor != 0 || progress.EventsProcessed != 0 {
		t.Fatal("failed page advanced cursor")
	}
	if err := db.Transaction(func(tx *gorm.DB) error {
		_, err := accumulatePage(tx, tenant, "hour", start, start.Add(time.Hour), map[string]bool{"r2": true}, &progress)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	var row models.ExperienceMetricRollup
	if err := db.Where("tenant_id=? AND metric_key=? AND cohort_dim='global'", tenant, mPlaybackStartSuccess).First(&row).Error; err != nil || row.Denominator != 1 {
		t.Fatalf("page replay duplicated evidence%d %v", row.Denominator, err)
	}
}
func TestExperienceBulkRollup_RefusesCompletedOrLegacyHistogramMutation(t *testing.T) {
	for _, version := range []int{1, 2} {
		t.Run(string(rune('0'+version)), func(t *testing.T) {
			db := experienceTestDB(t)
			tenant := "rux-bulk-" + uuid.NewString()
			start := floorBucket(time.Now()).Add(-time.Hour)
			row := models.ExperienceMetricRollup{TenantID: tenant, BucketStart: start, Resolution: "hour", MetricKey: mFeedRenderSuccess, Surface: "pods", CohortDim: "global", CohortVal: "all", Numerator: 10, Denominator: 10, HistogramVersion: version, Completed: version == 2}
			if err := db.Create(&row).Error; err != nil {
				t.Fatal(err)
			}
			err := db.Transaction(func(tx *gorm.DB) error {
				return upsertExperienceRollupDeltas(tx, tenant, "hour", start, map[rollupKey]*rollupAgg{{mFeedRenderSuccess, "pods", "global", "all", "web"}: {num: 1, denom: 1, outcomes: 1}})
			})
			if err == nil || !strings.Contains(err.Error(), "incompatible") {
				t.Fatal("incompatible/completed evidence accepted")
			}
			var actual models.ExperienceMetricRollup
			if err := db.Where("tenant_id=?", tenant).First(&actual).Error; err != nil || actual.Denominator != 10 {
				t.Fatalf("protected row changed %+v %v", actual, err)
			}
		})
	}
}
func TestExperienceBulkRollup_ExactSessionCountsUseThreeStatements(t *testing.T) {
	db := experienceTestDB(t)
	tenant := "rux-bulk-" + uuid.NewString()
	start := floorBucket(time.Now()).Add(-time.Hour)
	kept := map[string]bool{"r2": true, "browser:safari-18": true}
	seedExperienceTerminal(t, db, tenant, "startup", "failed", start.Add(time.Minute), "shared-tab")
	seedExperienceTerminal(t, db, tenant, "startup", "success", start.Add(2*time.Minute), "shared-tab")
	seedExperienceTerminal(t, db, tenant, "startup", "success", start.Add(3*time.Minute), "other-tab")
	if _, err := rollupBucket(db, tenant, start, kept, 1); err != nil {
		t.Fatal(err)
	}
	counter := &experienceStatementCounter{}
	if err := db.Transaction(func(tx *gorm.DB) error {
		return completeAffectedSessions(tx.Session(&gorm.Session{Logger: counter}), tenant, start, start.Add(time.Hour), "hour", kept)
	}); err != nil {
		t.Fatal(err)
	}
	if counter.statements.Load() != 3 {
		t.Fatalf("cohort completion incurred%d SQL statements", counter.statements.Load())
	}
	var row models.ExperienceMetricRollup
	if err := db.Where("tenant_id=? AND metric_key=? AND cohort_dim='global'", tenant, mPlaybackStartSuccess).First(&row).Error; err != nil {
		t.Fatal(err)
	}
	if row.Denominator != 3 || row.ActiveSessions != 2 || row.AffectedSessions != 1 || !row.Completed {
		t.Fatalf("exact completed impact lost: %+v", row)
	}
}
