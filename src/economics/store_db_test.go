package economics

import (
	"content-management-system/src/tests/testdb"
	"errors"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"os"
	"testing"
	"time"
)

var testTime = time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC)

func economicsDB(t *testing.T) *gorm.DB {
	t.Helper()
	if os.Getenv("CMS_TEST_ADMIN_URL") == "" {
		t.Skip("explicit disposable local PostgreSQL required")
	}
	t.Setenv("JWT_SECRET", "economics-unit-test-only-key")
	db := testdb.Open(t)
	if e := db.Exec("CREATE TABLE transcription_jobs(id bigserial PRIMARY KEY)").Error; e != nil {
		t.Fatal(e)
	}
	sql, e := os.ReadFile("../../migrations/20261016010000_platform_user_economics.sql")
	if e != nil {
		t.Fatal(e)
	}
	if e = db.Exec(string(sql)).Error; e != nil {
		t.Fatal(e)
	}
	upgrade, e := os.ReadFile("../../migrations/20261017020000_economics_resumable_projection.sql")
	if e != nil {
		t.Fatal(e)
	}
	if e = db.Exec(string(upgrade)).Error; e != nil {
		t.Fatal(e)
	}
	db.Create(&Policy{TenantID: "a", CollectionEnabled: true, ReportsEnabled: true, AllocationEnabled: true})
	DefaultCollector.SetPolicies([]Policy{{TenantID: "a", CollectionEnabled: true, ReportsEnabled: true}})
	markLifecycleReady(db, time.Now())
	return db
}
func TestUsageBatchReplayConflictAndTenantIsolation(t *testing.T) {
	db := economicsDB(t)
	now := time.Now().UTC()
	day := now.Truncate(24 * time.Hour)
	id := "account:" + uuid.NewString()
	b := Batch{Environment: EnvironmentName(), Version: 1, TenantID: "a", Epoch: uuid.NewString(), Sequence: 1, StartedAt: day, EndedAt: now, Rows: []Usage{{Day: day, Subject: id, Operation: "search", Unit: "requests", Value: 3}}}
	if e := Ingest(db, "cms", b); e != nil {
		t.Fatal(e)
	}
	if e := Ingest(db, "cms", b); e != nil {
		t.Fatal(e)
	}
	var value int64
	db.Table("economics_usage_daily").Select("sum(value)").Scan(&value)
	if value != 3 {
		t.Fatal(value)
	}
	b.Rows[0].Value = 4
	if !errors.Is(Ingest(db, "cms", b), ErrConflict) {
		t.Fatal("changed replay accepted")
	}
	b.Sequence = 3
	if !errors.Is(Ingest(db, "cms", b), ErrConflict) {
		t.Fatal("sequence gap accepted")
	}
	b.TenantID = "b"
	if e := Ingest(db, "cms", b); e == nil {
		t.Fatal("unconfigured tenant accepted")
	}
}
func TestRevisionReplacementAndPublicationConservation(t *testing.T) {
	db := economicsDB(t)
	day := time.Now().UTC().Truncate(24 * time.Hour)
	c := Cost{TenantID: "a", SourceID: "fixture", LogicalKey: "bill-1", Provider: "fixture", Resource: "database", Feature: "serving", PeriodStart: day, PeriodEnd: day.AddDate(0, 0, 1), Net: mustAmount(t, "100"), Currency: "USD", Destination: "infrastructure", Metric: "requests", AllocableBPS: 2000, Reason: "controlled resource qualification", Actor: "test"}
	first, e := AppendCost(db, c)
	if e != nil {
		t.Fatal(e)
	}
	again, e := AppendCost(db, c)
	if e != nil || again.ID != first.ID {
		t.Fatal("same input was not idempotent", e)
	}
	c.Net = mustAmount(t, "120")
	latest, e := AppendCost(db, c)
	if e != nil || latest.Revision != 2 {
		t.Fatal("revision", e)
	}
	if e := Rebuild(db, "a"); e != nil {
		t.Fatal(e)
	}
	var sum string
	if e := db.Table("economics_cost_summaries").Select("sum(direct+shared)::text").Scan(&sum).Error; e != nil {
		t.Fatal(e)
	}
	if mustAmount(t, sum) != c.Net {
		t.Fatal("revisions added as charges", sum)
	}
}
func TestErasureKeepsMoneyRejectsDelayedAttribution(t *testing.T) {
	db := economicsDB(t)
	id := uuid.NewString()
	subject := "account:" + id
	day := time.Now().UTC().Truncate(24 * time.Hour)
	cost := Cost{TenantID: "a", SourceID: "fixture", LogicalKey: "private", Provider: "test", Resource: "ai", Feature: "test", PeriodStart: day, PeriodEnd: day.AddDate(0, 0, 1), Net: mustAmount(t, "7"), Currency: "USD", Destination: "direct", Subject: subject, Reason: "owner fixture", Actor: "test"}
	if _, e := AppendCost(db, cost); e != nil {
		t.Fatal(e)
	}
	if e := Rebuild(db, "a"); e != nil {
		t.Fatal(e)
	}
	if e := db.Transaction(func(tx *gorm.DB) error { return Erase(tx, id) }); e != nil {
		t.Fatal(e)
	}
	if e := Rebuild(db, "a"); e != nil {
		t.Fatal(e)
	}
	var rows []Summary
	db.Find(&rows)
	sum := Zero
	for _, r := range rows {
		if r.Subject == subject {
			t.Fatal("erased profile restored")
		}
		sum = Add(sum, Add(r.Direct, r.Shared))
	}
	if sum != cost.Net {
		t.Fatal("erasure lost money")
	}
	a := Attribution{ID: uuid.NewString(), TenantID: "a", Source: "cms", Subject: subject, Feature: "transcription", Benefit: "shared_content", ContentID: uuid.NewString()}
	if e := db.Transaction(func(tx *gorm.DB) error { return RegisterAttribution(tx, a) }); e != nil {
		t.Fatal(e)
	}
	var stored Attribution
	db.Where("id=?", a.ID).First(&stored)
	if stored.Subject != "removed" || stored.ContentID != "" {
		t.Fatal("delayed identity restored", stored)
	}
}
