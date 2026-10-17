package economics

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"sync/atomic"
	"testing"
	"time"
)

func seedProjectionCosts(t *testing.T, db *gorm.DB, n int) {
	t.Helper()
	day := time.Now().UTC().Truncate(24 * time.Hour)
	rows := make([]Cost, 0, n)
	for i := 0; i < n; i++ {
		rows = append(rows, Cost{ID: uuid.NewString(), TenantID: "a", SourceID: "fixture", LogicalKey: fmt.Sprintf("cost-%06d", i), Revision: 1, Provider: "test", Resource: "hosting", Feature: "pods", PeriodStart: day, PeriodEnd: day.AddDate(0, 0, 1), Net: mustAmount(t, "1"), Currency: "USD", Destination: "unallocated", Reason: "disposable fixture", CreatedAt: time.Now().UTC()})
	}
	if e := db.CreateInBatches(rows, 500).Error; e != nil {
		t.Fatal(e)
	}
}
func stepProjection(t *testing.T, db *gorm.DB) bool {
	t.Helper()
	done, e := projectionStep(db, "a")
	if e != nil {
		t.Fatal(e)
	}
	return done
}

func TestProjectionResumesPrivateChunksAndPublishesOnce(t *testing.T) {
	db := economicsDB(t)
	seedProjectionCosts(t, db, 61)
	if stepProjection(t, db) || stepProjection(t, db) || stepProjection(t, db) {
		t.Fatal("published an incomplete chunk")
	}
	var visible, staged int64
	db.Model(&Summary{}).Count(&visible)
	db.Table("economics_projection_summaries").Count(&staged)
	if visible != 0 || staged != projectionPage {
		t.Fatal(visible, staged)
	}
	var job projectionJob
	db.Take(&job)
	// A corrected bill arriving mid-build belongs to the next coherent snapshot.
	var corrected Cost
	db.Where("tenant_id='a' AND logical_key='cost-000000'").Take(&corrected)
	corrected.ID = uuid.NewString()
	corrected.Revision = 2
	corrected.Net = mustAmount(t, "2")
	if e := db.Create(&corrected).Error; e != nil {
		t.Fatal(e)
	}
	resumed := db.Session(&gorm.Session{NewDB: true})
	for i := 0; i < 10; i++ {
		if stepProjection(t, resumed) {
			break
		}
		if i == 9 {
			t.Fatal("did not finish")
		}
	}
	var total string
	db.Model(&Summary{}).Select("sum(direct+shared)::text").Scan(&total)
	if mustAmount(t, total) != mustAmount(t, "61") {
		t.Fatal(total)
	}
	var jobs, inputs int64
	db.Model(&projectionJob{}).Count(&jobs)
	db.Table("economics_projection_inputs").Count(&inputs)
	if jobs != 0 || inputs != 0 {
		t.Fatal("private snapshot retained", jobs, inputs)
	}
	if e := Rebuild(db, "a"); e != nil {
		t.Fatal(e)
	}
	db.Model(&Summary{}).Select("sum(direct+shared)::text").Scan(&total)
	if mustAmount(t, total) != mustAmount(t, "62") {
		t.Fatal("late correction lost", total)
	}
}
func TestLifecycleInvalidatesEveryPrivateProjectionSnapshot(t *testing.T) {
	db := economicsDB(t)
	seedProjectionCosts(t, db, 26)
	stepProjection(t, db)
	stepProjection(t, db)
	stepProjection(t, db)
	if e := db.Transaction(func(tx *gorm.DB) error { return Clear(tx, uuid.NewString(), time.Now().UTC()) }); e != nil {
		t.Fatal(e)
	}
	for _, table := range []string{"economics_projection_jobs", "economics_projection_inputs", "economics_projection_summaries"} {
		var n int64
		db.Table(table).Count(&n)
		if n != 0 {
			t.Fatal("private lifecycle snapshot survived", table, n)
		}
	}
}
func TestProjectionSnapshotExceedsFormerGlobalLimitsWithoutLoadingThem(t *testing.T) {
	if testing.Short() {
		t.Skip("large disposable fixture")
	}
	db := economicsDB(t)
	seedProjectionCosts(t, db, 20001)
	if e := db.Exec(`INSERT INTO economics_usage_daily SELECT 'a',current_date,'account:'||gen_random_uuid()::text,'pods_page','requests','ok',1 FROM generate_series(1,100001)`).Error; e != nil {
		t.Fatal(e)
	}
	stepProjection(t, db)
	stepProjection(t, db)
	stepProjection(t, db)
	var costs, usage, staged int64
	db.Table("economics_projection_inputs").Where("kind='cost'").Count(&costs)
	db.Table("economics_projection_inputs").Where("kind='usage'").Count(&usage)
	db.Table("economics_projection_summaries").Count(&staged)
	if costs != 20001 || usage != 100001 || staged != projectionPage {
		t.Fatal(costs, usage, staged)
	}
}
func TestPagedInvoiceReconciliationConservesSignedMoney(t *testing.T) {
	db := economicsDB(t)
	seedProjectionCosts(t, db, 0)
	day := time.Now().UTC().Truncate(24 * time.Hour)
	for i, value := range []string{"1", "-0.2", "0.000000000001"} {
		c := Cost{ID: uuid.NewString(), TenantID: "a", SourceID: "ai-owner", LogicalKey: fmt.Sprint(i), Revision: 1, Provider: "test", Resource: "ai", Feature: "pods", PeriodStart: day, PeriodEnd: day.AddDate(0, 0, 1), Net: mustAmount(t, value), Currency: "USD", Destination: "unallocated", Reason: "fixture", CreatedAt: time.Now().UTC()}
		if e := db.Create(&c).Error; e != nil {
			t.Fatal(e)
		}
	}
	bill := Cost{ID: uuid.NewString(), TenantID: "a", SourceID: "billing", LogicalKey: "invoice", Revision: 1, Provider: "test", Resource: "ai", Feature: "pods", PeriodStart: day, PeriodEnd: day.AddDate(0, 0, 1), Net: mustAmount(t, "-0.400000000001"), Currency: "USD", Destination: "reconciliation", Reconciled: true, Reason: "signed fixture", CreatedAt: time.Now().UTC()}
	if e := db.Create(&bill).Error; e != nil {
		t.Fatal(e)
	}
	if e := Rebuild(db, "a"); e != nil {
		t.Fatal(e)
	}
	var total string
	db.Model(&Summary{}).Select("sum(direct+shared)::text").Scan(&total)
	if mustAmount(t, total) != bill.Net {
		t.Fatal("invoice did not conserve", total)
	}
}

func TestFailedProjectionChunkRollsBackItsCursorAndRetries(t *testing.T) {
	db := economicsDB(t)
	seedProjectionCosts(t, db, 26)
	stepProjection(t, db)
	stepProjection(t, db)
	stepProjection(t, db)
	var before projectionJob
	db.Take(&before)
	if e := db.Exec(`CREATE FUNCTION reject_projection_fixture() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture unavailable'; END $$; CREATE TRIGGER reject_projection_fixture BEFORE INSERT ON economics_projection_summaries FOR EACH STATEMENT EXECUTE FUNCTION reject_projection_fixture()`).Error; e != nil {
		t.Fatal(e)
	}
	if _, e := projectionStep(db, "a"); e == nil {
		t.Fatal("chunk ignored storage failure")
	}
	var after projectionJob
	db.Take(&after)
	if before.Cursor != after.Cursor || before.Rows != after.Rows {
		t.Fatal("failed chunk advanced checkpoint", before, after)
	}
	if e := db.Exec("DROP TRIGGER reject_projection_fixture ON economics_projection_summaries").Error; e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 5; i++ {
		if stepProjection(t, db) {
			return
		}
	}
	t.Fatal("failed chunk did not resume")
}

type projectionRoundTrips struct {
	logger.Interface
	count *atomic.Int64
}

func (l projectionRoundTrips) Trace(_ context.Context, _ time.Time, _ func() (string, int64), _ error) {
	l.count.Add(1)
}
func TestHistoricExpensePageUsesBoundedRemoteRoundTrips(t *testing.T) {
	db := economicsDB(t)
	seedProjectionCosts(t, db, projectionPage+1)
	stepProjection(t, db)
	stepProjection(t, db)
	var count atomic.Int64
	measured := db.Session(&gorm.Session{Logger: projectionRoundTrips{logger.Discard, &count}})
	if stepProjection(t, measured) {
		t.Fatal("incomplete source page was published")
	}
	if n := count.Load(); n > 32 {
		t.Fatalf("receipt page made %d database round trips; shared evidence must be batched", n)
	}
	var staged int64
	db.Table("economics_projection_summaries").Count(&staged)
	if staged != projectionPage {
		t.Fatal("batching lost expense rows", staged)
	}
}

func TestReportFreshnessSeparatesSnapshotFromPublication(t *testing.T) {
	db := economicsDB(t)
	start := time.Now().UTC().Truncate(time.Microsecond).Add(-time.Hour)
	end := start.Add(20 * time.Minute)
	if e := db.Create(&Run{ID: "freshness", TenantID: "a", Status: "completed", StartedAt: start, CompletedAt: &end}).Error; e != nil {
		t.Fatal(e)
	}
	if e := db.Exec("INSERT INTO economics_publications(tenant_id,revision,published_at) VALUES(?,?,?)", "a", "freshness", start).Error; e != nil {
		t.Fatal(e)
	}
	m := meta(RequestScope{DB: db, Tenant: "a"})
	if !m["as_of"].(*time.Time).Equal(start) || !m["published_at"].(*time.Time).Equal(end) {
		t.Fatal(m)
	}
}
