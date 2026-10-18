package supply

import (
	"testing"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/tests/testdb"
	"github.com/google/uuid"
)

func TestPodsBoundaryObservationRejectsBrowserLikeAuthority(t *testing.T) {
	row := models.PodsBoundaryObservation{TenantID: "tenant-a", ContentItemID: uuid.New(), Boundary: "page_render", ProbeKind: "frozen_session", ProbeID: uuid.NewString(), Verdict: string(VerdictPresent), ObservedAt: time.Now().UTC()}
	if err := RecordPodsBoundaryObservation(nil, row); err == nil {
		t.Fatal("a caller without the CMS evidence store was accepted")
	}
	row.Boundary = "ranking_success"
	if err := RecordPodsBoundaryObservation(nil, row); err == nil {
		t.Fatal("an unregistered consumer boundary was accepted")
	}
}

func TestPodsBoundaryBatchCountsOnlyNewTenantScopedEvidence(t *testing.T) {
	if !supplyDisposableDatabaseConfigured() {
		t.Skip("requires guarded disposable local Postgres")
	}
	db := testdb.Open(t)
	// Keep the fixture deliberately narrow; no owner or app runtime uses it.
	for _, sql := range []string{
		`CREATE TABLE content_items (tenant_id text, public_id uuid, content_source_id uuid)`,
		`CREATE TABLE pods_boundary_observations (public_id uuid PRIMARY KEY,tenant_id text,content_item_id uuid,generation_id uuid,feed_session_id uuid,boundary text,probe_kind text,probe_id text,source_run_request_row_id bigint,verdict text,provenance_digest text,observed_at timestamp,created_at timestamp DEFAULT now(), UNIQUE(tenant_id,boundary,probe_kind,probe_id,content_item_id))`,
		`CREATE TABLE media_source_yield_daily (tenant_id text,content_source_id uuid,yield_date date,public_returns bigint DEFAULT 0,first_page_returns bigint DEFAULT 0,updated_at timestamp,PRIMARY KEY(tenant_id,content_source_id,yield_date))`,
	} {
		if err := db.Exec(sql).Error; err != nil {
			t.Fatal(err)
		}
	}
	itemID, sourceA, sourceB := uuid.New(), uuid.New(), uuid.New()
	if err := db.Exec("INSERT INTO content_items VALUES (?,?,?),(?,?,?)", "a", itemID, sourceA, "b", itemID, sourceB).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	rows := []models.PodsBoundaryObservation{}
	for _, tenant := range []string{"a", "b"} {
		for _, boundary := range []string{"page_render", "feed_return"} {
			rows = append(rows, models.PodsBoundaryObservation{TenantID: tenant, ContentItemID: itemID, Boundary: boundary, ProbeKind: "frozen_session", ProbeID: "page", Verdict: string(VerdictPresent), ObservedAt: now})
		}
	}
	rows = append(rows, rows[0])
	for i := 0; i < 2; i++ {
		if err := RecordPodsBoundaryObservations(db, rows); err != nil {
			t.Fatal(err)
		}
	}
	var yields []struct {
		TenantID                        string
		ContentSourceID                 uuid.UUID
		PublicReturns, FirstPageReturns int64
	}
	if err := db.Table("media_source_yield_daily").Order("tenant_id").Find(&yields).Error; err != nil {
		t.Fatal(err)
	}
	if len(yields) != 2 || yields[0].ContentSourceID != sourceA || yields[1].ContentSourceID != sourceB {
		t.Fatalf("source lookup crossed tenants: %+v", yields)
	}
	for _, y := range yields {
		if y.PublicReturns != 1 || y.FirstPageReturns != 1 {
			t.Fatalf("retry duplicated yield: %+v", y)
		}
	}
	// A yield failure rolls back the new exact evidence as well.
	if err := db.Exec("ALTER TABLE media_source_yield_daily ADD CHECK (first_page_returns <= 1)").Error; err != nil {
		t.Fatal(err)
	}
	rows[0].ProbeID = "later-page"
	if err := RecordPodsBoundaryObservations(db, rows[:1]); err == nil {
		t.Fatal("injected yield failure was ignored")
	}
	var count int64
	if err := db.Table("pods_boundary_observations").Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 4 {
		t.Fatalf("failed transaction left evidence: %d", count)
	}
}
