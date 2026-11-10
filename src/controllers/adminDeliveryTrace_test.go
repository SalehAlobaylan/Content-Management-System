package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

func TestDeliverySummaryIgnoresHistoricalAndSupersededBlockers(t *testing.T) {
	requests := []models.ContentStageRequest{
		{ProcessingGeneration: 6, Stage: "embedding", State: models.ContentStageFailed, BlockingScope: models.ContentStageBlockingContentReady},
		{ProcessingGeneration: 7, Stage: "embedding", State: models.ContentStageSuperseded, BlockingScope: models.ContentStageBlockingContentReady},
		{ProcessingGeneration: 7, Stage: "embedding", State: models.ContentStageVerified, BlockingScope: models.ContentStageBlockingContentReady},
		{ProcessingGeneration: 7, Stage: "metadata", State: models.ContentStageFailed, BlockingScope: models.ContentStageBlockingOptional},
	}
	if result := summarizeDeliveryProcessing(7, requests); result.Status != "Optional stage needs review" || result.Detail != "metadata" {
		t.Fatalf("unexpected processing result: %+v", result)
	}
	requests = append(requests, models.ContentStageRequest{ProcessingGeneration: 7, Stage: "transcript", State: models.ContentStageUncertain, BlockingScope: models.ContentStageBlockingFeedDelivery})
	if result := summarizeDeliveryProcessing(7, requests); result.Status != "Stage outcome uncertain" || result.Detail != "transcript" {
		t.Fatalf("unexpected processing result: %+v", result)
	}
}

func TestDeliveryObservationsKeepOldGenerationAndFailedReadsDistinct(t *testing.T) {
	item := models.ContentItem{TenantID: "fixture-tenant", PublicID: uuid.New()}
	active, older := uuid.New(), uuid.New()
	for _, failure := range []bool{false, true} {
		db, mock := newMockGorm(t)
		mock.ExpectQuery(`SELECT count\(\*\) FROM information_schema.tables`).
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
		query := `SELECT .* FROM "pods_boundary_observations"`
		if failure {
			mock.ExpectQuery(query).WillReturnError(errors.New("fixture observation read failed"))
		} else {
			columns := []string{"boundary", "probe_kind", "observed_at", "generation_id"}
			mock.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(columns))
			mock.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(columns).AddRow("feed_return", "cms_probe", time.Now(), older.String()))
			mock.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(columns))
			mock.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(columns))
		}
		row, observations := readDeliveryObservations(db, item, true, &active)
		if failure && row.Status != "Evidence unavailable" {
			t.Fatalf("failed observation read returned %+v", row)
		}
		if !failure && (row.Status != "Historical evidence only" || len(observations) != 1 || !observations[0].Historical) {
			t.Fatalf("old proof classified as current: %+v %+v", row, observations)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestNextModerationCaseScopesAnchorAndReturnsEnd(t *testing.T) {
	anchor, next := uuid.New(), uuid.New()
	for _, test := range []struct {
		name string
		rows *sqlmock.Rows
		code int
		text string
	}{
		{"next", sqlmock.NewRows([]string{"id"}).AddRow(next.String()), 200, next.String()},
		{"end", sqlmock.NewRows([]string{"id"}).AddRow(nil), 200, `"id":null`},
		{"missing anchor", sqlmock.NewRows([]string{"id"}), 404, "Report not found"},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, mock := newMockGorm(t)
			mock.ExpectQuery(`(?s)LEFT JOIN LATERAL .* candidate.status = 'open'.*candidate.created_at, candidate.id.*anchor.tenant_id = \$1 AND anchor.public_id = \$2`).
				WithArgs("fixture-tenant", anchor.String()).WillReturnRows(test.rows)
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodGet, "/", nil)
			c.Set("db", db)
			c.Set(utils.AdminPrincipalContextKey, utils.AdminPrincipal{TenantID: "fixture-tenant", UserID: "reviewer"})
			c.Params = gin.Params{{Key: "id", Value: anchor.String()}}
			AdminNextModerationReport(c)
			if rec.Code != test.code || !strings.Contains(rec.Body.String(), test.text) {
				t.Fatalf("got %d: %s", rec.Code, rec.Body.String())
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestOpsExactIssueLinkUsesTenantAndOwnerIdentity(t *testing.T) {
	id := uuid.NewString()
	item := opsAttentionItem{TenantID: "a", Kind: "episode", NativeID: id, System: "experience", ClientPlatform: "ios"}
	p := utils.AdminPrincipal{TenantID: "a", Permissions: []string{"feed:read"}}
	if href := opsExactIssueHref(item, p); href != "/platform/real-experience?client=ios&incident="+id {
		t.Fatalf("wrong exact issue link: %q", href)
	}
	item.TenantID = "b"
	if href := opsExactIssueHref(item, p); href != "" {
		t.Fatalf("cross-tenant exact link: %q", href)
	}
	item.TenantID, item.Kind = "a", "approval"
	if href := opsExactIssueHref(item, p); href != "" {
		t.Fatalf("aggregate exact link: %q", href)
	}
}

func TestDeliveryParentKeepsEarlierPublishedChapterGeneration(t *testing.T) {
	db, mock := newMockGorm(t)
	parent := models.ContentItem{TenantID: "fixture-tenant", PublicID: uuid.New(), ProcessingGeneration: 8}
	active, atomization := uuid.New(), uuid.New()
	mock.ExpectQuery(`SELECT count\(\*\) FROM information_schema.tables`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectQuery(`SELECT .* FROM "atomization_generations"`).
		WillReturnRows(sqlmock.NewRows([]string{"public_id", "generation_number", "processing_generation"}).AddRow(atomization.String(), 3, 7))
	mock.ExpectQuery(`SELECT count\(\*\) FROM feed_generation_memberships AS fgm`).
		WithArgs(active.String(), parent.TenantID, parent.PublicID.String(), atomization.String()).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(2))
	row := readParentPublication(db, parent, active)
	if row.Status != "Published chapters" || !strings.Contains(row.Detail, "earlier processing generation remains published") {
		t.Fatalf("unexpected parent publication result: %+v", row)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestDeliverySummaryRejectsForeignOrMissingItemBeforeRelatedReads(t *testing.T) {
	db, mock := newMockGorm(t)
	id := uuid.NewString()
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT .* FROM "content_items" WHERE tenant_id = \$1 AND public_id = \$2`).
		WithArgs("fixture-tenant", id, 1).WillReturnRows(sqlmock.NewRows([]string{"public_id"}))
	mock.ExpectRollback()
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodGet, "/", nil)
	c.Set("db", db)
	c.Set(utils.AdminPrincipalContextKey, utils.AdminPrincipal{TenantID: "fixture-tenant", UserID: "reviewer"})
	c.Params = gin.Params{{Key: "id", Value: id}}
	AdminGetDeliverySummary(c)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("got %d: %s", rec.Code, rec.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestDeliverySummaryOnlyReturnsEvidenceAfterSnapshotCommits(t *testing.T) {
	for _, test := range []struct {
		name      string
		commitErr error
		code      int
	}{
		{"consistent", nil, http.StatusOK},
		{"snapshot failed", errors.New("fixture snapshot commit failed"), http.StatusServiceUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, mock := newMockGorm(t)
			id := uuid.NewString()
			mock.ExpectBegin()
			mock.ExpectQuery(`SELECT .* FROM "content_items" WHERE tenant_id = \$1 AND public_id = \$2`).
				WithArgs("fixture-tenant", id, 1).
				WillReturnRows(sqlmock.NewRows([]string{"public_id", "tenant_id", "type", "title"}).AddRow(id, "fixture-tenant", "COMMENT", "Fixture content"))
			mock.ExpectQuery(`SELECT count\(\*\) FROM information_schema.tables`).
				WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
			mock.ExpectCommit().WillReturnError(test.commitErr)
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodGet, "/", nil)
			c.Set("db", db)
			c.Set(utils.AdminPrincipalContextKey, utils.AdminPrincipal{TenantID: "fixture-tenant", UserID: "reviewer"})
			c.Params = gin.Params{{Key: "id", Value: id}}
			AdminGetDeliverySummary(c)
			if rec.Code != test.code || strings.Contains(rec.Body.String(), "Fixture content") != (test.commitErr == nil) {
				t.Fatalf("got %d: %s", rec.Code, rec.Body.String())
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
