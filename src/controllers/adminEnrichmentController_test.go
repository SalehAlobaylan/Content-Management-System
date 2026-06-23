package controllers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func newMockGorm(t *testing.T) (*gorm.DB, sqlmock.Sqlmock) {
	t.Helper()
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })

	db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{})
	if err != nil {
		t.Fatalf("gorm open: %v", err)
	}
	return db, mock
}

func serveWithDB(db *gorm.DB, handler gin.HandlerFunc, target string) *httptest.ResponseRecorder {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Set("db", db)
	c.Request = httptest.NewRequest(http.MethodGet, target, nil)
	handler(c)
	return w
}

func TestGetMissingEnrichmentCountsReturnsCombinedCounts(t *testing.T) {
	db, mock := newMockGorm(t)
	countSQL := regexp.QuoteMeta(`SELECT count(*) FROM "content_items"`)
	for _, n := range []int64{2, 3, 5, 7, 11, 13} {
		mock.ExpectQuery(countSQL).
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(n))
	}

	w := serveWithDB(db, GetMissingEnrichmentCounts, "/admin/enrichment/missing-counts?type=VIDEO,PODCAST")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}

	var body struct {
		Data missingEnrichmentCountsResponse `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Data.Transcript != 2 || body.Data.Embedding != 3 || body.Data.Sparse != 5 ||
		body.Data.Image != 7 || body.Data.TranscriptImage != 11 || body.Data.EmbeddingSparse != 13 {
		t.Fatalf("unexpected counts: %+v", body.Data)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestGetMissingEnrichmentsLimitOneCountsOnly(t *testing.T) {
	db, mock := newMockGorm(t)
	countSQL := regexp.QuoteMeta(`SELECT count(*) FROM "content_items"`)
	mock.ExpectQuery(countSQL).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(42))

	w := serveWithDB(db, GetMissingEnrichments, "/admin/enrichment/missing?missing=embedding&type=NEWS&status=READY&limit=1&offset=0")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}

	var body struct {
		Data struct {
			Items []missingEnrichmentItem `json:"items"`
			Total int64                   `json:"total"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Data.Total != 42 {
		t.Fatalf("total = %d, want 42", body.Data.Total)
	}
	if len(body.Data.Items) != 0 {
		t.Fatalf("items len = %d, want count-only empty list", len(body.Data.Items))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unexpected extra query or unmet expectation: %v", err)
	}
}
