package controllers

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

func callInternalMerge(
	db *gorm.DB, id string, body string,
) *httptest.ResponseRecorder {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Set("db", db)
	c.Params = gin.Params{{Key: "id", Value: id}}
	c.Request = httptest.NewRequest(
		http.MethodPatch,
		"/internal/content-items/"+id+"/enrichment-metadata",
		bytes.NewBufferString(body),
	)
	c.Request.Header.Set("Content-Type", "application/json")
	InternalMergeEnrichmentMetadata(c)
	return w
}

func TestInternalMergeEnrichmentMetadataUsesAtomicJSONBMerge(t *testing.T) {
	db, mock := newMockGorm(t)
	id := uuid.New()
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(
		`UPDATE "content_items" SET "metadata"=COALESCE(metadata, '{}'::jsonb) || $1::jsonb WHERE public_id = $2`,
	)).
		WithArgs(sqlmock.AnyArg(), id.String()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	w := callInternalMerge(db, id.String(), `{"fields":{"summary":"safe summary","key_points":["one"]}}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s, sql = %v", w.Code, w.Body.String(), mock.ExpectationsWereMet())
	}
	var response struct {
		Success bool                   `json:"success"`
		Fields  map[string]interface{} `json:"fields"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !response.Success || response.Fields["summary"] != "safe summary" {
		t.Fatalf("unexpected response: %+v", response)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectation: %v", err)
	}
}

func TestInternalMergeEnrichmentMetadataRejectsUnownedFieldsBeforeDB(t *testing.T) {
	db, mock := newMockGorm(t)
	w := callInternalMerge(db, uuid.NewString(), `{"fields":{"ingest_source":"forbidden"}}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unexpected SQL for rejected field: %v", err)
	}
}

func TestInternalMergeEnrichmentMetadataReportsNotFound(t *testing.T) {
	db, mock := newMockGorm(t)
	id := uuid.New()
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(
		`UPDATE "content_items" SET "metadata"=COALESCE(metadata, '{}'::jsonb) || $1::jsonb WHERE public_id = $2`,
	)).
		WithArgs(sqlmock.AnyArg(), id.String()).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	w := callInternalMerge(db, id.String(), `{"fields":{"translation_ar":"ترجمة"}}`)
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, body = %s, sql = %v", w.Code, w.Body.String(), mock.ExpectationsWereMet())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectation: %v", err)
	}
}
