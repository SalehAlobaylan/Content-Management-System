package unit

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"
	"time"

	"content-management-system/src/controllers"
	"content-management-system/src/models"
	"content-management-system/src/utils"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

func setupPageRouter(t *testing.T) (*gin.Engine, *gorm.DB, sqlmock.Sqlmock) {
	gin.SetMode(gin.TestMode)
	router, db, mock := utils.SetupRouterAndMockDB(t)
	router.POST("/pages", controllers.CreatePage)
	router.GET("/pages/:id", controllers.GetPage)
	router.PUT("/pages/:id", controllers.UpdatePage)
	router.DELETE("/pages/:id", controllers.DeletePage)
	return router, db, mock
}

func TestCreatePage_Success(t *testing.T) {
	router, _, mock := setupPageRouter(t)

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(`INSERT INTO "pages"`)).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
	mock.ExpectCommit()

	payload := models.Page{Title: "Test Page", Content: "Hello"}
	body, _ := json.Marshal(payload)

	req := httptest.NewRequest(http.MethodPost, "/pages", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d, body=%s", http.StatusCreated, w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestCreatePage_DBError(t *testing.T) {
	router, _, mock := setupPageRouter(t)

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(`INSERT INTO "pages"`)).
		WillReturnError(assertErr())
	mock.ExpectRollback()

	payload := models.Page{Title: "Bad", Content: "Bad"}
	body, _ := json.Marshal(payload)
	req := httptest.NewRequest(http.MethodPost, "/pages", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected status %d, got %d", http.StatusInternalServerError, w.Code)
	}
}

func TestGetPage_InvalidID(t *testing.T) {
	router, _, _ := setupPageRouter(t)

	req := httptest.NewRequest(http.MethodGet, "/pages/abc", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected %d, got %d", http.StatusBadRequest, w.Code)
	}
}

func TestGetPage_NotFound(t *testing.T) {
	router, _, mock := setupPageRouter(t)

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT * FROM "pages"`)).
		WillReturnError(gorm.ErrRecordNotFound)

	req := httptest.NewRequest(http.MethodGet, "/pages/1", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected %d, got %d", http.StatusNotFound, w.Code)
	}
}

func TestGetPage_Success(t *testing.T) {
	router, _, mock := setupPageRouter(t)

	cols := []string{"id", "public_id", "title", "content", "created_at", "updated_at"}
	now := time.Now()
	pid := uuid.New()
	row := sqlmock.NewRows(cols).AddRow(1, pid, "Title", "Body", now, now)
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT * FROM "pages"`)).
		WillReturnRows(row)
	// second First call inside handler
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT * FROM "pages"`)).
		WillReturnRows(row)

	req := httptest.NewRequest(http.MethodGet, "/pages/1", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected %d, got %d, body=%s", http.StatusOK, w.Code, w.Body.String())
	}
}

func TestUpdatePage_Success(t *testing.T) {
	router, _, mock := setupPageRouter(t)

	// Find existing
	cols := []string{"id", "public_id", "title", "content", "created_at", "updated_at"}
	now := time.Now()
	pid := uuid.New()
	row := sqlmock.NewRows(cols).AddRow(1, pid, "Old", "Old", now, now)
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT * FROM "pages"`)).
		WillReturnRows(row)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE "pages" SET`)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	payload := models.Page{Title: "New", Content: "New"}
	body, _ := json.Marshal(payload)
	req := httptest.NewRequest(http.MethodPut, "/pages/1", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected %d, got %d", http.StatusOK, w.Code)
	}
}

func TestUpdatePage_SaveError(t *testing.T) {
	router, _, mock := setupPageRouter(t)

	cols := []string{"id", "public_id", "title", "content", "created_at", "updated_at"}
	now := time.Now()
	pid := uuid.New()
	row := sqlmock.NewRows(cols).AddRow(1, pid, "Old", "Old", now, now)
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT * FROM "pages"`)).
		WillReturnRows(row)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE "pages" SET`)).
		WillReturnError(assertErr())
	mock.ExpectRollback()

	payload := models.Page{Title: "New", Content: "New"}
	body, _ := json.Marshal(payload)
	req := httptest.NewRequest(http.MethodPut, "/pages/1", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected %d, got %d", http.StatusInternalServerError, w.Code)
	}
}

func TestDeletePage_Success(t *testing.T) {
	router, _, mock := setupPageRouter(t)

	cols := []string{"id", "public_id", "title", "content", "created_at", "updated_at"}
	now := time.Now()
	pid := uuid.New()
	row := sqlmock.NewRows(cols).AddRow(1, pid, "T", "C", now, now)
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT * FROM "pages"`)).
		WillReturnRows(row)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM "pages"`)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	payload := models.Page{Title: "T", Content: "C"}
	body, _ := json.Marshal(payload)
	req := httptest.NewRequest(http.MethodDelete, "/pages/1", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected %d, got %d", http.StatusOK, w.Code)
	}
}

func TestDeletePage_NotFound(t *testing.T) {
	router, _, mock := setupPageRouter(t)

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT * FROM "pages"`)).
		WillReturnError(gorm.ErrRecordNotFound)

	payload := models.Page{Title: "T", Content: "C"}
	body, _ := json.Marshal(payload)
	req := httptest.NewRequest(http.MethodDelete, "/pages/1", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected %d, got %d", http.StatusNotFound, w.Code)
	}
}

func TestDeletePage_InvalidJSON(t *testing.T) {
	router, _, _ := setupPageRouter(t)
	req := httptest.NewRequest(http.MethodDelete, "/pages/1", bytes.NewBufferString("{"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected %d, got %d", http.StatusBadRequest, w.Code)
	}
}

func TestDeletePage_InvalidID(t *testing.T) {
	router, _, _ := setupPageRouter(t)
	payload := models.Page{Title: "T", Content: "C"}
	body, _ := json.Marshal(payload)
	req := httptest.NewRequest(http.MethodDelete, "/pages/abc", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected %d, got %d", http.StatusBadRequest, w.Code)
	}
}

// assertErr provides a stable error instance for mocking failures
func assertErr() error { return &mockErr{"mock error"} }

type mockErr struct{ s string }

func (e *mockErr) Error() string { return e.s }
