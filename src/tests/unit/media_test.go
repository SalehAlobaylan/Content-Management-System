package unit

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"

	"content-management-system/src/controllers"
	"content-management-system/src/models"
	"content-management-system/src/utils"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
)

func setupMediaRouter(t *testing.T) (*gin.Engine, sqlmock.Sqlmock) {
	gin.SetMode(gin.TestMode)
	router, _, mock := utils.SetupRouterAndMockDB(t)
	router.POST("/media", controllers.CreateMedia)
	router.GET("/media/:id", controllers.GetMedia)
	router.GET("/media", controllers.GetMedia)
	router.DELETE("/media/:id", controllers.DeleteMedia)
	return router, mock
}

func TestCreateMedia_Success(t *testing.T) {
	router, mock := setupMediaRouter(t)

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(`INSERT INTO "media"`)).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
	mock.ExpectCommit()

	payload := models.Media{URL: "https://x", Type: "image"}
	body, _ := json.Marshal(payload)
	req := httptest.NewRequest(http.MethodPost, "/media", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("expected %d, got %d", http.StatusCreated, w.Code)
	}
}

func TestCreateMedia_DBError(t *testing.T) {
	router, mock := setupMediaRouter(t)
	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(`INSERT INTO "media"`)).
		WillReturnError(assertErr())
	mock.ExpectRollback()

	payload := models.Media{URL: "https://x", Type: "image"}
	body, _ := json.Marshal(payload)
	req := httptest.NewRequest(http.MethodPost, "/media", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected %d, got %d", http.StatusInternalServerError, w.Code)
	}
}

func TestDeleteMedia_InvalidID(t *testing.T) {
	router, _ := setupMediaRouter(t)
	body, _ := json.Marshal(models.Media{URL: "u"})
	req := httptest.NewRequest(http.MethodDelete, "/media/not-a-uuid", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected %d, got %d", http.StatusBadRequest, w.Code)
	}
}
