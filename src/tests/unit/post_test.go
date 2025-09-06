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

func setupPostRouter(t *testing.T) (*gin.Engine, sqlmock.Sqlmock) {
	gin.SetMode(gin.TestMode)
	router, _, mock := utils.SetupRouterAndMockDB(t)
	router.POST("/posts", controllers.CreatePost)
	router.GET("/posts", controllers.GetPosts)
	router.GET("/posts/:id", controllers.GetPost)
	router.PUT("/posts/:id", controllers.UpdatePost)
	router.DELETE("/posts/:id", controllers.DeletePost)
	return router, mock
}

func TestCreatePost_Success(t *testing.T) {
	router, mock := setupPostRouter(t)

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(`INSERT INTO "posts"`)).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
	// Preload after create
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT * FROM "posts"`)).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT * FROM "post_media"`)).
		WillReturnRows(sqlmock.NewRows([]string{"post_id", "media_id"}))
	mock.ExpectCommit()

	payload := models.Post{Title: "T", Content: "C", Author: "A"}
	body, _ := json.Marshal(payload)
	req := httptest.NewRequest(http.MethodPost, "/posts", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected %d, got %d, body=%s", http.StatusCreated, w.Code, w.Body.String())
	}
}

func TestCreatePost_DBError(t *testing.T) {
	router, mock := setupPostRouter(t)

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(`INSERT INTO "posts"`)).
		WillReturnError(assertErr())
	mock.ExpectRollback()

	payload := models.Post{Title: "T", Content: "C", Author: "A"}
	body, _ := json.Marshal(payload)
	req := httptest.NewRequest(http.MethodPost, "/posts", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected %d, got %d", http.StatusInternalServerError, w.Code)
	}
}

func TestGetPosts_Success(t *testing.T) {
	router, mock := setupPostRouter(t)

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT`)).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT * FROM "post_media"`)).
		WillReturnRows(sqlmock.NewRows([]string{"post_id", "media_id"}))

	req := httptest.NewRequest(http.MethodGet, "/posts", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected %d, got %d", http.StatusOK, w.Code)
	}
}

func TestGetPost_InvalidID(t *testing.T) {
	router, _ := setupPostRouter(t)
	req := httptest.NewRequest(http.MethodGet, "/posts/not-a-uuid", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected %d, got %d", http.StatusBadRequest, w.Code)
	}
}

func TestUpdatePost_InvalidID(t *testing.T) {
	router, _ := setupPostRouter(t)
	body, _ := json.Marshal(models.Post{Title: "T", Content: "C", Author: "A"})
	req := httptest.NewRequest(http.MethodPut, "/posts/not-a-uuid", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected %d, got %d", http.StatusBadRequest, w.Code)
	}
}

func TestDeletePost_InvalidID(t *testing.T) {
	router, _ := setupPostRouter(t)
	req := httptest.NewRequest(http.MethodDelete, "/posts/not-a-uuid", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected %d, got %d", http.StatusBadRequest, w.Code)
	}
}
