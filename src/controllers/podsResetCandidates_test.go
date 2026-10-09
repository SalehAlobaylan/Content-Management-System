package controllers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"content-management-system/src/utils"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

func TestPodsResetLargePlanReachesPreflightWithoutAnItemCap(t *testing.T) {
	for _, count := range []int{31, 435} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			db, mock := newMockGorm(t)
			ids := make([]uuid.UUID, count)
			for index := range ids {
				ids[index] = uuid.New()
			}
			body, _ := json.Marshal(map[string]any{"content_ids": ids, "selection_reason": "Failed imports"})
			// Unavailable schema proof must still stop a valid large selection
			// before any preview persistence or owner effects.
			mock.ExpectQuery(`SELECT`).WillReturnError(sqlmock.ErrCancelled)
			response := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(response)
			c.Set("db", db)
			c.Set(utils.AdminPrincipalContextKey, utils.AdminPrincipal{TenantID: "tenant-a", UserID: "admin"})
			c.Request = httptest.NewRequest(http.MethodPost, "/admin/pods-reset/plans", bytes.NewReader(body))
			c.Request.Header.Set("Content-Type", "application/json")
			CreatePodsResetPlan(c)
			if response.Code != http.StatusServiceUnavailable || !bytes.Contains(response.Body.Bytes(), []byte("schema fingerprint is unavailable")) {
				t.Fatalf("large selection must reach preflight: %d %s", response.Code, response.Body)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPodsResetPreviewReportsUnknownSchemaReferenceWithoutPersisting(t *testing.T) {
	db, mock := newMockGorm(t)
	mock.ExpectQuery(`SELECT current_database`).WillReturnRows(sqlmock.NewRows([]string{"database_name", "schema_name"}).AddRow("test", "public"))
	mock.ExpectQuery(`SELECT table_name, column_name`).WillReturnRows(sqlmock.NewRows([]string{"table_name", "column_name", "column_type", "is_nullable", "default_value"}))
	mock.ExpectQuery(`SELECT ic.table_name`).WillReturnRows(sqlmock.NewRows([]string{"table_name", "column_name"}).AddRow("future_helper", "content_item_id"))
	mock.ExpectQuery(`SELECT c.relname`).WillReturnRows(sqlmock.NewRows([]string{"table_name", "trigger_name", "definition", "enabled"}))
	body, _ := json.Marshal(map[string]any{"content_ids": []uuid.UUID{uuid.New()}, "selection_reason": "Failed imports"})
	response := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(response)
	c.Set("db", db)
	c.Set(utils.AdminPrincipalContextKey, utils.AdminPrincipal{TenantID: "tenant-a", UserID: "admin"})
	c.Request = httptest.NewRequest(http.MethodPost, "/admin/pods-reset/plans", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")
	CreatePodsResetPlan(c)
	var result struct{ Error, Code string }
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if response.Code != http.StatusServiceUnavailable || result.Code != "pods_reset_schema_unqualified" || result.Error != "Reset preview is blocked: content reference future_helper.content_item_id has no explicit reset disposition." {
		t.Fatalf("schema incompatibility lost its safe diagnostic: %d %s", response.Code, response.Body)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPodsResetCandidatesStageEvidenceIsAlwaysAnArray(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, withStages := range []bool{false, true} {
		name := "no_stage_history"
		if withStages {
			name = "recorded_stages"
		}
		t.Run(name, func(t *testing.T) {
			db, mock := newMockGorm(t)
			id, now := uuid.New(), time.Now().UTC()
			mock.ExpectQuery(`SELECT count\(\*\) FROM "content_items"`).WithArgs("tenant-a", "VIDEO", "PODCAST").
				WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
			mock.ExpectQuery(`SELECT .*FROM "content_items"`).WithArgs("tenant-a", "VIDEO", "PODCAST", 51).
				WillReturnRows(sqlmock.NewRows([]string{"public_id", "type", "source", "status", "created_at", "updated_at", "processing_generation", "idempotency_key"}).
					AddRow(id, "VIDEO", "YOUTUBE", "READY", now, now, 1, "candidate-key"))
			stages := sqlmock.NewRows([]string{"content_item_id", "processing_generation", "stage", "state", "policy_version", "updated_at"})
			if withStages {
				stages.AddRow(id, 1, "media", "succeeded", "v1", now)
			}
			mock.ExpectQuery(`SELECT .*FROM "content_stage_requests"`).WithArgs("tenant-a", id).WillReturnRows(stages)
			response := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(response)
			c.Set("db", db)
			c.Set(utils.AdminPrincipalContextKey, utils.AdminPrincipal{TenantID: "tenant-a", UserID: "admin"})
			c.Request = httptest.NewRequest(http.MethodGet, "/admin/pods-reset/candidates", nil)
			ListPodsResetCandidates(c)
			if response.Code != http.StatusOK {
				t.Fatalf("candidate search: %d %s", response.Code, response.Body)
			}
			var body struct {
				Data struct {
					Items []struct {
						StageEvidence json.RawMessage `json:"stage_evidence"`
					} `json:"items"`
				} `json:"data"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil || len(body.Data.Items) != 1 {
				t.Fatalf("candidate response: %s %v", response.Body, err)
			}
			var evidence []podsResetCandidateStage
			if err := json.Unmarshal(body.Data.Items[0].StageEvidence, &evidence); err != nil || evidence == nil {
				t.Fatalf("stage evidence must be an array: %s %v", body.Data.Items[0].StageEvidence, err)
			}
			if withStages && (len(evidence) != 1 || evidence[0].Stage != "media") {
				t.Fatalf("recorded evidence was lost: %#v", evidence)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPodsResetCandidatesCountIncludesFullFilteredScopeAcrossPages(t *testing.T) {
	gin.SetMode(gin.TestMode)
	first, second := uuid.New(), uuid.New()
	for _, paged := range []bool{false, true} {
		t.Run(map[bool]string{false: "first_page", true: "next_page"}[paged], func(t *testing.T) {
			db, mock := newMockGorm(t)
			mock.ExpectQuery(`SELECT count\(\*\) FROM "content_items" WHERE \(tenant_id=\$1 AND type IN \(\$2,\$3\) AND retired_payload_at IS NULL\) AND type=\$4 AND status=\$5`).
				WithArgs("tenant-a", "VIDEO", "PODCAST", "PODCAST", "FAILED").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1234))
			rows := sqlmock.NewRows([]string{"public_id", "title", "type", "source", "status", "created_at"})
			query := mock.ExpectQuery(`SELECT .*FROM "content_items"`)
			url := "/admin/pods-reset/candidates?type=PODCAST&status=FAILED&limit=1"
			if paged {
				url += "&cursor=" + first.String()
				query.WithArgs("tenant-a", "VIDEO", "PODCAST", "PODCAST", "FAILED", first.String(), 2)
			} else {
				query.WithArgs("tenant-a", "VIDEO", "PODCAST", "PODCAST", "FAILED", 2)
				rows.AddRow(first, "عنوان الحلقة", "PODCAST", "PODCAST", "FAILED", time.Now().UTC())
			}
			rows.AddRow(second, "Next episode", "PODCAST", "PODCAST", "FAILED", time.Now().UTC())
			query.WillReturnRows(rows)
			pageID := first
			if paged {
				pageID = second
			}
			mock.ExpectQuery(`SELECT .*FROM "content_stage_requests"`).WithArgs("tenant-a", pageID).
				WillReturnRows(sqlmock.NewRows([]string{"content_item_id"}))
			response := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(response)
			c.Set("db", db)
			c.Set(utils.AdminPrincipalContextKey, utils.AdminPrincipal{TenantID: "tenant-a", UserID: "admin"})
			c.Request = httptest.NewRequest(http.MethodGet, url, nil)
			ListPodsResetCandidates(c)
			var body struct {
				Data struct {
					TotalCount int64                `json:"total_count"`
					Items      []podsResetCandidate `json:"items"`
					HasMore    bool                 `json:"has_more"`
				} `json:"data"`
			}
			if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &body) != nil || body.Data.TotalCount != 1234 || len(body.Data.Items) != 1 || body.Data.HasMore == paged {
				t.Fatalf("filtered count/page: %d %s", response.Code, response.Body)
			}
			wantTitle := "عنوان الحلقة"
			if paged {
				wantTitle = "Next episode"
			}
			if body.Data.Items[0].Title == nil || *body.Data.Items[0].Title != wantTitle {
				t.Fatalf("readable title missing: %s", response.Body)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPodsResetCandidatesCountFailureDoesNotClaimAnEmptyInventory(t *testing.T) {
	db, mock := newMockGorm(t)
	mock.ExpectQuery(`SELECT count\(\*\) FROM "content_items"`).WithArgs("tenant-a", "VIDEO", "PODCAST").WillReturnError(sqlmock.ErrCancelled)
	response := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(response)
	c.Set("db", db)
	c.Set(utils.AdminPrincipalContextKey, utils.AdminPrincipal{TenantID: "tenant-a", UserID: "admin"})
	c.Request = httptest.NewRequest(http.MethodGet, "/admin/pods-reset/candidates", nil)
	ListPodsResetCandidates(c)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("count failure: %d %s", response.Code, response.Body)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
