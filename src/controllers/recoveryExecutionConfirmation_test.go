package controllers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"content-management-system/src/utils"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

func TestFeedRecoveryExecutionRequiresManifestAndTypedCount(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name, body, level string
		status            int
		claim             bool
	}{
		{"missing_count", `{"manifest_hash":"frozen"}`, "repair", 400, false},
		{"null_count", `{"manifest_hash":"frozen","confirmed_item_count":null}`, "repair", 400, false},
		{"wrong_count", `{"manifest_hash":"frozen","confirmed_item_count":1}`, "repair", 400, false},
		{"negative_count", `{"manifest_hash":"frozen","confirmed_item_count":-1}`, "rotate", 400, false},
		{"wrong_manifest", `{"manifest_hash":"other","confirmed_item_count":0}`, "repair", 400, false},
		{"legacy_password_proof", `{"reauth_proof":"old-proof"}`, "repair", 400, false},
		{"legacy_purge", `{"manifest_hash":"frozen","confirmed_item_count":0}`, "purge_reseed", 409, false},
		{"unsupported_level", `{"manifest_hash":"frozen","confirmed_item_count":0}`, "unknown", 409, false},
		{"confirmed_repair", `{"manifest_hash":"frozen","confirmed_item_count":0}`, "repair", 409, true},
		{"confirmed_rotate", `{"manifest_hash":"frozen","confirmed_item_count":0}`, "rotate", 409, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := newMockGorm(t)
			id, planID := uuid.New(), uuid.New()
			mock.ExpectQuery(`SELECT .*FROM "feed_recovery_runs"`).WithArgs("tenant-a", id, 1).
				WillReturnRows(sqlmock.NewRows([]string{"id", "public_id", "plan_id", "tenant_id"}).AddRow(1, id, 2, "tenant-a"))
			mock.ExpectQuery(`SELECT .*FROM "feed_recovery_plans"`).WithArgs("tenant-a", 2, 1).
				WillReturnRows(sqlmock.NewRows([]string{"public_id", "level", "manifest_hash", "target_count"}).AddRow(planID, tc.level, "frozen", 0))
			if tc.claim {
				// A correct confirmation advances to the existing cancel-window guard,
				// without IAM/JWT password proof or any lease/content mutation.
				mock.ExpectBegin()
				mock.ExpectQuery(`SELECT .*FROM "feed_recovery_runs"`).WithArgs("tenant-a", id, 1).
					WillReturnRows(sqlmock.NewRows([]string{"phase", "not_before"}).AddRow("cancel_window", time.Now().Add(time.Hour)))
				mock.ExpectRollback()
			}
			response := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(response)
			c.Set("db", db)
			c.Set(utils.AdminPrincipalContextKey, utils.AdminPrincipal{TenantID: "tenant-a", UserID: "admin"})
			c.Params = gin.Params{{Key: "id", Value: id.String()}}
			c.Request = httptest.NewRequest(http.MethodPost, "/execute", strings.NewReader(tc.body))
			c.Request.Header.Set("Content-Type", "application/json")
			ExecuteFeedRecoveryRun(c)
			if response.Code != tc.status || (tc.claim && !strings.Contains(response.Body.String(), "cancel window remains open")) {
				t.Fatalf("execution: %d %s", response.Code, response.Body)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPodsResetExecutionRequiresExactManifestCountBeforeExecution(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name, body string
		confirmed  bool
	}{
		{"missing_count", `{"manifest_hash":"frozen"}`, false},
		{"null_count", `{"manifest_hash":"frozen","confirmed_item_count":null}`, false},
		{"wrong_count", `{"manifest_hash":"frozen","confirmed_item_count":2}`, false},
		{"wrong_manifest", `{"manifest_hash":"other","confirmed_item_count":3}`, false},
		{"exact_count", `{"manifest_hash":"frozen","confirmed_item_count":3}`, true},
		{"large_exact_count", `{"manifest_hash":"frozen","confirmed_item_count":435}`, true},
		{"large_wrong_count", `{"manifest_hash":"frozen","confirmed_item_count":434}`, false},
		{"expired_approved", `{"manifest_hash":"frozen","confirmed_item_count":3}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := newMockGorm(t)
			id := uuid.New()
			count := 3
			if strings.HasPrefix(tc.name, "large_") {
				count = 435
			}
			ids := make([]uuid.UUID, count)
			for index := range ids {
				ids[index] = uuid.New()
			}
			manifest, _ := json.Marshal(map[string]any{"requested_ids": ids})
			mock.ExpectQuery(`SELECT .*FROM "pods_reset_runs"`).WithArgs("tenant-a", id, 1).
				WillReturnRows(sqlmock.NewRows([]string{"public_id", "tenant_id", "state", "manifest_hash", "manifest", "pause_requested", "expires_at"}).AddRow(id, "tenant-a", "approved", "frozen", string(manifest), tc.name != "expired_approved", time.Now().Add(-time.Hour)))
			response := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(response)
			c.Set("db", db)
			c.Set(utils.AdminPrincipalContextKey, utils.AdminPrincipal{TenantID: "tenant-a", UserID: "admin"})
			c.Params = gin.Params{{Key: "id", Value: id.String()}}
			c.Request = httptest.NewRequest(http.MethodPost, "/execute", strings.NewReader(tc.body))
			c.Request.Header.Set("Content-Type", "application/json")
			ExecutePodsResetRun(c)
			want := http.StatusBadRequest
			if tc.confirmed {
				want = http.StatusConflict
			}
			// Correct confirmation reaches the run's pause control without any
			// retention rollout lookup. Invalid counts never enter execution.
			executionCode := "RESET_PAUSED"
			if tc.name == "expired_approved" {
				executionCode = "POLICY_DRIFT"
			}
			if response.Code != want || (tc.confirmed && !strings.Contains(response.Body.String(), executionCode)) {
				t.Fatalf("execution: %d %s", response.Code, response.Body)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
