package controllers

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"content-management-system/src/models"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
)

func TestRetentionPolicySettingsAPI(t *testing.T) {
	for _, test := range []struct {
		name, payload string
		status        int
	}{
		{"partial settings", `{"database_critical_bytes":838860800,"schedule_interval_minutes":60}`, http.StatusOK},
		{"negative target", `{"database_target_bytes":-1}`, http.StatusBadRequest},
		{"zero target", `{"database_target_bytes":0}`, http.StatusBadRequest},
		{"unordered critical", `{"database_critical_bytes":461373440}`, http.StatusBadRequest},
		{"short interval", `{"schedule_interval_minutes":14}`, http.StatusBadRequest},
		{"long interval", `{"schedule_interval_minutes":10081}`, http.StatusBadRequest},
		{"oversized row cap", `{"max_rows_per_run":10001}`, http.StatusBadRequest},
		{"empty byte cap", `{"max_bytes_per_run":0}`, http.StatusBadRequest},
		{"fractional interval", `{"schedule_interval_minutes":15.5}`, http.StatusBadRequest},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, mock := retentionIsolatedDB(t)
			mock.ExpectQuery(`SELECT .*FROM "retention_policies"`).WithArgs("default", 1).
				WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "mode", "policy_version"}).AddRow("default", "observe", 3))
			if test.name != "fractional interval" {
				mock.ExpectQuery(`SELECT .*FROM "news_circulation_policies"`).WillReturnRows(sqlmock.NewRows([]string{"timezone"}).AddRow("Asia/Riyadh"))
			}
			if test.status == http.StatusOK {
				mock.ExpectQuery(`INSERT INTO "retention_policies"`).WillReturnRows(sqlmock.NewRows([]string{"id", "public_id"}).AddRow(1, uuid.New()))
				mock.ExpectQuery(`INSERT INTO "audit_logs"`).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
			}
			response := retentionTestRequest(db, UpdateRetentionPolicy, uuid.Nil, test.payload)
			if response.Code != test.status {
				t.Fatalf("HTTP %d: %s", response.Code, response.Body)
			}
			if test.status == http.StatusOK {
				var result struct {
					Data models.RetentionPolicy `json:"data"`
				}
				if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
					t.Fatal(err)
				}
				policy := result.Data
				if policy.DatabaseCriticalBytes != 800*retentionTestMiB || policy.ScheduleIntervalMinutes != 60 || policy.PolicyVersion != 4 || policy.UpdatedBy != "reviewer@example.test" || policy.Enabled || policy.Mode != "observe" || policy.MaxRowsPerRun != 500 || policy.MaxBytesPerRun != 32*retentionTestMiB || policy.DatabaseTargetBytes != 400*retentionTestMiB || policy.DatabaseActionBytes != 440*retentionTestMiB {
					t.Fatalf("partial settings changed unrelated policy: %+v", policy)
				}
				if got := retentionVerdict(policy, &models.RetentionDBSample{DatabaseBytes: 600 * retentionTestMiB}, retentionForecast{}); got != models.RetentionVerdictActionRequired {
					t.Fatalf("new critical threshold was not used: %s", got)
				}
			}
		})
	}
}

func TestRetentionHistoricalRejectsReducedCleanupLimitsBeforeEffects(t *testing.T) {
	for _, cap := range []string{"rows", "bytes"} {
		t.Run(cap, func(t *testing.T) {
			db, mock := retentionIsolatedDB(t)
			id, ids, stories := uuid.New(), []uuid.UUID{uuid.New()}, []uuid.UUID{uuid.New()}
			rowCap, byteCap := 500, int64(32*retentionTestMiB)
			if cap == "rows" {
				ids = append(ids, uuid.New())
				rowCap = 1
			}
			if cap == "bytes" {
				byteCap = 1
			}
			hash := historicalManifestHash("default", "Asia/Riyadh", ids, stories)
			actionRows := func() *sqlmock.Rows {
				return sqlmock.NewRows([]string{"id", "public_id", "tenant_id", "run_id", "action_class", "outcome", "manifest_hash"}).AddRow(7, id, "default", 8, models.RetentionActionPrepareHistoricalPurge, models.RetentionActionApproved, hash)
			}
			mock.ExpectQuery(`SELECT .*FROM "retention_actions"`).WillReturnRows(actionRows())
			mock.ExpectQuery(`SELECT .*FROM "retention_execution_controls"`).WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "historical_enabled"}).AddRow("default", true))
			mock.ExpectQuery(`SELECT .*FROM "retention_policies"`).WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "enabled", "mode", "max_rows_per_run", "max_bytes_per_run"}).AddRow("default", true, "assist", rowCap, byteCap))
			mock.ExpectBegin()
			mock.ExpectQuery(`SELECT .*FROM "retention_actions"`).WillReturnRows(actionRows())
			mock.ExpectQuery(`SELECT .*FROM "retention_historical_manifests"`).WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "run_id", "action_id", "state", "timezone", "manifest_hash", "content_ids", "story_ids", "content_count", "story_count", "estimated_bytes", "expires_at"}).AddRow(9, "default", 8, 7, "approved", "Asia/Riyadh", hash, []byte(idsJSON(ids)), []byte(idsJSON(stories)), len(ids), len(stories), 2, time.Now().Add(time.Hour)))
			mock.ExpectRollback()
			response := retentionTestRequest(db, ExecuteHistoricalRetention, id)
			if response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), "current cleanup limits") {
				t.Fatalf("reduced cap did not block before content/storage effects: %d %s", response.Code, response.Body)
			}
		})
	}
}
