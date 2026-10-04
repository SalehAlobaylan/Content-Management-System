package supply

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"regexp"
	"testing"
	"time"

	"content-management-system/src/models"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type repairAuditPayload struct{}

func (repairAuditPayload) Match(v driver.Value) bool {
	var b []byte
	switch x := v.(type) {
	case string:
		b = []byte(x)
	case []byte:
		b = x
	default:
		return false
	}
	var payload map[string]any
	return json.Unmarshal(b, &payload) == nil && payload["request_target_state"] == "expired" && payload["evidence_state"] == "unknown" && payload["actor"] == "operator"
}

func TestLegacyRepairRecordsCompatibleAuditAndRollsBackOnAuditFailure(t *testing.T) {
	for _, failAudit := range []bool{false, true} {
		name := "commit with audit"
		if failAudit {
			name = "rollback failed audit"
		}
		t.Run(name, func(t *testing.T) {
			t.Setenv("WAHB_LOCAL_PREPARATION", "")
			sqlDB, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer sqlDB.Close()
			db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
			if err != nil {
				t.Fatal(err)
			}
			now := time.Date(2026, 10, 3, 8, 0, 0, 0, time.UTC)
			updated := now.Add(-24 * time.Hour)
			requestID, sourceID := uuid.New(), uuid.New()
			requestRows := func() *sqlmock.Rows {
				return sqlmock.NewRows([]string{"id", "public_id", "tenant_id", "content_source_id", "state", "updated_at"}).AddRow(1, requestID, "default", sourceID, "requested", updated)
			}
			preview := func() {
				mock.ExpectQuery("SELECT count.*information_schema.tables").WithArgs("source_run_requests", "BASE TABLE").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
				mock.ExpectQuery(`SELECT \* FROM "source_run_requests"`).WillReturnRows(requestRows())
				mock.ExpectQuery("SELECT count.*information_schema.tables").WithArgs("content_processing_events", "BASE TABLE").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
				// Repair-completion audit rows must never become provider evidence.
				mock.ExpectQuery(`SELECT \* FROM "content_processing_events".*stage <> .*event_class <>`).WithArgs(1, "completed", "succeeded", "failed", "cancelled", "expired", "source_run.repair", "legacy_source_run_repaired").WillReturnRows(sqlmock.NewRows([]string{"id", "state"}))
			}
			preview()
			mock.ExpectBegin()
			mock.ExpectExec("LOCK TABLE source_run_requests, content_processing_events").WillReturnResult(sqlmock.NewResult(0, 0))
			preview()
			mock.ExpectQuery(`SELECT \* FROM "source_run_requests".*FOR UPDATE`).WillReturnRows(requestRows())
			mock.ExpectExec(`UPDATE "source_run_requests"`).WillReturnResult(sqlmock.NewResult(0, 1))
			eventArgs := make([]driver.Value, 22)
			for i := range eventArgs {
				eventArgs[i] = sqlmock.AnyArg()
			}
			eventArgs[4], eventArgs[5], eventArgs[17], eventArgs[19] = "source_run.repair", "completed", "legacy_source_run_repaired", repairAuditPayload{}
			insert := mock.ExpectQuery(regexp.QuoteMeta(`INSERT INTO "content_processing_events"`)).WithArgs(eventArgs...)
			if failAudit {
				insert.WillReturnError(errors.New("audit unavailable"))
				mock.ExpectRollback()
			} else {
				insert.WillReturnRows(sqlmock.NewRows([]string{"public_id", "id"}).AddRow(uuid.New(), 1))
				mock.ExpectCommit()
			}
			report := LegacySourceRunRepairReport{SchemaVersion: "legacy-source-run-repair/v1", Actions: []LegacySourceRunRepairAction{{RequestID: requestID, TenantID: "default", ContentSourceID: sourceID, ExpectedState: "requested", ExpectedUpdated: updated, TargetState: models.SourceRunExpired, EvidenceState: "unknown", Reason: "stale active legacy request"}}}
			_, err = ApplyLegacySourceRunRepair(db, legacyRepairDigest(report), "operator", now)
			if (err != nil) != failAudit {
				t.Fatalf("audit failure=%v, error=%v", failAudit, err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
