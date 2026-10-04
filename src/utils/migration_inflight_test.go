package utils

import (
	"errors"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func migrationWorkDB(t *testing.T) (*gorm.DB, sqlmock.Sqlmock) {
	t.Helper()
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sqlDB.Close() })
	db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	return db, mock
}

func TestMigrationWorkHistoricalSchemaHasNoOwnerWork(t *testing.T) {
	db, mock := migrationWorkDB(t)
	for _, subject := range []string{"lifecycle_operation_claims", "pipeline_stage_leases", "source_run_attempts", "content_stage_attempts", "pipeline_repair_requests", "artifact_coverage_requests", "atomization_work_requests", "studio_clearance_requests", "media_supply_actions"} {
		mock.ExpectQuery(`SELECT to_regclass`).WithArgs("public." + subject).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
	}
	r, err := InspectCMSMigrationWork(db)
	if err != nil || r.State != "ready" || r.Count != 0 || r.Checks == nil {
		t.Fatalf("unexpected report: %+v, %v", r, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestMigrationWorkExpiredAttemptsAndClosedRequestsRemainBlocked(t *testing.T) {
	db, mock := migrationWorkDB(t)
	mock.ExpectQuery(`SELECT to_regclass`).WithArgs("public.lifecycle_operation_claims").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectQuery(`SELECT count\(\*\) FROM lifecycle_operation_claims`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	for _, probe := range CMSMigrationInflightQueries {
		mock.ExpectQuery(`SELECT to_regclass`).WithArgs("public." + probe.Subject).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
		count := int64(0)
		if probe.Subject == "content_stage_attempts" {
			count = 513
		}
		mock.ExpectQuery(regexp.QuoteMeta(probe.Query)).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(count))
		if count > 0 {
			mock.ExpectQuery(`SELECT count\(\*\) FROM content_stage_attempts WHERE .*lease_expires_at`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(513))
			mock.ExpectQuery(`SELECT count\(\*\) FROM content_stage_attempts a`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(369))
		}
	}
	r, err := InspectCMSMigrationWork(db)
	if err != nil || r.State != "blocked" || r.Count != 514 || r.Checks[3].Expired != 513 || r.Checks[3].TerminalRequests != 369 {
		t.Fatalf("unfinished effects were treated as settled: %+v, %v", r, err)
	}
	// No writes are expected; sqlmock rejects any attempt to settle or delete work.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestMigrationWorkUnknownCountCannotAuthorizePreparation(t *testing.T) {
	db, mock := migrationWorkDB(t)
	mock.ExpectQuery(`SELECT to_regclass`).WithArgs("public.lifecycle_operation_claims").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectQuery(`SELECT count\(\*\) FROM lifecycle_operation_claims`).WillReturnError(errors.New("read denied"))
	if _, err := InspectCMSMigrationWork(db); err == nil {
		t.Fatal("unknown inventory was accepted")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
