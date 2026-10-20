package workload

import (
	"context"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func mockDB(t *testing.T) (*gorm.DB, sqlmock.Sqlmock) {
	t.Helper()
	conn, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	db, err := gorm.Open(postgres.New(postgres.Config{Conn: conn}), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	return db, mock
}

func TestOptionalGovernorFailureRecoversNativeTransactionSavepoint(t *testing.T) {
	db, mock := mockDB(t)
	mock.ExpectBegin()
	mock.ExpectExec("SAVEPOINT wg_[a-z0-9]+").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("SELECT pg_advisory_xact_lock_shared").WithArgs("workload-governor/policy/v1").WillReturnError(errors.New("ledger unavailable"))
	mock.ExpectExec("ROLLBACK TO SAVEPOINT wg_[a-z0-9]+").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("SELECT native_receipt_still_commits")).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	err := db.Transaction(func(tx *gorm.DB) error {
		if err := SettleOwner(tx, "tenant-a", "content_stage", "owner-a", 1, "verified receipt"); err != nil {
			t.Fatal(err)
		}
		return tx.Exec("SELECT native_receipt_still_commits").Error
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestAdmissionSQLFailureAndPanicReturnExplicitFailOpen(t *testing.T) {
	for _, panicValue := range []bool{false, true} {
		t.Run(map[bool]string{false: "sql_error", true: "panic"}[panicValue], func(t *testing.T) {
			db, mock := mockDB(t)
			mock.ExpectBegin()
			if panicValue {
				mock.ExpectExec("SELECT pg_advisory_xact_lock_shared").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectQuery(`SELECT \* FROM "workload_governor_policies"`).WillReturnRows(sqlmock.NewRows([]string{"scope"}))
			} else {
				mock.ExpectExec("SELECT pg_advisory_xact_lock_shared").WillReturnError(errors.New("unavailable"))
			}
			mock.ExpectRollback()
			e := NewEngine(db)
			if panicValue {
				e.Clock = func() time.Time { panic("injected clock panic") }
			}
			d := e.RequestPermit(validRequest("text_embedding", 1))
			if !d.Allowed() || !d.Unavailable {
				t.Fatalf("SQL failure froze native caller: %+v", d)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestBoundedContextHonorsShorterNativeDeadline(t *testing.T) {
	db, _ := mockDB(t)
	parent, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	ctx, stop := boundedContext(db.WithContext(parent), time.Second)
	defer stop()
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > 30*time.Millisecond {
		t.Fatal("optional ledger extended native deadline")
	}
}

func TestFailedSnapshotCollectionCannotFabricateHeartbeat(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(map[bool]string{false: "provider_error", true: "deadline"}[canceled], func(t *testing.T) {
			db, mock := mockDB(t)
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			e := NewEngine(db.WithContext(parent))
			failure := errors.New("sample read unavailable")
			e.SampleProvider = func(ctx context.Context, _ []string) (map[string]Sample, error) {
				if canceled {
					cancel()
					return map[string]Sample{}, nil
				}
				return nil, failure
			}
			err := e.RefreshSnapshots()
			if (!canceled && !errors.Is(err, failure)) || (canceled && !errors.Is(err, context.Canceled)) {
				t.Fatalf("failed collection did not preserve its error: %v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
