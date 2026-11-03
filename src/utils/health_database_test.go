package utils

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func healthDatabaseFixture(t *testing.T) (*gorm.DB, sqlmock.Sqlmock, string, string) {
	t.Helper()
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{DisableAutomaticPing: true, Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	version, content := "20261102000000_health.sql", []byte("SELECT 1;\n")
	if err := os.WriteFile(filepath.Join(dir, version), content, 0600); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(content)
	ledger, _ := json.Marshal([]schemaMigrationRecord{{Version: version, Checksum: hex.EncodeToString(sum[:])}})
	return db, mock, dir, string(ledger)
}

func healthFlags(mock sqlmock.Sqlmock, ledger, identity, fence bool) {
	mock.ExpectQuery(`SELECT to_regclass`).WillReturnRows(sqlmock.NewRows([]string{"ledger", "identity", "fence"}).AddRow(ledger, identity, fence))
}

func healthData(mock sqlmock.Sqlmock, ledger, identity, fence any) {
	mock.ExpectQuery(`(?s)^SELECT .*NULL::text|(?s)^SELECT .*json_agg`).WillReturnRows(sqlmock.NewRows([]string{"ledger", "identity", "fence"}).AddRow(ledger, identity, fence))
}

const healthIdentity = `{"database_id":"db","lineage_root_id":"root","epoch":3}`
const healthFence = `{"state":"open","epoch":3,"fence_token":"token"}`

func TestHealthDatabaseNormalPathUsesTwoQueries(t *testing.T) {
	db, mock, dir, ledger := healthDatabaseFixture(t)
	healthFlags(mock, true, true, true)
	healthData(mock, ledger, healthIdentity, healthFence)
	state := ReadHealthDatabaseState(context.Background(), db, dir)
	if state.ContractError != nil || state.FenceError != nil || state.Contract.LedgerState != "verified" || state.Contract.IdentityState != "present" || state.Contract.DatabaseID != "db" || state.Contract.Epoch != 3 || state.Fence.State != "open" {
		t.Fatalf("unexpected health state: %+v", state)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	if pool, _ := db.DB(); pool.Stats().InUse != 0 {
		t.Fatal("health connection was not released")
	}
}

func TestHealthDatabaseConnectionAcquisitionFailuresRemainUnknown(t *testing.T) {
	for _, cause := range []string{"cancelled", "closed pool"} {
		t.Run(cause, func(t *testing.T) {
			db, mock, dir, _ := healthDatabaseFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if cause == "cancelled" {
				cancel()
			} else {
				pool, _ := db.DB()
				_ = pool.Close()
			}
			state := ReadHealthDatabaseState(ctx, db, dir)
			if state.ContractError == nil || state.FenceError == nil || state.Contract.IdentityState != "unknown" || state.Fence.State != "" {
				t.Fatalf("connection failure became valid evidence: %+v", state)
			}
			if cause == "cancelled" && !errors.Is(state.ContractError, context.Canceled) {
				t.Fatalf("request cancellation was lost: %v", state.ContractError)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestHealthDatabaseAllTablesAbsentNeedsOnlyCatalogRead(t *testing.T) {
	db, mock, dir, _ := healthDatabaseFixture(t)
	healthFlags(mock, false, false, false)
	state := ReadHealthDatabaseState(context.Background(), db, filepath.Join(dir, "not-read"))
	if state.ContractError != nil || state.FenceError != nil || state.Contract.LedgerState != "absent" || state.Contract.IdentityState != "unknown" || state.Fence.State != "compatibility_open" {
		t.Fatalf("unexpected absent state: %+v", state)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestHealthDatabaseNullScalarsRemainErrors(t *testing.T) {
	for _, section := range []string{"identity", "fence"} {
		valid, fields := healthIdentity, []string{"database_id", "lineage_root_id", "epoch"}
		if section == "fence" {
			valid, fields = healthFence, []string{"state", "epoch", "fence_token"}
		}
		for _, field := range fields {
			t.Run(section+"/"+field, func(t *testing.T) {
				db, mock, dir, ledger := healthDatabaseFixture(t)
				var row map[string]any
				_ = json.Unmarshal([]byte(valid), &row)
				row[field] = nil
				invalid, _ := json.Marshal(row)
				identity, fence := healthIdentity, healthFence
				if section == "identity" {
					identity = string(invalid)
				} else {
					fence = string(invalid)
				}
				healthFlags(mock, true, true, true)
				healthData(mock, ledger, identity, fence)
				state := ReadHealthDatabaseState(context.Background(), db, dir)
				if (state.ContractError != nil) != (section == "identity") || (state.FenceError != nil) != (section == "fence") {
					t.Fatalf("NULL became valid evidence: %+v", state)
				}
				if section == "identity" && state.Contract.IdentityState != "unknown" {
					t.Fatal("NULL identity was marked present")
				}
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestHealthDatabaseCompatibilityStates(t *testing.T) {
	for _, name := range []string{"ledger absent", "identity table absent", "identity row absent", "fence absent", "fence row absent", "incomplete ledger", "drifted ledger", "files unreadable"} {
		t.Run(name, func(t *testing.T) {
			db, mock, dir, ledger := healthDatabaseFixture(t)
			var identity, fence any = healthIdentity, healthFence
			ledgerPresent, identityPresent, fencePresent := name != "ledger absent", name != "identity table absent", name != "fence absent"
			wantLedger, wantIdentity, wantFence, wantContractError, wantFenceError := "verified", "present", "open", false, false
			if !ledgerPresent {
				ledger = ""
				identity = nil
				wantLedger, wantIdentity = "absent", "unknown"
				dir = filepath.Join(dir, "not-read")
			}
			if !identityPresent || name == "identity row absent" {
				identity = nil
				wantIdentity = "unknown"
			}
			if !fencePresent {
				fence = nil
				wantFence = "compatibility_open"
			}
			if name == "fence row absent" {
				fence = nil
				wantFence, wantFenceError = "", true
			}
			if name == "incomplete ledger" {
				ledger, wantLedger = "[]", "incomplete"
			}
			if name == "drifted ledger" {
				ledger = `[{"Version":"20261102000000_health.sql","Checksum":"changed"}]`
				wantLedger = "drifted"
			}
			if name == "files unreadable" {
				dir = filepath.Join(dir, "missing")
				identity = nil
				wantContractError, wantLedger, wantIdentity = true, "unknown", "unknown"
			}
			healthFlags(mock, ledgerPresent, identityPresent, fencePresent)
			healthData(mock, ledger, identity, fence)
			state := ReadHealthDatabaseState(context.Background(), db, dir)
			if state.Contract.LedgerState != wantLedger || state.Contract.IdentityState != wantIdentity || state.Fence.State != wantFence || (state.ContractError != nil) != wantContractError || (state.FenceError != nil) != wantFenceError {
				t.Fatalf("unexpected compatibility state: %+v", state)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestHealthDatabaseFallbackPreservesIndependentErrors(t *testing.T) {
	for _, denied := range []string{"contract", "fence"} {
		t.Run(denied, func(t *testing.T) {
			db, mock, dir, ledger := healthDatabaseFixture(t)
			healthFlags(mock, true, true, true)
			permission := errors.New("permission denied")
			mock.ExpectQuery(`(?s)^SELECT .*json_agg`).WillReturnError(permission)
			mock.ExpectQuery(`SELECT to_regclass`).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
			contractRead := mock.ExpectQuery(`SELECT version`)
			if denied == "contract" {
				contractRead.WillReturnError(permission)
			} else {
				var applied []schemaMigrationRecord
				_ = json.Unmarshal([]byte(ledger), &applied)
				contractRead.WillReturnRows(sqlmock.NewRows([]string{"version", "checksum"}).AddRow(applied[0].Version, applied[0].Checksum))
				mock.ExpectQuery(`SELECT to_regclass`).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
				mock.ExpectQuery(`SELECT database_id`).WillReturnRows(sqlmock.NewRows([]string{"database_id", "lineage_root_id", "database_epoch"}).AddRow("db", "root", 3))
			}
			mock.ExpectQuery(`SELECT to_regclass`).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
			fenceRead := mock.ExpectQuery(`SELECT state`)
			if denied == "fence" {
				fenceRead.WillReturnError(permission)
			} else {
				fenceRead.WillReturnRows(sqlmock.NewRows([]string{"state", "epoch", "fence_token"}).AddRow("open", 3, "token"))
			}
			state := ReadHealthDatabaseState(context.Background(), db, dir)
			if (state.ContractError != nil) != (denied == "contract") || (state.FenceError != nil) != (denied == "fence") {
				t.Fatalf("errors were not independent: %+v", state)
			}
			if denied == "contract" && state.Fence.State != "open" {
				t.Fatal("readable fence was lost")
			}
			if denied == "fence" && state.Contract.LedgerState != "verified" {
				t.Fatal("readable contract was lost")
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestHealthDatabaseCancellationDoesNotStartFallbackQueries(t *testing.T) {
	for _, at := range []string{"catalog", "data"} {
		t.Run(at, func(t *testing.T) {
			db, mock, dir, _ := healthDatabaseFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			defer cancel()
			if at == "data" {
				healthFlags(mock, true, true, true)
			}
			mock.ExpectQuery(`SELECT`).WillDelayFor(100 * time.Millisecond).WillReturnRows(sqlmock.NewRows([]string{"ignored"}))
			state := ReadHealthDatabaseState(ctx, db, dir)
			if ctx.Err() == nil || state.ContractError == nil || state.FenceError == nil {
				t.Fatalf("cancelled observation became ready: %+v", state)
			}
			if !strings.Contains(state.ContractError.Error(), "cancel") {
				t.Fatalf("missing cancellation error: %v", state.ContractError)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
