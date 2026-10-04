package utils

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func TestAdoptionLegacyHistoryRequiresExactReviewedHistoricalEvidence(t *testing.T) {
	const old = "20260113000000_init.sql"
	const anchor = "20260716000000_migration_ledger_checksums.sql"
	const modern = "20260809000000_source_run_reliability.sql"
	dir := t.TempDir()
	for _, file := range []string{old, anchor, modern} {
		if err := os.WriteFile(filepath.Join(dir, file), []byte("-- canonical "+file), 0600); err != nil {
			t.Fatal(err)
		}
	}
	hashes, err := canonicalCMSMigrationChecksums(dir)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		name, mode, recorded, introduced, modernHash, reviewed string
		applied                                                time.Time
		accepted                                               bool
	}{
		{"known pre-checksum history", "legacy", "", hashes[anchor], hashes[modern], old, at.Add(-time.Hour), true},
		{"later missing hash", "legacy", "", hashes[anchor], hashes[modern], old, at.Add(time.Hour), false},
		{"runner missing hash", "runner", "", hashes[anchor], hashes[modern], old, at.Add(-time.Hour), false},
		{"changed introduction", "legacy", "", "changed", hashes[modern], old, at.Add(-time.Hour), false},
		{"recorded drift", "legacy", "changed", hashes[anchor], hashes[modern], old, at.Add(-time.Hour), false},
		{"modern missing hash", "legacy", "", hashes[anchor], "", old + "," + modern, at.Add(-time.Hour), false},
		{"different review", "legacy", "", hashes[anchor], hashes[modern], modern, at.Add(-time.Hour), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sqlDB, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer sqlDB.Close()
			db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{})
			if err != nil {
				t.Fatal(err)
			}
			mock.ExpectExec("LOCK TABLE cms_schema_migrations IN SHARE MODE").WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectQuery("SELECT version,COALESCE").WillReturnRows(sqlmock.NewRows([]string{"version", "checksum", "applied_at", "execution_mode"}).AddRow(old, tc.recorded, tc.applied, tc.mode).AddRow(anchor, tc.introduced, at, "runner").AddRow(modern, tc.modernHash, at.Add(time.Hour), "runner"))
			history, err := VerifyReviewedLegacyCMSHistory(db, dir, tc.reviewed)
			if (err == nil) != tc.accepted {
				t.Fatalf("accepted=%v history=%v error=%v", tc.accepted, history, err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
