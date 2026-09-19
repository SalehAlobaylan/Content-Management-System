package contentstage

import (
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"testing"
)

func TestJourneyGenerationFenceSurvivesTransactionAndRejectsStaleClick(t *testing.T) {
	for _, current := range []int64{3, 4} {
		t.Run(string(rune('0'+current)), func(t *testing.T) {
			sqlDB, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer sqlDB.Close()
			db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{})
			if err != nil {
				t.Fatal(err)
			}
			id := uuid.New()
			mock.ExpectBegin()
			mock.ExpectQuery(`SELECT .*content_items.*tenant_id=.*public_id=.*FOR UPDATE`).WithArgs("tenant-a", id, 1).WillReturnRows(sqlmock.NewRows([]string{"public_id", "processing_generation"}).AddRow(id, current))
			if current == 3 {
				mock.ExpectCommit()
			} else {
				mock.ExpectRollback()
			}
			err = db.Set(JourneyExpectedGeneration, int64(3)).Transaction(func(tx *gorm.DB) error { return CheckJourneyGeneration(tx, "tenant-a", id) })
			if (err == nil) != (current == 3) {
				t.Fatalf("current %d: %v", current, err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestJourneyLegacyCallerDoesNotAddDatabaseWork(t *testing.T) {
	db := claimQueryDryRunDB(t)
	if err := CheckJourneyGeneration(db, "tenant-a", uuid.New()); err != nil {
		t.Fatal(err)
	}
}
