package utils

import (
	"github.com/DATA-DOG/go-sqlmock"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"strings"
	"testing"
)

func TestManagedPreparationStopsAtChangedIdentity(t *testing.T) {
	t.Setenv("WAHB_LOCAL_PREPARATION", "1")
	t.Setenv("WAHB_PREPARATION_DATABASE_ID", "expected")
	t.Setenv("WAHB_PREPARATION_DATABASE_EPOCH", "3")
	sqlDB, mock, e := sqlmock.New()
	if e != nil {
		t.Fatal(e)
	}
	defer sqlDB.Close()
	db, e := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{})
	if e != nil {
		t.Fatal(e)
	}
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT to_regclass`).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectExec(`LOCK TABLE wahb_database_identity`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT database_id::text`).WillReturnRows(sqlmock.NewRows([]string{"id", "epoch"}).AddRow("other", 3))
	mock.ExpectRollback()
	e = db.Transaction(func(tx *gorm.DB) error { return CheckLocalPreparationAuthority(tx) })
	if e == nil || !strings.Contains(e.Error(), "identity") {
		t.Fatal(e)
	}
	if e = mock.ExpectationsWereMet(); e != nil {
		t.Fatal(e)
	}
}
