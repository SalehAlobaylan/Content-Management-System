package controllers

import (
	"context"
	"errors"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestFeedIntegrityLockHoldsTransactionUntilOwnerFinishes(t *testing.T) {
	db, mock := newMockGorm(t)
	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_try_advisory_xact_lock($1)")).
		WithArgs(sqlmock.AnyArg()).WillReturnRows(sqlmock.NewRows([]string{"acquired"}).AddRow(true))
	ctx, cancel := context.WithCancel(context.Background())
	release, ok := withFeedIntegrityLock(ctx, db, "default")
	if !ok {
		t.Fatal("could not acquire owner guard")
	}
	// Expiry of the probe context must not release the guard while inventory
	// and ledger persistence are still running outside that probe context.
	cancel()
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	mock.ExpectRollback()
	release()
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestFeedIntegrityLockRollsBackDeniedOrFailedAcquisition(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(map[bool]string{false: "denied", true: "query_failed"}[failed], func(t *testing.T) {
			db, mock := newMockGorm(t)
			mock.ExpectBegin()
			query := mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_try_advisory_xact_lock($1)")).WithArgs(sqlmock.AnyArg())
			if failed {
				query.WillReturnError(errors.New("provider unavailable"))
			} else {
				query.WillReturnRows(sqlmock.NewRows([]string{"acquired"}).AddRow(false))
			}
			mock.ExpectRollback()
			release, ok := withFeedIntegrityLock(context.Background(), db, "default")
			if ok {
				t.Fatal("failed owner guard granted execution")
			}
			release()
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestFeedIntegrityLockHandlesBeginFailure(t *testing.T) {
	db, mock := newMockGorm(t)
	mock.ExpectBegin().WillReturnError(errors.New("begin failed"))
	release, ok := withFeedIntegrityLock(context.Background(), db, "default")
	if ok {
		t.Fatal("begin failure granted execution")
	}
	release()
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
