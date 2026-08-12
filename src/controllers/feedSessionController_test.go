package controllers

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func TestFrozenPodsSessionCursorRoundTrip(t *testing.T) {
	cursor := frozenSessionCursor(20, 50)
	if cursor == nil {
		t.Fatal("expected a cursor before the frozen snapshot is exhausted")
	}
	offset, err := parseFrozenSessionCursor(*cursor)
	if err != nil || offset != 20 {
		t.Fatalf("expected offset 20, got offset=%d err=%v", offset, err)
	}
	if cursor := frozenSessionCursor(50, 50); cursor != nil {
		t.Fatal("expected no cursor after the frozen snapshot is exhausted")
	}
}

func TestFrozenPodsSessionCursorRejectsMalformedValues(t *testing.T) {
	if _, err := parseFrozenSessionCursor("not-a-cursor"); err == nil {
		t.Fatal("expected malformed cursor to be rejected")
	}
}

func TestHasNewFrozenPodsCandidateOnlyReportsUnseenIDs(t *testing.T) {
	first := uuid.New()
	second := uuid.New()
	if hasNewFrozenPodsCandidate(
		[]PodsItem{{ID: first}},
		[]PodsItem{{ID: first}},
	) {
		t.Fatal("existing candidate must not claim freshness")
	}
	if !hasNewFrozenPodsCandidate(
		[]PodsItem{{ID: first}},
		[]PodsItem{{ID: first}, {ID: second}},
	) {
		t.Fatal("unseen candidate must claim freshness")
	}
}

func TestVisibleFrozenPodsPageQueriesContentItems(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })

	db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}

	visibleID := uuid.New()
	hiddenID := uuid.New()
	mock.ExpectQuery(`SELECT .*public_id.*FROM "content_items"`).
		WillReturnRows(sqlmock.NewRows([]string{"public_id"}).AddRow(visibleID))

	page, nextOffset := visibleFrozenPodsPage(db, "tenant-a", []PodsItem{
		{ID: visibleID},
		{ID: hiddenID},
	}, 0, 10)

	if len(page) != 1 || page[0].ID != visibleID {
		t.Fatalf("expected the visible content item, got %#v", page)
	}
	if nextOffset != 2 {
		t.Fatalf("expected the snapshot to be consumed through offset 2, got %d", nextOffset)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
