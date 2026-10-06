package controllers

import (
	"content-management-system/src/models"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func expectLegacyFrozenPodsSchema(mock sqlmock.Sqlmock) {
	for _, column := range []string{"is_feed_unit", "storage_state"} {
		mock.ExpectQuery(`SELECT count\(\*\) FROM INFORMATION_SCHEMA.columns`).
			WithArgs("content_items", column).
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	}
	mock.ExpectQuery(`SELECT count\(\*\) FROM information_schema.tables`).
		WithArgs("feed_generation_heads", "BASE TABLE").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
}

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
	expectLegacyFrozenPodsSchema(mock)
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

func TestVisibleFrozenPodsPageCorrectsLegacyAudioWithoutReplacingSnapshot(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	stored := legacyAudioPodcastFixture()
	expectLegacyFrozenPodsSchema(mock)
	mock.ExpectQuery(`SELECT .*public_id.*source.*metadata.*rendition_set_version.*FROM "content_items"`).
		WillReturnRows(sqlmock.NewRows([]string{"public_id", "source", "metadata", "rendition_set_version"}).
			AddRow(stored.PublicID, string(models.SourceTypePodcast), string(datatypes.JSON(`{"enclosureType":"audio/mpeg"}`)), 1))
	frozen := PodsItem{ID: stored.PublicID, PlaybackURL: stored.PlaybackURL, PlaybackType: stored.PlaybackType, HasVideo: stored.HasVideo, RenditionSetVersion: 1}
	page, offset := visibleFrozenPodsPage(db, "tenant-a", []PodsItem{frozen}, 0, 10)
	if len(page) != 1 || page[0].ID != frozen.ID || page[0].PlaybackURL != frozen.PlaybackURL || offset != 1 || page[0].HasVideo == nil || *page[0].HasVideo {
		t.Fatalf("expected corrected audio presentation with captured identity/URL, got %#v", page)
	}
	if !*frozen.HasVideo {
		t.Fatal("stored frozen snapshot changed")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
