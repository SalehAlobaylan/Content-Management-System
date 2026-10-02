package feedstate

import (
	"content-management-system/src/models"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"testing"
)

func newsProjectionTestDB(t *testing.T) (*gorm.DB, sqlmock.Sqlmock) {
	t.Helper()
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sqlDB.Close() })
	db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{SkipDefaultTransaction: true})
	if err != nil {
		t.Fatal(err)
	}
	return db, mock
}

func TestResetNewsStoryQueryPinsProjectionAndRefusesCanonicalFallback(t *testing.T) {
	db, mock := newsProjectionTestDB(t)
	generation := uuid.New()
	storyID := uuid.New()
	mock.ExpectQuery(`SELECT count\(\*\) FROM information_schema.tables`).WithArgs("news_feed_story_projections", "BASE TABLE").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectQuery(`(?s)SELECT .*jsonb_populate_record.*generation.purpose<>'content_reset' OR projection.story_id IS NOT NULL`).WithArgs(generation, generation, "tenant").WillReturnRows(sqlmock.NewRows([]string{"public_id", "label", "article_count"}).AddRow(storyID, "candidate label", 2))
	var stories []models.Story
	if err := NewsStoryQuery(db, "tenant", generation).Select("public_id,label,article_count").Find(&stories).Error; err != nil {
		t.Fatal(err)
	}
	if len(stories) != 1 || stories[0].Label != "candidate label" || stories[0].ArticleCount != 2 {
		t.Fatalf("projection was not hydrated: %+v", stories)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestResetNewsProjectionRejectsMixedOrMissingSpacesBeforeWriting(t *testing.T) {
	db, mock := newsProjectionTestDB(t)
	generation, story := uuid.New(), uuid.New()
	mock.ExpectQuery(`(?s)SELECT COUNT\(\*\).*COUNT\(DISTINCT c.embedding_space_id\)<>1`).WithArgs(generation, "tenant", story, story).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	if RebuildNewsStoryProjection(db, "tenant", generation, story) == nil {
		t.Fatal("invalid embedding membership was accepted")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestResetNewsProjectionBuildUsesExactMembersAndHashesSemanticInputs(t *testing.T) {
	db, mock := newsProjectionTestDB(t)
	generation, story := uuid.New(), uuid.New()
	mock.ExpectQuery(`(?s)SELECT COUNT\(\*\).*COUNT\(DISTINCT c.embedding_space_id\)<>1`).WithArgs(generation, "tenant", story, story).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectExec(`(?s)WITH members AS.*m.member_type='news_item'.*AVG\(embedding\).*sha256.*'summary',NULL.*ON CONFLICT`).WithArgs(generation, "tenant", story, story, "tenant", generation, "tenant").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`(?s)DELETE FROM news_feed_story_projections.*m.member_type='news_item'`).WithArgs("tenant", generation, story, story).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`(?s)DELETE FROM feed_generation_memberships.*m.member_type='story'`).WithArgs(generation, story, story, "tenant").WillReturnResult(sqlmock.NewResult(0, 0))
	if err := RebuildNewsStoryProjection(db, "tenant", generation, story); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestResetNewsMetadataRejectsStructuralFields(t *testing.T) {
	if WriteNewsMetadata(nil, "tenant", uuid.New(), NewsMetadataScope{}, map[string]any{"embedding": "[1]"}) == nil {
		t.Fatal("metadata decoration changed a structural field")
	}
}

func TestResetNewsMetadataRejectsDelayedResultAfterPublication(t *testing.T) {
	db, mock := newsProjectionTestDB(t)
	scope := NewsMetadataScope{GenerationID: uuid.New(), MemberHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Projected: true}
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT .* FROM "feed_generation_heads".*FOR UPDATE`).WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "lane", "active_generation_id"}).AddRow("tenant", "news", uuid.New()))
	mock.ExpectRollback()
	if WriteNewsMetadata(db, "tenant", uuid.New(), scope, map[string]any{"summary": "old population"}) == nil {
		t.Fatal("delayed result wrote across publication")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestResetNewsMetadataRejectsChangedMembers(t *testing.T) {
	db, mock := newsProjectionTestDB(t)
	story := uuid.New()
	scope := NewsMetadataScope{GenerationID: uuid.New(), MemberHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Projected: true}
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT .* FROM "feed_generation_heads".*FOR UPDATE`).WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "lane", "active_generation_id"}).AddRow("tenant", "news", scope.GenerationID))
	mock.ExpectExec(`(?s)UPDATE news_feed_story_projections.*member_hash=\$5`).WithArgs(`{"summary":"stale members"}`, "tenant", scope.GenerationID, story, scope.MemberHash).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()
	if WriteNewsMetadata(db, "tenant", story, scope, map[string]any{"summary": "stale members"}) == nil {
		t.Fatal("delayed result wrote across membership changes")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
