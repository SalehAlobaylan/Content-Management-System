package feedstate

import (
	"regexp"
	"testing"
	"time"

	"content-management-system/src/models"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func TestAdvisoryKeyIsTenantScopedAndStable(t *testing.T) {
	if advisoryKey("wahb:feed-membership:tenant-a") != advisoryKey("wahb:feed-membership:tenant-a") {
		t.Fatal("advisory key must be stable")
	}
	if advisoryKey("wahb:feed-membership:tenant-a") == advisoryKey("wahb:feed-membership:tenant-b") {
		t.Fatal("tenants must not share a reconciliation lock")
	}
}

func TestAttachReadyNewsStoryAddsActiveAndCandidateMembership(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{SkipDefaultTransaction: true})
	if err != nil {
		t.Fatal(err)
	}

	active, candidate, storyID, itemID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	for _, table := range []string{"database_migration_owner_control", "lifecycle_operation_claims"} {
		mock.ExpectQuery(`SELECT count\(\*\) FROM information_schema.tables`).WithArgs(table, "BASE TABLE").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	}
	mock.ExpectQuery(`SELECT .* FROM "feed_generation_heads".*FOR UPDATE`).
		WithArgs("tenant-a", newsLane, 1).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "lane", "active_generation_id", "candidate_generation_id", "generation", "updated_at"}).
			AddRow("tenant-a", newsLane, active, candidate, 1, time.Now()))
	mock.ExpectQuery(`SELECT .* FROM "feed_generations"`).WithArgs("tenant-a", active, candidate).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "public_id", "purpose", "state"}).
			AddRow("tenant-a", active, "recovery", "active").AddRow("tenant-a", candidate, "recovery", "candidate"))
	for _, generation := range []uuid.UUID{active, candidate} {
		mock.ExpectQuery(`SELECT count\(\*\) FROM information_schema.tables`).WithArgs("source_item_instances", "BASE TABLE").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
		for _, member := range []struct {
			kind string
			id   uuid.UUID
		}{{"story", storyID}, {"news_item", itemID}} {
			mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO "feed_generation_memberships" ("generation_id","member_type","member_id","attached_at") VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING`)).
				WithArgs(generation, member.kind, member.id, sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 1))
		}
	}

	item := models.ContentItem{PublicID: itemID, TenantID: "tenant-a", Type: models.ContentTypeNews, Status: models.ContentStatusReady, StoryID: &storyID}
	if err := AttachReadyNewsStory(db, item); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestContentResetNewsCandidateRejectsSharedStoryProjection(t *testing.T) {
	// A reset candidate must wait for its isolated story projection owner.
	// This refusal happens before any lookup or membership mutation.
	generation := models.FeedGeneration{TenantID: "tenant-a", Purpose: "content_reset", State: "candidate"}
	allowed, err := generationAcceptsNewsItem(nil, generation, uuid.New())
	if err != nil || allowed {
		t.Fatalf("shared News story projection entered reset candidate: %v", err)
	}
}

func TestAttachReadyNewsStoryWaitsForBothReadyAndClassified(t *testing.T) {
	storyID := uuid.New()
	cases := []models.ContentItem{
		{PublicID: uuid.New(), TenantID: "tenant-a", Type: models.ContentTypeNews, Status: models.ContentStatusPending, StoryID: &storyID},
		{PublicID: uuid.New(), TenantID: "tenant-a", Type: models.ContentTypeNews, Status: models.ContentStatusReady},
	}
	for _, item := range cases {
		if err := AttachReadyNewsStory(&gorm.DB{}, item); err != nil {
			t.Fatalf("ineligible item should be a no-op: %v", err)
		}
	}
}
