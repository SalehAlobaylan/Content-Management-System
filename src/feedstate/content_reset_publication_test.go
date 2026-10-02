package feedstate

import (
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"testing"
	"time"
)

func TestResetPublicationRequiresExactLaneSet(t *testing.T) {
	news := ResetPublicationFence{Lane: "news", HeadVersion: 1, ActiveID: uuid.New(), CandidateID: uuid.New()}
	media := ResetPublicationFence{Lane: "media", HeadVersion: 2, ActiveID: uuid.New(), CandidateID: uuid.New()}
	for _, valid := range []struct {
		lane   string
		fences []ResetPublicationFence
	}{{"news", []ResetPublicationFence{news}}, {"pods", []ResetPublicationFence{media}}, {"both", []ResetPublicationFence{news, media}}} {
		if err := ValidateResetPublicationFences(valid.lane, valid.fences); err != nil {
			t.Fatal(err)
		}
	}
	for name, fences := range map[string][]ResetPublicationFence{
		"one lane missing": {news}, "duplicate lane": {news, news}, "empty": {},
		"same active and candidate":    {news, {Lane: "media", HeadVersion: 1, ActiveID: media.ActiveID, CandidateID: media.ActiveID}},
		"cross lane reused generation": {news, {Lane: "media", HeadVersion: 1, ActiveID: news.ActiveID, CandidateID: media.CandidateID}},
		"unfenced head":                {news, {Lane: "media", HeadVersion: 0, ActiveID: media.ActiveID, CandidateID: media.CandidateID}},
	} {
		t.Run(name, func(t *testing.T) {
			if ValidateResetPublicationFences("both", fences) == nil {
				t.Fatal("invalid publication scope was accepted")
			}
		})
	}
}

func TestResetPublicationRefusesAutocommit(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if _, err := PublishContentResetViews(db, "tenant", 1, 2, nil, now, now.Add(time.Hour), true); err == nil {
		t.Fatal("publication accepted an autocommit connection")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestResetPublicationRollsBackBothAfterSecondLaneFenceChanges(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{SkipDefaultTransaction: true})
	if err != nil {
		t.Fatal(err)
	}
	campaignID := uint(1)
	revisionID := uint(2)
	manifest := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	media := ResetPublicationFence{Lane: "media", HeadVersion: 3, ActiveID: uuid.New(), CandidateID: uuid.New()}
	news := ResetPublicationFence{Lane: "news", HeadVersion: 4, ActiveID: uuid.New(), CandidateID: uuid.New()}
	mock.ExpectBegin()
	for range 2 {
		mock.ExpectQuery(`SELECT .* FROM "content_reset_campaigns".*FOR UPDATE`).WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "state", "operation", "lane", "current_revision"}).AddRow(campaignID, "tenant", "executing", "fresh_start", "both", 1))
	}
	mock.ExpectQuery(`SELECT .* FROM "content_reset_executions".*FOR UPDATE`).WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "campaign_id", "revision_id", "phase", "manifest_hash", "pause_requested", "started_at"}).AddRow("tenant", campaignID, revisionID, "publishing", manifest, false, time.Now().UTC()))
	mock.ExpectQuery(`SELECT .* FROM "content_reset_revisions"`).WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "campaign_id", "revision", "manifest_hash"}).AddRow(revisionID, "tenant", campaignID, 1, manifest))
	mock.ExpectQuery(`SELECT .* FROM "source_item_instances"`).WillReturnRows(sqlmock.NewRows([]string{"id"}))
	mock.ExpectQuery(`SELECT .* FROM "feed_generation_heads".*FOR UPDATE`).WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "lane", "generation", "active_generation_id", "candidate_generation_id"}).AddRow("tenant", "media", 3, media.ActiveID, media.CandidateID))
	mock.ExpectQuery(`SELECT .* FROM "feed_generations".*FOR UPDATE`).WillReturnRows(sqlmock.NewRows([]string{"public_id", "tenant_id", "lane", "state", "purpose", "content_reset_campaign_id", "previous_generation_id"}).AddRow(media.CandidateID, "tenant", "media", "candidate", "content_reset", campaignID, media.ActiveID))
	mock.ExpectQuery(`SELECT .* FROM "feed_generations".*FOR UPDATE`).WillReturnRows(sqlmock.NewRows([]string{"public_id", "tenant_id", "lane", "state"}).AddRow(media.ActiveID, "tenant", "media", "active"))
	for range 2 {
		mock.ExpectExec(`UPDATE "feed_generations"`).WillReturnResult(sqlmock.NewResult(0, 1))
	}
	mock.ExpectExec(`UPDATE "feed_generation_heads"`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`SELECT .* FROM "feed_generation_heads".*FOR UPDATE`).WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "lane", "generation", "active_generation_id", "candidate_generation_id"}).AddRow("tenant", "news", 5, news.ActiveID, news.CandidateID))
	mock.ExpectRollback()
	now := time.Now().UTC()
	err = db.Transaction(func(tx *gorm.DB) error {
		_, err := PublishContentResetViews(tx, "tenant", campaignID, revisionID, []ResetPublicationFence{news, media}, now, now.Add(time.Hour), true)
		return err
	})
	if err == nil {
		t.Fatal("Both publication committed after the News head changed")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
