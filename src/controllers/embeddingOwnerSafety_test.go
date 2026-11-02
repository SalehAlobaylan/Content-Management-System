package controllers

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"content-management-system/src/models"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func ownerSafetyDB(t *testing.T) (*gorm.DB, sqlmock.Sqlmock) {
	t.Helper()
	conn, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	db, err := gorm.Open(postgres.New(postgres.Config{Conn: conn}), &gorm.Config{DisableAutomaticPing: true, SkipDefaultTransaction: true})
	if err != nil {
		t.Fatal(err)
	}
	return db, mock
}

func TestCampaignCompletionRejectsFailedInventory(t *testing.T) {
	db, mock := ownerSafetyDB(t)
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM content_items x`).WillReturnError(errors.New("inventory unavailable"))
	campaign := &models.EmbeddingCampaign{ID: 1, Space: EmbeddingSpaceImage, TargetSpaceID: "target"}
	if campaignFullyCoherent(db, campaign, expectedSpace{SpaceID: "target"}) {
		t.Fatal("failed inventory was treated as zero debt")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCampaignCompletionRejectsOpenExceptionsAndUnfinishedActions(t *testing.T) {
	for _, openException := range []bool{true, false} {
		t.Run(map[bool]string{true: "open_exception", false: "unfinished_action"}[openException], func(t *testing.T) {
			db, mock := ownerSafetyDB(t)
			mock.ExpectQuery(`SELECT COUNT\(\*\) FROM content_items x`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
			count := 0
			if openException {
				count = 1
			}
			mock.ExpectQuery(`SELECT count\(\*\) FROM "embedding_campaign_exceptions"`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(count))
			if !openException {
				mock.ExpectQuery(`SELECT count\(\*\) FROM "embedding_campaign_actions"`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
			}
			campaign := &models.EmbeddingCampaign{ID: 1, Space: EmbeddingSpaceImage, TargetSpaceID: "target"}
			if campaignFullyCoherent(db, campaign, expectedSpace{SpaceID: "target"}) {
				t.Fatal("campaign completed without all owner proofs")
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestOwnerReadbackRequiresSpaceAndProducer(t *testing.T) {
	db, mock := ownerSafetyDB(t)
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM topics WHERE public_id::text=\$1 AND centroid IS NOT NULL AND centroid_space_id=\$2 AND centroid_producer_id=\$3`).
		WithArgs("topic", "target-space", "target-producer").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	surface, _ := EmbeddingSurfaceByKey("topic_centroid")
	if ownerReadback(db, surface, "topic", "target-space", "target-producer") == nil {
		t.Fatal("wrong-space vector passed producer readback")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestTopicMappingWaitsForCompleteCatalog(t *testing.T) {
	for _, pending := range []int{0, 1} {
		db, mock := ownerSafetyDB(t)
		mock.ExpectQuery(`SELECT count\(\*\) FROM "topics" WHERE .*centroid IS NULL OR centroid_space_id IS DISTINCT FROM`).
			WithArgs("tenant", true, "target").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(pending))
		ready, err := topicMappingReady(db, "tenant", "target")
		if err != nil || ready != (pending == 0) {
			t.Fatalf("pending=%d ready=%v err=%v", pending, ready, err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestTopicMappingInventoryFailureIsHeld(t *testing.T) {
	db, mock := ownerSafetyDB(t)
	mock.ExpectQuery(`SELECT count\(\*\) FROM "topics"`).WillReturnError(errors.New("read failed"))
	if ready, err := topicMappingReady(db, "tenant", "target"); ready || err == nil {
		t.Fatalf("ready=%v err=%v", ready, err)
	}
	if ready, err := topicMappingReady(db, "tenant", ""); ready || err != nil {
		t.Fatalf("unknown space ready=%v err=%v", ready, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestDirtyTopicSweepPreservesFlagsAndCursorsWhileCatalogHeld(t *testing.T) {
	descriptorMu.Lock()
	previous := descriptorCache
	descriptorCache = map[string]expectedSpace{EmbeddingSpaceText: {SpaceID: strings.Repeat("a", 64), ObservedAt: time.Now()}}
	descriptorMu.Unlock()
	t.Cleanup(func() { descriptorMu.Lock(); descriptorCache = previous; descriptorMu.Unlock() })
	db, mock := ownerSafetyDB(t)
	mock.ExpectQuery(`SELECT count\(\*\) FROM "topics"`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	result, err := sweepDirtyTopics(db, "tenant", 5, 20, 20, 12, 34)
	if err != nil || result.TopicsProcessed != 0 || result.CycleComplete || result.NextItemCursor != 12 || result.NextStoryCursor != 34 {
		t.Fatalf("held topic sweep changed progress: %+v err=%v", result, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestStoryOwnerRejectsMixedMembersWithoutCommittingCentroid(t *testing.T) {
	db, mock := ownerSafetyDB(t)
	story := "a17bcdd0-c835-41c3-9cd7-0a4250d90f95"
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT public_id,tenant_id FROM "stories"`).WillReturnRows(sqlmock.NewRows([]string{"public_id", "tenant_id"}).AddRow(story, "tenant"))
	mock.ExpectQuery(`SELECT count\(\*\) FROM information_schema.tables`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectQuery(`(?s)SELECT embedding, embedding_space_id AS space_id FROM content_items.*FOR SHARE`).WillReturnRows(sqlmock.NewRows([]string{"embedding", "space_id"}).AddRow("[1,0]", "old-space"))
	mock.ExpectRollback()
	if _, err := rebuildStoryCentroid(db, story, expectedSpace{SpaceID: "target-space"}); err == nil {
		t.Fatal("mixed member centroid was accepted")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRelatedRefreshDoesNotStoreEmptyListAfterFailedRead(t *testing.T) {
	db, mock := ownerSafetyDB(t)
	mock.ExpectQuery(`SELECT count\(\*\) FROM information_schema.tables`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectQuery(`SELECT count\(\*\) FROM information_schema.tables`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectQuery(`SELECT embedding::text AS embedding, embedding_space_id AS space_id FROM "stories"`).WillReturnError(errors.New("kNN anchor unavailable"))
	story := uuid.MustParse("a17bcdd0-c835-41c3-9cd7-0a4250d90f95")
	if err := refreshStoryRelatedChecked(db, "tenant", story); err == nil {
		t.Fatal("failed relation read was reported as successful empty result")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRelatedOwnerReadbackRejectsMissingOrUnknownServingStory(t *testing.T) {
	for _, space := range []string{"", "old-space"} {
		db, mock := ownerSafetyDB(t)
		mock.ExpectQuery(`SELECT count\(\*\) FROM information_schema.tables`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
		mock.ExpectQuery(`SELECT related_ids, embedding_space_id AS space_id FROM "stories"`).WillReturnRows(sqlmock.NewRows([]string{"related_ids", "space_id"}).AddRow("[]", space))
		if err := readbackStoryRelations(db, "tenant", uuid.New(), "target-space"); err == nil {
			t.Fatal("unknown or wrong-space relation readback counted as completed")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCentroidWaiverNarrowsCompletionWithoutHidingAuditDebt(t *testing.T) {
	db, mock := ownerSafetyDB(t)
	campaign := &models.EmbeddingCampaign{ID: 7}
	surface, _ := EmbeddingSurfaceByKey("story_centroid")
	mock.ExpectQuery(`(?s)SELECT COUNT\(DISTINCT x.public_id\).*NOT EXISTS.*e.campaign_id=.*e.surface_key=.*e.status='waived'.*e.waiver_expires>now`).WithArgs(uint(7), "story_centroid").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	if count, err := countCampaignMixedSpaceCentroids(db, campaign, surface); err != nil || count != 0 {
		t.Fatalf("waived completion debt=%d err=%v", count, err)
	}
	mock.ExpectQuery(`(?s)SELECT COUNT\(DISTINCT st.public_id\).*ci.embedding_space_id <> st.embedding_space_id`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	if count, err := countMixedSpaceCentroids(db, surface); err != nil || count != 1 {
		t.Fatalf("audit debt=%d err=%v", count, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRelationBacklogIncludesTargetsWithoutAnAttempt(t *testing.T) {
	db, mock := ownerSafetyDB(t)
	campaign := &models.EmbeddingCampaign{ID: 8, Space: EmbeddingSpaceText, TargetSpaceID: "target"}
	mock.ExpectQuery(`SELECT count\(\*\) FROM information_schema.tables`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectQuery(`(?s)SELECT count\(\*\) FROM "stories".*NOT EXISTS.*surface_key='story_related'.*status='completed'`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(57))
	if count, err := countCampaignPendingStoryRelations(db, campaign); err != nil || count != 57 {
		t.Fatalf("unattempted relation backlog=%d err=%v", count, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCampaignClassificationReportsHeldCatalogInsteadOfSilentlyWaiting(t *testing.T) {
	db, mock := ownerSafetyDB(t)
	id := uuid.New()
	mock.ExpectQuery(`SELECT "public_id" FROM "content_items"`).WillReturnRows(sqlmock.NewRows([]string{"public_id"}).AddRow(id))
	mock.ExpectQuery(`SELECT count\(\*\) FROM "stories"`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	if err := runCampaignClassificationPage(db, &models.EmbeddingCampaign{ItemsPerBatch: 4}, &models.EmbeddingCampaignAction{TenantID: "tenant"}, expectedSpace{SpaceID: "target"}); err == nil || !strings.Contains(err.Error(), "explicitly waive held classification") {
		t.Fatalf("missing actionable held classification error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCampaignTopicRemapRunsWithPausedIndependentScheduler(t *testing.T) {
	normalWriterTestSpace(t, "target")
	db, mock := ownerSafetyDB(t)
	topic := uuid.New()
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT pg_try_advisory_xact_lock`).WillReturnRows(sqlmock.NewRows([]string{"locked"}).AddRow(true))
	mock.ExpectQuery(`SELECT count\(\*\) FROM "topics"`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectQuery(`SELECT count\(\*\) FROM "topics"`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectQuery(`SELECT .* FROM "preference_autopilot_policies"`).WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "enabled", "paused_until", "max_dirty_topics", "max_item_candidates", "max_story_candidates"}).AddRow("tenant", true, time.Now().Add(time.Hour), 3, 20, 10))
	mock.ExpectQuery(`SELECT .* FROM "embedding_campaign_actions"`).WillReturnRows(sqlmock.NewRows([]string{"id"}))
	mock.ExpectQuery(`SELECT count\(\*\) FROM "topics"`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectQuery(`SELECT .* FROM "topics".*needs_remap`).WillReturnRows(sqlmock.NewRows([]string{"public_id", "tenant_id", "active", "centroid", "centroid_space_id", "needs_remap"}).AddRow(topic, "tenant", true, "[1,0]", "target", true))
	mock.ExpectQuery(`SELECT count\(\*\) FROM "topics"`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectQuery(`SELECT .* FROM "topics".*centroid_space_id`).WillReturnRows(sqlmock.NewRows([]string{"public_id", "tenant_id", "active", "centroid", "centroid_space_id"}).AddRow(topic, "tenant", true, "[1,0]", "target"))
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id, public_id, embedding FROM "content_items"`).WithArgs("tenant", models.ContentStatusReady, "target", uint(0), 4).WillReturnRows(sqlmock.NewRows([]string{"id", "public_id", "embedding"}))
	mock.ExpectQuery(`SELECT id, public_id, embedding FROM "stories"`).WithArgs("tenant", "target", uint(0), 4).WillReturnRows(sqlmock.NewRows([]string{"id", "public_id", "embedding"}))
	mock.ExpectExec(`UPDATE "topics" SET "needs_remap"`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectExec(`(?s)UPDATE topics t.*SET member_count`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`(?s)UPDATE topics t SET member_count = 0`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()
	mock.ExpectRollback()
	action := &models.EmbeddingCampaignAction{ID: 9, TargetID: "tenant", TenantID: "tenant"}
	if err := runCampaignTopicRemapPage(db, &models.EmbeddingCampaign{ID: 1, ItemsPerBatch: 4}, action, expectedSpace{SpaceID: "target"}); err != nil {
		t.Fatal(err)
	}
	var result dirtySweepResult
	if err := json.Unmarshal(action.Evidence, &result); err != nil || !result.CycleComplete {
		t.Fatalf("owner did not checkpoint completed remap: %+v err=%v", result, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestEmbeddingAdvisoryLockStaysInOneTransaction(t *testing.T) {
	for _, available := range []bool{true, false} {
		db, mock := ownerSafetyDB(t)
		mock.ExpectBegin()
		mock.ExpectQuery(`SELECT pg_try_advisory_xact_lock`).WithArgs(int64(42)).WillReturnRows(sqlmock.NewRows([]string{"acquired"}).AddRow(available))
		mock.ExpectRollback()
		release, acquired := tryTransactionAdvisoryLock(db, 42)
		if acquired != available {
			t.Fatal("wrong lock result")
		}
		release()
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	}
}
