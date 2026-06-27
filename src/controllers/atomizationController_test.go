package controllers

import (
	"content-management-system/src/models"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func floatPtr(v float64) *float64 {
	return &v
}

func stringPtr(v string) *string {
	return &v
}

func TestComputeEndsPreservesExplicitEndMs(t *testing.T) {
	chapters := []studioChapterDTO{
		{Title: "Opening argument", StartMs: 0, EndMs: 420_000},
		{Title: "Follow-up", StartMs: 480_000},
	}

	computeEnds(chapters, 900_000)

	if got, want := chapters[0].EndMs, 420_000; got != want {
		t.Fatalf("explicit end_ms was overwritten: got %d, want %d", got, want)
	}
	if got, want := chapters[1].EndMs, 900_000; got != want {
		t.Fatalf("missing final end_ms = %d, want duration %d", got, want)
	}
}

func TestChaptersFromAtomizationRequestClassifiesReviewAndDurationBuckets(t *testing.T) {
	tenantID := "tenant-test"
	transcriptID := uuid.New()
	policy := defaultAtomizationPolicy()
	reason := "Boundary cuts through a topic transition."

	rows := chaptersFromAtomizationRequest(tenantID, transcriptID, []atomizationChapterRequest{
		{
			Title:      "Coherent minimum chapter",
			StartMs:    0,
			EndMs:      279_000,
			Confidence: floatPtr(0.91),
		},
		{
			Title:             "Uncertain boundary",
			StartMs:           279_000,
			EndMs:             22 * 60_000,
			Confidence:        floatPtr(0.76),
			NeedsReviewReason: &reason,
		},
	}, policy)

	if len(rows) != 2 {
		t.Fatalf("rows len = %d, want 2", len(rows))
	}
	if rows[0].Status != chapterStatusPublished {
		t.Fatalf("high-confidence status = %s, want %s", rows[0].Status, chapterStatusPublished)
	}
	if rows[0].DurationBucket == nil || *rows[0].DurationBucket != "5m" {
		t.Fatalf("4:39 chapter bucket = %v, want 5m", rows[0].DurationBucket)
	}
	if rows[1].Status != chapterStatusReview {
		t.Fatalf("uncertain status = %s, want %s", rows[1].Status, chapterStatusReview)
	}
	if rows[1].DurationBucket == nil || *rows[1].DurationBucket != "15m" {
		t.Fatalf("sixteen-minute chapter bucket = %v, want 15m", rows[1].DurationBucket)
	}
}

func TestChaptersFromAtomizationRequestForcesUnderMinimumToReview(t *testing.T) {
	policy := defaultAtomizationPolicy()
	rows := chaptersFromAtomizationRequest("tenant-test", uuid.New(), []atomizationChapterRequest{{
		Title:      "Too short",
		StartMs:    0,
		EndMs:      4 * 60_000,
		Confidence: floatPtr(0.97),
	}}, policy)

	if len(rows) != 1 {
		t.Fatalf("rows len = %d, want 1", len(rows))
	}
	if rows[0].Status != chapterStatusReview {
		t.Fatalf("under-minimum high-confidence status = %s, want %s", rows[0].Status, chapterStatusReview)
	}
	if rows[0].NeedsReviewReason == nil || *rows[0].NeedsReviewReason != "Chapter is below the 4:30 minimum feed duration." {
		t.Fatalf("under-minimum review reason = %v", rows[0].NeedsReviewReason)
	}
}

func TestHasReviewChaptersFlagsLowConfidenceAndExplicitReasons(t *testing.T) {
	policy := defaultAtomizationPolicy()
	if hasReviewChapters([]atomizationChapterRequest{{Title: "Strong", StartMs: 0, EndMs: 270_000, Confidence: floatPtr(0.9)}}, policy) {
		t.Fatal("high-confidence chapter unexpectedly requires review")
	}
	if !hasReviewChapters([]atomizationChapterRequest{{Title: "Too short", StartMs: 0, EndMs: 269_000, Confidence: floatPtr(0.9)}}, policy) {
		t.Fatal("under-minimum chapter did not require review")
	}
	if !hasReviewChapters([]atomizationChapterRequest{{Title: "Weak", StartMs: 0, EndMs: 300_000, Confidence: floatPtr(0.6)}}, policy) {
		t.Fatal("low-confidence chapter did not require review")
	}
	if !hasReviewChapters([]atomizationChapterRequest{{Title: "Reasoned", StartMs: 0, EndMs: 300_000, Confidence: floatPtr(0.9), NeedsReviewReason: stringPtr("sponsor removal uncertain")}}, policy) {
		t.Fatal("chapter with explicit review reason did not require review")
	}
	if !hasReviewChapters([]atomizationChapterRequest{{Title: "Too long", StartMs: 0, EndMs: 41 * 60_000, Confidence: floatPtr(0.95)}}, policy) {
		t.Fatal("over-hard-max chapter did not require review")
	}
}

func TestChaptersFromAtomizationRequestForcesOverHardMaxToReview(t *testing.T) {
	policy := defaultAtomizationPolicy()
	rows := chaptersFromAtomizationRequest("tenant-test", uuid.New(), []atomizationChapterRequest{{
		Title:      "Oversized but confident",
		StartMs:    0,
		EndMs:      41 * 60_000,
		Confidence: floatPtr(0.97),
	}}, policy)

	if len(rows) != 1 {
		t.Fatalf("rows len = %d, want 1", len(rows))
	}
	if rows[0].Status != chapterStatusReview {
		t.Fatalf("oversized high-confidence status = %s, want %s", rows[0].Status, chapterStatusReview)
	}
	if rows[0].NeedsReviewReason == nil || *rows[0].NeedsReviewReason != "Chapter exceeds hard maximum duration." {
		t.Fatalf("oversized review reason = %v", rows[0].NeedsReviewReason)
	}
}

func TestShouldPublishLinkedChapterOnlyForPublishedChildren(t *testing.T) {
	parentID := uuid.New()
	published := chapterStatusPublished
	tests := []struct {
		name string
		item models.ContentItem
		want bool
	}{
		{
			name: "feed visible child",
			item: models.ContentItem{ParentContentItemID: &parentID, FeedVisibility: feedVisibilityVisible},
			want: true,
		},
		{
			name: "published chaptering status child",
			item: models.ContentItem{ParentContentItemID: &parentID, FeedVisibility: feedVisibilityEmbeddingPending, ChapteringStatus: &published},
			want: true,
		},
		{
			name: "embedding pending child",
			item: models.ContentItem{ParentContentItemID: &parentID, FeedVisibility: feedVisibilityEmbeddingPending},
			want: false,
		},
		{
			name: "visible parent",
			item: models.ContentItem{FeedVisibility: feedVisibilityVisible},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldPublishLinkedChapter(tt.item); got != tt.want {
				t.Fatalf("shouldPublishLinkedChapter() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParentChapteringStatusFromRunKeepsQueuedVisible(t *testing.T) {
	if got := parentChapteringStatusFromRun("queued", "planning"); got != "queued" {
		t.Fatalf("queued run mapped to %q, want queued", got)
	}
	if got := parentChapteringStatusFromRun("running", "cutting"); got != "cutting" {
		t.Fatalf("running run mapped to %q, want cutting phase", got)
	}
	if got := parentChapteringStatusFromRun("completed", "embedding"); got != "completed" {
		t.Fatalf("completed run mapped to %q, want completed", got)
	}
}

func TestSpaceSiblingChaptersAvoidsAdjacentSiblingsWhenPossible(t *testing.T) {
	parentA := uuid.New()
	parentB := uuid.New()
	items := []models.ContentItem{
		{PublicID: uuid.New(), ParentContentItemID: &parentA},
		{PublicID: uuid.New(), ParentContentItemID: &parentA},
		{PublicID: uuid.New(), ParentContentItemID: &parentB},
		{PublicID: uuid.New()},
	}

	spaced := spaceSiblingChapters(items)
	if len(spaced) != len(items) {
		t.Fatalf("spaced len = %d, want %d", len(spaced), len(items))
	}
	for i := 1; i < len(spaced); i++ {
		if chapterSiblingKey(spaced[i-1]) == chapterSiblingKey(spaced[i]) {
			t.Fatalf("adjacent siblings at %d and %d share key %s", i-1, i, chapterSiblingKey(spaced[i]))
		}
	}
}

func TestForYouEligibleMediaQueryAppliesFeedDurationFloorAndCeiling(t *testing.T) {
	sqlDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("open sqlmock: %v", err)
	}
	defer sqlDB.Close()

	db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{})
	if err != nil {
		t.Fatalf("open gorm: %v", err)
	}

	query := forYouEligibleMediaQuery(db.Session(&gorm.Session{DryRun: true}), false).Find(&[]models.ContentItem{})
	sql := query.Statement.SQL.String()
	if ok, err := regexp.MatchString(`duration_sec IS NOT NULL AND duration_sec BETWEEN \$[0-9]+ AND \$[0-9]+`, sql); err != nil || !ok {
		t.Fatalf("query does not enforce duration floor and ceiling: %s", sql)
	}
	if len(query.Statement.Vars) == 0 {
		t.Fatalf("query has no vars; want duration floor and ceiling vars in %s", sql)
	}
	foundMin := false
	foundMax := false
	for _, v := range query.Statement.Vars {
		if n, ok := v.(int); ok {
			if n == forYouMinDurationSec {
				foundMin = true
			}
			if n == forYouHardMaxDurationSec {
				foundMax = true
			}
		}
	}
	if !foundMin || !foundMax {
		t.Fatalf("query vars %v do not include min %d and hard max %d", query.Statement.Vars, forYouMinDurationSec, forYouHardMaxDurationSec)
	}
}

func TestForYouEligibleMediaQueryRequiresFeedUnitsWithinDurationBounds(t *testing.T) {
	sqlDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("open sqlmock: %v", err)
	}
	defer sqlDB.Close()

	db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{})
	if err != nil {
		t.Fatalf("open gorm: %v", err)
	}

	query := forYouEligibleMediaQuery(db.Session(&gorm.Session{DryRun: true}), true).Find(&[]models.ContentItem{})
	sql := query.Statement.SQL.String()
	if !strings.Contains(sql, "is_feed_unit = TRUE") || !strings.Contains(sql, "feed_visibility =") {
		t.Fatalf("atomized query does not require visible feed units: %s", sql)
	}
	foundMin := false
	foundHard := false
	for _, v := range query.Statement.Vars {
		if n, ok := v.(int); ok {
			if n == forYouMinDurationSec {
				foundMin = true
			}
			if n == forYouHardMaxDurationSec {
				foundHard = true
			}
		}
	}
	if !foundMin || !foundHard {
		t.Fatalf("atomized query vars %v do not include min %d and hard max %d", query.Statement.Vars, forYouMinDurationSec, forYouHardMaxDurationSec)
	}
}

func TestVisibleLongParentLeakQueryTargetsOnlyVisibleLongParents(t *testing.T) {
	sqlDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("open sqlmock: %v", err)
	}
	defer sqlDB.Close()

	db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{})
	if err != nil {
		t.Fatalf("open gorm: %v", err)
	}

	query := visibleLongParentLeakQuery(db.Session(&gorm.Session{DryRun: true}), "tenant-test").Find(&[]models.ContentItem{})
	sql := query.Statement.SQL.String()
	required := []string{
		"parent_content_item_id IS NULL",
		"is_feed_unit = TRUE",
		"feed_visibility =",
		"duration_sec >",
	}
	for _, fragment := range required {
		if !strings.Contains(sql, fragment) {
			t.Fatalf("query missing %q: %s", fragment, sql)
		}
	}
	foundHardMax := false
	for _, v := range query.Statement.Vars {
		if n, ok := v.(int); ok && n == forYouHardMaxDurationSec {
			foundHardMax = true
			break
		}
	}
	if !foundHardMax {
		t.Fatalf("query vars %v do not include hard max %d", query.Statement.Vars, forYouHardMaxDurationSec)
	}
}

func TestValidParentFeedUnitQueryDoesNotRequireTranscript(t *testing.T) {
	sqlDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("open sqlmock: %v", err)
	}
	defer sqlDB.Close()

	db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{})
	if err != nil {
		t.Fatalf("open gorm: %v", err)
	}

	query := validParentFeedUnitQuery(db.Session(&gorm.Session{DryRun: true}), "tenant-test").Find(&[]models.ContentItem{})
	sql := query.Statement.SQL.String()
	if strings.Contains(strings.ToLower(sql), "transcript") {
		t.Fatalf("valid short parent feed query should not require transcript: %s", sql)
	}
	required := []string{
		"parent_content_item_id IS NULL",
		"duration_sec BETWEEN",
		"status =",
	}
	for _, fragment := range required {
		if !strings.Contains(sql, fragment) {
			t.Fatalf("query missing %q: %s", fragment, sql)
		}
	}
	foundMin := false
	foundAtomizationMax := false
	for _, v := range query.Statement.Vars {
		if n, ok := v.(int); ok {
			if n == forYouMinDurationSec {
				foundMin = true
			}
			if n == atomizationMinParentDurationSec {
				foundAtomizationMax = true
			}
		}
	}
	if !foundMin || !foundAtomizationMax {
		t.Fatalf("query vars %v do not include feed floor %d and atomization max %d", query.Statement.Vars, forYouMinDurationSec, atomizationMinParentDurationSec)
	}
}
