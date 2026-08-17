package controllers

import (
	"testing"

	"content-management-system/src/models"
	"github.com/google/uuid"
	"gorm.io/datatypes"
)

func TestTypedFallbackMetadataRequiresAuthoritativeRenditionTruth(t *testing.T) {
	fallback := "https://cdn.example.test/fallback.mp4"
	item := models.ContentItem{
		FallbackPlaybackURL: &fallback,
		MediaRenditions: datatypes.JSON(`[
			{"type":"hls","url":"https://cdn.example.test/primary.m3u8","has_video":true},
			{"type":"mp4","url":"https://cdn.example.test/fallback.mp4","has_video":true}
		]`),
	}
	typeName, hasVideo := typedFallbackMetadata(item)
	if typeName == nil || *typeName != "mp4" || hasVideo == nil || !*hasVideo {
		t.Fatalf("expected typed MP4 video fallback, got type=%v has_video=%v", typeName, hasVideo)
	}

	item.MediaRenditions = datatypes.JSON(`[{"type":"mp4","url":"https://cdn.example.test/fallback.mp4"}]`)
	if typeName, hasVideo := typedFallbackMetadata(item); typeName != nil || hasVideo != nil {
		t.Fatalf("fallback without stored has_video must be omitted, got type=%v has_video=%v", typeName, hasVideo)
	}
}

func TestPrioritizePodsForSessionRecyclesSoftSuppressionButNeverHides(t *testing.T) {
	unseenID, viewedID, hiddenID := uuid.New(), uuid.New(), uuid.New()
	items := []models.ContentItem{{PublicID: viewedID}, {PublicID: hiddenID}, {PublicID: unseenID}}

	got := prioritizePodsForSession(items, []uuid.UUID{viewedID, hiddenID}, []uuid.UUID{hiddenID})
	if len(got) != 2 || got[0].PublicID != unseenID || got[1].PublicID != viewedID {
		t.Fatalf("session order = %#v, want unseen then softly suppressed with hidden removed", got)
	}
}

func TestPrioritizeScoredPodsForSessionPreservesScoresAndOrder(t *testing.T) {
	unseenID, completedID := uuid.New(), uuid.New()
	items := []ScoredItem{
		{Item: models.ContentItem{PublicID: completedID}, FinalScore: 0.9},
		{Item: models.ContentItem{PublicID: unseenID}, FinalScore: 0.4},
	}

	got := prioritizeScoredPodsForSession(items, []uuid.UUID{completedID}, nil)
	if len(got) != 2 || got[0].Item.PublicID != unseenID || got[1].Item.PublicID != completedID || got[1].FinalScore != 0.9 {
		t.Fatalf("ranked session order = %#v, want unseen first and score-preserving recycle", got)
	}
}
