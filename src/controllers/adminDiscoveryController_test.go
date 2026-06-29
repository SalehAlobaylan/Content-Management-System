package controllers

import (
	"content-management-system/src/models"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

func rawMessage(t *testing.T, value map[string]interface{}) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal test json: %v", err)
	}
	return json.RawMessage(raw)
}

func rawArray(t *testing.T, value []map[string]interface{}) datatypes.JSON {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal test json array: %v", err)
	}
	return datatypes.JSON(raw)
}

func TestBuildMediaSourcesContextRollups(t *testing.T) {
	suggestions := []sourceSuggestionResponse{
		{
			ID:            "imported",
			DiscoveredVia: "youtube-import",
			Evidence:      rawMessage(t, map[string]interface{}{"caption_state": "none", "needs_chaptering": true}),
			Health:        rawMessage(t, map[string]interface{}{"audio_first": false}),
		},
		{
			ID:       "auto",
			Evidence: rawMessage(t, map[string]interface{}{"caption_state": "youtube_auto"}),
			Health:   rawMessage(t, map[string]interface{}{"audio_first": true}),
		},
	}
	sources := []newsSourceResponse{
		{Failed: 0},
		{Failed: 2},
	}
	stats := sourceStatsResponse{
		ByHealth: map[string]int64{
			"healthy":   3,
			"stale":     1,
			"never_run": 2,
			"disabled":  1,
		},
	}

	got := buildMediaSourcesContextRollups(suggestions, sources, stats)

	if got.Pending != 2 || got.Imported != 1 || got.AutoDiscovered != 1 || got.Active != 2 {
		t.Fatalf("unexpected base rollups: %+v", got)
	}
	if got.Healthy != 3 || got.Stale != 1 || got.NeverRun != 2 || got.Disabled != 1 {
		t.Fatalf("unexpected health rollups: %+v", got)
	}
	if got.Failed != 1 || got.NoTranscript != 1 || got.NeedsTrimming != 1 || got.NonAudioFirst != 1 {
		t.Fatalf("unexpected evidence rollups: %+v", got)
	}
}

func TestIsMediaConsoleSourceIncludesTelegramMediaKinds(t *testing.T) {
	raw, err := json.Marshal(map[string]interface{}{"media_types": []string{"voice"}})
	if err != nil {
		t.Fatalf("marshal api config: %v", err)
	}
	source := models.ContentSource{
		Type:      models.SourceTypeTelegram,
		Category:  models.SourceCategoryNews,
		APIConfig: datatypes.JSON(raw),
	}
	if !isMediaConsoleSource(source) {
		t.Fatal("expected Telegram voice source to count as media")
	}

	textOnlyRaw, err := json.Marshal(map[string]interface{}{"media_types": []string{"text"}})
	if err != nil {
		t.Fatalf("marshal text api config: %v", err)
	}
	textOnly := models.ContentSource{
		Type:      models.SourceTypeTelegram,
		Category:  models.SourceCategoryNews,
		APIConfig: datatypes.JSON(textOnlyRaw),
	}
	if isMediaConsoleSource(textOnly) {
		t.Fatal("expected text-only Telegram source to stay out of media")
	}
}

func TestBuildSuggestionRelationships(t *testing.T) {
	feed := "https://example.com/feed.xml"
	sourceID := uint(42)
	sourcePublicID := uuid.New()
	profileID := uint(7)
	recentFetch := time.Now().Add(-5 * time.Minute)
	source := models.ContentSource{
		ID:                   sourceID,
		PublicID:             sourcePublicID,
		Name:                 "Wahb Audio",
		FeedURL:              &feed,
		IsActive:             true,
		FetchIntervalMinutes: 60,
		LastFetchedAt:        &recentFetch,
		DiscoveryProfileID:   &profileID,
	}

	approvedSourceID := sourceID
	cases := []struct {
		name         string
		suggestion   models.SourceSuggestion
		relationship string
		reason       string
	}{
		{
			name: "exact duplicate by feed",
			suggestion: models.SourceSuggestion{
				PublicID: uuid.New(),
				Name:     "Different Name",
				FeedURL:  "http://www.example.com/feed.xml/",
			},
			relationship: "duplicate",
			reason:       "same_feed_url",
		},
		{
			name: "similar by normalized name",
			suggestion: models.SourceSuggestion{
				PublicID: uuid.New(),
				Name:     "wahb-audio",
				FeedURL:  "https://elsewhere.example/feed.xml",
			},
			relationship: "similar",
			reason:       "same_normalized_name",
		},
		{
			name: "similar includes same interest profile",
			suggestion: models.SourceSuggestion{
				PublicID:  uuid.New(),
				ProfileID: &profileID,
				Name:      "wahb-audio",
				FeedURL:   "https://same-profile.example/feed.xml",
			},
			relationship: "similar",
			reason:       "same_interest_profile",
		},
		{
			name: "improves existing with stronger evidence",
			suggestion: models.SourceSuggestion{
				PublicID:    uuid.New(),
				ProfileID:   &profileID,
				Name:        "wahb-audio",
				FeedURL:     "https://better.example/feed.xml",
				Health:      datatypes.JSON(rawMessage(t, map[string]interface{}{"audio_first": true})),
				Evidence:    datatypes.JSON(rawMessage(t, map[string]interface{}{"caption_state": "youtube_human"})),
				SampleItems: rawArray(t, []map[string]interface{}{{"title": "A"}, {"title": "B"}, {"title": "C"}}),
			},
			relationship: "improves_existing",
			reason:       "stronger_audio_evidence",
		},
		{
			name: "already approved by linked source",
			suggestion: models.SourceSuggestion{
				PublicID:         uuid.New(),
				Name:             "Wahb Audio",
				FeedURL:          "https://elsewhere.example/feed.xml",
				ApprovedSourceID: &approvedSourceID,
			},
			relationship: "already_approved",
			reason:       "approved_source",
		},
		{
			name: "new suggestion",
			suggestion: models.SourceSuggestion{
				PublicID: uuid.New(),
				Name:     "Fresh Signal",
				FeedURL:  "https://fresh.example/feed.xml",
			},
			relationship: "new",
			reason:       "",
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			got := buildSuggestionRelationships([]models.SourceSuggestion{tt.suggestion}, []models.ContentSource{source}, nil)
			rel := got[tt.suggestion.PublicID.String()]
			if rel.Relationship != tt.relationship {
				t.Fatalf("relationship = %q, want %q", rel.Relationship, tt.relationship)
			}
			if tt.reason != "" {
				found := false
				for _, reason := range rel.Reasons {
					if reason == tt.reason {
						found = true
						break
					}
				}
				if !found {
					t.Fatalf("reasons = %#v, want %q", rel.Reasons, tt.reason)
				}
			}
			if tt.relationship != "new" && (rel.MatchedSourceID == nil || *rel.MatchedSourceID != sourcePublicID.String()) {
				t.Fatalf("matched source = %#v, want %s", rel.MatchedSourceID, sourcePublicID.String())
			}
		})
	}
}

func TestBuildSuggestionRelationshipsImprovementComparesExistingEvidence(t *testing.T) {
	profileID := uint(7)
	sourcePublicID := uuid.New()
	recentFetch := time.Now().Add(-5 * time.Minute)
	sourceMetadata := rawMessage(t, map[string]interface{}{
		"media_finding_health":       map[string]interface{}{"audio_first": true},
		"media_finding_evidence":     map[string]interface{}{"caption_state": "youtube_human"},
		"media_finding_sample_items": []map[string]interface{}{{"title": "A"}, {"title": "B"}, {"title": "C"}},
	})
	source := models.ContentSource{
		ID:                   42,
		PublicID:             sourcePublicID,
		Name:                 "Wahb Audio",
		IsActive:             true,
		FetchIntervalMinutes: 60,
		LastFetchedAt:        &recentFetch,
		DiscoveryProfileID:   &profileID,
		Metadata:             datatypes.JSON(sourceMetadata),
	}
	suggestion := models.SourceSuggestion{
		PublicID:    uuid.New(),
		ProfileID:   &profileID,
		Name:        "wahb-audio",
		Health:      datatypes.JSON(rawMessage(t, map[string]interface{}{"audio_first": true})),
		Evidence:    datatypes.JSON(rawMessage(t, map[string]interface{}{"caption_state": "youtube_human"})),
		SampleItems: rawArray(t, []map[string]interface{}{{"title": "A"}, {"title": "B"}, {"title": "C"}}),
	}

	got := buildSuggestionRelationships([]models.SourceSuggestion{suggestion}, []models.ContentSource{source}, []newsSourceResponse{
		{contentSourceResponse: contentSourceResponse{ID: sourcePublicID.String()}, Failed: 2},
	})
	rel := got[suggestion.PublicID.String()]
	if rel.Relationship != "improves_existing" {
		t.Fatalf("relationship = %q, want improves_existing", rel.Relationship)
	}
	foundFailureReason := false
	foundBlindEvidenceReason := false
	for _, reason := range rel.Reasons {
		if reason == "existing_source_failed" {
			foundFailureReason = true
		}
		if reason == "stronger_audio_evidence" || reason == "transcript_available" || reason == "richer_sample_items" {
			foundBlindEvidenceReason = true
		}
	}
	if !foundFailureReason {
		t.Fatalf("reasons = %#v, want existing_source_failed", rel.Reasons)
	}
	if foundBlindEvidenceReason {
		t.Fatalf("reasons = %#v, should not include candidate evidence already present on source", rel.Reasons)
	}
}

func TestMapMediaSourceRecentItemResponse(t *testing.T) {
	title := "Fresh episode"
	publishedAt := time.Date(2026, 6, 29, 10, 0, 0, 0, time.UTC)
	duration := 1800
	captionState := models.CaptionStateYouTubeHuman
	chapteringStatus := "planned"
	item := models.ContentItem{
		PublicID:         uuid.New(),
		Title:            &title,
		Status:           models.ContentStatusReady,
		PublishedAt:      &publishedAt,
		DurationSec:      &duration,
		CaptionState:     &captionState,
		ChapteringStatus: &chapteringStatus,
		FeedVisibility:   "visible",
	}

	got := mapMediaSourceRecentItemResponse(item)
	if got.ID != item.PublicID.String() || got.Title != title || got.Status != string(models.ContentStatusReady) {
		t.Fatalf("unexpected identity/status mapping: %+v", got)
	}
	if got.PublishedAt == nil || *got.PublishedAt != publishedAt.Format(time.RFC3339) {
		t.Fatalf("published_at = %#v, want %s", got.PublishedAt, publishedAt.Format(time.RFC3339))
	}
	if got.DurationSec == nil || *got.DurationSec != duration {
		t.Fatalf("duration = %#v, want %d", got.DurationSec, duration)
	}
	if got.CaptionState == nil || *got.CaptionState != captionState {
		t.Fatalf("caption_state = %#v, want %s", got.CaptionState, captionState)
	}
	if got.ChapteringStatus == nil || *got.ChapteringStatus != chapteringStatus {
		t.Fatalf("chaptering_status = %#v, want %s", got.ChapteringStatus, chapteringStatus)
	}
	if got.FeedVisibility != "visible" {
		t.Fatalf("feed_visibility = %q", got.FeedVisibility)
	}
}

func TestMediaApprovalHandoffStatus(t *testing.T) {
	now := time.Now().UTC().Format(time.RFC3339)
	cases := []struct {
		name   string
		source newsSourceResponse
		want   string
	}{
		{name: "queued", source: newsSourceResponse{contentSourceResponse: contentSourceResponse{IsActive: true}}, want: "first_fetch_queued"},
		{name: "waiting after first fetch", source: newsSourceResponse{contentSourceResponse: contentSourceResponse{IsActive: true, LastFetchedAt: &now}}, want: "waiting_for_items"},
		{name: "producing", source: newsSourceResponse{contentSourceResponse: contentSourceResponse{IsActive: true}, ItemsCount: 3, Ready: 2}, want: "producing"},
		{name: "failed output", source: newsSourceResponse{contentSourceResponse: contentSourceResponse{IsActive: true}, Failed: 1}, want: "needs_attention"},
		{name: "disabled", source: newsSourceResponse{contentSourceResponse: contentSourceResponse{IsActive: false}}, want: "needs_attention"},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			if got := mediaApprovalHandoffStatus(tt.source); got != tt.want {
				t.Fatalf("status = %q, want %q", got, tt.want)
			}
		})
	}
}
