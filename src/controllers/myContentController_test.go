package controllers

import (
	"testing"

	"content-management-system/src/models"
	"gorm.io/datatypes"
)

func TestMapToMyContentItemIncludesPlaybackMetadata(t *testing.T) {
	primary := "https://cdn.example.test/primary.m3u8"
	playbackType := "hls"
	fallback := "https://cdn.example.test/fallback.mp4"
	hasVideo := true

	mapped := mapToMyContentItem(models.ContentItem{
		Type:                models.ContentTypeVideo,
		PlaybackURL:         &primary,
		PlaybackType:        &playbackType,
		FallbackPlaybackURL: &fallback,
		HasVideo:            &hasVideo,
		MediaRenditions: datatypes.JSON(`[
			{"type":"mp4","url":"https://cdn.example.test/fallback.mp4","has_video":true}
		]`),
	})

	if mapped.PlaybackURL != primary || mapped.PlaybackType != playbackType || mapped.FallbackPlaybackURL != fallback {
		t.Fatalf("expected primary and fallback playback URLs, got %#v", mapped)
	}
	if mapped.FallbackPlaybackType != "mp4" || mapped.FallbackHasVideo == nil || !*mapped.FallbackHasVideo {
		t.Fatalf("expected authoritative fallback metadata, got %#v", mapped)
	}
	if mapped.HasVideo == nil || !*mapped.HasVideo {
		t.Fatalf("expected primary has_video, got %#v", mapped)
	}
}
