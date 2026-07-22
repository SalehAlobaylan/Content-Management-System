package controllers

import (
	"testing"

	"content-management-system/src/models"
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
