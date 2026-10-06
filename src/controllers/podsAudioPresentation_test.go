package controllers

import (
	"content-management-system/src/models"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

func legacyAudioPodcastFixture() models.ContentItem {
	primary := "https://cdn.example.test/chapters/2/hls/index.m3u8"
	fallback := "https://cdn.example.test/chapters/2/processed.mp4"
	legacy := "https://cdn.example.test/processed.v2.mp4"
	transport := "hls"
	encodedVideo := true
	return models.ContentItem{
		PublicID: uuid.New(), Type: models.ContentTypePodcast, Source: models.SourceTypePodcast,
		PlaybackURL: &primary, PlaybackType: &transport, FallbackPlaybackURL: &fallback,
		MediaURL: &legacy, HasVideo: &encodedVideo, RenditionSetVersion: 1,
		Metadata: datatypes.JSON(`{"enclosureType":"audio/mpeg"}`),
		MediaRenditions: datatypes.JSON(`[
			{"type":"hls","url":"https://cdn.example.test/chapters/2/hls/index.m3u8","is_primary":true,"mime_type":"application/vnd.apple.mpegurl"},
			{"type":"mp4","url":"https://cdn.example.test/chapters/2/processed.mp4","is_primary":false,"mime_type":"video/mp4"}
		]`),
	}
}

func TestPodsProjectionRecognizesLegacyAudioEnclosureInsideVideoTransport(t *testing.T) {
	stored := legacyAudioPodcastFixture()
	before := string(stored.MediaRenditions)
	item := mapToPodsItem(stored, false, false)
	if item.HasVideo == nil || *item.HasVideo {
		t.Fatal("an audio enclosure with synthetic video must use audio presentation")
	}
	if item.PlaybackType == nil || *item.PlaybackType != "hls" || *item.PlaybackURL != *stored.PlaybackURL || item.MediaURL != *stored.MediaURL {
		t.Fatal("presentation correction must preserve approved playback transport and URLs")
	}
	if item.FallbackPlaybackType == nil || *item.FallbackPlaybackType != "mp4" || item.FallbackHasVideo == nil || *item.FallbackHasVideo {
		t.Fatal("source audio evidence must also describe the legacy MP4 fallback")
	}
	encoded, _ := json.Marshal(item.MediaRenditions)
	var renditions []storedPlaybackRendition
	_ = json.Unmarshal(encoded, &renditions)
	for _, rendition := range renditions {
		if rendition.HasVideo == nil || *rendition.HasVideo {
			t.Fatal("selected rendition must retain the same audio presentation evidence")
		}
	}
	if string(stored.MediaRenditions) != before || !*stored.HasVideo {
		t.Fatal("public projection must not change stored artifacts or metadata")
	}
}

func TestLegacyPodcastPresentationRequiresAudioProvenanceAndLegacyContract(t *testing.T) {
	for _, test := range []struct {
		name   string
		modify func(*models.ContentItem)
	}{
		{"video podcast enclosure", func(i *models.ContentItem) { i.Metadata = datatypes.JSON(`{"enclosureType":"video/mp4"}`) }},
		{"missing provenance", func(i *models.ContentItem) { i.Metadata = nil }},
		{"malformed provenance", func(i *models.ContentItem) { i.Metadata = datatypes.JSON(`{"enclosureType":false}`) }},
		{"malformed MIME", func(i *models.ContentItem) { i.Metadata = datatypes.JSON(`{"enclosureType":"audio/"}`) }},
		{"YouTube podcast", func(i *models.ContentItem) { i.Source = models.SourceTypeYouTube }},
		{"v2 typed contract", func(i *models.ContentItem) { i.RenditionSetVersion = 2 }},
		{"v3 typed contract", func(i *models.ContentItem) { i.RenditionSetVersion = 3 }},
		{"active generation", func(i *models.ContentItem) { id := uuid.New(); i.ActiveMediaRenditionGenerationID = &id }},
	} {
		t.Run(test.name, func(t *testing.T) {
			stored := legacyAudioPodcastFixture()
			test.modify(&stored)
			item := mapToPodsItem(stored, false, false)
			if item.HasVideo == nil || !*item.HasVideo {
				t.Fatal("unverified audio or newer track truth must retain video presentation")
			}
		})
	}
}

func TestLegacyPodcastPresentationDoesNotMutateFrozenSnapshot(t *testing.T) {
	stored := legacyAudioPodcastFixture()
	item := PodsItem{
		ID: stored.PublicID, HasVideo: stored.HasVideo, PlaybackURL: stored.PlaybackURL,
		PlaybackType: stored.PlaybackType, FallbackPlaybackURL: stored.FallbackPlaybackURL,
		RenditionSetVersion: 1,
		MediaRenditions:     []map[string]any{{"type": "hls", "url": *stored.PlaybackURL, "has_video": true}},
	}
	before, _ := json.Marshal(item)
	got := normalizeLegacyPodcastPresentation(item, stored.Source, stored.Metadata, 1)
	after, _ := json.Marshal(item)
	if !reflect.DeepEqual(before, after) || got.HasVideo == nil || *got.HasVideo {
		t.Fatal("normalizing a frozen response must leave the snapshot untouched")
	}
	if upgraded := normalizeLegacyPodcastPresentation(item, stored.Source, stored.Metadata, 3); upgraded.HasVideo == nil || !*upgraded.HasVideo {
		t.Fatal("a newer current generation must not rewrite captured track truth")
	}
}
