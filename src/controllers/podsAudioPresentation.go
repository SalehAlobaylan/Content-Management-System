package controllers

import (
	"content-management-system/src/models"
	"encoding/json"
	"mime"
	"strings"

	"gorm.io/datatypes"
)

// Legacy podcast delivery sometimes wrapped an audio enclosure in black video.
// The enclosure MIME is source evidence; the encoded track is not meaningful
// picture. Correct only v1 presentation, preserving every approved transport URL
// and type. Newer rendition contracts and video podcasts keep their track truth.
func normalizeLegacyPodcastPresentation(item PodsItem, source models.SourceType, metadata datatypes.JSON, currentVersion int) PodsItem {
	if source != models.SourceTypePodcast || item.RenditionSetVersion > 1 || currentVersion > 1 || item.ActiveRenditionGenerationID != nil {
		return item
	}
	var provenance struct {
		EnclosureType string `json:"enclosureType"`
	}
	if json.Unmarshal(metadata, &provenance) != nil {
		return item
	}
	enclosureType, _, err := mime.ParseMediaType(strings.TrimSpace(provenance.EnclosureType))
	if err != nil || !strings.HasPrefix(enclosureType, "audio/") || enclosureType == "audio/" {
		return item
	}

	hasVideo := false
	item.HasVideo = &hasVideo
	// Work on a decoded copy: frozen snapshot maps must not be changed in place.
	encoded, err := json.Marshal(item.MediaRenditions)
	if err != nil {
		return item
	}
	var renditions []map[string]any
	if json.Unmarshal(encoded, &renditions) != nil {
		return item
	}
	for _, rendition := range renditions {
		if rendition == nil {
			continue
		}
		typeName, _ := rendition["type"].(string)
		if typeName != "hls" && typeName != "mp4" && typeName != "audio" {
			continue
		}
		rendition["has_video"] = false
		if item.FallbackPlaybackURL != nil && rendition["url"] == *item.FallbackPlaybackURL {
			item.FallbackPlaybackType = &typeName
			item.FallbackHasVideo = &hasVideo
		}
	}
	if len(renditions) > 0 {
		item.MediaRenditions = renditions
	}
	return item
}
