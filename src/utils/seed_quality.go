package utils

import (
	"log"

	"content-management-system/src/models"

	"gorm.io/gorm"
)

// SeedDefaultQualityProfiles inserts baseline ingest profiles when the
// quality_profiles table is empty. Idempotent — safe to call on every boot.
//
// Profiles seeded (Phase 7 schema — scoped by tenant_id + source_type, both
// NULL here so they apply to any tenant + any source as platform defaults):
//   - global-default       : everyday recipe matched to historical pipeline.
//   - global-mobile-720p   : 720p cap / CRF 26 / AAC 96k. Sweet spot for phones.
//   - global-archival-480p : 480p cap / CRF 28 / AAC 64k. Aggressive shrink
//                            target — the storage system uses this for
//                            archive_action='re_encode'.
func SeedDefaultQualityProfiles(db *gorm.DB) error {
	var count int64
	if err := db.Model(&models.QualityProfile{}).Count(&count).Error; err != nil {
		return err
	}
	if count > 0 {
		return nil
	}

	profiles := []models.QualityProfile{
		{
			Name:                   "global-default",
			Description:            "Default ingest recipe (matches the historical hard-coded pipeline). Picked when nothing more specific applies.",
			VideoCodec:             "h264",
			MaxHeight:              0,
			CRF:                    23,
			Preset:                 "fast",
			AudioCodec:             "aac",
			AudioBitrateKbps:       128,
			OutputContainer:        "mp4",
			ThumbnailOffsetSeconds: 2,
			ThumbnailMaxHeight:     360,
			// Closest match in the Phase 8 preset library.
			PresetKey: "high-quality",
			IsActive:  true,
		},
		{
			Name:                   "global-mobile-720p",
			Description:            "Mobile-optimized — 720p cap, CRF 26, AAC 96k. ~50% smaller than default with no visible difference on phones.",
			VideoCodec:             "h264",
			MaxHeight:              720,
			CRF:                    26,
			Preset:                 "fast",
			AudioCodec:             "aac",
			AudioBitrateKbps:       96,
			OutputContainer:        "mp4",
			ThumbnailOffsetSeconds: 2,
			ThumbnailMaxHeight:     360,
			PresetKey:              "mobile-feed",
			IsActive:               true,
		},
		{
			Name:                   "global-archival-480p",
			Description:            "Aggressive shrink target — 480p / CRF 28 / AAC 64k. Pair with a storage policy whose archive_action='re_encode' to compress old / low-engagement content in place.",
			VideoCodec:             "h264",
			MaxHeight:              480,
			CRF:                    28,
			Preset:                 "medium",
			AudioCodec:             "aac",
			AudioBitrateKbps:       64,
			OutputContainer:        "mp4",
			ThumbnailOffsetSeconds: 2,
			ThumbnailMaxHeight:     360,
			PresetKey:              "storage-saver",
			IsActive:               true,
		},
	}

	if err := db.Create(&profiles).Error; err != nil {
		return err
	}
	log.Printf("Seeded %d default ingest profiles", len(profiles))
	return nil
}
