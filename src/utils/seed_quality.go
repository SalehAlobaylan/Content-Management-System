package utils

import (
	"log"

	"content-management-system/src/models"

	"gorm.io/gorm"
)

// SeedDefaultQualityProfiles inserts the three baseline quality profiles when
// the quality_profiles table is empty. Idempotent — safe to call on every boot.
//
// The profiles seeded:
//   - ingest-default : matches the historical hard-coded ffmpeg recipe.
//                      Marked IsDefault so it is applied at first ingest until
//                      an admin promotes a different profile.
//   - mobile-720p    : 720p cap, CRF 26, AAC 96k. Sweet spot for phone playback.
//   - archival-480p  : 480p cap, CRF 28, AAC 64k. Aggressive shrink for old, low-view items.
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
			Name:             "ingest-default",
			Description:      "Default recipe applied at first ingest. Matches the historical pipeline (H.264 baseline, CRF 23, AAC 128k, no resolution cap).",
			VideoCodec:       "h264",
			MaxHeight:        0,
			CRF:              23,
			Preset:           "fast",
			AudioCodec:       "aac",
			AudioBitrateKbps: 128,
			IsDefault:        true,
			IsActive:         true,
		},
		{
			Name:             "mobile-720p",
			Description:      "Mobile-optimized. 720p cap, CRF 26, AAC 96k. ~50% smaller than the default with no visible difference on phones.",
			VideoCodec:       "h264",
			MaxHeight:        720,
			CRF:              26,
			Preset:           "fast",
			AudioCodec:       "aac",
			AudioBitrateKbps: 96,
			IsDefault:        false,
			IsActive:         true,
		},
		{
			Name:             "archival-480p",
			Description:      "Aggressive shrink for old / low-engagement content. 480p cap, CRF 28, AAC 64k. Use via a quality rule.",
			VideoCodec:       "h264",
			MaxHeight:        480,
			CRF:              28,
			Preset:           "medium",
			AudioCodec:       "aac",
			AudioBitrateKbps: 64,
			IsDefault:        false,
			IsActive:         true,
		},
	}

	if err := db.Create(&profiles).Error; err != nil {
		return err
	}
	log.Printf("Seeded %d default quality profiles", len(profiles))
	return nil
}
