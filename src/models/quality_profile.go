package models

import "time"

// QualityProfile defines an FFmpeg encode recipe. Profiles are referenced by
// QualityRule and stamped onto each ContentItem after a re-encode (or at
// first ingest if the profile is marked IsDefault).
//
// A row with TenantID == nil is a global profile available to every tenant.
// A row with a non-nil TenantID is private to that tenant.
type QualityProfile struct {
	ID          uint    `gorm:"primaryKey" json:"id"`
	TenantID    *string `gorm:"type:varchar(64);index" json:"tenant_id,omitempty"`
	Name        string  `gorm:"type:varchar(64);not null;uniqueIndex:idx_quality_profile_tenant_name" json:"name"`
	Description string  `gorm:"type:text" json:"description"`

	// Video parameters
	VideoCodec        string `gorm:"type:varchar(16);default:'h264'" json:"video_codec"` // h264 | h265 | av1
	MaxHeight         int    `gorm:"default:720" json:"max_height"`                      // 0 = no cap
	TargetBitrateKbps int    `gorm:"default:0" json:"target_bitrate_kbps"`               // 0 = use CRF
	CRF               int    `gorm:"default:23" json:"crf"`                              // 18 (best) – 28 (worst)
	Preset            string `gorm:"type:varchar(16);default:'fast'" json:"preset"`      // ultrafast..veryslow

	// Audio parameters
	AudioCodec       string `gorm:"type:varchar(16);default:'aac'" json:"audio_codec"`
	AudioBitrateKbps int    `gorm:"default:128" json:"audio_bitrate_kbps"`

	IsDefault bool      `gorm:"default:false" json:"is_default"` // applied at first ingest
	IsActive  bool      `gorm:"default:true" json:"is_active"`
	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (QualityProfile) TableName() string {
	return "quality_profiles"
}
