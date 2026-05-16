package models

import (
	"time"

	"github.com/lib/pq"
)

// QualityProfile is the platform's ingest configuration for a given
// (tenant, source_type) combination. It captures every knob the Aggregation
// media pipeline needs to process a fresh content item: video/audio encode
// recipe, output container, thumbnail extraction params, allowed input
// formats, and hard limits.
//
// Resolution at ingest time picks the most-specific match:
//   1. tenant_id=X AND source_type=Y    (most specific)
//   2. tenant_id=X AND source_type=NULL (tenant default)
//   3. tenant_id=NULL AND source_type=Y (per-source global)
//   4. tenant_id=NULL AND source_type=NULL (global default)
//
// Profiles are also referenced by storage policies whose archive_action is
// 're_encode' — the storage worker shrinks eligible items down to the
// resolved (or explicitly chosen) profile.
type QualityProfile struct {
	ID          uint    `gorm:"primaryKey" json:"id"`
	TenantID    *string `gorm:"type:varchar(64);index:idx_quality_profile_scope,priority:1" json:"tenant_id,omitempty"`
	SourceType  *string `gorm:"type:varchar(20);index:idx_quality_profile_scope,priority:2" json:"source_type,omitempty"`
	Name        string  `gorm:"type:varchar(64);not null" json:"name"`
	Description string  `gorm:"type:text" json:"description"`

	// Video parameters
	VideoCodec        string `gorm:"type:varchar(16);default:'h264'" json:"video_codec"` // h264 | h265 | av1
	MaxHeight         int    `gorm:"default:720" json:"max_height"`                      // 0 = no cap
	TargetBitrateKbps int    `gorm:"default:0" json:"target_bitrate_kbps"`               // 0 = use CRF
	CRF               int    `gorm:"default:23" json:"crf"`                              // 0 (lossless) – 51 (worst)
	Preset            string `gorm:"type:varchar(16);default:'fast'" json:"preset"`      // ultrafast..veryslow

	// Audio parameters
	AudioCodec       string `gorm:"type:varchar(16);default:'aac'" json:"audio_codec"` // aac | opus
	AudioBitrateKbps int    `gorm:"default:128" json:"audio_bitrate_kbps"`

	// Output container — drives file extension and container-specific flags.
	// HLS / DASH listed for forward-compat but unsupported by the v1 pipeline.
	OutputContainer string `gorm:"type:varchar(8);default:'mp4'" json:"output_container"` // mp4 | webm | mov

	// Thumbnail extraction params used by extractThumbnail in Aggregation.
	ThumbnailOffsetSeconds int `gorm:"default:2" json:"thumbnail_offset_seconds"`
	ThumbnailMaxHeight     int `gorm:"default:360" json:"thumbnail_max_height"`

	// Input whitelist — empty/nil array means accept anything (current behaviour).
	// MIME types e.g. "video/mp4", "audio/mpeg", "image/jpeg".
	AllowedInputMimeTypes pq.StringArray `gorm:"type:text[]" json:"allowed_input_mime_types,omitempty"`

	// Hard input limits — NULL = no limit. Aggregation rejects offending jobs
	// during pre-flight (status=FAILED, failure_reason captured) before any
	// transcode work runs.
	MaxInputSizeBytes   *int64 `gorm:"type:bigint" json:"max_input_size_bytes,omitempty"`
	MaxInputDurationSec *int   `gorm:"type:int" json:"max_input_duration_sec,omitempty"`

	// PresetKey records which named preset (if any) the profile was spawned
	// from. Empty string = custom / unknown. Pure metadata: Aggregation
	// never reads it; the Console uses it for the "From: Mobile Feed" badge
	// and the "Reset to preset defaults" affordance.
	PresetKey string `gorm:"type:varchar(32);default:''" json:"preset_key"`

	IsActive  bool      `gorm:"default:true" json:"is_active"`
	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (QualityProfile) TableName() string {
	return "quality_profiles"
}
