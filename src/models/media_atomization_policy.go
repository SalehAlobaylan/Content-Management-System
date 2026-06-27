package models

import "time"

// MediaAtomizationPolicy is the tenant-level, admin-tunable baseline for the
// long-media atomization engine. Source api_config and per-episode overrides
// may override selected fields.
type MediaAtomizationPolicy struct {
	ID       uint   `gorm:"primaryKey" json:"-"`
	TenantID string `gorm:"type:varchar(64);not null;uniqueIndex:idx_media_atomization_policy_tenant" json:"tenant_id"`

	ChapteringEnabled           bool    `gorm:"default:true" json:"chaptering_enabled"`
	AutoPublishHighConfidence   bool    `gorm:"default:true" json:"auto_publish_high_confidence"`
	ParentFeedVisible           bool    `gorm:"default:false" json:"parent_feed_visible"`
	PreserveVideo               bool    `gorm:"default:true" json:"preserve_video"`
	RemoveSponsorSegments       bool    `gorm:"default:true" json:"remove_sponsor_segments"`
	MinChapterMinutes           int     `gorm:"type:integer;default:5" json:"min_chapter_minutes"`
	MinFeedUnitSeconds          int     `gorm:"type:integer;default:270" json:"min_feed_unit_seconds"`
	SoftMaxChapterMinutes       int     `gorm:"type:integer;default:30" json:"soft_max_chapter_minutes"`
	HardMaxChapterMinutes       int     `gorm:"type:integer;default:40" json:"hard_max_chapter_minutes"`
	AtomizationMinParentSeconds int     `gorm:"type:integer;default:2400" json:"atomization_min_parent_seconds"`
	MaxChaptersPerParent        int     `gorm:"type:integer;default:5" json:"max_chapters_per_parent"`
	ChapteringMode              string  `gorm:"type:varchar(32);default:'contextual'" json:"chaptering_mode"`
	HighConfidenceThreshold     float64 `gorm:"type:double precision;default:0.82" json:"high_confidence_threshold"`
	PreferredPlaybackRendition  string  `gorm:"type:varchar(16);default:'hls'" json:"preferred_playback_rendition"`
	FallbackPlaybackRendition   string  `gorm:"type:varchar(16);default:'mp4'" json:"fallback_playback_rendition"`
	AudioOnlyAllowed            bool    `gorm:"default:true" json:"audio_only_allowed"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (MediaAtomizationPolicy) TableName() string {
	return "media_atomization_policies"
}

func DefaultMediaAtomizationPolicy(tenantID string) MediaAtomizationPolicy {
	return MediaAtomizationPolicy{
		TenantID:                    tenantID,
		ChapteringEnabled:           true,
		AutoPublishHighConfidence:   true,
		ParentFeedVisible:           false,
		PreserveVideo:               true,
		RemoveSponsorSegments:       true,
		MinChapterMinutes:           5,
		MinFeedUnitSeconds:          270,
		SoftMaxChapterMinutes:       30,
		HardMaxChapterMinutes:       40,
		AtomizationMinParentSeconds: 2400,
		MaxChaptersPerParent:        5,
		ChapteringMode:              "contextual",
		HighConfidenceThreshold:     0.82,
		PreferredPlaybackRendition:  "hls",
		FallbackPlaybackRendition:   "mp4",
		AudioOnlyAllowed:            true,
	}
}
