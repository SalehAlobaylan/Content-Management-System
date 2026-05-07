package models

import (
	"time"

	"github.com/google/uuid"
)

// QualityHistory is one row per re-encode event (success or failure). Powers
// the Activity panel and the lifetime savings stats.
type QualityHistory struct {
	ID            uint      `gorm:"primaryKey" json:"id"`
	ContentItemID uuid.UUID `gorm:"type:uuid;index;not null" json:"content_item_id"`
	TenantID      string    `gorm:"type:varchar(64);index" json:"tenant_id"`

	FromProfileID *uint `json:"from_profile_id,omitempty"`
	ToProfileID   uint  `gorm:"not null" json:"to_profile_id"`

	OriginalSizeBytes   int64 `json:"original_size_bytes"`
	NewSizeBytes        int64 `json:"new_size_bytes"`
	SavingsBytes        int64 `json:"savings_bytes"`
	OriginalBitrateKbps int   `json:"original_bitrate_kbps"`
	NewBitrateKbps      int   `json:"new_bitrate_kbps"`

	DurationMs int    `json:"duration_ms"` // wall-clock ffmpeg time
	Trigger    string `gorm:"type:varchar(16)" json:"trigger"` // manual | rule | ingest
	RuleID     *uint  `json:"rule_id,omitempty"`
	Error      string `gorm:"type:text" json:"error,omitempty"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
}

func (QualityHistory) TableName() string {
	return "quality_history"
}
