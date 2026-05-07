package models

import "time"

// QualityRule expresses "when {trigger} apply {target_profile}". Rules are
// evaluated in priority order (lower runs first); the first matching rule per
// item wins. The Aggregation quality-sweeper worker registers a repeatable
// BullMQ job per enabled rule and processes its candidates each tick.
type QualityRule struct {
	ID       uint    `gorm:"primaryKey" json:"id"`
	TenantID *string `gorm:"type:varchar(64);index" json:"tenant_id,omitempty"`
	Name     string  `gorm:"type:varchar(64);not null" json:"name"`
	Enabled  bool    `gorm:"default:false" json:"enabled"`
	Priority int     `gorm:"default:100" json:"priority"`

	// Trigger conditions — all must match for an item to be eligible.
	MinAgeDays       int      `gorm:"default:0" json:"min_age_days"`
	MaxViewCount     *int     `gorm:"type:int" json:"max_view_count,omitempty"`
	MaxViewsPerDay   *float64 `gorm:"type:float" json:"max_views_per_day,omitempty"`
	ContentType      string   `gorm:"type:varchar(16)" json:"content_type"` // empty = any
	SourceID         *uint    `gorm:"index" json:"source_id,omitempty"`
	OnlyIfHigherThan *uint    `gorm:"index" json:"only_if_higher_than,omitempty"`

	// Action: which profile to re-encode the matched items into.
	TargetProfileID   uint `gorm:"not null;index" json:"target_profile_id"`
	SweepIntervalMins int  `gorm:"default:1440" json:"sweep_interval_minutes"`

	LastSweepAt *time.Time `gorm:"type:timestamp" json:"last_sweep_at,omitempty"`
	UpdatedAt   time.Time  `gorm:"autoUpdateTime" json:"updated_at"`
	CreatedAt   time.Time  `gorm:"autoCreateTime" json:"created_at"`
}

func (QualityRule) TableName() string {
	return "quality_rules"
}
