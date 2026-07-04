package models

import "time"

// MediaIntelligenceConfig is the per-tenant runtime override for the media-value
// engine's operational tunables (Platform-Console control room). A missing row
// means "use the code defaults" — src/intelligence/DefaultTuning is the source
// of truth for those defaults, and this struct's field defaults mirror them.
//
// Only OPERATIONAL knobs are here. Model-shape constants (shrinkage k's, priors,
// confidence half, refresh cadence, storage-eligibility thresholds) stay in code
// deliberately to avoid knob sprawl — see docs/ranking-intelligence-system.md §11.
type MediaIntelligenceConfig struct {
	ID       uint   `gorm:"primaryKey" json:"-"`
	TenantID string `gorm:"type:varchar(64);not null;uniqueIndex:idx_media_intel_config_tenant" json:"tenant_id"`

	// Four rate/state signal weights (server-normalized to sum 1.0 on write).
	EngagementWeight float64 `gorm:"type:double precision;default:0.35" json:"engagement_weight"`
	CompletionWeight float64 `gorm:"type:double precision;default:0.25" json:"completion_weight"`
	QualityWeight    float64 `gorm:"type:double precision;default:0.20" json:"quality_weight"`
	VelocityWeight   float64 `gorm:"type:double precision;default:0.20" json:"velocity_weight"`

	// Exploration.
	ExplorationSliceEvery   int `gorm:"type:integer;default:10" json:"exploration_slice_every"`
	ExploreImpressionTarget int `gorm:"type:integer;default:50" json:"explore_impression_target"`
	LegacyExposureViewFloor int `gorm:"type:integer;default:25" json:"legacy_exposure_view_floor"`

	// Soft-eviction demotion decay.
	DemotionDefaultFactor float64 `gorm:"type:double precision;default:0.5" json:"demotion_default_factor"`
	DemotionHalfLifeDays  int     `gorm:"type:integer;default:14" json:"demotion_half_life_days"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (MediaIntelligenceConfig) TableName() string {
	return "media_intelligence_configs"
}

// DefaultMediaIntelligenceConfig returns the code defaults for a tenant with no
// override row. Values MUST match src/intelligence/DefaultTuning.
func DefaultMediaIntelligenceConfig(tenantID string) MediaIntelligenceConfig {
	return MediaIntelligenceConfig{
		TenantID:                tenantID,
		EngagementWeight:        0.35,
		CompletionWeight:        0.25,
		QualityWeight:           0.20,
		VelocityWeight:          0.20,
		ExplorationSliceEvery:   10,
		ExploreImpressionTarget: 50,
		LegacyExposureViewFloor: 25,
		DemotionDefaultFactor:   0.5,
		DemotionHalfLifeDays:    14,
	}
}
