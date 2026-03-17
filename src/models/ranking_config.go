package models

import (
	"time"
)

// RankingConfig stores global feed ranking algorithm parameters.
// Single-row-per-tenant config table with 7 tunable weights (sum ≈ 1.0).
type RankingConfig struct {
	ID       uint   `gorm:"primaryKey" json:"-"`
	TenantID string `gorm:"type:varchar(64);not null;uniqueIndex:idx_ranking_config_tenant" json:"tenant_id"`

	// 7 algorithm weights (sum ≈ 1.0)
	FreshnessWeight  float64 `gorm:"type:double precision;default:0.25" json:"freshness_weight"`
	EngagementWeight float64 `gorm:"type:double precision;default:0.20" json:"engagement_weight"`
	VelocityWeight   float64 `gorm:"type:double precision;default:0.15" json:"velocity_weight"`
	SimilarityWeight float64 `gorm:"type:double precision;default:0.15" json:"similarity_weight"`
	QualityWeight    float64 `gorm:"type:double precision;default:0.10" json:"quality_weight"`
	DiversityWeight  float64 `gorm:"type:double precision;default:0.10" json:"diversity_weight"`
	TrendingWeight   float64 `gorm:"type:double precision;default:0.05" json:"trending_weight"`

	// Freshness decay: number of hours for score to halve
	FreshnessDecayHours int `gorm:"type:integer;default:72" json:"freshness_decay_hours"`

	// Velocity: time window in hours for velocity calculation
	VelocityWindowHours int `gorm:"type:integer;default:6" json:"velocity_window_hours"`

	// Trending: multiplier threshold for spike detection
	TrendingThresholdMultiplier float64 `gorm:"type:double precision;default:2.0" json:"trending_threshold_multiplier"`

	// Recirculation: re-surface high-quality low-view older content
	RecirculationEnabled    bool `gorm:"default:false" json:"recirculation_enabled"`
	RecirculationMaxAgeDays int  `gorm:"type:integer;default:30" json:"recirculation_max_age_days"`

	// Engagement normalization strategy
	EngagementNormalization string `gorm:"type:varchar(20);default:'log'" json:"engagement_normalization"`

	// Opt-in toggle: feeds stay chronological until enabled
	IsActive bool `gorm:"default:false" json:"is_active"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (RankingConfig) TableName() string {
	return "ranking_configs"
}

// DefaultRankingConfig returns a config with default values for a tenant.
func DefaultRankingConfig(tenantID string) RankingConfig {
	return RankingConfig{
		TenantID:                    tenantID,
		FreshnessWeight:             0.25,
		EngagementWeight:            0.20,
		VelocityWeight:              0.15,
		SimilarityWeight:            0.15,
		QualityWeight:               0.10,
		DiversityWeight:             0.10,
		TrendingWeight:              0.05,
		FreshnessDecayHours:         72,
		VelocityWindowHours:         6,
		TrendingThresholdMultiplier: 2.0,
		RecirculationEnabled:        false,
		RecirculationMaxAgeDays:     30,
		EngagementNormalization:     "log",
		IsActive:                    false,
	}
}
