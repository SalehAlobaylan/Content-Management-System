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

	// Active mode: fresh_first, trending, most_relevant, ai_curated, balanced, custom
	Mode string `gorm:"type:varchar(20);default:'balanced'" json:"mode"`

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
		Mode:                        "balanced",
		IsActive:                    true,
	}
}

// ModeDefinition describes a ranking mode for the frontend.
type ModeDefinition struct {
	Mode        string  `json:"mode"`
	Name        string  `json:"name"`
	Description string  `json:"description"`
	Icon        string  `json:"icon"`
	Weights     map[string]float64 `json:"weights"`
}

// ModePresets returns all available ranking mode presets.
func ModePresets() map[string]RankingConfig {
	return map[string]RankingConfig{
		"fresh_first": {
			FreshnessWeight:  0.50, EngagementWeight: 0.10, VelocityWeight: 0.10,
			SimilarityWeight: 0.05, QualityWeight: 0.10, DiversityWeight: 0.10, TrendingWeight: 0.05,
			FreshnessDecayHours: 24, VelocityWindowHours: 6, TrendingThresholdMultiplier: 2.0,
			EngagementNormalization: "log",
		},
		"trending": {
			FreshnessWeight:  0.10, EngagementWeight: 0.15, VelocityWeight: 0.30,
			SimilarityWeight: 0.05, QualityWeight: 0.05, DiversityWeight: 0.05, TrendingWeight: 0.30,
			FreshnessDecayHours: 72, VelocityWindowHours: 3, TrendingThresholdMultiplier: 1.5,
			EngagementNormalization: "log",
		},
		"most_relevant": {
			FreshnessWeight:  0.10, EngagementWeight: 0.15, VelocityWeight: 0.05,
			SimilarityWeight: 0.35, QualityWeight: 0.15, DiversityWeight: 0.15, TrendingWeight: 0.05,
			FreshnessDecayHours: 72, VelocityWindowHours: 6, TrendingThresholdMultiplier: 2.0,
			EngagementNormalization: "log",
		},
		"ai_curated": {
			FreshnessWeight:  0.15, EngagementWeight: 0.15, VelocityWeight: 0.10,
			SimilarityWeight: 0.20, QualityWeight: 0.15, DiversityWeight: 0.15, TrendingWeight: 0.10,
			FreshnessDecayHours: 72, VelocityWindowHours: 6, TrendingThresholdMultiplier: 2.0,
			EngagementNormalization: "log",
		},
		"balanced": {
			FreshnessWeight:  0.25, EngagementWeight: 0.20, VelocityWeight: 0.15,
			SimilarityWeight: 0.15, QualityWeight: 0.10, DiversityWeight: 0.10, TrendingWeight: 0.05,
			FreshnessDecayHours: 72, VelocityWindowHours: 6, TrendingThresholdMultiplier: 2.0,
			EngagementNormalization: "log",
		},
	}
}

// ModeDefinitions returns mode metadata for the frontend.
func ModeDefinitions() []ModeDefinition {
	return []ModeDefinition{
		{
			Mode: "fresh_first", Name: "Fresh First",
			Description: "Prioritizes the newest content. Best for breaking news and time-sensitive feeds.",
			Icon: "clock",
			Weights: map[string]float64{
				"freshness": 0.50, "engagement": 0.10, "velocity": 0.10,
				"similarity": 0.05, "quality": 0.10, "diversity": 0.10, "trending": 0.05,
			},
		},
		{
			Mode: "trending", Name: "Trending",
			Description: "Surfaces viral content with interaction spikes. Best for discovering what's hot right now.",
			Icon: "trending-up",
			Weights: map[string]float64{
				"freshness": 0.10, "engagement": 0.15, "velocity": 0.30,
				"similarity": 0.05, "quality": 0.05, "diversity": 0.05, "trending": 0.30,
			},
		},
		{
			Mode: "most_relevant", Name: "Most Relevant",
			Description: "Personalizes feeds using user preferences, topics, and content similarity.",
			Icon: "user-check",
			Weights: map[string]float64{
				"freshness": 0.10, "engagement": 0.15, "velocity": 0.05,
				"similarity": 0.35, "quality": 0.15, "diversity": 0.15, "trending": 0.05,
			},
		},
		{
			Mode: "ai_curated", Name: "AI Curated",
			Description: "AI-driven ranking that balances all signals intelligently. Coming soon.",
			Icon: "sparkles",
			Weights: map[string]float64{
				"freshness": 0.15, "engagement": 0.15, "velocity": 0.10,
				"similarity": 0.20, "quality": 0.15, "diversity": 0.15, "trending": 0.10,
			},
		},
		{
			Mode: "balanced", Name: "Balanced",
			Description: "Equal mix of all ranking signals. A good default for general-purpose feeds.",
			Icon: "scale",
			Weights: map[string]float64{
				"freshness": 0.25, "engagement": 0.20, "velocity": 0.15,
				"similarity": 0.15, "quality": 0.10, "diversity": 0.10, "trending": 0.05,
			},
		},
	}
}

// ApplyPreset sets the config weights and params from a known mode preset.
// Returns true if the mode was found, false otherwise.
func (c *RankingConfig) ApplyPreset(mode string) bool {
	presets := ModePresets()
	preset, ok := presets[mode]
	if !ok {
		return false
	}
	c.Mode = mode
	c.FreshnessWeight = preset.FreshnessWeight
	c.EngagementWeight = preset.EngagementWeight
	c.VelocityWeight = preset.VelocityWeight
	c.SimilarityWeight = preset.SimilarityWeight
	c.QualityWeight = preset.QualityWeight
	c.DiversityWeight = preset.DiversityWeight
	c.TrendingWeight = preset.TrendingWeight
	c.FreshnessDecayHours = preset.FreshnessDecayHours
	c.VelocityWindowHours = preset.VelocityWindowHours
	c.TrendingThresholdMultiplier = preset.TrendingThresholdMultiplier
	c.EngagementNormalization = preset.EngagementNormalization
	return true
}
