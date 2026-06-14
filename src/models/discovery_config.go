package models

import (
	"time"
)

// DiscoveryConfig stores tunable Feeds-Finding parameters — single row per
// tenant (mirrors RankingConfig). Surfaces the discovery knobs in the admin UI
// instead of hardcoded consts / env vars (Config Discipline).
type DiscoveryConfig struct {
	ID       uint   `gorm:"primaryKey" json:"-"`
	TenantID string `gorm:"type:varchar(64);not null;uniqueIndex:idx_discovery_config_tenant" json:"tenant_id"`

	// Scheduling
	AutomationEnabled bool `gorm:"default:false" json:"automation_enabled"`
	SweepIntervalHours int `gorm:"type:integer;default:24" json:"sweep_interval_hours"`

	// Scoring / filtering knobs (replace the hardcoded consts)
	MinConfidence float64 `gorm:"type:double precision;default:0.15" json:"min_confidence"`
	MinRelevance  float64 `gorm:"type:double precision;default:0.10" json:"min_relevance"`
	DupThreshold  float64 `gorm:"type:double precision;default:0.92" json:"dup_threshold"`
	DupPenalty    float64 `gorm:"type:double precision;default:0.50" json:"dup_penalty"`

	// Discovery capacity / behaviour
	RecencyWindowDays       int    `gorm:"type:integer;default:30" json:"recency_window_days"`
	MaxCandidatesPerProfile int    `gorm:"type:integer;default:15" json:"max_candidates_per_profile"`
	SearchProvider          string `gorm:"type:varchar(16);default:auto" json:"search_provider"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

// TableName returns the table name for DiscoveryConfig.
func (DiscoveryConfig) TableName() string {
	return "discovery_configs"
}

// DefaultDiscoveryConfig returns the default tuning for a tenant.
func DefaultDiscoveryConfig(tenantID string) DiscoveryConfig {
	return DiscoveryConfig{
		TenantID:                tenantID,
		AutomationEnabled:       false,
		SweepIntervalHours:      24,
		MinConfidence:           0.15,
		MinRelevance:            0.10,
		DupThreshold:            0.92,
		DupPenalty:              0.50,
		RecencyWindowDays:       30,
		MaxCandidatesPerProfile: 15,
		SearchProvider:          "auto",
	}
}
