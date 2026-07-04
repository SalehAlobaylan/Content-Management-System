package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

// MediaIntelligenceScore is the persisted Value-surface output of the
// Ranking/Intelligence System (stage 4) — one row per media content item,
// owned by src/intelligence. Persisted (rather than computed on read) because
// Storage orders whole-corpus candidate queries by value in SQL; consumers
// JOIN this table (`ORDER BY value ASC NULLS FIRST` — a NULL/absent row means
// "never scored", which composes with the exploration guard instead of being
// mistaken for value zero).
//
// demotion_factor/demoted_at carry the soft-eviction state: an applied
// rank_down sets them, the feed hook applies the half-life-decayed multiplier,
// revert clears them. Kept here (engine-owned) deliberately separate from the
// human-owned editorial ContentFlag.
type MediaIntelligenceScore struct {
	ContentItemID uuid.UUID `gorm:"type:uuid;primaryKey" json:"content_item_id"`
	TenantID      string    `gorm:"type:varchar(64);not null;index:idx_media_intel_scores_tenant_value,priority:1" json:"tenant_id"`

	Value            float64 `gorm:"not null;index:idx_media_intel_scores_tenant_value,priority:2" json:"value"`
	Confidence       float64 `gorm:"not null" json:"confidence"`
	ExplorationState string  `gorm:"type:varchar(16);not null;default:'exploring';index:idx_media_intel_scores_exploration" json:"exploration_state"`

	// Counter snapshots at compute time — the event-nudged refresh trigger
	// compares live counters against these to find items needing recompute.
	ImpressionsAtCompute int64 `gorm:"not null;default:0" json:"impressions_at_compute"`
	EngagementAtCompute  int64 `gorm:"not null;default:0" json:"engagement_at_compute"`

	// Soft-eviction demotion (slice 4). 0 or NULL factor = not demoted.
	DemotionFactor *float64   `gorm:"type:double precision" json:"demotion_factor,omitempty"`
	DemotedAt      *time.Time `gorm:"type:timestamp" json:"demoted_at,omitempty"`

	Breakdown datatypes.JSON `gorm:"type:jsonb" json:"breakdown,omitempty"`
	Reasons   datatypes.JSON `gorm:"type:jsonb" json:"reasons,omitempty"`

	ComputedAt time.Time `gorm:"type:timestamp;not null;index:idx_media_intel_scores_computed_at" json:"computed_at"`
	CreatedAt  time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt  time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (MediaIntelligenceScore) TableName() string {
	return "media_intelligence_scores"
}

// MediaDemandStat is one serve-side demand-telemetry window: what the For You
// feed actually served (and failed to serve) for one tenant × duration bucket ×
// hour. Written in batch at feed-assembly time (slice 2), read by the Demand
// surface (slice 3). The topic axis (slice 6) uses the same shape with a topic
// discriminator; bucket-level rows keep topic = ''.
type MediaDemandStat struct {
	ID       uint   `gorm:"primaryKey" json:"-"`
	TenantID string `gorm:"type:varchar(64);not null;uniqueIndex:idx_media_demand_window,priority:1" json:"tenant_id"`
	Bucket   string `gorm:"type:varchar(8);not null;uniqueIndex:idx_media_demand_window,priority:2" json:"bucket"`
	Topic    string `gorm:"type:varchar(120);not null;default:'';uniqueIndex:idx_media_demand_window,priority:3" json:"topic"`

	WindowStart time.Time `gorm:"type:timestamp;not null;uniqueIndex:idx_media_demand_window,priority:4;index:idx_media_demand_window_start" json:"window_start"`

	// Serve share: units of this bucket served into feed pages.
	Serves int64 `gorm:"not null;default:0" json:"serves"`
	// Exhaustions: the true cache miss — a page came back shorter than asked,
	// or the cursor ran off the end while this bucket was the duration filter.
	Exhaustions int64 `gorm:"not null;default:0" json:"exhaustions"`
	// Repeat serves: the same item served again within the repeat window — the
	// small-library symptom (we circulate the same units for lack of others).
	RepeatServes int64 `gorm:"not null;default:0" json:"repeat_serves"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (MediaDemandStat) TableName() string {
	return "media_demand_stats"
}
