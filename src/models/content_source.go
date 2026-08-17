package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// ContentSource represents a content ingestion source configuration
type ContentSource struct {
	ID       uint      `gorm:"primaryKey" json:"-"`
	PublicID uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID string    `gorm:"type:varchar(64);not null;default:default;index:idx_content_sources_tenant_id" json:"tenant_id"`

	// Source identification
	Name string     `gorm:"type:varchar(255);not null" json:"name"`
	Type SourceType `gorm:"type:varchar(20);not null" json:"type"`

	// Category ('news' | 'media') decides which feed/management surface owns the
	// source. Defaulted by type; editable so dual-type sources (e.g. a Telegram
	// channel) are unambiguous instead of appearing in both surfaces.
	Category string `gorm:"type:varchar(16);not null;default:news" json:"category"`

	// Configuration
	FeedURL   *string        `gorm:"type:text" json:"feed_url,omitempty"`
	ImageURL  *string        `gorm:"type:text" json:"image_url,omitempty"`
	APIConfig datatypes.JSON `gorm:"type:jsonb" json:"api_config,omitempty"`

	// Status
	IsActive             bool       `gorm:"default:true" json:"is_active"`
	FetchIntervalMinutes int        `gorm:"default:60" json:"fetch_interval_minutes"`
	LastFetchedAt        *time.Time `gorm:"type:timestamp" json:"last_fetched_at,omitempty"`

	// Supply-continuity checkpoints never infer provider success from a claim,
	// enqueue, or HTTP acceptance. LastFetchedAt remains a legacy read model;
	// new scheduling is driven exclusively by the explicit evidence-backed
	// fields below.
	LastClaimedAt          *time.Time `gorm:"type:timestamptz" json:"last_claimed_at,omitempty"`
	LastAttemptedAt        *time.Time `gorm:"type:timestamptz" json:"last_attempted_at,omitempty"`
	LastProviderSuccessAt  *time.Time `gorm:"type:timestamptz" json:"last_provider_success_at,omitempty"`
	LastUpstreamObservedAt *time.Time `gorm:"type:timestamptz" json:"last_upstream_observed_at,omitempty"`
	LastNoChangeAt         *time.Time `gorm:"type:timestamptz" json:"last_no_change_at,omitempty"`
	LastNewItemAt          *time.Time `gorm:"type:timestamptz" json:"last_new_item_at,omitempty"`
	LastDeliveryVerifiedAt *time.Time `gorm:"type:timestamptz" json:"last_delivery_verified_at,omitempty"`
	NextDueAt              *time.Time `gorm:"type:timestamptz;index" json:"next_due_at,omitempty"`
	FailureStreak          int        `gorm:"not null;default:0" json:"failure_streak"`
	IntakeCircuitUntil     *time.Time `gorm:"type:timestamptz" json:"intake_circuit_until,omitempty"`
	SourceConfigVersion    int64      `gorm:"not null;default:1" json:"source_config_version"`

	// DiscoveryProfileID links a source to the discovery profile it was
	// approved from, so the News Feeds hub can group active sources by interest.
	// NULL = manually-added / ungrouped.
	DiscoveryProfileID *uint `gorm:"index:idx_content_sources_discovery_profile" json:"discovery_profile_id,omitempty"`

	// Metadata
	Metadata  datatypes.JSON `gorm:"type:jsonb" json:"metadata,omitempty"`
	CreatedAt time.Time      `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time      `gorm:"autoUpdateTime" json:"updated_at"`
}

// TableName returns the table name for ContentSource
func (ContentSource) TableName() string {
	return "content_sources"
}

// EnsureInitialSchedule gives a newly active Media source an explicit first
// poll. Media circulation no longer has a legacy fallback scheduler, and the
// durable source-run scheduler deliberately ignores NULL next_due_at values.
// News remains on its separate circulation path during this cutover.
func (source *ContentSource) EnsureInitialSchedule(now time.Time) {
	if source == nil || !source.IsActive || source.Category != SourceCategoryMedia || source.NextDueAt != nil {
		return
	}
	due := now.UTC()
	source.NextDueAt = &due
}

// BeforeCreate enforces the Media scheduling invariant at every creation
// boundary, including admin creation and approved discovery suggestions.
func (source *ContentSource) BeforeCreate(_ *gorm.DB) error {
	source.EnsureInitialSchedule(time.Now())
	return nil
}
