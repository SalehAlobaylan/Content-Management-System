package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

// SourceSuggestion statuses.
const (
	SuggestionStatusPending  = "PENDING"
	SuggestionStatusApproved = "APPROVED"
	SuggestionStatusRejected = "REJECTED"
)

// SourceSuggestion is a candidate news source discovered by a sweep, awaiting
// admin review. Approving one creates a ContentSource; rejecting keeps the row
// so the unique (tenant_id, canonical_key) suppresses re-suggestion.
type SourceSuggestion struct {
	ID       uint      `gorm:"primaryKey" json:"-"`
	PublicID uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex:idx_source_suggestions_public_id" json:"id"`
	TenantID string    `gorm:"type:varchar(64);not null;default:default;index:idx_source_suggestions_tenant_status,priority:1;uniqueIndex:idx_ss_tenant_profile_canonical,priority:1" json:"tenant_id"`

	// ProfileID is the discovery profile this candidate was found for (nullable).
	// Part of the dedupe key: the same feed can be a suggestion for several
	// profiles, each scored against its own interest.
	ProfileID *uint `gorm:"index;uniqueIndex:idx_ss_tenant_profile_canonical,priority:2" json:"-"`

	Name     string     `gorm:"type:varchar(255);not null" json:"name"`
	Type     SourceType `gorm:"type:varchar(20);not null" json:"type"`
	FeedURL  string     `gorm:"type:text;not null" json:"feed_url"`
	SiteURL  *string    `gorm:"type:text" json:"site_url,omitempty"`
	ImageURL *string    `gorm:"type:text" json:"image_url,omitempty"`
	Language *string    `gorm:"type:varchar(16)" json:"language,omitempty"`

	// CanonicalKey is the canonicalized feed URL used for dedupe. Unique per
	// tenant so re-runs and rejected rows are never re-suggested.
	CanonicalKey string `gorm:"type:text;not null;uniqueIndex:idx_ss_tenant_profile_canonical,priority:3" json:"canonical_key"`

	Confidence     float64  `gorm:"default:0" json:"confidence"`
	RelevanceScore *float64 `gorm:"type:double precision" json:"relevance_score,omitempty"`

	// Health: {items_count, last_item_at, parse_ok}. SampleItems: [{title,url,published_at}].
	Health      datatypes.JSON `gorm:"type:jsonb" json:"health,omitempty"`
	SampleItems datatypes.JSON `gorm:"type:jsonb" json:"sample_items,omitempty"`

	DiscoveredVia string `gorm:"type:varchar(20)" json:"discovered_via,omitempty"`

	// Category ('news' | 'media') — defaulted by type at ingest, carried to the
	// ContentSource on approve.
	Category string `gorm:"type:varchar(16);not null;default:news" json:"category"`

	Status           string  `gorm:"type:varchar(20);not null;default:PENDING;index:idx_source_suggestions_tenant_status,priority:2" json:"status"`
	RejectReason     *string `gorm:"type:text" json:"reject_reason,omitempty"`
	ApprovedSourceID *uint   `gorm:"index" json:"-"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

// TableName returns the table name for SourceSuggestion.
func (SourceSuggestion) TableName() string {
	return "source_suggestions"
}
