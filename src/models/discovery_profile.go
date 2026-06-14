package models

import (
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/pgvector/pgvector-go"
)

// DiscoveryProfile is an admin-defined interest (e.g. "Saudi Economy") that
// drives auto source-discovery. A discovery sweep searches the open web for
// new sources matching the profile's keywords/languages and files them as
// SourceSuggestions for admin review.
type DiscoveryProfile struct {
	ID       uint      `gorm:"primaryKey" json:"-"`
	PublicID uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex:idx_discovery_profiles_public_id" json:"id"`
	TenantID string    `gorm:"type:varchar(64);not null;default:default;index:idx_discovery_profiles_tenant" json:"tenant_id"`

	Name        string         `gorm:"type:varchar(255);not null" json:"name"`
	Description string         `gorm:"type:text" json:"description"`
	Keywords    pq.StringArray `gorm:"type:text[]" json:"keywords"`
	Languages   pq.StringArray `gorm:"type:text[]" json:"languages"`

	Enabled              bool       `gorm:"default:true" json:"enabled"`
	MaxSuggestionsPerRun int        `gorm:"default:10" json:"max_suggestions_per_run"`
	LastRunAt            *time.Time `gorm:"type:timestamp" json:"last_run_at,omitempty"`

	// Embedding is the cached Qwen vector of (name + description + keywords),
	// used to score candidate relevance. Cleared on profile edit to recompute.
	Embedding *pgvector.Vector `gorm:"type:vector(1024)" json:"-"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

// TableName returns the table name for DiscoveryProfile.
func (DiscoveryProfile) TableName() string {
	return "discovery_profiles"
}
