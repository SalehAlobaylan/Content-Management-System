package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

// ContentSource represents a content ingestion source configuration
type ContentSource struct {
	ID       uint      `gorm:"primaryKey" json:"-"`
	PublicID uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`

	// Source identification
	Name string     `gorm:"type:varchar(255);not null" json:"name"`
	Type SourceType `gorm:"type:varchar(20);not null" json:"type"`

	// Configuration
	FeedURL   *string        `gorm:"type:text" json:"feed_url,omitempty"`
	APIConfig datatypes.JSON `gorm:"type:jsonb" json:"api_config,omitempty"`

	// Status
	IsActive             bool       `gorm:"default:true" json:"is_active"`
	FetchIntervalMinutes int        `gorm:"default:60" json:"fetch_interval_minutes"`
	LastFetchedAt        *time.Time `gorm:"type:timestamp" json:"last_fetched_at,omitempty"`

	// Metadata
	Metadata  datatypes.JSON `gorm:"type:jsonb" json:"metadata,omitempty"`
	CreatedAt time.Time      `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time      `gorm:"autoUpdateTime" json:"updated_at"`
}

// TableName returns the table name for ContentSource
func (ContentSource) TableName() string {
	return "content_sources"
}
