package models

import (
	"time"

	"github.com/google/uuid"
)

// RSSFeed is a saved, named syndication feed an admin defines in the News
// manager. It captures the filters (topic, type, item count) + presentation
// (title, description) and is served publicly at a stable slug in RSS/Atom/JSON.
type RSSFeed struct {
	ID       uint      `gorm:"primaryKey" json:"-"`
	PublicID uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex:idx_rss_feeds_public_id" json:"id"`
	TenantID string    `gorm:"type:varchar(64);not null;index:idx_rss_feeds_tenant;uniqueIndex:idx_rss_feeds_tenant_slug,priority:1" json:"tenant_id"`

	// Slug is the stable public identifier in the feed URL (/feed/saved/:slug).
	Slug        string `gorm:"type:varchar(160);not null;uniqueIndex:idx_rss_feeds_tenant_slug,priority:2" json:"slug"`
	Name        string `gorm:"type:varchar(200);not null" json:"name"`
	Title       string `gorm:"type:text" json:"title"`
	Description string `gorm:"type:text" json:"description"`

	// Filters. TopicID NULL = all topics; ContentType "" = all article types.
	TopicID     *uuid.UUID `gorm:"type:uuid;index:idx_rss_feeds_topic_id" json:"topic_id,omitempty"`
	ContentType string     `gorm:"type:varchar(20)" json:"content_type"`
	ItemLimit   int        `gorm:"default:50" json:"item_limit"`

	Enabled bool `gorm:"default:true" json:"enabled"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (RSSFeed) TableName() string {
	return "rss_feeds"
}
