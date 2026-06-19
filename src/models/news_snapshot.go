package models

import (
	"time"

	"gorm.io/datatypes"
)

// NewsSnapshot is a freshness-bounded read-through CACHE of the News-feed
// story-slides for a tenant — never an editorial artifact. The live assembly
// path is the product (PRD: "write-time intelligence, read-time freshness");
// this row only short-circuits the ~100-300ms live query while it is fresh
// (within the SWR TTL) and clean (Dirty=false). Classification marks it dirty
// the moment a story gains a member, so the cache is never older than the last
// news event. Single row per tenant + window.
type NewsSnapshot struct {
	ID         uint           `gorm:"primaryKey" json:"-"`
	TenantID   string         `gorm:"type:varchar(64);not null;uniqueIndex:idx_news_snapshot_tenant_window,priority:1" json:"tenant_id"`
	Window     string         `gorm:"type:varchar(16);not null;default:'today';uniqueIndex:idx_news_snapshot_tenant_window,priority:2" json:"window"`
	Slides     datatypes.JSON `gorm:"type:jsonb" json:"slides"`
	SlideCount int            `gorm:"default:0" json:"slide_count"`
	// Dirty marks the cache invalid ahead of its TTL — set when a new item is
	// classified into a story (event-driven invalidation). Reads then assemble
	// live and refresh the cache in the background.
	Dirty bool `gorm:"default:false" json:"dirty"`
	// BuiltAt is set EXPLICITLY by buildNewsSnapshot (µs-truncated so the
	// in-process copy compares Equal to what Postgres returns). No
	// autoUpdateTime — GORM would overwrite the explicit value on upsert and
	// the memory cache's built_at would never match the DB header.
	BuiltAt time.Time `json:"built_at"`
}

func (NewsSnapshot) TableName() string {
	return "news_snapshots"
}
