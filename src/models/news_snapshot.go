package models

import (
	"time"

	"gorm.io/datatypes"
)

// NewsSnapshot is the precomputed News-feed story-slides for a tenant, served
// off the read path when RankingConfig.NewsFeedMode = "precompute". Rebuilt by
// the admin "Refresh" endpoint (or an external cron hitting it) and lazily on
// first read if empty. Single row per tenant.
type NewsSnapshot struct {
	ID         uint           `gorm:"primaryKey" json:"-"`
	TenantID   string         `gorm:"type:varchar(64);not null;uniqueIndex:idx_news_snapshot_tenant" json:"tenant_id"`
	Slides     datatypes.JSON `gorm:"type:jsonb" json:"slides"`
	SlideCount int            `gorm:"default:0" json:"slide_count"`
	BuiltAt    time.Time      `gorm:"autoUpdateTime" json:"built_at"`
}

func (NewsSnapshot) TableName() string {
	return "news_snapshots"
}
