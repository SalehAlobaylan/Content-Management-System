package models

import (
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"time"
)

type NewsFeedStoryProjection struct {
	TenantID     string
	GenerationID uuid.UUID      `gorm:"type:uuid;primaryKey"`
	StoryID      uuid.UUID      `gorm:"type:uuid;primaryKey"`
	Projection   datatypes.JSON `gorm:"type:jsonb"`
	MemberHash   string
	UpdatedAt    time.Time
}

func (NewsFeedStoryProjection) TableName() string { return "news_feed_story_projections" }
