package models

import (
	"time"
)

type Media struct {
	ID uint `gorm:"primaryKey" json:"id"`
	URL string `gorm:"size:255;not null" json:"url" binding:"required"`
	Type string `gorm:"size:50" json:"type"`
	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
	Post []Post `gorm:"many2many:post_media"`  // (slice of Media, representing a many-to-many relationship)
}