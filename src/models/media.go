package models

import (
	"time"

	"github.com/google/uuid"
)

type Media struct {
	ID        uint      `gorm:"primaryKey" json:"-"`
	PublicID  uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex:idx_media_public_id" json:"id"`
	URL       string    `gorm:"size:255;not null" json:"url" binding:"required"`
	Type      string    `gorm:"size:50" json:"type"`
	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
	Post      []Post    `gorm:"many2many:post_media" json:"posts,omitempty"`
}
