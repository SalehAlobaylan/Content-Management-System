package models

import (
	"time"

	"github.com/google/uuid"
)

type Post struct {
	ID        uint      `gorm:"primaryKey" json:"-"`
	PublicID  uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	Title     string    `gorm:"size:255;not null" json:"title" binding:"required"`
	Content   string    `gorm:"type:text;not null" json:"content" binding:"required"`
	Author    string    `gorm:"size:255;not null" json:"author" binding:"required"`
	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
	Media     []Media   `gorm:"many2many:post_media" json:"media,omitempty"`
}
