package models

import (
	"time"

	"github.com/google/uuid"
)

type Page struct {
	ID        uint      `gorm:"primaryKey" json:"-"`
	PublicID  uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex:idx_pages_public_id" json:"id"`
	Title     string    `gorm:"size:255;not null" json:"title" binding:"required"`
	Content   string    `gorm:"type:text;not null" json:"content" binding:"required"`
	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}
