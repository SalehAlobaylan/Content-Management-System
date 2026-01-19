package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

// Transcript stores transcriptions for audio/video content
type Transcript struct {
	ID       uint      `gorm:"primaryKey" json:"-"`
	PublicID uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`

	// Association
	ContentItemID uuid.UUID `gorm:"type:uuid;not null;index" json:"content_item_id"`

	// Content
	FullText       string         `gorm:"type:text;not null" json:"full_text"`
	Summary        *string        `gorm:"type:text" json:"summary,omitempty"`
	WordTimestamps datatypes.JSON `gorm:"type:jsonb" json:"word_timestamps,omitempty"`
	Language       *string        `gorm:"type:varchar(10)" json:"language,omitempty"`

	// Timestamps
	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
}

// TableName returns the table name for Transcript
func (Transcript) TableName() string {
	return "transcripts"
}
