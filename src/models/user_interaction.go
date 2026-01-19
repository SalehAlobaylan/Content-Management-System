package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

// InteractionType enum for user interactions
type InteractionType string

const (
	InteractionTypeLike     InteractionType = "like"
	InteractionTypeBookmark InteractionType = "bookmark"
	InteractionTypeShare    InteractionType = "share"
	InteractionTypeView     InteractionType = "view"
	InteractionTypeComplete InteractionType = "complete"
)

// UserInteraction tracks user interactions with content items
type UserInteraction struct {
	ID       uint      `gorm:"primaryKey" json:"-"`
	PublicID uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`

	// User identification (either UserID or SessionID)
	UserID    *uuid.UUID `gorm:"type:uuid;index" json:"user_id,omitempty"`
	SessionID *string    `gorm:"type:varchar(255);index" json:"session_id,omitempty"`

	// Content reference
	ContentItemID uuid.UUID `gorm:"type:uuid;not null;index" json:"content_item_id"`

	// Interaction details
	Type     InteractionType `gorm:"type:varchar(50);not null" json:"interaction_type"`
	Metadata datatypes.JSON  `gorm:"type:jsonb" json:"metadata,omitempty"`

	// Timestamp
	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
}

// TableName returns the table name for UserInteraction
func (UserInteraction) TableName() string {
	return "user_interactions"
}

// CreateInteractionRequest is the request body for creating an interaction
type CreateInteractionRequest struct {
	ContentItemID   string          `json:"content_item_id" binding:"required"`
	InteractionType InteractionType `json:"interaction_type" binding:"required"`
	SessionID       *string         `json:"session_id,omitempty"`
	UserID          *string         `json:"user_id,omitempty"`
	Metadata        datatypes.JSON  `json:"metadata,omitempty"`
}
