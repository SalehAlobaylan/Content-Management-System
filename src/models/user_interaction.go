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
	// InteractionTypeComment stores the comment body in Metadata:
	// {"text": "...", "author": "display name (optional)"}
	InteractionTypeComment InteractionType = "comment"
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

// ConsumerRequestIdempotency stores the result identity for a replayable
// consumer mutation. It is deliberately separate from user_interactions: a
// client event can be replayed exactly once even for non-toggle interactions
// such as view, share, and complete.
type ConsumerRequestIdempotency struct {
	IdentityScope  string    `gorm:"column:identity_scope;primaryKey;type:varchar(320)"`
	Endpoint       string    `gorm:"column:endpoint;primaryKey;type:varchar(120)"`
	IdempotencyKey string    `gorm:"column:idempotency_key;primaryKey;type:varchar(160)"`
	RequestDigest  string    `gorm:"column:request_digest;type:char(64);not null"`
	InteractionID  uuid.UUID `gorm:"column:interaction_public_id;type:uuid;not null"`
	CreatedAt      time.Time `gorm:"column:created_at;autoCreateTime"`
}

func (ConsumerRequestIdempotency) TableName() string {
	return "consumer_request_idempotency"
}

// ConsumerFeedSession is a short-lived, server-owned frozen For You ordering.
// The snapshot is intentionally a CMS concern; mobile stores a local recovery
// ledger but must not invent ranking continuity by itself.
type ConsumerFeedSession struct {
	ID            uuid.UUID      `gorm:"column:id;type:uuid;primaryKey"`
	IdentityScope string         `gorm:"column:identity_scope;type:varchar(320);not null;index"`
	FeedType      string         `gorm:"column:feed_type;type:varchar(24);not null"`
	Snapshot      datatypes.JSON `gorm:"column:snapshot_json;type:jsonb;not null"`
	ExpiresAt     time.Time      `gorm:"column:expires_at;not null;index"`
	CreatedAt     time.Time      `gorm:"column:created_at;autoCreateTime"`
}

func (ConsumerFeedSession) TableName() string {
	return "consumer_feed_sessions"
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
	// Deprecated: ignored by the server. The acting user's identity is derived
	// from the verified JWT (Authorization: Bearer …), never from this field.
	// Kept only so older clients that still send it do not fail binding.
	UserID   *string        `json:"user_id,omitempty"`
	Metadata datatypes.JSON `json:"metadata,omitempty"`
}
