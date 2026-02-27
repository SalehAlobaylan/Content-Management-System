package models

import (
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/pgvector/pgvector-go"
	"gorm.io/datatypes"
)

// ContentType enum
type ContentType string

const (
	ContentTypeArticle ContentType = "ARTICLE"
	ContentTypeVideo   ContentType = "VIDEO"
	ContentTypeTweet   ContentType = "TWEET"
	ContentTypeComment ContentType = "COMMENT"
	ContentTypePodcast ContentType = "PODCAST"
)

// SourceType enum
type SourceType string

const (
	SourceTypeRSS     SourceType = "RSS"
	SourceTypeWebsite SourceType = "WEBSITE"
	SourceTypePodcast SourceType = "PODCAST"
	SourceTypeYouTube SourceType = "YOUTUBE"
	SourceTypeUpload  SourceType = "UPLOAD"
	SourceTypeManual  SourceType = "MANUAL"
)

// ContentStatus enum
type ContentStatus string

const (
	ContentStatusPending    ContentStatus = "PENDING"
	ContentStatusProcessing ContentStatus = "PROCESSING"
	ContentStatusReady      ContentStatus = "READY"
	ContentStatusFailed     ContentStatus = "FAILED"
	ContentStatusArchived   ContentStatus = "ARCHIVED"
)

// ContentItem represents all content types in the Wahb platform feeds
type ContentItem struct {
	ID       uint      `gorm:"primaryKey" json:"-"`
	PublicID uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex:idx_content_items_public_id" json:"id"`
	TenantID string    `gorm:"type:varchar(64);not null;default:default;index:idx_content_items_tenant_id" json:"tenant_id"`

	// Classification
	Type   ContentType   `gorm:"type:varchar(20);not null" json:"type"`
	Source SourceType    `gorm:"type:varchar(20);not null" json:"source,omitempty"`
	Status ContentStatus `gorm:"type:varchar(20);default:'READY'" json:"status,omitempty"`
	// Idempotency
	IdempotencyKey *string `gorm:"type:varchar(512);uniqueIndex:idx_content_items_idempotency_key" json:"-"`

	// Content
	Title    *string `gorm:"type:text" json:"title,omitempty"`
	BodyText *string `gorm:"type:text" json:"body_text,omitempty"`
	Excerpt  *string `gorm:"type:text" json:"excerpt,omitempty"`

	// Media
	MediaURL     *string `gorm:"type:text" json:"media_url,omitempty"`
	ThumbnailURL *string `gorm:"type:text" json:"thumbnail_url,omitempty"`
	OriginalURL  *string `gorm:"type:text" json:"original_url,omitempty"`
	DurationSec  *int    `gorm:"type:integer" json:"duration_sec,omitempty"`

	// Attribution
	Author        *string `gorm:"type:varchar(255)" json:"author,omitempty"`
	SourceName    *string `gorm:"type:varchar(255)" json:"source_name,omitempty"`
	SourceFeedURL *string `gorm:"type:text" json:"-"`

	// Tags & AI
	TopicTags pq.StringArray   `gorm:"type:text[]" json:"topic_tags,omitempty"`
	Embedding *pgvector.Vector `gorm:"type:vector(384)" json:"-"`
	Metadata  datatypes.JSON   `gorm:"type:jsonb" json:"metadata,omitempty"`

	// Transcript link (optional)
	TranscriptID *uuid.UUID `gorm:"type:uuid" json:"transcript_id,omitempty"`

	// Engagement counters
	LikeCount    int `gorm:"default:0" json:"like_count"`
	CommentCount int `gorm:"default:0" json:"comment_count"`
	ShareCount   int `gorm:"default:0" json:"share_count"`
	ViewCount    int `gorm:"default:0" json:"view_count"`

	// Timestamps
	PublishedAt *time.Time `gorm:"type:timestamp" json:"published_at,omitempty"`
	CreatedAt   time.Time  `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt   time.Time  `gorm:"autoUpdateTime" json:"updated_at"`
}

// TableName returns the table name for ContentItem
func (ContentItem) TableName() string {
	return "content_items"
}
