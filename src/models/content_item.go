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
	SourceTypeRSS      SourceType = "RSS"
	SourceTypeWebsite  SourceType = "WEBSITE"
	SourceTypeTelegram SourceType = "TELEGRAM"
	SourceTypePodcast  SourceType = "PODCAST"
	SourceTypeYouTube  SourceType = "YOUTUBE"
	SourceTypeUpload   SourceType = "UPLOAD"
	SourceTypeManual   SourceType = "MANUAL"
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
	Author        *string    `gorm:"type:varchar(255)" json:"author,omitempty"`
	SourceName    *string    `gorm:"type:varchar(255)" json:"source_name,omitempty"`
	SourceFeedURL *string    `gorm:"type:text" json:"-"`
	// AuthorID points at the IAM user who submitted user-generated content.
	// NULL for ingested content (RSS, scrapes, etc.) — keeps backwards-compat.
	AuthorID *uuid.UUID `gorm:"type:uuid;index:idx_content_items_author_id" json:"author_id,omitempty"`

	// Tags & AI
	TopicTags pq.StringArray `gorm:"type:text[]" json:"topic_tags,omitempty"`
	// TopicID is the first-class topic this article is classified into
	// (Enrichment LLM label + embedding centroid). NULL until classified.
	// Distinct from the legacy free-form TopicTags above.
	TopicID *uuid.UUID `gorm:"type:uuid;index:idx_content_items_topic_id" json:"topic_id,omitempty"`
	// Embedding is the BAAI/bge-m3 dense text vector (1024-dim), populated by
	// Enrichment-Service. Multilingual — performs well on Arabic + English.
	Embedding *pgvector.Vector `gorm:"type:vector(1024)" json:"-"`
	// EmbeddingSparse is BGE-M3's learned sparse output (250002-dim sparsevec),
	// added for forward compatibility with hybrid retrieval (Slice A). Stays
	// NULL until Slice A wires FlagEmbedding into the embedder.
	EmbeddingSparse *pgvector.SparseVector `gorm:"type:sparsevec(250002)" json:"-"`
	// ImageEmbedding is a CLIP-ViT-B-32 image vector (512-dim), populated by
	// Media-Service when content has a hero image or video thumbnail.
	// Independent from Embedding (text, 1024-dim) — both can coexist.
	ImageEmbedding *pgvector.Vector `gorm:"type:vector(512)" json:"-"`
	Metadata       datatypes.JSON   `gorm:"type:jsonb" json:"metadata,omitempty"`

	// Transcript link (optional)
	TranscriptID *uuid.UUID `gorm:"type:uuid" json:"transcript_id,omitempty"`

	// Engagement counters
	LikeCount    int `gorm:"default:0" json:"like_count"`
	CommentCount int `gorm:"default:0" json:"comment_count"`
	ShareCount   int `gorm:"default:0" json:"share_count"`
	ViewCount    int `gorm:"default:0" json:"view_count"`

	// Storage accounting (set by Aggregation on upload, cleared by storage circulation)
	FileSizeBytes    int64      `gorm:"type:bigint;default:0" json:"file_size_bytes"`
	ArchivedAt       *time.Time `gorm:"type:timestamp" json:"archived_at,omitempty"`
	LastStorageCheck *time.Time `gorm:"type:timestamp" json:"last_storage_check,omitempty"`
	LastRestoredAt   *time.Time `gorm:"type:timestamp" json:"last_restored_at,omitempty"`

	// Storage tier — which configured S3 backend currently holds the artifacts.
	// NULL = primary (default). 'cold' = moved to the secondary bucket.
	StorageTier *string `gorm:"type:varchar(16)" json:"storage_tier,omitempty"`

	// Quality accounting (set by Aggregation on first ingest, updated on re-encode).
	// CurrentQualityProfileID points at quality_profiles.id; NULL = unknown / never re-encoded.
	// OriginalSizeBytes/OriginalBitrateKbps capture the as-ingested values exactly once.
	// CurrentBitrateKbps tracks the current encode after any re-encode passes.
	CurrentQualityProfileID *uint  `gorm:"index" json:"current_quality_profile_id,omitempty"`
	OriginalSizeBytes       *int64 `gorm:"type:bigint" json:"original_size_bytes,omitempty"`
	OriginalBitrateKbps     *int   `gorm:"type:int" json:"original_bitrate_kbps,omitempty"`
	CurrentBitrateKbps      *int   `gorm:"type:int" json:"current_bitrate_kbps,omitempty"`

	// MediaVersion increments on each re-encode so the worker can derive the next
	// versioned key (`content/{id}/processed.v{N}.mp4`) without a S3 LIST.
	MediaVersion int `gorm:"default:1" json:"media_version"`

	// Timestamps
	PublishedAt *time.Time `gorm:"type:timestamp" json:"published_at,omitempty"`
	CreatedAt   time.Time  `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt   time.Time  `gorm:"autoUpdateTime" json:"updated_at"`
}

// TableName returns the table name for ContentItem
func (ContentItem) TableName() string {
	return "content_items"
}
