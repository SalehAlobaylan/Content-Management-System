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
	// ContentTypeNews is the primary news-feed kind. NEWS items carry a Format
	// sub-classification (ARTICLE/TWEET/COMMENT) describing the original content
	// shape. VIDEO/PODCAST are the For-You media kinds.
	ContentTypeNews    ContentType = "NEWS"
	ContentTypeVideo   ContentType = "VIDEO"
	ContentTypePodcast ContentType = "PODCAST"

	// Legacy kinds — retained for the type→format migration mapping and
	// historical references. New NEWS items use Format instead of these.
	ContentTypeArticle ContentType = "ARTICLE"
	ContentTypeTweet   ContentType = "TWEET"
	ContentTypeComment ContentType = "COMMENT"
)

// ContentFormat sub-classifies a NEWS item by its original content shape.
type ContentFormat string

const (
	ContentFormatArticle ContentFormat = "ARTICLE"
	ContentFormatTweet   ContentFormat = "TWEET"
	ContentFormatComment ContentFormat = "COMMENT"
)

// SourceType enum
type SourceType string

const (
	SourceTypeRSS      SourceType = "RSS"
	SourceTypeWebsite  SourceType = "WEBSITE"
	SourceTypeTelegram SourceType = "TELEGRAM"
	SourceTypeTwitter  SourceType = "TWITTER"
	SourceTypePodcast  SourceType = "PODCAST"
	SourceTypeYouTube  SourceType = "YOUTUBE"
	SourceTypeUpload   SourceType = "UPLOAD"
	SourceTypeManual   SourceType = "MANUAL"
)

// Source categories decide which feed/management surface owns a source.
const (
	SourceCategoryNews  = "news"
	SourceCategoryMedia = "media"
)

// DefaultCategoryForType picks a source's category from its type. YOUTUBE and
// PODCAST are media (For You); everything else defaults to news. TELEGRAM is
// genuinely dual — it defaults to news but is meant to be set explicitly.
func DefaultCategoryForType(t SourceType) string {
	switch t {
	case SourceTypeYouTube, SourceTypePodcast:
		return SourceCategoryMedia
	default:
		return SourceCategoryNews
	}
}

// NewsSourceTypes are the source types eligible for the News Feeds hub. NOTE:
// the hub now filters by category='news' (not by this list) so a media-category
// Telegram source stays out — kept for reference / fallbacks.
var NewsSourceTypes = []SourceType{
	SourceTypeRSS,
	SourceTypeWebsite,
	SourceTypeTelegram,
	SourceTypeTwitter,
}

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
	Type ContentType `gorm:"type:varchar(20);not null" json:"type"`
	// Format sub-classifies a NEWS item (ARTICLE/TWEET/COMMENT). NULL for
	// VIDEO/PODCAST media. Populated by the type→format migration + ingest.
	Format *string       `gorm:"type:varchar(20)" json:"format,omitempty"`
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

	// Media atomization/feed-unit lineage. Parent long-form media rows keep
	// IsFeedUnit=false by default; chapter children become the feed units.
	ParentContentItemID          *uuid.UUID     `gorm:"type:uuid;index:idx_content_items_parent" json:"parent_content_item_id,omitempty"`
	IsFeedUnit                   bool           `gorm:"not null;default:true;index:idx_content_items_feed_unit" json:"is_feed_unit"`
	FeedVisibility               string         `gorm:"type:varchar(24);not null;default:'visible';index:idx_content_items_feed_visibility" json:"feed_visibility"`
	ChapterIndex                 *int           `gorm:"type:integer" json:"chapter_index,omitempty"`
	ChapterStartMs               *int           `gorm:"type:integer" json:"chapter_start_ms,omitempty"`
	ChapterEndMs                 *int           `gorm:"type:integer" json:"chapter_end_ms,omitempty"`
	ChapterConfidence            *float64       `gorm:"type:double precision" json:"chapter_confidence,omitempty"`
	ChapteringStatus             *string        `gorm:"type:varchar(32);index:idx_content_items_chaptering_status" json:"chaptering_status,omitempty"`
	DurationBucket               *string        `gorm:"type:varchar(8);index:idx_content_items_duration_bucket" json:"duration_bucket,omitempty"`
	SourceEpisodeID              *string        `gorm:"type:varchar(255)" json:"source_episode_id,omitempty"`
	PlaybackURL                  *string        `gorm:"type:text" json:"playback_url,omitempty"`
	PlaybackType                 *string        `gorm:"type:varchar(16)" json:"playback_type,omitempty"`
	FallbackPlaybackURL          *string        `gorm:"type:text" json:"fallback_playback_url,omitempty"`
	HasVideo                     *bool          `gorm:"type:boolean" json:"has_video,omitempty"`
	MediaRenditions              datatypes.JSON `gorm:"type:jsonb" json:"media_renditions,omitempty"`
	MediaSuitability             string         `gorm:"type:varchar(40);not null;default:'unknown';index:idx_content_items_media_suitability" json:"media_suitability"`
	MediaSuitabilityConfidence   *float64       `gorm:"type:double precision" json:"media_suitability_confidence,omitempty"`
	MediaSuitabilityReasons      datatypes.JSON `gorm:"type:jsonb" json:"media_suitability_reasons,omitempty"`
	MediaSuitabilityReviewedAt   *time.Time     `gorm:"type:timestamp" json:"media_suitability_reviewed_at,omitempty"`
	MediaSuitabilityReviewedBy   *uuid.UUID     `gorm:"type:uuid" json:"media_suitability_reviewed_by,omitempty"`
	AtomizationOverride          *string        `gorm:"type:varchar(16);index:idx_content_items_atomization_override" json:"atomization_override,omitempty"`
	AtomizationOverrideReason    *string        `gorm:"type:text" json:"atomization_override_reason,omitempty"`
	AtomizationOverrideBy        *uuid.UUID     `gorm:"type:uuid" json:"atomization_override_by,omitempty"`
	AtomizationOverrideAt        *time.Time     `gorm:"type:timestamp" json:"atomization_override_at,omitempty"`
	ManualAtomizationRequestedAt *time.Time     `gorm:"type:timestamp" json:"manual_atomization_requested_at,omitempty"`

	// Attribution
	Author        *string `gorm:"type:varchar(255)" json:"author,omitempty"`
	SourceName    *string `gorm:"type:varchar(255)" json:"source_name,omitempty"`
	SourceFeedURL *string `gorm:"type:text" json:"-"`
	// AuthorID points at the IAM user who submitted user-generated content.
	// NULL for ingested content (RSS, scrapes, etc.) — keeps backwards-compat.
	AuthorID *uuid.UUID `gorm:"type:uuid;index:idx_content_items_author_id" json:"author_id,omitempty"`

	// Tags & AI
	TopicTags pq.StringArray `gorm:"type:text[]" json:"topic_tags,omitempty"`
	// TopicID is the first-class topic this article is classified into
	// (Enrichment LLM label + embedding centroid). NULL until classified.
	// Distinct from the legacy free-form TopicTags above.
	TopicID *uuid.UUID `gorm:"type:uuid;index:idx_content_items_topic_id" json:"topic_id,omitempty"`
	// Embedding is the Qwen/Qwen3-Embedding-0.6B dense text vector (1024-dim),
	// populated by Enrichment-Service. Multilingual — strong on Arabic + English.
	// (Replaced BGE-M3; semantic similarity is dense cosine only.)
	Embedding *pgvector.Vector `gorm:"type:vector(1024)" json:"-"`
	// EmbeddingSparse is a dead BGE-M3-era column (250002-dim sparsevec). It was
	// for hybrid retrieval (Slice A) but is unused post-Qwen — always NULL for
	// new content. Retained only so the legacy knn-sparse path still type-checks.
	EmbeddingSparse *pgvector.SparseVector `gorm:"type:sparsevec(250002)" json:"-"`
	// EmbeddingModel records WHICH model produced the current Embedding
	// (e.g. "Qwen/Qwen3-Embedding-0.6B"). Set by the write-back; NULL means the
	// vector has no provenance (written by a pre-provenance service) and the
	// reconcile sweep treats it as missing so it gets re-embedded. This is the
	// guard against silently mixing embedding spaces across model migrations.
	EmbeddingModel *string `gorm:"type:varchar(80)" json:"-"`
	// ImageEmbedding is a CLIP-ViT-B-32 image vector (512-dim), populated by
	// Media-Service when content has a hero image or video thumbnail.
	// Independent from Embedding (text, 1024-dim) — both can coexist.
	ImageEmbedding *pgvector.Vector `gorm:"type:vector(512)" json:"-"`
	Metadata       datatypes.JSON   `gorm:"type:jsonb" json:"metadata,omitempty"`

	// Transcript link (optional)
	TranscriptID *uuid.UUID `gorm:"type:uuid" json:"transcript_id,omitempty"`

	// Caption/transcript provenance — lightweight denormalized state for fast
	// feed filtering + console badges WITHOUT joining the large Transcript row
	// (decoupling). CaptionState drives the never-downgrade state machine
	// (none → youtube_auto → stt_done; youtube_human terminal) and the
	// idempotency/budget guard. TranscriptSource is the concrete source string
	// (youtube_human|youtube_auto|stt_deepgram; stt_whisper is historical).
	// See models.CaptionState*.
	CaptionState     *string `gorm:"type:varchar(20);index:idx_content_items_caption_state" json:"caption_state,omitempty"`
	TranscriptSource *string `gorm:"type:varchar(32)" json:"transcript_source,omitempty"`

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

	// Storage lifecycle state is separate from content/feed Status. It describes
	// the current artifact posture (hot/cold/recoverable_deleted/missing/etc.)
	// without implying a feed visibility change.
	StorageState          string     `gorm:"type:varchar(32);not null;default:'hot';index:idx_content_items_storage_state" json:"storage_state"`
	StorageStateReason    *string    `gorm:"type:text" json:"storage_state_reason,omitempty"`
	StorageRecoveryStatus string     `gorm:"type:varchar(32);not null;default:'recoverable';index:idx_content_items_storage_recovery_status" json:"storage_recovery_status"`
	StorageDeletedAt      *time.Time `gorm:"type:timestamp;index:idx_content_items_storage_deleted_at" json:"storage_deleted_at,omitempty"`
	StorageLastVerifiedAt *time.Time `gorm:"type:timestamp" json:"storage_last_verified_at,omitempty"`

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
