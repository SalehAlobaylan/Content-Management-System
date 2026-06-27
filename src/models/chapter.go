package models

import (
	"time"

	"github.com/google/uuid"
)

// Chapter source provenance.
const (
	ChapterSourceYouTube = "youtube" // native YouTube chapters (lazy-seeded from transcript jsonb)
	ChapterSourceDerived = "derived" // LLM-generated from the transcript
	ChapterSourceManual  = "manual"  // hand-created/edited in the Media Studio
)

// Chapter is a first-class, editable chapter marker for a transcript. Chapters
// are contiguous and ordered by StartMs; the END of a chapter is DERIVED (the
// next chapter's StartMs, or the media duration for the last one) — so the set
// is always gapless and duration-covering, with no overlap bugs to maintain.
type Chapter struct {
	ID       uint      `gorm:"primaryKey" json:"-"`
	PublicID uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex:idx_chapters_public_id" json:"id"`

	TranscriptID uuid.UUID `gorm:"type:uuid;not null;index:idx_chapters_transcript" json:"transcript_id"`
	TenantID     string    `gorm:"type:varchar(64);not null;index:idx_chapters_tenant" json:"tenant_id"`

	Title   string  `gorm:"type:text;not null" json:"title"`
	Summary *string `gorm:"type:text" json:"summary,omitempty"`
	StartMs int     `gorm:"type:integer;not null" json:"start_ms"`
	EndMs   *int    `gorm:"type:integer" json:"end_ms,omitempty"`

	// youtube | derived | manual
	Source string `gorm:"type:varchar(16);not null;default:'manual'" json:"source"`

	// Atomization/review metadata. Nullable for legacy/editor-created markers.
	Status               string     `gorm:"type:varchar(24);not null;default:'draft';index:idx_chapters_status" json:"status"`
	Confidence           *float64   `gorm:"type:double precision" json:"confidence,omitempty"`
	ContextLabel         *string    `gorm:"type:text" json:"context_label,omitempty"`
	BoundaryReason       *string    `gorm:"type:text" json:"boundary_reason,omitempty"`
	StandaloneScore      *float64   `gorm:"type:double precision" json:"standalone_score,omitempty"`
	ContainsSponsorIntro bool       `gorm:"not null;default:false" json:"contains_sponsor_intro"`
	NeedsReviewReason    *string    `gorm:"type:text" json:"needs_review_reason,omitempty"`
	DurationBucket       *string    `gorm:"type:varchar(8)" json:"duration_bucket,omitempty"`
	ChildContentItemID   *uuid.UUID `gorm:"type:uuid;index:idx_chapters_child_content" json:"child_content_item_id,omitempty"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (Chapter) TableName() string {
	return "chapters"
}
