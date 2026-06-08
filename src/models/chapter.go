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

	// youtube | derived | manual
	Source string `gorm:"type:varchar(16);not null;default:'manual'" json:"source"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (Chapter) TableName() string {
	return "chapters"
}
