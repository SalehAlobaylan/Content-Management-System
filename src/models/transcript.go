package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

// Caption/transcript provenance states. Tracked on ContentItem.CaptionState as a
// lightweight, indexed field so the feed + console can filter/badge without
// joining the (large) transcript row. State machine (never downgrades):
//
//	none → youtube_auto → stt_done
//	youtube_human is terminal-good (a human-authored caption beats our STT).
const (
	CaptionStateNone         = "none"          // no transcript/caption yet
	CaptionStateYouTubeAuto  = "youtube_auto"  // YouTube auto-generated caption (weak; upgradeable)
	CaptionStateYouTubeHuman = "youtube_human" // human-uploaded caption (trusted; terminal)
	CaptionStateSTTDone      = "stt_done"      // upgraded by a paid/hosted STT engine
)

// Transcript source values written by producers (Aggregation caption fetch,
// Media-Service STT). Drives ContentItem.TranscriptSource + CaptionState.
const (
	TranscriptSourceYouTubeHuman = "youtube_human"
	TranscriptSourceYouTubeAuto  = "youtube_auto"
	TranscriptSourceSTTDeepgram  = "stt_deepgram"
	TranscriptSourceSTTWhisper   = "stt_whisper"
)

// CaptionStateForSource maps a transcript source to its lightweight ContentItem
// caption_state. Any stt_* source collapses to stt_done.
func CaptionStateForSource(source string) string {
	switch source {
	case TranscriptSourceYouTubeHuman:
		return CaptionStateYouTubeHuman
	case TranscriptSourceYouTubeAuto:
		return CaptionStateYouTubeAuto
	default:
		// stt_deepgram, stt_whisper, or any direct transcript write.
		return CaptionStateSTTDone
	}
}

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

	// Caption-first data (heavy; lives here, not on ContentItem — decoupling).
	// Segments: timestamped [{start,end,text}] — powers subtitles, tap-to-seek,
	// and per-segment embedding chunking.
	// Chapters: [{start,end,title,source}] — native YouTube chapters (source=youtube)
	// or future LLM-derived (source=derived).
	Segments datatypes.JSON `gorm:"type:jsonb" json:"segments,omitempty"`
	Chapters datatypes.JSON `gorm:"type:jsonb" json:"chapters,omitempty"`

	// Provenance: source = youtube_human|youtube_auto|stt_deepgram|stt_whisper
	// (historical); provider = concrete hosted engine name (e.g. "deepgram").
	Source   *string `gorm:"type:varchar(32)" json:"source,omitempty"`
	Provider *string `gorm:"type:varchar(64)" json:"provider,omitempty"`

	// Timestamps
	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
}

// TableName returns the table name for Transcript
func (Transcript) TableName() string {
	return "transcripts"
}
