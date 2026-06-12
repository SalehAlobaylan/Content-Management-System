package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

type TranscriptVersion struct {
	ID       uint      `gorm:"primaryKey" json:"-"`
	PublicID uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex:idx_transcript_versions_public_id" json:"id"`
	TenantID string    `gorm:"type:varchar(64);not null;index:idx_transcript_versions_tenant" json:"tenant_id"`

	ContentItemID uuid.UUID `gorm:"type:uuid;not null;index:idx_transcript_versions_content" json:"content_item_id"`
	TranscriptID  uuid.UUID `gorm:"type:uuid;not null;index:idx_transcript_versions_transcript" json:"transcript_id"`

	FullText       string         `gorm:"type:text;not null" json:"full_text"`
	Summary        *string        `gorm:"type:text" json:"summary,omitempty"`
	WordTimestamps datatypes.JSON `gorm:"type:jsonb" json:"word_timestamps,omitempty"`
	Segments       datatypes.JSON `gorm:"type:jsonb" json:"segments,omitempty"`
	Chapters       datatypes.JSON `gorm:"type:jsonb" json:"chapters,omitempty"`
	Language       *string        `gorm:"type:varchar(10)" json:"language,omitempty"`
	Source         *string        `gorm:"type:varchar(32)" json:"source,omitempty"`
	Provider       *string        `gorm:"type:varchar(64)" json:"provider,omitempty"`
	ApprovedAt     *time.Time     `gorm:"type:timestamp" json:"approved_at,omitempty"`
	ApprovedBy     *string        `gorm:"type:varchar(255)" json:"approved_by,omitempty"`
	ApprovalReason *string        `gorm:"type:text" json:"approval_reason,omitempty"`

	Checksum              string `gorm:"type:varchar(64);not null;index:idx_transcript_versions_checksum" json:"checksum"`
	Reason                string `gorm:"type:varchar(64);not null;default:'stt_replacement'" json:"reason"`
	Actor                 string `gorm:"type:varchar(255)" json:"actor,omitempty"`
	EmbeddingsRegenerated bool   `gorm:"default:false" json:"embeddings_regenerated"`

	CreatedAt time.Time `gorm:"autoCreateTime;index:idx_transcript_versions_created" json:"created_at"`
}

func (TranscriptVersion) TableName() string {
	return "transcript_versions"
}
