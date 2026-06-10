package models

import (
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"gorm.io/datatypes"
)

const (
	TranscriptQualityOK          = "ok"
	TranscriptQualityNeedsReview = "needs_review"
	TranscriptQualityAutoRepair  = "auto_repair"
)

type TranscriptQuality struct {
	ID       uint      `gorm:"primaryKey" json:"-"`
	PublicID uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex:idx_transcript_quality_public_id" json:"id"`
	TenantID string    `gorm:"type:varchar(64);not null;index:idx_transcript_quality_tenant" json:"tenant_id"`

	ContentItemID uuid.UUID `gorm:"type:uuid;not null;uniqueIndex:idx_transcript_quality_content" json:"content_item_id"`
	TranscriptID  uuid.UUID `gorm:"type:uuid;not null;index:idx_transcript_quality_transcript" json:"transcript_id"`

	Score      float64        `gorm:"type:double precision;not null;default:1" json:"score"`
	Status     string         `gorm:"type:varchar(24);not null;index:idx_transcript_quality_status" json:"status"`
	IssueCodes pq.StringArray `gorm:"type:text[]" json:"issue_codes,omitempty"`
	Details    datatypes.JSON `gorm:"type:jsonb" json:"details,omitempty"`

	ComputedAt time.Time `gorm:"type:timestamp;index:idx_transcript_quality_computed" json:"computed_at"`
	CreatedAt  time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt  time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (TranscriptQuality) TableName() string {
	return "transcript_quality"
}
