package models

import (
	"github.com/google/uuid"
	"time"
)

// Re-observing an unchanged campaign instance authorizes a read reference,
// never another reconstruction or a write through the original grant token.
type ContentResetReplayReuse struct {
	ID                 uint      `gorm:"primaryKey"`
	PublicID           uuid.UUID `gorm:"type:uuid;default:gen_random_uuid()"`
	TenantID           string
	CampaignID         uint
	RevisionID         uint
	GrantID            *uint
	ObservationID      uuid.UUID `gorm:"type:uuid"`
	SourceRunRequestID uuid.UUID `gorm:"type:uuid"`
	ExecutionUnitID    uuid.UUID `gorm:"type:uuid"`
	ContentItemID      uuid.UUID `gorm:"type:uuid"`
	InstanceGeneration int
	Fingerprint        string
	CreatedAt          time.Time
}

func (ContentResetReplayReuse) TableName() string { return "content_reset_replay_reuses" }
