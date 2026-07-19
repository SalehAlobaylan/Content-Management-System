package models

import (
	"time"

	"github.com/google/uuid"
)

const (
	ModerationTargetContent = "content"
	ModerationTargetComment = "comment"
)

// ModerationReport is an immutable user report. Moderators own its status;
// consumers can create reports but cannot inspect another reporter's history.
type ModerationReport struct {
	ID         uint      `gorm:"primaryKey" json:"-"`
	PublicID   uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID   string    `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	ReporterID uuid.UUID `gorm:"type:uuid;not null;index" json:"-"`
	TargetType string    `gorm:"type:varchar(16);not null;index" json:"target_type"`
	TargetID   uuid.UUID `gorm:"type:uuid;not null;index" json:"target_id"`
	Reason     string    `gorm:"type:varchar(64);not null" json:"reason"`
	Detail     *string   `gorm:"type:text" json:"detail,omitempty"`
	Status     string    `gorm:"type:varchar(24);not null;default:open;index" json:"status"`
	CreatedAt  time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt  time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (ModerationReport) TableName() string { return "moderation_reports" }

// UserBlock hides a blocked author's comments for the blocking account only.
// It is not an enforcement or ban mechanism.
type UserBlock struct {
	ID            uint      `gorm:"primaryKey" json:"-"`
	TenantID      string    `gorm:"type:varchar(64);not null;uniqueIndex:idx_user_blocks_identity" json:"-"`
	UserID        uuid.UUID `gorm:"type:uuid;not null;uniqueIndex:idx_user_blocks_identity" json:"-"`
	BlockedUserID uuid.UUID `gorm:"type:uuid;not null;uniqueIndex:idx_user_blocks_identity" json:"-"`
	CreatedAt     time.Time `gorm:"autoCreateTime" json:"created_at"`
}

func (UserBlock) TableName() string { return "user_blocks" }

// ConsumerModerationIdempotency prevents mobile retries from producing more
// than one moderation report for a request key and authenticated identity.
type ConsumerModerationIdempotency struct {
	ReporterID     uuid.UUID `gorm:"type:uuid;primaryKey"`
	Endpoint       string    `gorm:"type:varchar(120);primaryKey"`
	IdempotencyKey string    `gorm:"type:varchar(160);primaryKey"`
	RequestDigest  string    `gorm:"type:char(64);not null"`
	ReportID       uuid.UUID `gorm:"type:uuid;not null"`
	CreatedAt      time.Time `gorm:"autoCreateTime"`
}

func (ConsumerModerationIdempotency) TableName() string {
	return "consumer_moderation_idempotency"
}
