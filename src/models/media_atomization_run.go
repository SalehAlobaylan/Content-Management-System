package models

import (
	"time"

	"github.com/google/uuid"
)

// MediaAtomizationRun records the operational state of one parent atomization
// attempt so admin dashboards can show phase, failures, and throughput.
type MediaAtomizationRun struct {
	ID       uint      `gorm:"primaryKey" json:"-"`
	PublicID uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex:idx_media_atomization_runs_public_id" json:"id"`
	TenantID string    `gorm:"type:varchar(64);not null;index:idx_media_atomization_runs_tenant" json:"tenant_id"`

	ParentContentItemID uuid.UUID `gorm:"type:uuid;not null;index:idx_media_atomization_runs_parent" json:"parent_content_item_id"`
	Status              string    `gorm:"type:varchar(24);not null;index:idx_media_atomization_runs_status" json:"status"`
	Phase               string    `gorm:"type:varchar(32);not null;index:idx_media_atomization_runs_phase" json:"phase"`
	ChildCount          int       `gorm:"not null;default:0" json:"child_count"`
	ReviewCount         int       `gorm:"not null;default:0" json:"review_count"`
	ErrorMessage        *string   `gorm:"type:text" json:"error_message,omitempty"`

	StartedAt   *time.Time `gorm:"type:timestamp;index:idx_media_atomization_runs_started_at" json:"started_at,omitempty"`
	CompletedAt *time.Time `gorm:"type:timestamp" json:"completed_at,omitempty"`
	CreatedAt   time.Time  `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt   time.Time  `gorm:"autoUpdateTime" json:"updated_at"`
}

func (MediaAtomizationRun) TableName() string {
	return "media_atomization_runs"
}
