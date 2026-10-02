package models

import (
	"github.com/google/uuid"
	"time"
)

// Planner cursors commit with the outbox page they describe. Owner receipts,
// not these cursors, determine whether an effect actually succeeded.
type ContentResetWorkflow struct {
	ID                uint `gorm:"primaryKey"`
	TenantID          string
	CampaignID        uint
	RevisionID        uint
	ManifestHash      string
	PreparedCommandID uuid.UUID `gorm:"type:uuid"`
	Phase             string
	SourceCursor      int
	TargetCursor      int64
	BranchCursor      uint
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

func (ContentResetWorkflow) TableName() string { return "content_reset_workflows" }
