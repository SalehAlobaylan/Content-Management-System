package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

const (
	TranscriptionBatchStatusQueued    = "queued"
	TranscriptionBatchStatusRunning   = "running"
	TranscriptionBatchStatusCompleted = "completed"
	TranscriptionBatchStatusCanceled  = "canceled"
	TranscriptionBatchStatusFailed    = "failed"
)

const (
	TranscriptionBatchItemStatusPending  = "pending"
	TranscriptionBatchItemStatusAccepted = "accepted"
	TranscriptionBatchItemStatusSkipped  = "skipped"
	TranscriptionBatchItemStatusFailed   = "failed"
	TranscriptionBatchItemStatusCanceled = "canceled"
	TranscriptionBatchItemStatusDone     = "done"
)

type TranscriptionBatch struct {
	ID       uint      `gorm:"primaryKey" json:"-"`
	PublicID uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex:idx_transcription_batches_public_id" json:"id"`
	TenantID string    `gorm:"type:varchar(64);not null;index:idx_transcription_batches_tenant" json:"tenant_id"`

	Status string `gorm:"type:varchar(24);not null;index:idx_transcription_batches_status" json:"status"`
	Force  bool   `gorm:"default:true" json:"force"`
	Actor  string `gorm:"type:varchar(255)" json:"actor,omitempty"`

	TotalCount     int `gorm:"default:0" json:"total_count"`
	AcceptedCount  int `gorm:"default:0" json:"accepted_count"`
	SkippedCount   int `gorm:"default:0" json:"skipped_count"`
	FailedCount    int `gorm:"default:0" json:"failed_count"`
	CanceledCount  int `gorm:"default:0" json:"canceled_count"`
	CompletedCount int `gorm:"default:0" json:"completed_count"`

	LatestError string         `gorm:"type:text" json:"latest_error,omitempty"`
	Metadata    datatypes.JSON `gorm:"type:jsonb" json:"metadata,omitempty"`

	CanceledAt  *time.Time `gorm:"type:timestamp" json:"canceled_at,omitempty"`
	CompletedAt *time.Time `gorm:"type:timestamp" json:"completed_at,omitempty"`
	CreatedAt   time.Time  `gorm:"autoCreateTime;index:idx_transcription_batches_created" json:"created_at"`
	UpdatedAt   time.Time  `gorm:"autoUpdateTime" json:"updated_at"`
}

func (TranscriptionBatch) TableName() string {
	return "transcription_batches"
}

type TranscriptionBatchItem struct {
	ID       uint      `gorm:"primaryKey" json:"-"`
	PublicID uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex:idx_transcription_batch_items_public_id" json:"id"`
	TenantID string    `gorm:"type:varchar(64);not null;index:idx_transcription_batch_items_tenant" json:"tenant_id"`

	BatchID       uuid.UUID  `gorm:"type:uuid;not null;index:idx_transcription_batch_items_batch" json:"batch_id"`
	ContentItemID uuid.UUID  `gorm:"type:uuid;not null;index:idx_transcription_batch_items_content" json:"content_item_id"`
	JobID         *uuid.UUID `gorm:"type:uuid;index:idx_transcription_batch_items_job" json:"job_id,omitempty"`

	Status string `gorm:"type:varchar(24);not null;index:idx_transcription_batch_items_status" json:"status"`
	Reason string `gorm:"type:text" json:"reason,omitempty"`
	Error  string `gorm:"type:text" json:"error,omitempty"`

	CreatedAt time.Time `gorm:"autoCreateTime;index:idx_transcription_batch_items_created" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (TranscriptionBatchItem) TableName() string {
	return "transcription_batch_items"
}
