package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

const (
	TranscriptionJobStatusQueued          = "queued"
	TranscriptionJobStatusRunning         = "running"
	TranscriptionJobStatusSkipped         = "skipped"
	TranscriptionJobStatusSucceeded       = "succeeded"
	TranscriptionJobStatusFailed          = "failed"
	TranscriptionJobStatusWritebackFailed = "writeback_failed"
)

const (
	TranscriptionTriggerIngestAuto  = "ingest_auto"
	TranscriptionTriggerAutoQuality = "auto_quality"
	TranscriptionTriggerManual      = "manual"
	TranscriptionTriggerBulkManual  = "bulk_manual"
)

type TranscriptionJob struct {
	ID       uint      `gorm:"primaryKey" json:"-"`
	PublicID uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex:idx_transcription_jobs_public_id" json:"id"`
	TenantID string    `gorm:"type:varchar(64);not null;index:idx_transcription_jobs_tenant_id" json:"tenant_id"`

	ContentItemID uuid.UUID  `gorm:"type:uuid;not null;index:idx_transcription_jobs_content" json:"content_item_id"`
	TranscriptID  *uuid.UUID `gorm:"type:uuid;index:idx_transcription_jobs_transcript" json:"transcript_id,omitempty"`

	TriggerSource string `gorm:"type:varchar(32);not null;index:idx_transcription_jobs_trigger" json:"trigger_source"`
	Status        string `gorm:"type:varchar(24);not null;index:idx_transcription_jobs_status" json:"status"`

	Provider string `gorm:"type:varchar(64)" json:"provider,omitempty"`
	Model    string `gorm:"type:varchar(128)" json:"model,omitempty"`
	Language string `gorm:"type:varchar(16)" json:"language,omitempty"`

	SkipReason   string `gorm:"type:text" json:"skip_reason,omitempty"`
	ErrorMessage string `gorm:"type:text" json:"error_message,omitempty"`
	RetryCount   int    `gorm:"default:0" json:"retry_count"`

	EstimatedCostUsd float64 `gorm:"type:double precision;default:0" json:"estimated_cost_usd"`
	ReservedCostUsd  float64 `gorm:"type:double precision;default:0" json:"reserved_cost_usd"`
	ActualCostUsd    float64 `gorm:"type:double precision;default:0" json:"actual_cost_usd"`

	StartedAt   *time.Time `gorm:"type:timestamp" json:"started_at,omitempty"`
	CompletedAt *time.Time `gorm:"type:timestamp" json:"completed_at,omitempty"`

	Metadata datatypes.JSON `gorm:"type:jsonb" json:"metadata,omitempty"`

	CreatedAt time.Time `gorm:"autoCreateTime;index:idx_transcription_jobs_created_at" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (TranscriptionJob) TableName() string {
	return "transcription_jobs"
}
