package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

type MediaSourceYieldDaily struct {
	TenantID                string    `gorm:"primaryKey" json:"tenant_id"`
	ContentSourceID         uuid.UUID `gorm:"primaryKey;type:uuid" json:"content_source_id"`
	YieldDate               time.Time `gorm:"primaryKey;type:date" json:"yield_date"`
	FetchedCandidates       int       `json:"fetched_candidates"`
	LegalDurationCandidates int       `json:"legal_duration_candidates"`
	FilteredCandidates      int       `json:"filtered_candidates"`
	MaterializedItems       int       `json:"materialized_items"`
	VerifiedMedia           int       `json:"verified_media"`
	ReadyVisibleUnits       int       `json:"ready_visible_units"`
	PublicReturns           int       `json:"public_returns"`
	FirstPageReturns        int       `json:"first_page_returns"`
	CreatedAt               time.Time `json:"created_at"`
	UpdatedAt               time.Time `json:"updated_at"`
}

func (MediaSourceYieldDaily) TableName() string { return "media_source_yield_daily" }

type PipelineLaneHealthSnapshot struct {
	ID                       uint           `gorm:"primaryKey" json:"id"`
	TenantID                 string         `json:"tenant_id"`
	Lane                     string         `json:"lane"`
	OwnerPrincipal           string         `json:"owner_principal"`
	RequiredQueueDepth       int            `json:"required_queue_depth"`
	OptionalQueueDepth       int            `json:"optional_queue_depth"`
	RequiredOldestAgeSeconds float64        `json:"required_oldest_age_seconds"`
	OptionalOldestAgeSeconds float64        `json:"optional_oldest_age_seconds"`
	DLQDelta                 int            `json:"dlq_delta"`
	FailureClasses           datatypes.JSON `gorm:"type:jsonb" json:"failure_classes"`
	StageCounts              datatypes.JSON `gorm:"type:jsonb" json:"stage_counts"`
	EnrichmentCounts         datatypes.JSON `gorm:"type:jsonb" json:"enrichment_counts"`
	ProcessMetrics           datatypes.JSON `gorm:"type:jsonb" json:"process_metrics"`
	ResourceMetrics          datatypes.JSON `gorm:"type:jsonb" json:"resource_metrics"`
	CapturedAt               time.Time      `json:"captured_at"`
	CreatedAt                time.Time      `json:"created_at"`
}

func (PipelineLaneHealthSnapshot) TableName() string { return "pipeline_lane_health_snapshots" }

type SourceRunCutoverAuditEvent struct {
	ID                 uint           `gorm:"primaryKey" json:"id"`
	TenantID           *string        `json:"tenant_id,omitempty"`
	Lane               *string        `json:"lane,omitempty"`
	EventType          string         `json:"event_type"`
	FromEpoch          string         `json:"from_epoch,omitempty"`
	ToEpoch            string         `json:"to_epoch,omitempty"`
	VerificationDigest string         `json:"verification_digest,omitempty"`
	Actor              string         `json:"actor"`
	Payload            datatypes.JSON `gorm:"type:jsonb" json:"payload"`
	OccurredAt         time.Time      `json:"occurred_at"`
}

func (SourceRunCutoverAuditEvent) TableName() string { return "source_run_cutover_audit_events" }
