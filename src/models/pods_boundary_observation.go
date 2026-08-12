package models

import (
	"time"

	"github.com/google/uuid"
)

// PodsBoundaryObservation is immutable, exact consumer-boundary evidence.
// It never substitutes for user interaction, ranking, session, or RUX state.
type PodsBoundaryObservation struct {
	PublicID           uuid.UUID  `gorm:"column:public_id;type:uuid;default:gen_random_uuid();primaryKey" json:"id"`
	TenantID           string     `gorm:"column:tenant_id;type:varchar(64);not null;index" json:"tenant_id"`
	ContentItemID      uuid.UUID  `gorm:"column:content_item_id;type:uuid;not null;index" json:"content_item_id"`
	GenerationID       *uuid.UUID `gorm:"column:generation_id;type:uuid;index" json:"generation_id,omitempty"`
	Boundary           string     `gorm:"column:boundary;type:varchar(24);not null;index" json:"boundary"`
	ProbeKind          string     `gorm:"column:probe_kind;type:varchar(24);not null" json:"probe_kind"`
	ProbeID            string     `gorm:"column:probe_id;type:varchar(160);not null" json:"probe_id"`
	SourceRunRequestID *uint      `gorm:"column:source_run_request_row_id" json:"-"`
	Verdict            string     `gorm:"column:verdict;type:varchar(16);not null" json:"verdict"`
	ProvenanceDigest   string     `gorm:"column:provenance_digest;type:char(64);not null" json:"provenance_digest"`
	ObservedAt         time.Time  `gorm:"column:observed_at;not null;index" json:"observed_at"`
	CreatedAt          time.Time  `gorm:"column:created_at;autoCreateTime" json:"created_at"`
}

func (PodsBoundaryObservation) TableName() string { return "pods_boundary_observations" }
