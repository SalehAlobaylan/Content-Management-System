package models

import (
	"time"

	"github.com/google/uuid"
)

// SourceItemIdentity is the stable provider identity that survives content-row
// retirement. It points at exactly one current materialized instance.
type SourceItemIdentity struct {
	ID                        uint       `gorm:"primaryKey" json:"-"`
	PublicID                  uuid.UUID  `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID                  string     `gorm:"type:varchar(64);not null" json:"tenant_id"`
	ContentSourceID           uuid.UUID  `gorm:"type:uuid;not null" json:"content_source_id"`
	UpstreamItemID            string     `gorm:"type:varchar(255);not null" json:"upstream_item_id"`
	CurrentInstanceGeneration int        `gorm:"not null" json:"current_instance_generation"`
	CurrentContentItemID      *uuid.UUID `gorm:"type:uuid" json:"current_content_item_id,omitempty"`
	CreatedAt                 time.Time  `json:"created_at"`
	UpdatedAt                 time.Time  `json:"updated_at"`
}

func (SourceItemIdentity) TableName() string { return "source_item_identities" }

// SourceItemInstance is an immutable materialization of a source identity.
// Replacement generations require a campaign-bound reconstruction grant.
type SourceItemInstance struct {
	ID                  uint       `gorm:"primaryKey" json:"-"`
	PublicID            uuid.UUID  `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID            string     `gorm:"type:varchar(64);not null" json:"tenant_id"`
	IdentityID          uint       `gorm:"not null" json:"-"`
	InstanceGeneration  int        `gorm:"not null" json:"instance_generation"`
	ContentItemID       uuid.UUID  `gorm:"type:uuid;not null" json:"content_item_id"`
	SourceObservationID uuid.UUID  `gorm:"type:uuid;not null" json:"source_observation_id"`
	UpstreamFingerprint string     `gorm:"type:char(64);not null" json:"upstream_fingerprint"`
	ProviderVersion     string     `gorm:"type:varchar(64);not null" json:"provider_version"`
	CampaignID          *uint      `json:"-"`
	State               string     `gorm:"type:varchar(16);not null" json:"state"`
	CreatedAt           time.Time  `json:"created_at"`
	RetiredAt           *time.Time `json:"retired_at,omitempty"`
}

func (SourceItemInstance) TableName() string { return "source_item_instances" }
