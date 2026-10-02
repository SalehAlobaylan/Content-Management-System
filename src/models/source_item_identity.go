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

// ContentResetReconstructionGrant authorizes one exact replacement instance.
// Only a hash of the one-time opaque grant is persisted.
type ContentResetReconstructionGrant struct {
	ID                            uint       `gorm:"primaryKey" json:"-"`
	PublicID                      uuid.UUID  `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID                      string     `gorm:"type:varchar(64);not null" json:"tenant_id"`
	CampaignID                    uint       `gorm:"not null" json:"-"`
	RevisionID                    uint       `gorm:"not null" json:"-"`
	IdentityID                    uint       `gorm:"not null" json:"-"`
	GrantKind                     string     `gorm:"type:varchar(24);not null" json:"grant_kind"`
	TargetContentItemID           *uuid.UUID `gorm:"type:uuid" json:"target_content_item_id,omitempty"`
	SourceRunRequestID            uuid.UUID  `gorm:"type:uuid;not null" json:"-"`
	SourceRunAttemptID            uuid.UUID  `gorm:"type:uuid;not null" json:"-"`
	ExecutionUnitID               uuid.UUID  `gorm:"type:uuid;not null" json:"-"`
	ExecutionFenceToken           uuid.UUID  `gorm:"type:uuid;not null" json:"-"`
	PageID                        string     `gorm:"type:varchar(128);not null" json:"-"`
	BatchID                       string     `gorm:"type:varchar(128);not null" json:"-"`
	SourceObservationID           uuid.UUID  `gorm:"type:uuid;not null" json:"source_observation_id"`
	ReplacementInstanceGeneration int        `gorm:"not null" json:"replacement_instance_generation"`
	BaseInstanceGeneration        int        `gorm:"not null" json:"base_instance_generation"`
	ExpectedFingerprint           string     `gorm:"type:char(64);not null" json:"expected_fingerprint"`
	ProviderVersion               string     `gorm:"type:varchar(64);not null" json:"provider_version"`
	GrantTokenHash                string     `gorm:"type:char(64);not null" json:"-"`
	State                         string     `gorm:"type:varchar(16);not null" json:"state"`
	ExpiresAt                     time.Time  `json:"expires_at"`
	ConsumedAt                    *time.Time `json:"consumed_at,omitempty"`
	ReplacementContentItemID      *uuid.UUID `gorm:"type:uuid" json:"replacement_content_item_id,omitempty"`
	CreatedBy                     string     `gorm:"type:varchar(255);not null" json:"created_by"`
	CreatedAt                     time.Time  `json:"created_at"`
}

func (ContentResetReconstructionGrant) TableName() string {
	return "content_reset_reconstruction_grants"
}
