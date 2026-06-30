package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

const (
	StorageStateHot                = "hot"
	StorageStateCold               = "cold"
	StorageStateReencoded          = "reencoded"
	StorageStateRecoverableDeleted = "recoverable_deleted"
	StorageStateMissing            = "missing"
	StorageStateRecoveryPending    = "recovery_pending"
	StorageStateRecovered          = "recovered"
	StorageStateUnrecoverable      = "unrecoverable"

	StorageRecoveryRecoverable   = "recoverable"
	StorageRecoveryAtRisk        = "at_risk"
	StorageRecoveryPending       = "recovery_pending"
	StorageRecoveryRecovered     = "recovered"
	StorageRecoveryFailed        = "recovery_failed"
	StorageRecoveryUnrecoverable = "unrecoverable"

	StorageArtifactEventMovedCold           = "moved_cold"
	StorageArtifactEventReencoded           = "reencoded"
	StorageArtifactEventDeleted             = "deleted"
	StorageArtifactEventRecoverableDeleted  = "recoverable_deleted"
	StorageArtifactEventRestoreRequested    = "restore_requested"
	StorageArtifactEventReingestQueued      = "reingest_queued"
	StorageArtifactEventRecovered           = "recovered"
	StorageArtifactEventRecoveryFailed      = "recovery_failed"
	StorageArtifactEventMarkedUnrecoverable = "marked_unrecoverable"

	StorageArtifactEventStatusSuccess          = "success"
	StorageArtifactEventStatusSkipped          = "skipped"
	StorageArtifactEventStatusError            = "error"
	StorageArtifactEventStatusApprovalRequired = "approval_required"

	MediaSuitabilityAudioFirstTalkingHead = "audio_first_talking_head"
	MediaSuitabilityAudioFirstShow        = "audio_first_show"
	MediaSuitabilityVisualDependent       = "visual_dependent"
	MediaSuitabilityUnsuitable            = "unsuitable"
	MediaSuitabilityUnknown               = "unknown"
)

// MediaStorageArtifactEvent records every meaningful artifact lifecycle change
// for recoverability and Autopilot audit. It tracks bytes and keys, while
// ContentItem.StorageState keeps the current cheap-read state.
type MediaStorageArtifactEvent struct {
	ID       uint      `gorm:"primaryKey" json:"-"`
	PublicID uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex:idx_media_storage_events_public_id" json:"id"`
	TenantID string    `gorm:"type:varchar(64);not null;index:idx_media_storage_events_tenant_created,priority:1" json:"tenant_id"`

	ContentItemID       uuid.UUID      `gorm:"type:uuid;not null;index:idx_media_storage_events_content" json:"content_item_id"`
	ParentContentItemID *uuid.UUID     `gorm:"type:uuid;index:idx_media_storage_events_parent" json:"parent_content_item_id,omitempty"`
	EventType           string         `gorm:"type:varchar(48);not null;index:idx_media_storage_events_type" json:"event_type"`
	Status              string         `gorm:"type:varchar(24);not null;index:idx_media_storage_events_status" json:"status"`
	Reason              string         `gorm:"type:text" json:"reason,omitempty"`
	Trigger             string         `gorm:"type:varchar(32)" json:"trigger,omitempty"`
	Source              string         `gorm:"type:varchar(32)" json:"source,omitempty"`
	StorageTier         string         `gorm:"type:varchar(16)" json:"storage_tier,omitempty"`
	OldStorageTier      string         `gorm:"type:varchar(16)" json:"old_storage_tier,omitempty"`
	OldMediaURL         string         `gorm:"type:text" json:"old_media_url,omitempty"`
	NewMediaURL         string         `gorm:"type:text" json:"new_media_url,omitempty"`
	OldSizeBytes        int64          `gorm:"type:bigint;default:0" json:"old_size_bytes"`
	NewSizeBytes        int64          `gorm:"type:bigint;default:0" json:"new_size_bytes"`
	FreedBytes          int64          `gorm:"type:bigint;default:0" json:"freed_bytes"`
	DeletedBytes        int64          `gorm:"type:bigint;default:0" json:"deleted_bytes"`
	QualityProfileID    *uint          `gorm:"type:bigint" json:"quality_profile_id,omitempty"`
	ArtifactKeys        datatypes.JSON `gorm:"type:jsonb" json:"artifact_keys,omitempty"`
	RecoveryPayload     datatypes.JSON `gorm:"type:jsonb" json:"recovery_payload,omitempty"`
	Error               string         `gorm:"type:text" json:"error,omitempty"`
	CreatedBy           string         `gorm:"type:varchar(255)" json:"created_by,omitempty"`
	CreatedAt           time.Time      `gorm:"autoCreateTime;index:idx_media_storage_events_tenant_created,priority:2,sort:desc" json:"created_at"`
}

func (MediaStorageArtifactEvent) TableName() string {
	return "media_storage_artifact_events"
}
