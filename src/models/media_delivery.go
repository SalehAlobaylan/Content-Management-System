package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

// MediaDeliveryPolicy selects the legal delivery family for a media item. It
// is product policy (not process configuration): Aggregation resolves it
// before doing work and persists exactly what was produced in media_renditions.
type MediaDeliveryPolicy struct {
	ID                          uint                         `gorm:"primaryKey" json:"-"`
	PublicID                    uuid.UUID                    `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID                    *string                      `gorm:"type:varchar(64);index" json:"tenant_id,omitempty"`
	SourceType                  *string                      `gorm:"type:varchar(20);index" json:"source_type,omitempty"`
	MediaKind                   string                       `gorm:"type:varchar(16);not null" json:"media_kind"`
	Suitability                 *string                      `gorm:"type:varchar(40);index" json:"suitability,omitempty"`
	Name                        string                       `gorm:"type:varchar(96);not null" json:"name"`
	SchemaVersion               int                          `gorm:"not null;default:1" json:"schema_version"`
	PolicyDigest                string                       `gorm:"type:char(64)" json:"policy_digest"`
	PrimaryMode                 string                       `gorm:"type:varchar(16);not null;default:'progressive'" json:"primary_mode"`
	ShortFormDelivery           bool                         `gorm:"not null;default:true" json:"short_form_delivery"`
	AllowNativeAudio            bool                         `gorm:"not null;default:true" json:"allow_native_audio"`
	AllowMP4Fallback            bool                         `gorm:"not null;default:true" json:"allow_mp4_fallback"`
	AllowHLS                    bool                         `gorm:"not null;default:true" json:"allow_hls"`
	RequireAdaptiveHLS          bool                         `gorm:"not null;default:false" json:"require_adaptive_hls"`
	AllowPassthrough            bool                         `gorm:"not null;default:true" json:"allow_passthrough"`
	AllowRemux                  bool                         `gorm:"not null;default:true" json:"allow_remux"`
	GenerateAudioAlternate      bool                         `gorm:"not null;default:true" json:"generate_audio_alternate"`
	GenerateProgressiveFallback bool                         `gorm:"not null;default:true" json:"generate_progressive_fallback"`
	PreserveVideo               bool                         `gorm:"not null;default:true" json:"preserve_video"`
	HLSSegmentDurationSec       int                          `gorm:"not null;default:6" json:"hls_segment_duration_sec"`
	HLSSegmentFormat            string                       `gorm:"type:varchar(16);not null;default:'cmaf'" json:"hls_segment_format"`
	HLSMinVariants              int                          `gorm:"not null;default:2" json:"hls_min_variants"`
	RolloutState                string                       `gorm:"type:varchar(16);not null;default:'shadow'" json:"rollout_state"`
	MaxDeliveryHeight           int                          `gorm:"not null;default:720" json:"max_delivery_height"`
	MaxDeliveryBitrateKbps      int                          `gorm:"not null;default:0" json:"max_delivery_bitrate_kbps"`
	CacheProfile                string                       `gorm:"type:varchar(32);not null;default:'immutable-media'" json:"cache_profile"`
	Active                      bool                         `gorm:"not null;default:true" json:"active"`
	CreatedBy                   string                       `gorm:"type:varchar(128)" json:"created_by"`
	UpdatedBy                   string                       `gorm:"type:varchar(128)" json:"updated_by"`
	Variants                    []MediaDeliveryPolicyVariant `gorm:"foreignKey:PolicyID" json:"variants,omitempty"`
	CreatedAt                   time.Time                    `json:"created_at"`
	UpdatedAt                   time.Time                    `json:"updated_at"`
}

func (MediaDeliveryPolicy) TableName() string { return "media_delivery_policies" }

type MediaDeliveryPolicyVariant struct {
	ID               uint      `gorm:"primaryKey" json:"-"`
	PolicyID         uint      `gorm:"not null;index" json:"-"`
	QualityProfileID *uint     `gorm:"index" json:"quality_profile_id,omitempty"`
	RenditionType    string    `gorm:"type:varchar(16);not null" json:"rendition_type"`
	QualityTier      string    `gorm:"type:varchar(16);not null" json:"quality_tier"`
	Priority         int       `gorm:"not null;default:100" json:"priority"`
	Required         bool      `gorm:"not null;default:true" json:"required"`
	Enabled          bool      `gorm:"not null;default:true" json:"enabled"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

func (MediaDeliveryPolicyVariant) TableName() string { return "media_delivery_policy_variants" }

type MediaRenditionGeneration struct {
	ID               uint           `gorm:"primaryKey" json:"-"`
	PublicID         uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID         string         `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	ContentItemID    uuid.UUID      `gorm:"type:uuid;not null;index" json:"content_item_id"`
	GenerationNumber int            `gorm:"not null" json:"generation_number"`
	SourceManifestID *uuid.UUID     `gorm:"type:uuid;index" json:"source_manifest_id,omitempty"`
	RouteDecision    datatypes.JSON `gorm:"type:jsonb" json:"route_decision"`
	RouteDigest      string         `gorm:"type:char(64);not null" json:"route_digest"`
	ProbeSnapshot    datatypes.JSON `gorm:"type:jsonb" json:"probe_snapshot"`
	ProbeDigest      string         `gorm:"type:char(64);not null" json:"probe_digest"`
	PolicySnapshot   datatypes.JSON `gorm:"type:jsonb" json:"policy_snapshot"`
	PolicyDigest     string         `gorm:"type:char(64);not null" json:"policy_digest"`
	RenditionSet     datatypes.JSON `gorm:"type:jsonb" json:"rendition_set"`
	RenditionDigest  string         `gorm:"type:char(64)" json:"rendition_digest,omitempty"`
	State            string         `gorm:"type:varchar(24);not null;index" json:"state"`
	AttemptID        *uuid.UUID     `gorm:"type:uuid;index" json:"attempt_id,omitempty"`
	FenceToken       *uuid.UUID     `gorm:"type:uuid" json:"fence_token,omitempty"`
	TerminalProof    datatypes.JSON `gorm:"type:jsonb" json:"terminal_proof"`
	ActivationAt     *time.Time     `json:"activation_at,omitempty"`
	CreatedAt        time.Time      `json:"created_at"`
	UpdatedAt        time.Time      `json:"updated_at"`
}

func (MediaRenditionGeneration) TableName() string { return "media_rendition_generations" }

type MediaHLSPackage struct {
	ID                    uint           `gorm:"primaryKey" json:"-"`
	PublicID              uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID              string         `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	RenditionGenerationID uuid.UUID      `gorm:"type:uuid;not null;index" json:"rendition_generation_id"`
	MasterManifestID      uuid.UUID      `gorm:"type:uuid;not null" json:"master_manifest_id"`
	ProgressiveManifestID *uuid.UUID     `gorm:"type:uuid" json:"progressive_manifest_id,omitempty"`
	VariantCount          int            `gorm:"not null;default:0" json:"variant_count"`
	State                 string         `gorm:"type:varchar(24);not null;index" json:"state"`
	ValidationEvidence    datatypes.JSON `gorm:"type:jsonb" json:"validation_evidence"`
	ValidationDigest      string         `gorm:"type:char(64)" json:"validation_digest,omitempty"`
	CreatedAt             time.Time      `json:"created_at"`
	UpdatedAt             time.Time      `json:"updated_at"`
}

func (MediaHLSPackage) TableName() string { return "media_hls_packages" }

// MediaHLSAccessPoint is a verified, immutable master playlist that exposes
// only the variants legal for a mobile quality tier. It references the same
// CMAF segment set and shared audio group as its package.
type MediaHLSAccessPoint struct {
	ID               uint      `gorm:"primaryKey" json:"-"`
	PublicID         uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID         string    `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	PackageID        uuid.UUID `gorm:"type:uuid;not null;index" json:"package_id"`
	QualityTier      string    `gorm:"type:varchar(16);not null" json:"quality_tier"`
	ManifestID       uuid.UUID `gorm:"type:uuid;not null" json:"manifest_id"`
	MaxHeight        int       `gorm:"not null" json:"max_height"`
	MaxBandwidthKbps int       `gorm:"not null" json:"max_bandwidth_kbps"`
	ValidationDigest string    `gorm:"type:char(64);not null" json:"validation_digest"`
	State            string    `gorm:"type:varchar(24);not null;index" json:"state"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

func (MediaHLSAccessPoint) TableName() string { return "media_hls_access_points" }

type MediaPlaybackHealthReceipt struct {
	ID                    uint       `gorm:"primaryKey" json:"-"`
	PublicID              uuid.UUID  `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID              string     `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	ContentItemID         uuid.UUID  `gorm:"type:uuid;not null;index" json:"content_item_id"`
	RenditionGenerationID *uuid.UUID `gorm:"type:uuid;index" json:"rendition_generation_id,omitempty"`
	RenditionID           string     `gorm:"type:varchar(128);not null" json:"rendition_id"`
	FailureClass          string     `gorm:"type:varchar(32);not null" json:"failure_class"`
	Platform              string     `gorm:"type:varchar(16);not null" json:"platform"`
	AppBuild              string     `gorm:"type:varchar(64);not null" json:"app_build"`
	NetworkClass          string     `gorm:"type:varchar(16);not null" json:"network_class"`
	IdentityKey           string     `gorm:"type:char(64);not null" json:"-"`
	IdempotencyKey        string     `gorm:"type:varchar(128);not null" json:"-"`
	CreatedAt             time.Time  `json:"created_at"`
}

func (MediaPlaybackHealthReceipt) TableName() string { return "media_playback_health_receipts" }

type UserPlaybackPreference struct {
	ID                       uint      `gorm:"primaryKey" json:"-"`
	TenantID                 string    `gorm:"type:varchar(64);not null;uniqueIndex:idx_user_playback_preference,priority:1" json:"tenant_id"`
	UserID                   uuid.UUID `gorm:"type:uuid;not null;uniqueIndex:idx_user_playback_preference,priority:2" json:"user_id"`
	AudioQuality             string    `gorm:"type:varchar(16);not null;default:'standard'" json:"audio_quality"`
	StreamingQuality         string    `gorm:"type:varchar(16);not null;default:'auto'" json:"streaming_quality"`
	AllowCellularHighQuality bool      `gorm:"not null;default:false" json:"allow_cellular_high_quality"`
	PreferAudioWhenAvailable bool      `gorm:"not null;default:true" json:"prefer_audio_when_available"`
	UpdatedAt                time.Time `json:"updated_at"`
	CreatedAt                time.Time `json:"created_at"`
}

func (UserPlaybackPreference) TableName() string { return "user_playback_preferences" }
