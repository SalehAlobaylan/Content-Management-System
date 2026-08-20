package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

const (
	ContentStageLaneNews = "news"
	ContentStageLanePods = "pods"

	ContentStageNewsTextEmbedding       = "news_text_embedding"
	ContentStageNewsStoryClassification = "news_story_classification"
	ContentStageNewsAsset               = "news_asset"
	ContentStageNewsLLMMetadata         = "news_llm_metadata"
	ContentStagePodsMediaArtifacts      = "pods_media_artifacts"
	ContentStagePodsTextEmbedding       = "pods_text_embedding"
	ContentStagePodsTranscript          = "pods_transcript"
	ContentStagePodsAtomization         = "pods_atomization"
	ContentStagePodsCaptionReembedding  = "pods_caption_reembedding"
	ContentStagePodsImageEmbedding      = "pods_image_embedding"
	ContentStagePodsLLMMetadata         = "pods_llm_metadata"

	ContentStageOwnerCMS             = "cms"
	ContentStageOwnerAggregationNews = "aggregation_news"
	ContentStageOwnerAggregationPods = "aggregation_pods"
	ContentStageOwnerEnrichment      = "enrichment"
	ContentStageOwnerMedia           = "media"

	ContentStageBlockingContentReady = "content_ready"
	ContentStageBlockingFeedDelivery = "feed_delivery"
	ContentStageBlockingOptional     = "optional"

	ContentStageQueued      = "queued"
	ContentStageClaimed     = "claimed"
	ContentStageRunning     = "running"
	ContentStageVerifying   = "verifying"
	ContentStageVerified    = "verified"
	ContentStageDeferred    = "deferred"
	ContentStageUncertain   = "uncertain"
	ContentStageReconciling = "reconciling"
	ContentStageFailed      = "failed"
	ContentStageCancelled   = "cancelled"
	ContentStageSuperseded  = "superseded"

	ContentStageCutoverLegacy          = "legacy"
	ContentStageCutoverShadow          = "shadow"
	ContentStageCutoverDurableRequired = "durable_required"
)

type ContentStageRequest struct {
	ID                      uint           `gorm:"primaryKey" json:"-"`
	PublicID                uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID                string         `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	ContentItemID           uuid.UUID      `gorm:"type:uuid;not null;index" json:"content_item_id"`
	ProcessingGeneration    int64          `gorm:"not null" json:"processing_generation"`
	Lane                    string         `gorm:"type:varchar(16);not null;index" json:"lane"`
	Stage                   string         `gorm:"type:varchar(48);not null;index" json:"stage"`
	Owner                   string         `gorm:"type:varchar(32);not null;index" json:"owner"`
	BlockingScope           string         `gorm:"type:varchar(24);not null" json:"blocking_scope"`
	State                   string         `gorm:"type:varchar(24);not null;index" json:"state"`
	InputFingerprint        string         `gorm:"type:varchar(64);not null" json:"input_fingerprint"`
	PolicyVersion           string         `gorm:"type:varchar(64);not null" json:"policy_version"`
	ModelRecipe             string         `gorm:"type:varchar(128);not null" json:"model_recipe,omitempty"`
	IdempotencyKey          string         `gorm:"type:varchar(255);not null" json:"idempotency_key"`
	DependencyManifest      datatypes.JSON `gorm:"type:jsonb" json:"dependency_manifest"`
	WorkloadEstimate        datatypes.JSON `gorm:"type:jsonb" json:"workload_estimate"`
	NotBeforeAt             *time.Time     `json:"not_before_at,omitempty"`
	DeadlineAt              *time.Time     `json:"deadline_at,omitempty"`
	ClaimOwner              string         `json:"claim_owner,omitempty"`
	ClaimToken              *uuid.UUID     `json:"-"`
	ClaimEpoch              int64          `json:"claim_epoch"`
	ClaimExpiresAt          *time.Time     `json:"claim_expires_at,omitempty"`
	CancellationRequestedAt *time.Time     `json:"cancellation_requested_at,omitempty"`
	CancellationReason      string         `json:"cancellation_reason,omitempty"`
	AcceptedAt              *time.Time     `json:"accepted_at,omitempty"`
	VerifiedAt              *time.Time     `json:"verified_at,omitempty"`
	FinishedAt              *time.Time     `json:"finished_at,omitempty"`
	FailureClass            string         `json:"failure_class,omitempty"`
	TerminalProof           datatypes.JSON `gorm:"type:jsonb" json:"terminal_proof"`
	CreatedAt               time.Time      `json:"created_at"`
	UpdatedAt               time.Time      `json:"updated_at"`
}

func (ContentStageRequest) TableName() string { return "content_stage_requests" }

type ContentStageAttempt struct {
	ID                 uint       `gorm:"primaryKey" json:"-"`
	PublicID           uuid.UUID  `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID           string     `json:"tenant_id"`
	RequestID          uuid.UUID  `json:"request_id"`
	AttemptNumber      int        `json:"attempt_number"`
	Lane               string     `json:"lane"`
	Stage              string     `json:"stage"`
	Owner              string     `json:"owner"`
	InputFingerprint   string     `json:"input_fingerprint"`
	State              string     `json:"state"`
	ClaimToken         uuid.UUID  `json:"-"`
	FenceToken         uuid.UUID  `json:"fence_token"`
	LeaseEpoch         int64      `json:"lease_epoch"`
	DeterministicJobID string     `json:"deterministic_job_id"`
	LeaseExpiresAt     time.Time  `json:"lease_expires_at"`
	HeartbeatAt        time.Time  `json:"heartbeat_at"`
	EffectStartedAt    *time.Time `json:"effect_started_at,omitempty"`
	AcceptedAt         *time.Time `json:"accepted_at,omitempty"`
	FinishedAt         *time.Time `json:"finished_at,omitempty"`
	FailureClass       string     `json:"failure_class,omitempty"`
	FailureSummary     string     `json:"failure_summary,omitempty"`
	CreatedAt          time.Time  `json:"created_at"`
	UpdatedAt          time.Time  `json:"updated_at"`
}

func (ContentStageAttempt) TableName() string { return "content_stage_attempts" }

type ContentStageReceipt struct {
	ID                   uint           `gorm:"primaryKey" json:"-"`
	PublicID             uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID             string         `json:"tenant_id"`
	RequestID            uuid.UUID      `json:"request_id"`
	AttemptID            uuid.UUID      `json:"attempt_id"`
	ContentItemID        uuid.UUID      `json:"content_item_id"`
	ProcessingGeneration int64          `json:"processing_generation"`
	Lane                 string         `json:"lane"`
	Stage                string         `json:"stage"`
	Owner                string         `json:"owner"`
	ProducerEventID      uuid.UUID      `json:"producer_event_id"`
	FenceToken           uuid.UUID      `json:"fence_token"`
	InputFingerprint     string         `json:"input_fingerprint"`
	Outcome              string         `json:"outcome"`
	PayloadDigest        string         `json:"payload_digest"`
	ArtifactDigest       string         `json:"artifact_digest,omitempty"`
	ObservedAt           time.Time      `json:"observed_at"`
	Payload              datatypes.JSON `gorm:"type:jsonb" json:"payload"`
	CreatedAt            time.Time      `json:"created_at"`
}

func (ContentStageReceipt) TableName() string { return "content_stage_receipts" }

type ContentStageEvent struct {
	ID         uint           `gorm:"primaryKey" json:"-"`
	PublicID   uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID   string         `json:"tenant_id"`
	RequestID  uuid.UUID      `json:"request_id"`
	AttemptID  *uuid.UUID     `json:"attempt_id,omitempty"`
	Sequence   int64          `json:"sequence"`
	EventType  string         `json:"event_type"`
	Payload    datatypes.JSON `gorm:"type:jsonb" json:"payload"`
	OccurredAt time.Time      `json:"occurred_at"`
}

func (ContentStageEvent) TableName() string { return "content_stage_events" }

type ContentStageCutover struct {
	ID                 uint       `gorm:"primaryKey" json:"-"`
	TenantID           string     `json:"tenant_id"`
	Lane               string     `json:"lane"`
	Mode               string     `json:"mode"`
	ProtocolVersion    string     `json:"protocol_version"`
	PromotedBy         string     `json:"promoted_by,omitempty"`
	PromotedAt         *time.Time `json:"promoted_at,omitempty"`
	VerificationDigest string     `json:"verification_digest,omitempty"`
	CreatedAt          time.Time  `json:"created_at"`
	UpdatedAt          time.Time  `json:"updated_at"`
}

func (ContentStageCutover) TableName() string { return "content_stage_cutovers" }

type ContentStageControl struct {
	ID                         uint      `gorm:"primaryKey" json:"-"`
	TenantID                   string    `json:"tenant_id"`
	Lane                       string    `json:"lane"`
	SchedulingEnabled          bool      `json:"scheduling_enabled"`
	ExecutionEnabled           bool      `json:"execution_enabled"`
	OptionalMetadataEnabled    bool      `json:"optional_metadata_enabled"`
	TranscriptExecutionEnabled bool      `json:"transcript_execution_enabled"`
	Reason                     string    `json:"reason,omitempty"`
	UpdatedBy                  string    `json:"updated_by"`
	CreatedAt                  time.Time `json:"created_at"`
	UpdatedAt                  time.Time `json:"updated_at"`
}

func (ContentStageControl) TableName() string { return "content_stage_controls" }
