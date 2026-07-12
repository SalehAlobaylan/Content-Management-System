package models

import (
	"time"

	"gorm.io/datatypes"
)

// Embedding & Model Lifecycle System (stage 10) — campaign persistence (Slice 3).
//
// The campaign is the automation unit: one platform-scoped migration of one
// vector space. Actions are EXECUTION history (kept separate from audit
// findings by design). Exceptions are the human recovery path.

// Campaign states (§7). Only one NON-TERMINAL campaign may exist per space.
const (
	EmbeddingCampaignDraft               = "draft"
	EmbeddingCampaignRunning             = "running"
	EmbeddingCampaignPaused              = "paused"
	EmbeddingCampaignBlocked             = "blocked"
	EmbeddingCampaignVerifying           = "verifying"
	EmbeddingCampaignCompleted           = "completed"
	EmbeddingCampaignCompletedWithWaiver = "completed_with_waivers"
	EmbeddingCampaignAborted             = "aborted"
)

// Action ledger status.
const (
	EmbeddingActionAttempted = "attempted"
	EmbeddingActionCompleted = "completed"
	EmbeddingActionFailed    = "failed"
	EmbeddingActionSkipped   = "skipped"
	EmbeddingActionWouldRun  = "would_run"

	// Owner tools.
	EmbeddingToolTextEmbedding  = "text_embedding"
	EmbeddingToolImageEmbedding = "image_embedding"
	EmbeddingToolStoryCentroid  = "story_centroid_rebuild"
	EmbeddingToolTopicCentroid  = "topic_centroid_refresh"
	EmbeddingToolTopicProposal  = "topic_proposal_refresh"
	EmbeddingToolDiscovery      = "discovery_profile_refresh"
)

// Exception status.
const (
	EmbeddingExceptionOpen     = "open"
	EmbeddingExceptionRetrying = "retrying"
	EmbeddingExceptionWaived   = "waived"
	EmbeddingExceptionResolved = "resolved"
)

// EmbeddingCampaign is one bounded migration of one vector space to a frozen
// target identity.
type EmbeddingCampaign struct {
	ID       uint   `gorm:"primaryKey" json:"id"`
	TenantID string `gorm:"type:varchar(64);not null;index:idx_embedding_campaigns_tenant" json:"tenant_id"`

	Space string `gorm:"type:varchar(16);not null" json:"space"` // text | image
	State string `gorm:"type:varchar(24);not null;index:idx_embedding_campaigns_state" json:"state"`

	// Frozen target identity (snapshot at start; must equal fresh service report).
	TargetSpaceID  string `gorm:"type:char(64);not null" json:"target_space_id"`
	TargetModel    string `gorm:"type:varchar(80)" json:"target_model"`
	TargetRevision string `gorm:"type:varchar(80)" json:"target_revision"`
	// FromSpaceID nullable = "everything not target, incl. unstamped".
	FromSpaceID string `gorm:"type:char(64)" json:"from_space_id,omitempty"`

	// SurfaceScope is a json array of registry surface keys included in scope.
	SurfaceScope       datatypes.JSON `gorm:"type:jsonb" json:"surface_scope,omitempty"`
	DescriptorSnapshot datatypes.JSON `gorm:"type:jsonb" json:"descriptor_snapshot,omitempty"`

	// Caps.
	ItemsPerBatch int `gorm:"not null;default:200" json:"items_per_batch"`
	BatchesPerRun int `gorm:"not null;default:1" json:"batches_per_run"`
	DailyItemCap  int `gorm:"not null;default:5000" json:"daily_item_cap"`
	RetryCeiling  int `gorm:"not null;default:3" json:"retry_ceiling"`

	// Progress.
	CompletedCount int `gorm:"not null;default:0" json:"completed_count"`
	FailedCount    int `gorm:"not null;default:0" json:"failed_count"`
	SkippedCount   int `gorm:"not null;default:0" json:"skipped_count"`

	StartedBy      string     `gorm:"type:varchar(120)" json:"started_by,omitempty"`
	ApprovalReason string     `gorm:"type:text" json:"approval_reason,omitempty"`
	BlockedReason  string     `gorm:"type:text" json:"blocked_reason,omitempty"`
	LeaseExpiresAt *time.Time `json:"lease_expires_at,omitempty"`

	CreatedAt   time.Time  `gorm:"autoCreateTime" json:"created_at"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
	UpdatedAt   time.Time  `gorm:"autoUpdateTime" json:"updated_at"`
}

func (EmbeddingCampaign) TableName() string { return "embedding_campaigns" }

// IsTerminal reports whether a campaign is in an end state.
func (c EmbeddingCampaign) IsTerminal() bool {
	switch c.State {
	case EmbeddingCampaignCompleted, EmbeddingCampaignCompletedWithWaiver, EmbeddingCampaignAborted:
		return true
	}
	return false
}

// EmbeddingCampaignAction is one execution attempt on one target (durable ledger).
type EmbeddingCampaignAction struct {
	ID         uint   `gorm:"primaryKey" json:"id"`
	CampaignID uint   `gorm:"not null;index:idx_embedding_actions_campaign;uniqueIndex:idx_embedding_action_ownership,priority:1" json:"campaign_id"`
	TenantID   string `gorm:"type:varchar(64);not null" json:"tenant_id"`
	BatchID    string `gorm:"type:varchar(48);index:idx_embedding_actions_batch" json:"batch_id"`

	SurfaceKey string `gorm:"type:varchar(48);not null;uniqueIndex:idx_embedding_action_ownership,priority:2" json:"surface_key"`
	Tool       string `gorm:"type:varchar(32);not null" json:"tool"`
	TargetID   string `gorm:"type:varchar(80);not null;uniqueIndex:idx_embedding_action_ownership,priority:3" json:"target_id"`

	Status    string `gorm:"type:varchar(16);not null" json:"status"`
	Guardrail string `gorm:"type:varchar(48)" json:"guardrail,omitempty"`
	Reason    string `gorm:"type:text" json:"reason,omitempty"`

	// Uniqueness: one ownership per (campaign, surface, target, target_producer).
	ExpectedProducerID string `gorm:"type:char(64);not null;uniqueIndex:idx_embedding_action_ownership,priority:4" json:"expected_producer_id"`
	ObservedProducerID string `gorm:"type:char(64)" json:"observed_producer_id,omitempty"`

	RequestID   string         `gorm:"type:varchar(64)" json:"request_id,omitempty"`
	LatencyMS   int64          `gorm:"not null;default:0" json:"latency_ms"`
	RetryNumber int            `gorm:"not null;default:0;uniqueIndex:idx_embedding_action_ownership,priority:5" json:"retry_number"`
	Evidence    datatypes.JSON `gorm:"type:jsonb" json:"evidence,omitempty"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
}

func (EmbeddingCampaignAction) TableName() string { return "embedding_campaign_actions" }

// EmbeddingCampaignException is a target that repeatedly failed — a durable
// blocking record with the human waiver path.
type EmbeddingCampaignException struct {
	ID         uint   `gorm:"primaryKey" json:"id"`
	CampaignID uint   `gorm:"not null;index:idx_embedding_exceptions_campaign;uniqueIndex:idx_embedding_exception_target,priority:1" json:"campaign_id"`
	TenantID   string `gorm:"type:varchar(64);not null" json:"tenant_id"`

	SurfaceKey   string `gorm:"type:varchar(48);not null;uniqueIndex:idx_embedding_exception_target,priority:2" json:"surface_key"`
	TargetID     string `gorm:"type:varchar(80);not null;uniqueIndex:idx_embedding_exception_target,priority:3" json:"target_id"`
	FailureClass string `gorm:"type:varchar(48)" json:"failure_class,omitempty"`
	Attempts     int    `gorm:"not null;default:0" json:"attempts"`

	Status         string         `gorm:"type:varchar(16);not null;default:'open'" json:"status"`
	WaivedBy       string         `gorm:"type:varchar(120)" json:"waived_by,omitempty"`
	WaiverReason   string         `gorm:"type:text" json:"waiver_reason,omitempty"`
	WaiverExpires  *time.Time     `json:"waiver_expires,omitempty"`
	LatestEvidence datatypes.JSON `gorm:"type:jsonb" json:"latest_evidence,omitempty"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (EmbeddingCampaignException) TableName() string { return "embedding_campaign_exceptions" }
