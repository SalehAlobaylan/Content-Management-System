package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

type FeedGenerationMembershipRepair struct {
	ID                    uint           `gorm:"primaryKey" json:"-"`
	PublicID              uuid.UUID      `json:"id"`
	TenantID              string         `json:"tenant_id"`
	ActionRequestID       uuid.UUID      `json:"action_request_id"`
	ContentItemID         uuid.UUID      `json:"content_item_id"`
	ExpectedGenerationID  uuid.UUID      `json:"expected_generation_id"`
	ExpectedHeadVersion   int64          `json:"expected_head_version"`
	ExpectedItemUpdatedAt time.Time      `json:"expected_item_updated_at"`
	State                 string         `json:"state"`
	BeforeEffects         datatypes.JSON `json:"before_effects"`
	AfterEffects          datatypes.JSON `json:"after_effects"`
	VerifiedEffects       datatypes.JSON `json:"verified_effects"`
	FailureClass          string         `json:"failure_class,omitempty"`
	CreatedAt             time.Time      `json:"created_at"`
	UpdatedAt             time.Time      `json:"updated_at"`
	FinishedAt            *time.Time     `json:"finished_at,omitempty"`
}

func (FeedGenerationMembershipRepair) TableName() string {
	return "feed_generation_membership_repairs"
}
