package models

import (
	"time"

	"github.com/google/uuid"
)

// OperatorActionJob is the CMS-owned durable execution record for one signed
// Operator plan. It exists so plan approval is sufficient to continue work
// after a browser disconnect or CMS restart.
type OperatorActionJob struct {
	ID             uint       `gorm:"primaryKey" json:"-"`
	PublicID       uuid.UUID  `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	PlanID         uint       `gorm:"uniqueIndex" json:"-"`
	TenantID       string     `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	State          string     `json:"state"`
	AttemptCount   int        `json:"attempt_count"`
	AvailableAt    time.Time  `json:"available_at"`
	ClaimToken     *uuid.UUID `gorm:"type:uuid" json:"-"`
	ClaimExpiresAt *time.Time `json:"claim_expires_at,omitempty"`
	LastErrorClass string     `json:"last_error_class,omitempty"`
	CompletedAt    *time.Time `json:"completed_at,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
}

func (OperatorActionJob) TableName() string { return "operator_action_jobs" }
