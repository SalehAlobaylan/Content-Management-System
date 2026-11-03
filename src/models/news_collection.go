package models

import (
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"time"
)

type NewsCollectionControl struct {
	TenantID  string    `gorm:"primaryKey" json:"tenant_id"`
	Paused    bool      `json:"paused"`
	Version   int64     `json:"version"`
	UpdatedAt time.Time `json:"updated_at"`
	UpdatedBy string    `json:"updated_by"`
}

type NewsSourceOperation struct {
	PublicID  uuid.UUID      `gorm:"type:uuid;primaryKey" json:"id"`
	TenantID  string         `json:"tenant_id"`
	Actor     string         `json:"actor"`
	Action    string         `json:"action"`
	Snapshot  datatypes.JSON `json:"snapshot"`
	Result    datatypes.JSON `json:"result,omitempty"`
	CreatedAt time.Time      `json:"created_at"`
	ExpiresAt time.Time      `json:"expires_at"`
	AppliedAt *time.Time     `json:"applied_at,omitempty"`
}
