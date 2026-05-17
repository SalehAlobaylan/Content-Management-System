package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

// AuditLog records admin actions executed from Platform-Console System Health
// (and elsewhere). One row per attempted action — succeeded or not.
type AuditLog struct {
	ID       uint      `gorm:"primaryKey" json:"-"`
	PublicID uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID string    `gorm:"type:varchar(64);not null;default:default;index:idx_audit_logs_tenant_id" json:"tenant_id"`

	// Who
	UserID    string `gorm:"type:varchar(128);index:idx_audit_logs_user_id" json:"user_id"`
	UserEmail string `gorm:"type:varchar(255)" json:"user_email"`

	// What
	Action         string `gorm:"type:varchar(64);not null;index:idx_audit_logs_action" json:"action"`
	TargetService  string `gorm:"type:varchar(32);not null;index:idx_audit_logs_target_service" json:"target_service"`
	TargetResource string `gorm:"type:varchar(255)" json:"target_resource,omitempty"`

	// Result
	Status       string `gorm:"type:varchar(16);not null" json:"status"` // success | failure
	ErrorMessage string `gorm:"type:text" json:"error_message,omitempty"`

	// Payload — request/response context (kept small; truncate at the edge)
	Payload datatypes.JSON `gorm:"type:jsonb" json:"payload,omitempty"`

	CreatedAt time.Time `gorm:"autoCreateTime;index:idx_audit_logs_created_at" json:"created_at"`
}

func (AuditLog) TableName() string {
	return "audit_logs"
}
