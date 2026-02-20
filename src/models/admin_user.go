package models

import (
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
)

// AdminUser represents a platform console admin account
type AdminUser struct {
	ID           uint           `gorm:"primaryKey" json:"-"`
	PublicID     uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex:idx_admin_users_public_id" json:"id"`
	TenantID     string         `gorm:"type:varchar(64);not null;default:default;index:idx_admin_users_tenant_id" json:"tenant_id"`
	Email        string         `gorm:"type:varchar(255);uniqueIndex;not null" json:"email"`
	Role         string         `gorm:"type:varchar(50);not null" json:"role"`
	PasswordHash string         `gorm:"type:text;not null" json:"-"`
	Permissions  pq.StringArray `gorm:"type:text[]" json:"permissions,omitempty"`
	IsActive     bool           `gorm:"default:true" json:"is_active"`
	CreatedAt    time.Time      `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt    time.Time      `gorm:"autoUpdateTime" json:"updated_at"`
}

// TableName returns the table name for AdminUser
func (AdminUser) TableName() string {
	return "admin_users"
}
