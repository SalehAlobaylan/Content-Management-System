package models

import (
	"time"

	"github.com/google/uuid"
)

// ContentFlag stores per-content-item editorial flags (separate table from content_items).
type ContentFlag struct {
	ID       uint      `gorm:"primaryKey" json:"-"`
	TenantID string    `gorm:"type:varchar(64);not null;index:idx_content_flags_tenant" json:"tenant_id"`
	PublicID uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`

	// Reference to content item (public_id)
	ContentItemID uuid.UUID `gorm:"type:uuid;not null;uniqueIndex:idx_content_flags_content_tenant,priority:1" json:"content_item_id"`

	// Editorial flags
	Boost           bool    `gorm:"default:false" json:"boost"`
	Suppress        bool    `gorm:"default:false" json:"suppress"`
	PinToTop        bool    `gorm:"default:false" json:"pin_to_top"`
	ExcludeFromFeed bool    `gorm:"default:false" json:"exclude_from_feed"`
	BoostMultiplier float64 `gorm:"type:double precision;default:1.5" json:"boost_multiplier"`

	// Audit
	Notes string `gorm:"type:text" json:"notes,omitempty"`
	SetBy string `gorm:"type:varchar(255)" json:"set_by,omitempty"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (ContentFlag) TableName() string {
	return "content_flags"
}
