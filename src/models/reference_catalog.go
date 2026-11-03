package models

import "time"

// ReferenceCatalogBinding maps a portable key to one destination-local record.
type ReferenceCatalogBinding struct {
	ID             uint `gorm:"primaryKey"`
	CatalogName    string
	ScopeKey       string
	Kind           string
	CatalogKey     string
	TargetID       string
	Projection     []byte `gorm:"type:jsonb"`
	DefinitionHash string
	ReleaseVersion string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

func (ReferenceCatalogBinding) TableName() string { return "reference_catalog_bindings" }

// ReferenceCatalogApplication is committed atomically with its definition effects.
type ReferenceCatalogApplication struct {
	OperationID    string `gorm:"primaryKey;type:uuid"`
	PlanDigest     string
	CatalogHash    string
	ReleaseVersion string
	Target         string
	TenantID       string
	Actor          string
	Evidence       []byte `gorm:"type:jsonb"`
	CommittedAt    time.Time
}

func (ReferenceCatalogApplication) TableName() string { return "reference_catalog_applications" }
