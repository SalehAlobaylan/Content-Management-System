package models

import "time"

// StoragePolicy controls auto-circulation behavior for a tenant.
// A row with TenantID == nil is the global default. A row with a non-nil
// TenantID is a per-tenant override that fully replaces the global one
// for that tenant.
type StoragePolicy struct {
	ID       uint    `gorm:"primaryKey" json:"id"`
	TenantID *string `gorm:"type:varchar(64);uniqueIndex:idx_storage_policy_tenant" json:"tenant_id,omitempty"`

	Enabled                 bool  `gorm:"default:false" json:"enabled"`
	MaxStorageBytes         int64 `gorm:"type:bigint;default:5368709120" json:"max_storage_bytes"`
	TargetUtilizationPct    int   `gorm:"default:80" json:"target_utilization_pct"`
	MinAgeDays              int   `gorm:"default:14" json:"min_age_days"`
	MinViewCountForKeep     int   `gorm:"default:5" json:"min_view_count_for_keep"`
	SweepIntervalMinutes    int   `gorm:"default:60" json:"sweep_interval_minutes"`
	DeleteFailedImmediately bool  `gorm:"default:true" json:"delete_failed_immediately"`
	PreserveThumbnails      bool  `gorm:"default:true" json:"preserve_thumbnails"`

	// Hot-content protection: items in the top-N by view_count within the last
	// `ProtectTopNWindowDays` days are excluded from circulation regardless of
	// age/view thresholds. Set ProtectTopNByViews to 0 to disable.
	ProtectTopNByViews    int `gorm:"default:50" json:"protect_top_n_by_views"`
	ProtectTopNWindowDays int `gorm:"default:30" json:"protect_top_n_window_days"`

	// What to do with eligible content when a sweep fires.
	//   "delete"       — remove from primary storage, mark ARCHIVED
	//   "move_to_cold" — copy to the cold-tier bucket, then remove from primary,
	//                    keep status READY but set storage_tier='cold'
	ArchiveAction string `gorm:"type:varchar(20);default:'delete'" json:"archive_action"`

	LastSweepAt *time.Time `gorm:"type:timestamp" json:"last_sweep_at,omitempty"`
	UpdatedAt   time.Time  `gorm:"autoUpdateTime" json:"updated_at"`
	CreatedAt   time.Time  `gorm:"autoCreateTime" json:"created_at"`
}

func (StoragePolicy) TableName() string {
	return "storage_policies"
}

// StorageSweepRun is one circulation execution. Written by the Aggregation
// worker through the internal API after each tick.
type StorageSweepRun struct {
	ID           uint       `gorm:"primaryKey" json:"id"`
	TenantID     string     `gorm:"type:varchar(64);not null;index:idx_storage_sweep_runs_tenant" json:"tenant_id"`
	StartedAt    time.Time  `gorm:"type:timestamp;not null" json:"started_at"`
	FinishedAt   *time.Time `gorm:"type:timestamp" json:"finished_at,omitempty"`
	DeletedCount int        `gorm:"default:0" json:"deleted_count"`
	FreedBytes   int64      `gorm:"type:bigint;default:0" json:"freed_bytes"`
	Trigger      string     `gorm:"type:varchar(20);default:'auto'" json:"trigger"` // auto | manual
	Error        string     `gorm:"type:text" json:"error,omitempty"`
}

func (StorageSweepRun) TableName() string {
	return "storage_sweep_runs"
}
