package models

import "time"

// StorageOpMetric is one row per (date, tier, op_class, op_type, source).
// Each Aggregation hourly flush UPSERTs the row keyed by today's date —
// in-memory counters track the in-flight bucket; the persisted row is the
// source of truth for monthly totals.
//
// `source` distinguishes:
//   - 'internal'   : counted via the AWS SDK middleware in Aggregation
//                    (every PUT/HEAD/GET/LIST/DELETE we make ourselves).
//   - 'cloudflare' : pulled from Cloudflare Analytics GraphQL API (catches
//                    public CDN reads that bypass our backend).
//
// We keep both rather than merging server-side because they have different
// staleness characteristics (internal: real-time-ish, CF: ~30 min lag) and
// keeping them separate lets the dashboard show contribution per source.
type StorageOpMetric struct {
	ID       uint      `gorm:"primaryKey" json:"id"`
	TenantID string    `gorm:"type:varchar(64);not null;uniqueIndex:idx_op_metrics_unique,priority:1;index:idx_op_metrics_lookup,priority:1" json:"tenant_id"`
	Date     time.Time `gorm:"type:date;not null;uniqueIndex:idx_op_metrics_unique,priority:2;index:idx_op_metrics_lookup,priority:2" json:"date"`
	Tier     string    `gorm:"type:varchar(16);not null;uniqueIndex:idx_op_metrics_unique,priority:3" json:"tier"`     // primary | cold
	OpClass  string    `gorm:"type:varchar(2);not null;uniqueIndex:idx_op_metrics_unique,priority:4" json:"op_class"`  // A | B
	OpType   string    `gorm:"type:varchar(32);not null;uniqueIndex:idx_op_metrics_unique,priority:5" json:"op_type"`  // PUT, GET, HEAD, DELETE, DELETE_OBJECTS, LIST, COPY, OTHER
	Source   string    `gorm:"type:varchar(32);not null;uniqueIndex:idx_op_metrics_unique,priority:6" json:"source"`   // internal | cloudflare

	Count     int64     `gorm:"default:0" json:"count"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (StorageOpMetric) TableName() string {
	return "storage_op_metrics"
}
