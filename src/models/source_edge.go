package models

import "time"

// SourceEdge is one directed citation edge (from_host links to to_host) in the
// tenant's source graph. Built from crawling approved sources' pages + content
// citations; authority (PageRank) is recomputed from these edges.
type SourceEdge struct {
	ID        uint      `gorm:"primaryKey" json:"-"`
	TenantID  string    `gorm:"type:varchar(64);not null;default:default;uniqueIndex:idx_source_edges_tenant_pair,priority:1" json:"tenant_id"`
	FromHost  string    `gorm:"type:varchar(255);not null;uniqueIndex:idx_source_edges_tenant_pair,priority:2" json:"from_host"`
	ToHost    string    `gorm:"type:varchar(255);not null;uniqueIndex:idx_source_edges_tenant_pair,priority:3" json:"to_host"`
	Weight    int       `gorm:"default:1" json:"weight"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

// TableName returns the table name for SourceEdge.
func (SourceEdge) TableName() string {
	return "source_edges"
}
