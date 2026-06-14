package models

import (
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"gorm.io/datatypes"
)

// SourceCandidate statuses.
const (
	CandidateStatusCandidate = "candidate" // in the ledger, not yet surfaced
	CandidateStatusPromoted  = "promoted"  // surfaced as a source_suggestion
	CandidateStatusApproved  = "approved"
	CandidateStatusRejected  = "rejected"
)

// SourceCandidate is the persistent ledger of candidate news domains discovered
// from your trusted graph (corpus citations + link-graph). It accumulates
// signals across graph-build runs; high-scoring on-topic candidates auto-promote
// into the source_suggestions review queue.
type SourceCandidate struct {
	ID       uint      `gorm:"primaryKey" json:"-"`
	PublicID uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex:idx_source_candidates_public_id" json:"id"`
	TenantID string    `gorm:"type:varchar(64);not null;default:default;uniqueIndex:idx_source_candidates_tenant_domain,priority:1" json:"tenant_id"`

	Domain       string `gorm:"type:varchar(255);not null;uniqueIndex:idx_source_candidates_tenant_domain,priority:2" json:"domain"`
	CanonicalKey string `gorm:"type:text" json:"canonical_key"`

	// Resolution (a candidate becomes promotable once its feed is found + valid).
	ResolvedFeedURL *string    `gorm:"type:text" json:"resolved_feed_url,omitempty"`
	FeedValid       bool       `gorm:"default:false" json:"feed_valid"`
	LastResolvedAt  *time.Time `gorm:"type:timestamp" json:"last_resolved_at,omitempty"`

	// Graph signals.
	CitationCount   int     `gorm:"default:0" json:"citation_count"`   // times your content cites it
	CocitationCount int     `gorm:"default:0" json:"cocitation_count"` // distinct approved sources linking it
	AuthorityScore  float64 `gorm:"type:double precision;default:0" json:"authority_score"`
	Trend           string  `gorm:"type:varchar(12);default:flat" json:"trend"` // rising | flat | falling
	CompositeScore  float64 `gorm:"type:double precision;default:0" json:"composite_score"`

	Status        string         `gorm:"type:varchar(16);not null;default:candidate;index:idx_source_candidates_status" json:"status"`
	DiscoveredVia pq.StringArray `gorm:"type:text[]" json:"discovered_via"`

	SampleTitles datatypes.JSON `gorm:"type:jsonb" json:"sample_titles,omitempty"`
	FeedHealth   datatypes.JSON `gorm:"type:jsonb" json:"feed_health,omitempty"`
	// Evidence is the signal snapshot shown in the review UI.
	Evidence datatypes.JSON `gorm:"type:jsonb" json:"evidence,omitempty"`

	FirstSeenAt time.Time `gorm:"autoCreateTime" json:"first_seen_at"`
	LastSeenAt  time.Time `gorm:"autoUpdateTime" json:"last_seen_at"`
}

// TableName returns the table name for SourceCandidate.
func (SourceCandidate) TableName() string {
	return "source_candidates"
}
