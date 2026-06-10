package models

import (
	"time"

	"github.com/google/uuid"
	"github.com/pgvector/pgvector-go"
)

// Topic is a first-class, meaningful news topic. The Label is an
// LLM-generated sentence (in the content's language); the Embedding is the
// centroid (running mean) of the dense embeddings of its member articles.
// Articles point at a topic via content_items.topic_id.
type Topic struct {
	ID       uint      `gorm:"primaryKey" json:"-"`
	PublicID uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex:idx_topics_public_id" json:"id"`
	TenantID string    `gorm:"type:varchar(64);not null;index:idx_topics_tenant;uniqueIndex:idx_topics_tenant_label,priority:1" json:"tenant_id"`

	Label     string           `gorm:"type:text;not null;uniqueIndex:idx_topics_tenant_label,priority:2" json:"label"`
	Embedding *pgvector.Vector `gorm:"type:vector(1024)" json:"-"`

	ArticleCount int `gorm:"default:0" json:"article_count"`

	// LastMemberAt is the publish time of the story's most recent member. It
	// drives the story activity window: an item only joins a story that was
	// active near the item's own publish time, so stories stay bounded to
	// their event instead of absorbing semantically-similar items forever.
	LastMemberAt *time.Time `gorm:"index:idx_topics_last_member_at" json:"last_member_at,omitempty"`

	// Labeled is false for fresh clusters from a full re-cluster pass that still
	// carry a placeholder name and await LLM labeling. Growing-taxonomy topics
	// are labeled at creation, so they are true.
	Labeled bool `gorm:"default:true" json:"labeled"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (Topic) TableName() string {
	return "topics"
}
