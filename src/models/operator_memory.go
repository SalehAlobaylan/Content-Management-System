package models

import (
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/pgvector/pgvector-go"
)

// OperatorKnowledgeDocument and OperatorResolvedCase are reviewed CMS-owned
// records. Their chunks are retrieval aids only; neither is current evidence.
type OperatorKnowledgeDocument struct {
	ID                  uint           `gorm:"primaryKey" json:"-"`
	PublicID            uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID            string         `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	Key                 string         `gorm:"type:varchar(160);not null" json:"key"`
	Version             int            `gorm:"not null" json:"version"`
	Title               string         `gorm:"type:text;not null" json:"title"`
	Status              string         `gorm:"type:varchar(24);not null" json:"status"`
	RequiredPermissions pq.StringArray `gorm:"type:text[];not null;default:'{}'" json:"required_permissions"`
	SpaceID             string         `gorm:"type:char(64);not null" json:"space_id"`
	SourceVersion       string         `gorm:"type:varchar(160);not null" json:"source_version"`
	ValidFrom           time.Time      `json:"valid_from"`
	ValidUntil          *time.Time     `json:"valid_until,omitempty"`
	ReviewedBy          string         `gorm:"type:varchar(255);not null" json:"reviewed_by"`
	ReviewedAt          time.Time      `json:"reviewed_at"`
	RetiredAt           *time.Time     `json:"retired_at,omitempty"`
	CreatedAt           time.Time      `json:"created_at"`
	UpdatedAt           time.Time      `json:"updated_at"`
}

func (OperatorKnowledgeDocument) TableName() string { return "operator_knowledge_documents" }

type OperatorKnowledgeChunk struct {
	ID         uint            `gorm:"primaryKey" json:"-"`
	DocumentID uint            `gorm:"not null;index" json:"-"`
	TenantID   string          `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	Ordinal    int             `gorm:"not null" json:"ordinal"`
	Content    string          `gorm:"type:text;not null" json:"content"`
	Embedding  pgvector.Vector `gorm:"type:vector(1024);not null" json:"-"`
	SpaceID    string          `gorm:"type:char(64);not null;index" json:"space_id"`
	CreatedAt  time.Time       `json:"created_at"`
}

func (OperatorKnowledgeChunk) TableName() string { return "operator_knowledge_chunks" }

type OperatorResolvedCase struct {
	ID                  uint           `gorm:"primaryKey" json:"-"`
	PublicID            uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID            string         `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	Key                 string         `gorm:"type:varchar(160);not null" json:"key"`
	Version             int            `gorm:"not null" json:"version"`
	Title               string         `gorm:"type:text;not null" json:"title"`
	Status              string         `gorm:"type:varchar(24);not null" json:"status"`
	RequiredPermissions pq.StringArray `gorm:"type:text[];not null;default:'{}'" json:"required_permissions"`
	SpaceID             string         `gorm:"type:char(64);not null" json:"space_id"`
	Provenance          string         `gorm:"type:text;not null" json:"provenance"`
	ValidFrom           time.Time      `json:"valid_from"`
	ValidUntil          *time.Time     `json:"valid_until,omitempty"`
	ReviewedBy          string         `gorm:"type:varchar(255);not null" json:"reviewed_by"`
	ReviewedAt          time.Time      `json:"reviewed_at"`
	RetiredAt           *time.Time     `json:"retired_at,omitempty"`
	CreatedAt           time.Time      `json:"created_at"`
	UpdatedAt           time.Time      `json:"updated_at"`
}

func (OperatorResolvedCase) TableName() string { return "operator_resolved_cases" }

type OperatorResolvedCaseChunk struct {
	ID        uint            `gorm:"primaryKey" json:"-"`
	CaseID    uint            `gorm:"not null;index" json:"-"`
	TenantID  string          `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	Ordinal   int             `gorm:"not null" json:"ordinal"`
	Content   string          `gorm:"type:text;not null" json:"content"`
	Embedding pgvector.Vector `gorm:"type:vector(1024);not null" json:"-"`
	SpaceID   string          `gorm:"type:char(64);not null;index" json:"space_id"`
	CreatedAt time.Time       `json:"created_at"`
}

func (OperatorResolvedCaseChunk) TableName() string { return "operator_resolved_case_chunks" }
