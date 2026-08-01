package operator

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"content-management-system/src/models"

	"github.com/pgvector/pgvector-go"
	"gorm.io/gorm"
)

const maxMemoryCandidates = 50
const maxMemoryResults = 6

type MemoryEmbedder interface {
	EmbedQuery(context.Context, string) ([]float32, string, error)
	Rerank(context.Context, string, []string) ([]float64, error)
}
type MemoryHit struct {
	Kind, RecordID, Title, Content, SourceVersion string
	Score                                         float64
	RequiredPermissions                           []string
	ValidUntil                                    *time.Time
}
type MemoryStore struct {
	db  *gorm.DB
	now func() time.Time
}

// CreateReviewedKnowledge and CreateReviewedCase are intentionally CMS-local
// admission points. Callers provide already embedded, versioned chunks; this
// service refuses unpublished/empty material instead of allowing a model or
// retrieval result to self-train the corpus.
func (store *MemoryStore) CreateReviewedKnowledge(ctx context.Context, document models.OperatorKnowledgeDocument, chunks []models.OperatorKnowledgeChunk) error {
	if err := validateReviewedMemory(document.TenantID, document.Key, document.Version, document.Title, document.Status, document.SpaceID, document.ReviewedBy, document.ReviewedAt, chunks); err != nil {
		return err
	}
	return store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&document).Error; err != nil {
			return err
		}
		for i := range chunks {
			chunks[i].DocumentID = document.ID
			chunks[i].TenantID = document.TenantID
			chunks[i].SpaceID = document.SpaceID
		}
		return tx.Create(&chunks).Error
	})
}
func (store *MemoryStore) CreateReviewedCase(ctx context.Context, item models.OperatorResolvedCase, chunks []models.OperatorResolvedCaseChunk) error {
	if err := validateReviewedMemory(item.TenantID, item.Key, item.Version, item.Title, item.Status, item.SpaceID, item.ReviewedBy, item.ReviewedAt, caseChunksAsKnowledge(chunks)); err != nil {
		return err
	}
	return store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&item).Error; err != nil {
			return err
		}
		for i := range chunks {
			chunks[i].CaseID = item.ID
			chunks[i].TenantID = item.TenantID
			chunks[i].SpaceID = item.SpaceID
		}
		return tx.Create(&chunks).Error
	})
}
func (store *MemoryStore) RetireKnowledge(ctx context.Context, tenantID, key string, version int, at time.Time) error {
	return store.db.WithContext(ctx).Model(&models.OperatorKnowledgeDocument{}).Where("tenant_id=? AND key=? AND version=? AND status='reviewed'", tenantID, key, version).Updates(map[string]any{"status": "retired", "retired_at": at}).Error
}
func (store *MemoryStore) RetireCase(ctx context.Context, tenantID, key string, version int, at time.Time) error {
	return store.db.WithContext(ctx).Model(&models.OperatorResolvedCase{}).Where("tenant_id=? AND key=? AND version=? AND status='reviewed'", tenantID, key, version).Updates(map[string]any{"status": "retired", "retired_at": at}).Error
}
func validateReviewedMemory(tenant, key string, version int, title, status, space, reviewer string, reviewed time.Time, chunks []models.OperatorKnowledgeChunk) error {
	if strings.TrimSpace(tenant) == "" || strings.TrimSpace(key) == "" || version < 1 || strings.TrimSpace(title) == "" || status != "reviewed" || len(space) != 64 || strings.TrimSpace(reviewer) == "" || reviewed.IsZero() || len(chunks) == 0 || len(chunks) > maxMemoryCandidates {
		return fmt.Errorf("%w: invalid reviewed knowledge", ErrInvalidContract)
	}
	for i, c := range chunks {
		if c.Ordinal != i || strings.TrimSpace(c.Content) == "" || len(c.Embedding.Slice()) != 1024 {
			return fmt.Errorf("%w: invalid knowledge chunk", ErrInvalidContract)
		}
	}
	return nil
}
func caseChunksAsKnowledge(chunks []models.OperatorResolvedCaseChunk) []models.OperatorKnowledgeChunk {
	out := make([]models.OperatorKnowledgeChunk, len(chunks))
	for i, c := range chunks {
		out[i] = models.OperatorKnowledgeChunk{Ordinal: c.Ordinal, Content: c.Content, Embedding: c.Embedding}
	}
	return out
}

func NewMemoryStore(db *gorm.DB) *MemoryStore {
	return &MemoryStore{db: db, now: func() time.Time { return time.Now().UTC() }}
}

// Retrieve is CMS-authorized and returns only explicitly reviewed historical
// material. Callers must keep these hits labeled retrieved/memory, never live.
func (store *MemoryStore) Retrieve(ctx context.Context, access AccessSnapshot, query string, embedder MemoryEmbedder) ([]MemoryHit, string, error) {
	if store == nil || store.db == nil || embedder == nil || strings.TrimSpace(query) == "" || len(query) > 12000 || access.ValidateFor(access.UserID, access.TenantID) != nil {
		return nil, "", fmt.Errorf("%w: invalid memory retrieval", ErrInvalidContract)
	}
	vector, spaceID, err := embedder.EmbedQuery(ctx, query)
	if err != nil || len(vector) != 1024 || strings.TrimSpace(spaceID) == "" {
		return nil, "", fmt.Errorf("%w: query embedding unavailable", ErrInvalidContract)
	}
	now := store.now()
	hits := []MemoryHit{}
	var knowledge []models.OperatorKnowledgeChunk
	if err := store.db.WithContext(ctx).Joins("JOIN operator_knowledge_documents d ON d.id = operator_knowledge_chunks.document_id").Where("operator_knowledge_chunks.tenant_id=? AND operator_knowledge_chunks.space_id=? AND d.status='reviewed' AND d.valid_from<=? AND (d.valid_until IS NULL OR d.valid_until>?) AND d.retired_at IS NULL", access.TenantID, spaceID, now, now).Limit(maxMemoryCandidates).Find(&knowledge).Error; err != nil {
		return nil, "", err
	}
	for _, chunk := range knowledge {
		var d models.OperatorKnowledgeDocument
		if err := store.db.WithContext(ctx).First(&d, chunk.DocumentID).Error; err == nil && permissionsAllowed(access, d.RequiredPermissions) {
			hits = append(hits, MemoryHit{Kind: "knowledge", RecordID: d.PublicID.String(), Title: d.Title, Content: chunk.Content, SourceVersion: d.SourceVersion, Score: cosine(vector, chunk.Embedding), RequiredPermissions: d.RequiredPermissions, ValidUntil: d.ValidUntil})
		}
	}
	var cases []models.OperatorResolvedCaseChunk
	if err := store.db.WithContext(ctx).Joins("JOIN operator_resolved_cases c ON c.id = operator_resolved_case_chunks.case_id").Where("operator_resolved_case_chunks.tenant_id=? AND operator_resolved_case_chunks.space_id=? AND c.status='reviewed' AND c.valid_from<=? AND (c.valid_until IS NULL OR c.valid_until>?) AND c.retired_at IS NULL", access.TenantID, spaceID, now, now).Limit(maxMemoryCandidates).Find(&cases).Error; err != nil {
		return nil, "", err
	}
	for _, chunk := range cases {
		var c models.OperatorResolvedCase
		if err := store.db.WithContext(ctx).First(&c, chunk.CaseID).Error; err == nil && permissionsAllowed(access, c.RequiredPermissions) {
			hits = append(hits, MemoryHit{Kind: "resolved_case", RecordID: c.PublicID.String(), Title: c.Title, Content: chunk.Content, SourceVersion: c.Provenance, Score: cosine(vector, chunk.Embedding), RequiredPermissions: c.RequiredPermissions, ValidUntil: c.ValidUntil})
		}
	}
	sort.SliceStable(hits, func(i, j int) bool { return hits[i].Score > hits[j].Score })
	if len(hits) > 8 {
		hits = hits[:8]
	}
	if len(hits) == 0 {
		return nil, spaceID, nil
	}
	texts := make([]string, len(hits))
	for i := range hits {
		texts[i] = hits[i].Content
	}
	if scores, err := embedder.Rerank(ctx, query, texts); err == nil && len(scores) == len(hits) {
		for i := range hits {
			hits[i].Score = scores[i]
		}
		sort.SliceStable(hits, func(i, j int) bool { return hits[i].Score > hits[j].Score })
	}
	if len(hits) > maxMemoryResults {
		hits = hits[:maxMemoryResults]
	}
	return hits, spaceID, nil
}
func permissionsAllowed(access AccessSnapshot, required []string) bool {
	for _, p := range required {
		if !access.HasPermission(p) {
			return false
		}
	}
	return true
}
func cosine(query []float32, vector pgvector.Vector) float64 {
	values := vector.Slice()
	if len(values) != len(query) {
		return -1
	}
	var dot, a, b float64
	for i, v := range values {
		q := float64(query[i])
		x := float64(v)
		dot += q * x
		a += q * q
		b += x * x
	}
	if a == 0 || b == 0 {
		return -1
	}
	return dot / (a * b)
}
