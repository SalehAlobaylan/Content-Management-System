package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/pgvector/pgvector-go"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type internalCreateContentItemRequest struct {
	IdempotencyKey string                 `json:"idempotency_key"`
	Type           string                 `json:"type"`
	Source         string                 `json:"source"`
	Status         string                 `json:"status"`
	Title          string                 `json:"title"`
	BodyText       *string                `json:"body_text"`
	Excerpt        *string                `json:"excerpt"`
	Author         *string                `json:"author"`
	SourceName     string                 `json:"source_name"`
	SourceFeedURL  *string                `json:"source_feed_url"`
	OriginalURL    string                 `json:"original_url"`
	MediaURL       *string                `json:"media_url"`
	ThumbnailURL   *string                `json:"thumbnail_url"`
	DurationSec    *int                   `json:"duration_sec"`
	TopicTags      []string               `json:"topic_tags"`
	Metadata       map[string]interface{} `json:"metadata"`
	PublishedAt    *string                `json:"published_at"`
}

type internalCreateContentItemResponse struct {
	ID        string `json:"id"`
	Status    string `json:"status"`
	Created   bool   `json:"created"`
	CreatedAt string `json:"created_at"`
}

type internalUpdateContentItemRequest struct {
	Title       *string                `json:"title"`
	BodyText    *string                `json:"body_text"`
	Excerpt     *string                `json:"excerpt"`
	Author      *string                `json:"author"`
	SourceName  *string                `json:"source_name"`
	SourceFeed  *string                `json:"source_feed_url"`
	OriginalURL *string                `json:"original_url"`
	PublishedAt *string                `json:"published_at"`
	Metadata    map[string]interface{} `json:"metadata"`
}

type internalUpdateStatusRequest struct {
	Status        string  `json:"status"`
	FailureReason *string `json:"failure_reason"`
}

type internalUpdateArtifactsRequest struct {
	MediaURL      *string `json:"media_url"`
	ThumbnailURL  *string `json:"thumbnail_url"`
	DurationSec   *int    `json:"duration_sec"`
	FileSizeBytes *int64  `json:"file_size_bytes"`
	StorageTier   *string `json:"storage_tier"`

	// Quality bookkeeping. These are recorded once per item at first ingest;
	// the controller writes them only if the existing column is NULL.
	OriginalSizeBytes       *int64 `json:"original_size_bytes"`
	OriginalBitrateKbps     *int   `json:"original_bitrate_kbps"`
	CurrentBitrateKbps      *int   `json:"current_bitrate_kbps"`
	CurrentQualityProfileID *uint  `json:"current_quality_profile_id"`
}

type internalUpdateEmbeddingRequest struct {
	Embedding []float32 `json:"embedding"`
	// EmbeddingSparse is BGE-M3's learned sparse output: {token_id_string: weight}.
	// Optional — Slice 0 only sets the dense vector; Slice A starts populating
	// sparse once FlagEmbedding lands. JSON keys are stringified token IDs
	// (BGE-M3 returns them that way); converted to pgvector.SparseVector below.
	EmbeddingSparse map[string]float32 `json:"embedding_sparse"`
	TopicTags       []string           `json:"topic_tags"`
}

// bgeM3SparseDim is BGE-M3's vocabulary size — the dimension of its sparse
// output. Must match the sparsevec(N) column type in the schema.
const bgeM3SparseDim int32 = 250002

// textEmbeddingDim is the dense embedding length BGE-M3 produces. Mirrors
// the strict-dimension check on image embeddings (CLIP at 512).
const textEmbeddingDim = 1024

type internalUpdateImageEmbeddingRequest struct {
	Embedding []float32 `json:"embedding"`
}

type internalLinkTranscriptRequest struct {
	TranscriptID string `json:"transcript_id"`
}

const maxIdempotencyKeyLength = 512

func normalizeIdempotencyKey(key string) string {
	normalized := strings.TrimSpace(key)
	if utf8.RuneCountInString(normalized) <= maxIdempotencyKeyLength {
		return normalized
	}

	// Keep deterministic de-duplication for very long URLs/keys without DB length errors.
	sum := sha256.Sum256([]byte(normalized))
	return "sha256:" + hex.EncodeToString(sum[:])
}

// InternalCreateContentItem handles POST /internal/content-items
func InternalCreateContentItem(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	var req internalCreateContentItemRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	if strings.TrimSpace(req.IdempotencyKey) == "" || req.Type == "" || req.Source == "" || req.Status == "" || req.Title == "" || req.OriginalURL == "" || req.SourceName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Missing required fields"})
		return
	}

	idempotencyKey := normalizeIdempotencyKey(req.IdempotencyKey)

	// Check for existing item by idempotency key
	var existing models.ContentItem
	if err := db.Where("idempotency_key = ?", idempotencyKey).First(&existing).Error; err == nil {
		c.JSON(http.StatusOK, internalCreateContentItemResponse{
			ID:        existing.PublicID.String(),
			Status:    string(existing.Status),
			Created:   false,
			CreatedAt: existing.CreatedAt.UTC().Format(time.RFC3339),
		})
		return
	} else if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to check idempotency"})
		return
	}

	var publishedAt *time.Time
	if req.PublishedAt != nil && *req.PublishedAt != "" {
		if parsed, err := time.Parse(time.RFC3339, *req.PublishedAt); err == nil {
			publishedAt = &parsed
		}
	}

	metadataJSON, _ := json.Marshal(req.Metadata)

	item := models.ContentItem{
		Type:           models.ContentType(strings.ToUpper(req.Type)),
		Source:         models.SourceType(strings.ToUpper(req.Source)),
		Status:         models.ContentStatus(strings.ToUpper(req.Status)),
		IdempotencyKey: &idempotencyKey,
		Title:          &req.Title,
		BodyText:       req.BodyText,
		Excerpt:        req.Excerpt,
		Author:         req.Author,
		SourceName:     &req.SourceName,
		SourceFeedURL:  req.SourceFeedURL,
		MediaURL:       req.MediaURL,
		ThumbnailURL:   req.ThumbnailURL,
		OriginalURL:    &req.OriginalURL,
		DurationSec:    req.DurationSec,
		TopicTags:      req.TopicTags,
		Metadata:       datatypes.JSON(metadataJSON),
		PublishedAt:    publishedAt,
	}

	if err := db.Create(&item).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create content item"})
		return
	}

	c.JSON(http.StatusOK, internalCreateContentItemResponse{
		ID:        item.PublicID.String(),
		Status:    string(item.Status),
		Created:   true,
		CreatedAt: item.CreatedAt.UTC().Format(time.RFC3339),
	})
}

// InternalUpdateContentItem handles PUT /internal/content-items/:id
func InternalUpdateContentItem(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	publicID := c.Param("id")
	id, err := uuid.Parse(publicID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid content ID"})
		return
	}

	var req internalUpdateContentItemRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	var item models.ContentItem
	if err := db.Where("public_id = ?", id).First(&item).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Content not found"})
		return
	}

	if req.Title != nil {
		item.Title = req.Title
	}
	if req.BodyText != nil {
		item.BodyText = req.BodyText
	}
	if req.Excerpt != nil {
		item.Excerpt = req.Excerpt
	}
	if req.Author != nil {
		item.Author = req.Author
	}
	if req.SourceName != nil {
		item.SourceName = req.SourceName
	}
	if req.SourceFeed != nil {
		item.SourceFeedURL = req.SourceFeed
	}
	if req.OriginalURL != nil {
		item.OriginalURL = req.OriginalURL
	}
	if req.PublishedAt != nil && *req.PublishedAt != "" {
		if parsed, err := time.Parse(time.RFC3339, *req.PublishedAt); err == nil {
			item.PublishedAt = &parsed
		}
	}
	if req.Metadata != nil {
		if raw, err := json.Marshal(req.Metadata); err == nil {
			item.Metadata = datatypes.JSON(raw)
		}
	}

	if err := db.Save(&item).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update content item"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true})
}

// InternalUpdateContentStatus handles PATCH /internal/content-items/:id/status
func InternalUpdateContentStatus(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	publicID := c.Param("id")
	id, err := uuid.Parse(publicID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid content ID"})
		return
	}

	var req internalUpdateStatusRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	if req.Status == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Status is required"})
		return
	}

	var item models.ContentItem
	if err := db.Where("public_id = ?", id).First(&item).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Content not found"})
		return
	}

	item.Status = models.ContentStatus(strings.ToUpper(req.Status))

	if req.FailureReason != nil {
		metadata := map[string]interface{}{}
		if len(item.Metadata) > 0 {
			_ = json.Unmarshal(item.Metadata, &metadata)
		}
		metadata["failure_reason"] = *req.FailureReason
		if raw, err := json.Marshal(metadata); err == nil {
			item.Metadata = datatypes.JSON(raw)
		}
	}

	if err := db.Save(&item).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update status"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true})
}

// InternalUpdateContentArtifacts handles PATCH /internal/content-items/:id/artifacts
func InternalUpdateContentArtifacts(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	publicID := c.Param("id")
	id, err := uuid.Parse(publicID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid content ID"})
		return
	}

	var req internalUpdateArtifactsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	var item models.ContentItem
	if err := db.Where("public_id = ?", id).First(&item).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Content not found"})
		return
	}

	if req.MediaURL != nil {
		item.MediaURL = req.MediaURL
	}
	if req.ThumbnailURL != nil {
		item.ThumbnailURL = req.ThumbnailURL
	}
	if req.DurationSec != nil {
		item.DurationSec = req.DurationSec
	}
	if req.FileSizeBytes != nil {
		item.FileSizeBytes = *req.FileSizeBytes
	}
	if req.StorageTier != nil {
		val := strings.ToLower(strings.TrimSpace(*req.StorageTier))
		if val == "" || val == "primary" {
			item.StorageTier = nil
		} else {
			item.StorageTier = &val
		}
	}

	// Quality bookkeeping. Original* fields are write-once at first ingest.
	// Current* fields and the profile pointer can be updated freely (e.g. by
	// the quality re-encode worker).
	if req.OriginalSizeBytes != nil && item.OriginalSizeBytes == nil {
		v := *req.OriginalSizeBytes
		item.OriginalSizeBytes = &v
	}
	if req.OriginalBitrateKbps != nil && item.OriginalBitrateKbps == nil {
		v := *req.OriginalBitrateKbps
		item.OriginalBitrateKbps = &v
	}
	if req.CurrentBitrateKbps != nil {
		v := *req.CurrentBitrateKbps
		item.CurrentBitrateKbps = &v
	}
	if req.CurrentQualityProfileID != nil {
		v := *req.CurrentQualityProfileID
		item.CurrentQualityProfileID = &v
	}

	if err := db.Save(&item).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update artifacts"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true})
}

// InternalUpdateContentEmbedding handles PATCH /internal/content-items/:id/embedding
func InternalUpdateContentEmbedding(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	publicID := c.Param("id")
	id, err := uuid.Parse(publicID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid content ID"})
		return
	}

	var req internalUpdateEmbeddingRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	if len(req.Embedding) != textEmbeddingDim {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Text embedding must be " + strconv.Itoa(textEmbeddingDim) +
				"-dim (got " + strconv.Itoa(len(req.Embedding)) + ")",
		})
		return
	}

	var item models.ContentItem
	if err := db.Where("public_id = ?", id).First(&item).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Content not found"})
		return
	}

	vec := pgvector.NewVector(req.Embedding)
	item.Embedding = &vec

	// Sparse output is optional (Slice A populates it). Convert BGE-M3's
	// {token_id_string: weight} map to pgvector.SparseVector if supplied.
	if len(req.EmbeddingSparse) > 0 {
		elements := make(map[int32]float32, len(req.EmbeddingSparse))
		for k, v := range req.EmbeddingSparse {
			idx, parseErr := strconv.ParseInt(k, 10, 32)
			if parseErr != nil {
				c.JSON(http.StatusBadRequest, gin.H{
					"error": "embedding_sparse key '" + k + "' is not a valid token id",
				})
				return
			}
			elements[int32(idx)] = v
		}
		sparse := pgvector.NewSparseVectorFromMap(elements, bgeM3SparseDim)
		item.EmbeddingSparse = &sparse
	}

	if len(req.TopicTags) > 0 {
		item.TopicTags = req.TopicTags
	}

	if err := db.Save(&item).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update embedding"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true})
}

// InternalUpdateContentImageEmbedding handles PATCH /internal/content-items/:id/image-embedding.
// Stores a CLIP-ViT-B-32 image embedding (512-dim) on the content item.
// Independent from text Embedding (384-dim) — both can coexist.
func InternalUpdateContentImageEmbedding(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	publicID := c.Param("id")
	id, err := uuid.Parse(publicID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid content ID"})
		return
	}

	var req internalUpdateImageEmbeddingRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	if len(req.Embedding) != 512 {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Image embedding must be 512-dim (got " +
				strconv.Itoa(len(req.Embedding)) + ")",
		})
		return
	}

	var item models.ContentItem
	if err := db.Where("public_id = ?", id).First(&item).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Content not found"})
		return
	}

	vec := pgvector.NewVector(req.Embedding)
	item.ImageEmbedding = &vec

	if err := db.Save(&item).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update image embedding"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true})
}

// ─── Slice A: hybrid retrieval endpoints ────────────────────────────
//
// Three internal endpoints support Enrichment-Service's /v1/related:
//   1. InternalGetContentEmbeddings — fetch the (dense, sparse) tuple for
//      an anchor content_id so /v1/related can run hybrid kNN without
//      re-embedding what's already stored.
//   2. InternalKNNDense  — pgvector cosine kNN against `embedding`.
//   3. InternalKNNSparse — sparsevec inner-product kNN against
//      `embedding_sparse`.
//
// All three are POST (kNN payloads carry 1024-dim or larger vectors that
// don't belong in query strings) except the embeddings fetch, which is GET.
// Filtering by content_type and excluding the anchor + already-shown ids
// is built in.

type internalKNNDenseRequest struct {
	Embedding  []float32 `json:"embedding"`
	Types      []string  `json:"types"`       // optional — when empty, no type filter
	K          int       `json:"k"`           // required, >0
	ExcludeIDs []string  `json:"exclude_ids"` // optional public_ids to skip
}

type internalKNNSparseRequest struct {
	EmbeddingSparse map[string]float32 `json:"embedding_sparse"` // {token_id_str: weight}
	Types           []string           `json:"types"`
	K               int                `json:"k"`
	ExcludeIDs      []string           `json:"exclude_ids"`
}

type internalKNNHit struct {
	ID          string  `json:"id"`   // public_id (UUID string)
	Type        string  `json:"type"` // ContentType (TWEET, COMMENT, ARTICLE, ...)
	Score       float64 `json:"score"`
	// SourceName + PublishedAt let downstream ranking rules (source
	// diversity, freshness decay) run on the kNN results directly,
	// without a second round-trip to /internal/content-items/batch-text.
	// Critical for the rerank-disabled path where batch-text is skipped.
	SourceName  *string `json:"source_name,omitempty"`
	PublishedAt *string `json:"published_at,omitempty"`
}

type internalKNNResponse struct {
	Hits []internalKNNHit `json:"hits"`
}

type internalEmbeddingsResponse struct {
	Embedding       []float32          `json:"embedding"`        // 1024 dense, null if missing
	EmbeddingSparse map[string]float32 `json:"embedding_sparse"` // BGE-M3 sparse, null if missing
}

// InternalGetContentEmbeddings handles GET /internal/content-items/:id/embeddings.
// Returns the dense + sparse vectors for one content item so the caller
// (Enrichment /v1/related) can skip re-embedding when given an anchor id.
func InternalGetContentEmbeddings(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	publicID := c.Param("id")
	id, err := uuid.Parse(publicID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid content ID"})
		return
	}

	var item models.ContentItem
	if err := db.Where("public_id = ?", id).
		Select("embedding", "embedding_sparse").
		First(&item).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Content not found"})
		return
	}

	resp := internalEmbeddingsResponse{}
	if item.Embedding != nil {
		resp.Embedding = item.Embedding.Slice()
	}
	if item.EmbeddingSparse != nil {
		// Convert pgvector.SparseVector → BGE-M3 wire format {token_id_str: weight}.
		indices := item.EmbeddingSparse.Indices()
		values := item.EmbeddingSparse.Values()
		sparse := make(map[string]float32, len(indices))
		for i, idx := range indices {
			sparse[strconv.FormatInt(int64(idx), 10)] = values[i]
		}
		resp.EmbeddingSparse = sparse
	}
	c.JSON(http.StatusOK, resp)
}

// InternalKNNDense handles POST /internal/content-items/knn.
// Runs cosine-similarity kNN against the `embedding` HNSW index added in
// migration 20260522000000_bge_m3_retrieval.sql.
func InternalKNNDense(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	var req internalKNNDenseRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}
	if len(req.Embedding) != textEmbeddingDim {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Embedding must be " + strconv.Itoa(textEmbeddingDim) +
				"-dim (got " + strconv.Itoa(len(req.Embedding)) + ")",
		})
		return
	}
	if req.K <= 0 || req.K > 200 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "k must be in [1, 200]"})
		return
	}

	hits := runKNNQuery(db, "embedding", utils.PgvectorToLiteral(req.Embedding),
		req.Types, req.K, req.ExcludeIDs)
	c.JSON(http.StatusOK, internalKNNResponse{Hits: hits})
}

// InternalKNNSparse handles POST /internal/content-items/knn-sparse.
// Runs inner-product kNN against the `embedding_sparse` HNSW index. Sparse
// inputs are sent in BGE-M3's wire format ({token_id_str: weight}) — same
// shape InternalUpdateContentEmbedding accepts.
func InternalKNNSparse(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	var req internalKNNSparseRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}
	if len(req.EmbeddingSparse) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "embedding_sparse is required"})
		return
	}
	if req.K <= 0 || req.K > 200 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "k must be in [1, 200]"})
		return
	}

	// Convert BGE-M3 sparse map → pgvector.SparseVector (same conversion as
	// InternalUpdateContentEmbedding) then serialize to the literal form
	// pgvector accepts in raw SQL: '{idx1:val1,idx2:val2,…}/N'.
	elements := make(map[int32]float32, len(req.EmbeddingSparse))
	for k, v := range req.EmbeddingSparse {
		idx, parseErr := strconv.ParseInt(k, 10, 32)
		if parseErr != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "embedding_sparse key '" + k + "' is not a valid token id",
			})
			return
		}
		elements[int32(idx)] = v
	}
	sparse := pgvector.NewSparseVectorFromMap(elements, bgeM3SparseDim)

	hits := runKNNQuery(db, "embedding_sparse", sparse.String(),
		req.Types, req.K, req.ExcludeIDs)
	c.JSON(http.StatusOK, internalKNNResponse{Hits: hits})
}

// ─── Slice B: batch text fetch for the reranker stage ────────────────
//
// Reranker needs candidate text. kNN handlers return only {id, type, score}
// to keep the search payload lean; this endpoint fans the resulting id list
// back out into the full (title, excerpt, body_text, source_name, published_at)
// tuple for the small post-RRF candidate set (typically top-30).

type internalBatchTextRequest struct {
	IDs []string `json:"ids"`
}

type internalBatchTextItem struct {
	ID          string  `json:"id"`           // public_id (UUID string)
	Type        string  `json:"type"`
	Title       *string `json:"title"`
	Excerpt     *string `json:"excerpt"`
	BodyText    *string `json:"body_text"`
	SourceName  *string `json:"source_name"`
	PublishedAt *string `json:"published_at"` // ISO-8601, nil if missing
}

type internalBatchTextResponse struct {
	Items []internalBatchTextItem `json:"items"`
}

// Cap on a single batch — high enough to cover post-RRF candidate pools
// (RERANK_INPUT_K=30 by default) but low enough to bound payload size.
const batchTextMaxIDs = 200

// InternalBatchText handles POST /internal/content-items/batch-text.
// Returns text + metadata for the requested ids, used by Enrichment's
// reranker stage (Slice B). Order of items in the response is unspecified;
// caller looks them up by id.
func InternalBatchText(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	var req internalBatchTextRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}
	if len(req.IDs) == 0 {
		c.JSON(http.StatusOK, internalBatchTextResponse{Items: []internalBatchTextItem{}})
		return
	}
	if len(req.IDs) > batchTextMaxIDs {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "ids exceeds maximum batch size of " + strconv.Itoa(batchTextMaxIDs),
		})
		return
	}

	// Parse UUIDs; skip malformed ones silently. Caller may interleave
	// invalid ids without us bailing on the whole batch.
	parsed := make([]uuid.UUID, 0, len(req.IDs))
	for _, s := range req.IDs {
		if u, err := uuid.Parse(s); err == nil {
			parsed = append(parsed, u)
		}
	}
	if len(parsed) == 0 {
		c.JSON(http.StatusOK, internalBatchTextResponse{Items: []internalBatchTextItem{}})
		return
	}

	type row struct {
		PublicID    uuid.UUID
		Type        string
		Title       *string
		Excerpt     *string
		BodyText    *string
		SourceName  *string
		PublishedAt *time.Time
	}
	var rows []row
	if err := db.Model(&models.ContentItem{}).
		Where("public_id IN ?", parsed).
		Select("public_id, type, title, excerpt, body_text, source_name, published_at").
		Scan(&rows).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch batch text"})
		return
	}

	items := make([]internalBatchTextItem, 0, len(rows))
	for _, r := range rows {
		var publishedAtStr *string
		if r.PublishedAt != nil {
			s := r.PublishedAt.UTC().Format(time.RFC3339)
			publishedAtStr = &s
		}
		items = append(items, internalBatchTextItem{
			ID:          r.PublicID.String(),
			Type:        r.Type,
			Title:       r.Title,
			Excerpt:     r.Excerpt,
			BodyText:    r.BodyText,
			SourceName:  r.SourceName,
			PublishedAt: publishedAtStr,
		})
	}
	c.JSON(http.StatusOK, internalBatchTextResponse{Items: items})
}

// runKNNQuery is the shared GORM body for dense + sparse kNN.
// column is "embedding" or "embedding_sparse" — both indexes already exist.
// vecLiteral is the pgvector literal form (dense `[…]` or sparse `{…}/N`).
//
// Uses `<=>` (cosine distance) regardless of mode — pgvector's cosine
// operator works on both vector and sparsevec when the matching ops class
// is on the index. The RRF fusion in Enrichment only uses RANK, not raw
// scores, so cross-mode score scales don't need to match.
func runKNNQuery(db *gorm.DB, column, vecLiteral string, types []string, k int, excludeIDs []string) []internalKNNHit {
	q := db.Model(&models.ContentItem{}).
		Where("status = ?", models.ContentStatusReady).
		Where(column + " IS NOT NULL")

	if len(types) > 0 {
		q = q.Where("type IN ?", types)
	}
	if len(excludeIDs) > 0 {
		// Parse UUIDs once; skip invalid ones silently.
		parsed := make([]uuid.UUID, 0, len(excludeIDs))
		for _, s := range excludeIDs {
			if u, err := uuid.Parse(s); err == nil {
				parsed = append(parsed, u)
			}
		}
		if len(parsed) > 0 {
			q = q.Where("public_id NOT IN ?", parsed)
		}
	}

	type row struct {
		PublicID    uuid.UUID
		Type        string
		Distance    float64
		SourceName  *string
		PublishedAt *time.Time
	}
	var rows []row

	// Distance via the cosine operator; convert to score = 1 - distance so
	// higher is better. Both columns + their HNSW indexes are guarded by
	// `<column> IS NOT NULL`, so the planner uses the index. source_name
	// + published_at are pulled so callers can run freshness + diversity
	// rules without a second round-trip.
	err := q.Select("public_id, type, source_name, published_at, (" + column + " <=> '" + vecLiteral + "') AS distance").
		Order(column + " <=> '" + vecLiteral + "'").
		Limit(k).
		Scan(&rows).Error
	if err != nil {
		return nil
	}

	hits := make([]internalKNNHit, 0, len(rows))
	for _, r := range rows {
		var publishedAt *string
		if r.PublishedAt != nil {
			s := r.PublishedAt.UTC().Format(time.RFC3339)
			publishedAt = &s
		}
		hits = append(hits, internalKNNHit{
			ID:          r.PublicID.String(),
			Type:        r.Type,
			Score:       1.0 - r.Distance,
			SourceName:  r.SourceName,
			PublishedAt: publishedAt,
		})
	}
	return hits
}

// InternalLinkTranscript handles PATCH /internal/content-items/:id/transcript
func InternalLinkTranscript(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	publicID := c.Param("id")
	id, err := uuid.Parse(publicID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid content ID"})
		return
	}

	var req internalLinkTranscriptRequest
	if err := c.ShouldBindJSON(&req); err != nil || req.TranscriptID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "transcript_id is required"})
		return
	}

	transcriptUUID, err := uuid.Parse(req.TranscriptID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid transcript ID"})
		return
	}

	var item models.ContentItem
	if err := db.Where("public_id = ?", id).First(&item).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Content not found"})
		return
	}

	item.TranscriptID = &transcriptUUID

	if err := db.Save(&item).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to link transcript"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true})
}

type internalListContentItemResponse struct {
	ID          string                 `json:"id"`
	Type        string                 `json:"type"`
	Source      string                 `json:"source"`
	Status      string                 `json:"status"`
	OriginalURL string                 `json:"original_url"`
	Metadata    map[string]interface{} `json:"metadata"`
}

// InternalListContentItems handles GET /internal/content-items
// Supports ?status=FAILED&source=TELEGRAM&limit=100&page=1
func InternalListContentItems(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	status := strings.ToUpper(strings.TrimSpace(c.Query("status")))
	source := strings.ToUpper(strings.TrimSpace(c.Query("source")))

	limit := 100
	if l, err := strconv.Atoi(c.Query("limit")); err == nil && l > 0 && l <= 500 {
		limit = l
	}
	page := 1
	if p, err := strconv.Atoi(c.Query("page")); err == nil && p > 0 {
		page = p
	}
	offset := (page - 1) * limit

	query := db.Model(&models.ContentItem{})
	if status != "" {
		query = query.Where("status = ?", status)
	}
	if source != "" {
		query = query.Where("source = ?", source)
	}

	var total int64
	if err := query.Count(&total).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to count content items"})
		return
	}

	var items []models.ContentItem
	if err := query.Offset(offset).Limit(limit).Order("created_at DESC").Find(&items).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to list content items"})
		return
	}

	data := make([]internalListContentItemResponse, 0, len(items))
	for _, item := range items {
		var meta map[string]interface{}
		if item.Metadata != nil {
			_ = json.Unmarshal(item.Metadata, &meta)
		}
		originalURL := ""
		if item.OriginalURL != nil {
			originalURL = *item.OriginalURL
		}
		data = append(data, internalListContentItemResponse{
			ID:          item.PublicID.String(),
			Type:        string(item.Type),
			Source:      string(item.Source),
			Status:      string(item.Status),
			OriginalURL: originalURL,
			Metadata:    meta,
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"data":  data,
		"total": total,
		"page":  page,
		"limit": limit,
	})
}

// InternalGetContentItem handles GET /internal/content-items/:id
// Returns the fields the Aggregation quality worker needs to drive a
// re-encode: tier, current media URL, version, active profile id (for
// idempotency), current bitrate and duration. Auth: InternalAuthMiddleware.
func InternalGetContentItem(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid id"})
		return
	}
	var item models.ContentItem
	if err := db.Where("public_id = ?", id).First(&item).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Content not found"})
		return
	}
	// Serialize published_at as RFC3339 UTC so Enrichment's ISO parser
	// (datetime.fromisoformat after a Z→+00:00 swap) gets an aware datetime.
	var publishedAt *string
	if item.PublishedAt != nil {
		s := item.PublishedAt.UTC().Format(time.RFC3339)
		publishedAt = &s
	}
	c.JSON(http.StatusOK, gin.H{
		"id":        item.PublicID.String(),
		"tenant_id": item.TenantID,
		// Content type (TWEET/ARTICLE/…) — distinct from source_type below.
		// FeedNewsService anchors read this; without it the slide anchor's
		// type field is the empty string.
		"type": string(item.Type),
		// source_type is required by the quality re-encode auto-resolve path
		// — without it the resolver can never pick a source-scoped ingest
		// profile (e.g. "YouTube items use mobile-720p"). Stringified so
		// callers can match against the string values in QualityProfile.SourceType.
		"source_type":   string(item.Source),
		"title":         item.Title,
		"excerpt":       item.Excerpt,
		"source_name":   item.SourceName,
		"published_at":  publishedAt,
		"media_url":     item.MediaURL,
		"thumbnail_url": item.ThumbnailURL,
		"storage_tier":  item.StorageTier, // nil = primary
		"media_version": item.MediaVersion,
		"file_size_bytes":            item.FileSizeBytes,
		"current_quality_profile_id": item.CurrentQualityProfileID,
		"current_bitrate_kbps":       item.CurrentBitrateKbps,
		"duration_sec":               item.DurationSec,
	})
}
