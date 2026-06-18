-- Slice 0: BGE-M3 text embedder swap.
--
-- Switches text embedding from all-MiniLM-L6-v2 (384-dim, English-centric) to
-- BAAI/bge-m3 (1024-dim, multilingual, Arabic-native). Adds a forward-compatible
-- sparsevec column for future hybrid retrieval (Slice A populates it).
--
-- Dev DB is disposable; this migration is destructive on the `embedding` column.
-- Image embedding (CLIP, 512-dim) is intentionally untouched — it has its own
-- IVFFlat index from migration 20260521000000_image_embedding.sql.

-- 1. Drop the legacy 384-dim column. No index existed on it (original
--    migration's HNSW note was commented out — text embedding was unindexed).
ALTER TABLE content_items DROP COLUMN IF EXISTS embedding;

-- 2. Add BGE-M3 dense + future-sparse columns.
ALTER TABLE content_items
    ADD COLUMN embedding vector(1024),
    ADD COLUMN embedding_sparse sparsevec(250002);

-- 3. HNSW index on the dense column — better recall than IVFFlat at our scale
--    and no training-data requirement (works from the first insert). pgvector
--    >= 0.5.0 supports HNSW; the running ankane/pgvector:latest image is past
--    that threshold.
CREATE INDEX IF NOT EXISTS content_items_embedding_idx
    ON content_items USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);

-- 4. Sparse index — defined now so the contract is set; column stays NULL
--    until Slice A adds FlagEmbedding to Enrichment-Service and populates it.
--    Index on all-NULL is cheap.
CREATE INDEX IF NOT EXISTS content_items_embedding_sparse_idx
    ON content_items USING hnsw (embedding_sparse sparsevec_ip_ops);
