-- Phase 3, Feature 5 — CLIP image embeddings.
--
-- Adds a 512-dim image embedding column on content_items, populated by
-- Enrichment-Service via PATCH /internal/content-items/:id/image-embedding.
-- Independent from the existing 384-dim text Embedding column; both coexist.
--
-- Powers: "find similar by image", near-duplicate hero detection, and
-- (future) cross-modal text↔image search.

ALTER TABLE content_items
    ADD COLUMN IF NOT EXISTS image_embedding vector(512);

-- IVFFlat index for cosine-similarity search. lists=100 is the typical
-- starting point for tables up to ~1M rows; revisit after row count grows.
CREATE INDEX IF NOT EXISTS content_items_image_embedding_idx
    ON content_items
    USING ivfflat (image_embedding vector_cosine_ops)
    WITH (lists = 100);
