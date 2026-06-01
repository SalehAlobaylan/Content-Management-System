-- First-class topics: meaningful, LLM-labeled topics with a centroid embedding.
--
-- Replaces the free-form keyword grouping (UNNEST(topic_tags)) for the News
-- manager. Each article is classified into ONE primary topic by embedding
-- similarity (pgvector cosine `<=>`); when no existing topic is close enough,
-- Enrichment's /v1/topics/label writes a meaningful sentence label and a new
-- topic is created with the article's embedding as its initial centroid.

CREATE TABLE IF NOT EXISTS topics (
    id            SERIAL PRIMARY KEY,
    public_id     UUID NOT NULL DEFAULT gen_random_uuid(),
    tenant_id     VARCHAR(64) NOT NULL DEFAULT 'default',
    label         TEXT NOT NULL,
    embedding     vector(1024),
    article_count INTEGER NOT NULL DEFAULT 0,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_topics_public_id ON topics (public_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_topics_tenant_label ON topics (tenant_id, label);
CREATE INDEX IF NOT EXISTS idx_topics_tenant ON topics (tenant_id);

-- HNSW cosine index for fast nearest-topic classification.
CREATE INDEX IF NOT EXISTS topics_embedding_idx
    ON topics USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);

-- One primary topic per article.
ALTER TABLE content_items
    ADD COLUMN IF NOT EXISTS topic_id UUID;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'content_items_topic_id_fkey'
    ) THEN
        ALTER TABLE content_items
            ADD CONSTRAINT content_items_topic_id_fkey
            FOREIGN KEY (topic_id) REFERENCES topics(public_id) ON DELETE SET NULL;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_content_items_topic_id ON content_items (topic_id);
