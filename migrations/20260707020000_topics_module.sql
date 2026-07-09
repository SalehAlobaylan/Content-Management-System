-- Canonical CMS-owned topic catalog for user preferences and feed boosts.

CREATE TABLE IF NOT EXISTS topic_categories (
    id         BIGSERIAL PRIMARY KEY,
    tenant_id  VARCHAR(64) NOT NULL DEFAULT 'default',
    slug       TEXT NOT NULL,
    label_ar   TEXT NOT NULL,
    label_en   TEXT NOT NULL,
    sort_order INT NOT NULL DEFAULT 0,
    active     BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, slug)
);

CREATE TABLE IF NOT EXISTS topics (
    id            BIGSERIAL PRIMARY KEY,
    public_id     UUID NOT NULL DEFAULT gen_random_uuid(),
    tenant_id     VARCHAR(64) NOT NULL DEFAULT 'default',
    slug          TEXT NOT NULL,
    label_ar      TEXT NOT NULL,
    label_en      TEXT NOT NULL,
    category_slug TEXT,
    centroid      vector(1024),
    member_count  INT NOT NULL DEFAULT 0,
    active        BOOLEAN NOT NULL DEFAULT TRUE,
    featured      BOOLEAN NOT NULL DEFAULT FALSE,
    created_from  TEXT NOT NULL DEFAULT 'mined',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, slug)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_topics_public_id ON topics (public_id);
CREATE INDEX IF NOT EXISTS idx_topics_tenant ON topics (tenant_id);
CREATE INDEX IF NOT EXISTS idx_topics_category ON topics (tenant_id, category_slug);

CREATE TABLE IF NOT EXISTS topic_proposals (
    id                 BIGSERIAL PRIMARY KEY,
    tenant_id          VARCHAR(64) NOT NULL DEFAULT 'default',
    suggested_slug     TEXT NOT NULL,
    suggested_label_ar TEXT,
    suggested_label_en TEXT,
    suggested_category TEXT,
    evidence           JSONB,
    status             TEXT NOT NULL DEFAULT 'pending',
    merged_into        UUID,
    resolved_by        TEXT,
    resolved_at        TIMESTAMPTZ,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, suggested_slug)
);

CREATE INDEX IF NOT EXISTS idx_topic_proposals_status ON topic_proposals (tenant_id, status);

CREATE TABLE IF NOT EXISTS content_item_topics (
    content_item_id UUID NOT NULL,
    topic_id        UUID NOT NULL,
    score           DOUBLE PRECISION NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (content_item_id, topic_id)
);

CREATE INDEX IF NOT EXISTS idx_cit_topic ON content_item_topics (topic_id);
CREATE INDEX IF NOT EXISTS idx_cit_content ON content_item_topics (content_item_id);

CREATE TABLE IF NOT EXISTS story_topics (
    story_id   UUID NOT NULL,
    topic_id   UUID NOT NULL,
    score      DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (story_id, topic_id)
);

CREATE INDEX IF NOT EXISTS idx_story_topics_topic ON story_topics (topic_id);
CREATE INDEX IF NOT EXISTS idx_story_topics_story ON story_topics (story_id);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'content_item_topics_content_fkey'
    ) THEN
        ALTER TABLE content_item_topics
            ADD CONSTRAINT content_item_topics_content_fkey
            FOREIGN KEY (content_item_id) REFERENCES content_items(public_id) ON DELETE CASCADE;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'content_item_topics_topic_fkey'
    ) THEN
        ALTER TABLE content_item_topics
            ADD CONSTRAINT content_item_topics_topic_fkey
            FOREIGN KEY (topic_id) REFERENCES topics(public_id) ON DELETE CASCADE;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'story_topics_story_fkey'
    ) THEN
        ALTER TABLE story_topics
            ADD CONSTRAINT story_topics_story_fkey
            FOREIGN KEY (story_id) REFERENCES stories(public_id) ON DELETE CASCADE;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'story_topics_topic_fkey'
    ) THEN
        ALTER TABLE story_topics
            ADD CONSTRAINT story_topics_topic_fkey
            FOREIGN KEY (topic_id) REFERENCES topics(public_id) ON DELETE CASCADE;
    END IF;
END $$;
