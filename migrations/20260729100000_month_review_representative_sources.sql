-- Month in Review remediation: immutable representative source snapshots.

CREATE TABLE IF NOT EXISTS news_month_archive_story_sources (
    id BIGSERIAL PRIMARY KEY,
    public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    archive_story_id BIGINT NOT NULL REFERENCES news_month_archive_stories(id) ON DELETE RESTRICT,
    ordinal INTEGER NOT NULL CHECK (ordinal BETWEEN 1 AND 3),
    original_content_id UUID NOT NULL,
    source_id UUID,
    source_name TEXT NOT NULL,
    headline TEXT NOT NULL,
    original_url TEXT,
    published_at TIMESTAMPTZ,
    evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (archive_story_id, ordinal),
    UNIQUE (archive_story_id, original_content_id)
);
