-- Feeds Finding Slice 2 — semantic relevance scoring + per-source category.
--
-- discovery_profiles.embedding: cached Qwen vector of (name + description +
-- keywords), used to score candidate relevance (cosine vs candidate sample).
-- content_sources.category / source_suggestions.category: 'news' | 'media' —
-- so a dual-type source (e.g. a Telegram channel) declares which feed it serves
-- instead of appearing in both management surfaces.
-- (Dev gets all of this via GORM AutoMigrate; this migration is for prod.)

ALTER TABLE discovery_profiles
    ADD COLUMN IF NOT EXISTS embedding vector(1024);

ALTER TABLE content_sources
    ADD COLUMN IF NOT EXISTS category VARCHAR(16) NOT NULL DEFAULT 'news';

ALTER TABLE source_suggestions
    ADD COLUMN IF NOT EXISTS category VARCHAR(16) NOT NULL DEFAULT 'news';

-- Backfill: media sources are YouTube/podcast; everything else is news.
UPDATE content_sources SET category = 'media' WHERE type IN ('YOUTUBE', 'PODCAST');
