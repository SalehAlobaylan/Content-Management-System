-- Phase 13 Slices 3 & 5: story clustering threshold, News-feed mode toggle, and
-- the precomputed story-slide snapshot table.
--
-- (In dev these are created by GORM AutoMigrate; this migration is for prod,
-- where AutoMigrate is disabled.)

-- ranking_configs: Phase-13 story + feed-mode knobs.
ALTER TABLE ranking_configs
    ADD COLUMN IF NOT EXISTS story_match_threshold double precision DEFAULT 0.70;
ALTER TABLE ranking_configs
    ADD COLUMN IF NOT EXISTS news_feed_mode varchar(20) DEFAULT 'precompute';
ALTER TABLE ranking_configs
    ADD COLUMN IF NOT EXISTS news_rerank_enabled boolean DEFAULT false;

-- Precomputed News-feed story-slide snapshot (served off the read path in
-- precompute mode). One row per tenant.
CREATE TABLE IF NOT EXISTS news_snapshots (
    id          bigserial PRIMARY KEY,
    tenant_id   varchar(64) NOT NULL,
    slides      jsonb,
    slide_count integer DEFAULT 0,
    built_at    timestamptz DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_news_snapshot_tenant ON news_snapshots (tenant_id);
