-- Media Finding (For You) — extends the shared Source Discovery spine to media.
--
-- Adds a `category` dimension to discovery_profiles (so a profile is scoped to
-- the news OR media hub) and four media contributor toggles to discovery_configs
-- (YouTube via InnerTube, podcasts via RSS/iTunes, plus the two scraped
-- owner-curated relation shelves gated behind a live probe).
--
-- source_candidates.kind is already free-text varchar(16); the new values
-- 'youtube' | 'podcast' need no schema change. Promotion is category-isolated in
-- code (CategoryForCandidateKind vs profile.category) — no cross-category bleed.
-- (Dev gets all of this via GORM AutoMigrate; this migration is for prod.)

ALTER TABLE discovery_profiles
    ADD COLUMN IF NOT EXISTS category VARCHAR(16) NOT NULL DEFAULT 'news';

ALTER TABLE discovery_configs
    ADD COLUMN IF NOT EXISTS youtube_discovery_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS podcast_discovery_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS youtube_related_enabled   BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS apple_related_enabled     BOOLEAN NOT NULL DEFAULT FALSE;
