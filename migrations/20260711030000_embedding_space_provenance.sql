-- Embedding & Model Lifecycle System (stage 10) — Migration 1 of 3.
-- Immutable vector-space provenance + comparability stamping.
--
-- Canonical location: Content-Management-System/migrations/ (never
-- supabase/migrations/). Applied DELIBERATELY, not at startup. Dev boots may
-- AutoMigrate the added columns from the Go model tags; the indexes below are
-- the deliberate part.
--
-- WHAT THIS ADDS: two immutable identities per stored vector —
--   *_space_id     (char(64)): may these vectors be compared? (SHA-256 basis)
--   *_producer_id  (char(64)): must this surface be recomputed? (SHA-256 recipe)
-- plus a display model label on the surfaces that lacked one.
--
-- WHAT THIS DOES NOT DO — deliberately NO provenance backfill. A SQL migration
-- cannot prove which weights produced an existing vector, and stamping a guessed
-- identity is exactly the silent mixed-space failure this system exists to
-- prevent. Every pre-existing vector therefore keeps NULL space_id/producer_id
-- and surfaces as `unstamped_debt`, to be resolved by a human-started campaign.
-- (An optional display-only label backfill to 'legacy_assumed:*' is left
-- commented at the bottom; it never grants comparability.)

BEGIN;

-- ── content_items.embedding (text, 1024) ──────────────────────────────────────
-- embedding_model already exists (display label). Add the two identities.
ALTER TABLE content_items
    ADD COLUMN IF NOT EXISTS embedding_space_id    char(64),
    ADD COLUMN IF NOT EXISTS embedding_producer_id char(64);

-- ── content_items.image_embedding (image, 512) ───────────────────────────────
ALTER TABLE content_items
    ADD COLUMN IF NOT EXISTS image_embedding_model       varchar(80),
    ADD COLUMN IF NOT EXISTS image_embedding_space_id    char(64),
    ADD COLUMN IF NOT EXISTS image_embedding_producer_id char(64);

-- ── stories.embedding (story centroid, 1024) ─────────────────────────────────
ALTER TABLE stories
    ADD COLUMN IF NOT EXISTS embedding_model       varchar(80),
    ADD COLUMN IF NOT EXISTS embedding_space_id    char(64),
    ADD COLUMN IF NOT EXISTS embedding_producer_id char(64);

-- ── topics.centroid (topic centroid, 1024) ───────────────────────────────────
ALTER TABLE topics
    ADD COLUMN IF NOT EXISTS centroid_model       varchar(80),
    ADD COLUMN IF NOT EXISTS centroid_space_id    char(64),
    ADD COLUMN IF NOT EXISTS centroid_producer_id char(64);

-- ── topic_proposals.embedding (cache, 1024) ──────────────────────────────────
ALTER TABLE topic_proposals
    ADD COLUMN IF NOT EXISTS embedding_model       varchar(80),
    ADD COLUMN IF NOT EXISTS embedding_space_id    char(64),
    ADD COLUMN IF NOT EXISTS embedding_producer_id char(64);

-- ── discovery_profiles.embedding (profile, 1024) ─────────────────────────────
ALTER TABLE discovery_profiles
    ADD COLUMN IF NOT EXISTS embedding_model       varchar(80),
    ADD COLUMN IF NOT EXISTS embedding_space_id    char(64),
    ADD COLUMN IF NOT EXISTS embedding_producer_id char(64);

-- ── Indexes ───────────────────────────────────────────────────────────────────
-- Inventory lane scans (tenant, producer_id) to bucket current/stale; the
-- consumer/coherence lane scans (tenant, space_id). Partial on vector NOT NULL
-- because NULL-vector rows are coverage's lane, not lifecycle's. Shape confirmed
-- against the inventory/coherence predicates in §5; re-EXPLAIN before tuning.
CREATE INDEX IF NOT EXISTS idx_ci_text_producer
    ON content_items (tenant_id, embedding_producer_id) WHERE embedding IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_ci_text_space
    ON content_items (tenant_id, embedding_space_id) WHERE embedding IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_ci_image_producer
    ON content_items (tenant_id, image_embedding_producer_id) WHERE image_embedding IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_ci_image_space
    ON content_items (tenant_id, image_embedding_space_id) WHERE image_embedding IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_stories_emb_producer
    ON stories (tenant_id, embedding_producer_id) WHERE embedding IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_stories_emb_space
    ON stories (tenant_id, embedding_space_id) WHERE embedding IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_topics_centroid_producer
    ON topics (tenant_id, centroid_producer_id) WHERE centroid IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_topics_centroid_space
    ON topics (tenant_id, centroid_space_id) WHERE centroid IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_proposals_emb_producer
    ON topic_proposals (tenant_id, embedding_producer_id) WHERE embedding IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_discovery_emb_producer
    ON discovery_profiles (tenant_id, embedding_producer_id) WHERE embedding IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_discovery_emb_space
    ON discovery_profiles (tenant_id, embedding_space_id) WHERE embedding IS NOT NULL;

COMMIT;

-- Optional DISPLAY-ONLY label backfill (NEVER run to grant comparability).
-- Uncomment only if the cockpit should show a human hint for legacy rows; the
-- space_id/producer_id stay NULL so comparability guards still exclude them.
-- UPDATE content_items   SET embedding_model       = COALESCE(embedding_model, 'legacy_assumed:Qwen/Qwen3-Embedding-0.6B') WHERE embedding IS NOT NULL;
-- UPDATE content_items   SET image_embedding_model = 'legacy_assumed:clip-ViT-B-32' WHERE image_embedding IS NOT NULL;
-- UPDATE stories         SET embedding_model       = 'legacy_assumed:Qwen/Qwen3-Embedding-0.6B' WHERE embedding IS NOT NULL;
-- UPDATE topics          SET centroid_model        = 'legacy_assumed:Qwen/Qwen3-Embedding-0.6B' WHERE centroid IS NOT NULL;
-- UPDATE topic_proposals SET embedding_model       = 'legacy_assumed:Qwen/Qwen3-Embedding-0.6B' WHERE embedding IS NOT NULL;
-- UPDATE discovery_profiles SET embedding_model    = 'legacy_assumed:Qwen/Qwen3-Embedding-0.6B' WHERE embedding IS NOT NULL;
