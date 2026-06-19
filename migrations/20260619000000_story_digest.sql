-- Slice 8 — per-story AI digest (source-grounded headline + bullets).
--
-- topics.summary / bullets / summary_built_at: a write-time LLM digest of the
--   story's members (one-line lede + JSON array of bullets). NULL = not yet
--   digested; the feed falls back to headline + lead-member excerpt.
-- ranking_configs.story_summary_*: admin-tunable gates (Config Discipline).
-- (Dev gets these via GORM AutoMigrate; this migration is for prod / AUTO_MIGRATE off.)

ALTER TABLE topics
    ADD COLUMN IF NOT EXISTS summary TEXT,
    ADD COLUMN IF NOT EXISTS bullets JSONB,
    ADD COLUMN IF NOT EXISTS summary_built_at TIMESTAMP;

CREATE INDEX IF NOT EXISTS idx_topics_summary_built_at ON topics (summary_built_at);

ALTER TABLE ranking_configs
    ADD COLUMN IF NOT EXISTS story_summary_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS story_summary_min_members INTEGER NOT NULL DEFAULT 3,
    ADD COLUMN IF NOT EXISTS story_summary_min_interval_minutes INTEGER NOT NULL DEFAULT 30;
