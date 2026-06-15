-- Feeds Finding Slice 5 — Telegram source discovery.
--
-- source_candidates.kind: 'rss' | 'telegram' — a telegram candidate is a channel
--   (domain = username, resolved_feed_url = https://t.me/<username>).
-- discovery_configs.telegram_discovery_enabled: gate the Telegram contributor.
-- (Dev gets this via GORM AutoMigrate; this migration is for prod.)

ALTER TABLE source_candidates
    ADD COLUMN IF NOT EXISTS kind VARCHAR(16) NOT NULL DEFAULT 'rss';

ALTER TABLE discovery_configs
    ADD COLUMN IF NOT EXISTS telegram_discovery_enabled BOOLEAN NOT NULL DEFAULT FALSE;
