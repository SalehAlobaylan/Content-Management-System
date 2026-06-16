-- Feeds Finding Slice 7 — Twitter/X source discovery.
--
-- source_candidates.kind already accepts 'twitter' (varchar(16), no DDL needed).
-- discovery_configs.twitter_discovery_enabled: gate the X interaction-graph contributor.
-- (Dev gets this via GORM AutoMigrate; this migration is for prod where AUTO_MIGRATE is off.)

ALTER TABLE discovery_configs
    ADD COLUMN IF NOT EXISTS twitter_discovery_enabled BOOLEAN NOT NULL DEFAULT FALSE;
