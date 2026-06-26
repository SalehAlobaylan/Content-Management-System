-- Storage guard for media discovery — cap how many episodes/videos a newly
-- approved media source pulls on first fetch, so a deep podcast back-catalog
-- doesn't flood ingestion + S3. Admin-tunable; applied to the source's
-- api_config.max_results at approve time. 0 = no cap.
-- (Dev gets this via GORM AutoMigrate; this migration is for prod.)

ALTER TABLE discovery_configs
    ADD COLUMN IF NOT EXISTS media_initial_max_episodes INTEGER NOT NULL DEFAULT 5;
