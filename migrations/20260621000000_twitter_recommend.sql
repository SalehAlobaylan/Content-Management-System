-- Feeds Finding — X "who to follow" / قد يعجبك relatedness discovery.
--
-- discovery_configs.twitter_recommend_enabled gates the recommendations
-- contributor (Aggregation calls the guest-accessible legacy REST
-- users/recommendations.json per approved handle → related accounts → ledger).
-- (Dev gets this via GORM AutoMigrate; this migration is for prod where
-- AUTO_MIGRATE is off.)

ALTER TABLE discovery_configs
    ADD COLUMN IF NOT EXISTS twitter_recommend_enabled BOOLEAN NOT NULL DEFAULT FALSE;
