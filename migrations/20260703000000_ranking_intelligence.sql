-- Ranking/Intelligence System — stage 4 foundation.
--
-- Adds:
--  - exposure telemetry columns on content_items (serve-side impressions —
--    interim proxy until Wahb-Platform fires viewport impression events)
--  - media_intelligence_scores: the persisted Value surface (one row per media
--    item; storage orders candidate queries by value in SQL via this table)
--  - media_demand_stats: serve-side demand telemetry windows (serves,
--    exhaustions, repeat serves per tenant × bucket × topic × hour)
--
-- Design: docs/ranking-intelligence-stage4-plan.md (grilling Q4–Q9).

ALTER TABLE content_items
    ADD COLUMN IF NOT EXISTS impression_count bigint NOT NULL DEFAULT 0;
ALTER TABLE content_items
    ADD COLUMN IF NOT EXISTS last_served_at timestamp;

CREATE TABLE IF NOT EXISTS media_intelligence_scores (
    content_item_id uuid PRIMARY KEY REFERENCES content_items (public_id) ON DELETE CASCADE,
    tenant_id varchar(64) NOT NULL,
    value double precision NOT NULL,
    confidence double precision NOT NULL,
    exploration_state varchar(16) NOT NULL DEFAULT 'exploring',
    impressions_at_compute bigint NOT NULL DEFAULT 0,
    engagement_at_compute bigint NOT NULL DEFAULT 0,
    demotion_factor double precision,
    demoted_at timestamp,
    breakdown jsonb,
    reasons jsonb,
    computed_at timestamp NOT NULL,
    created_at timestamp NOT NULL DEFAULT now(),
    updated_at timestamp NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_media_intel_scores_tenant_value
    ON media_intelligence_scores (tenant_id, value);
CREATE INDEX IF NOT EXISTS idx_media_intel_scores_exploration
    ON media_intelligence_scores (exploration_state);
CREATE INDEX IF NOT EXISTS idx_media_intel_scores_computed_at
    ON media_intelligence_scores (computed_at);

CREATE TABLE IF NOT EXISTS media_demand_stats (
    id bigserial PRIMARY KEY,
    tenant_id varchar(64) NOT NULL,
    bucket varchar(8) NOT NULL,
    topic varchar(120) NOT NULL DEFAULT '',
    window_start timestamp NOT NULL,
    serves bigint NOT NULL DEFAULT 0,
    exhaustions bigint NOT NULL DEFAULT 0,
    repeat_serves bigint NOT NULL DEFAULT 0,
    created_at timestamp NOT NULL DEFAULT now(),
    updated_at timestamp NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_media_demand_window
    ON media_demand_stats (tenant_id, bucket, topic, window_start);
CREATE INDEX IF NOT EXISTS idx_media_demand_window_start
    ON media_demand_stats (window_start);
