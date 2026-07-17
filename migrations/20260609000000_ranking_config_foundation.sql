-- Canonical foundation missing from the original SQL chain.  Existing
-- deployments created this table through the retired GORM AutoMigrate path;
-- this idempotent definition makes fresh migration application authoritative.
CREATE TABLE IF NOT EXISTS ranking_configs (
    id bigserial PRIMARY KEY,
    tenant_id varchar(64) NOT NULL,
    freshness_weight double precision NOT NULL DEFAULT 0.25,
    engagement_weight double precision NOT NULL DEFAULT 0.20,
    velocity_weight double precision NOT NULL DEFAULT 0.15,
    similarity_weight double precision NOT NULL DEFAULT 0.15,
    quality_weight double precision NOT NULL DEFAULT 0.10,
    diversity_weight double precision NOT NULL DEFAULT 0.10,
    trending_weight double precision NOT NULL DEFAULT 0.05,
    freshness_decay_hours integer NOT NULL DEFAULT 72,
    velocity_window_hours integer NOT NULL DEFAULT 6,
    trending_threshold_multiplier double precision NOT NULL DEFAULT 2.0,
    recirculation_enabled boolean NOT NULL DEFAULT false,
    recirculation_max_age_days integer NOT NULL DEFAULT 30,
    show_watched_when_unseen_exhausted boolean NOT NULL DEFAULT true,
    engagement_normalization varchar(20) NOT NULL DEFAULT 'log',
    mode varchar(20) NOT NULL DEFAULT 'balanced',
    is_active boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_ranking_config_tenant ON ranking_configs (tenant_id);
