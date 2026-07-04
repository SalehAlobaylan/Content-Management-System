-- Ranking/Intelligence — per-tenant tuning surface for the media-value engine
-- (Platform-Console control room). The code defaults in src/intelligence
-- (DefaultTuning) remain the fallback whenever no row exists for a tenant, so
-- this table is purely an override layer.
--
-- Only OPERATIONAL knobs live here (signal weights, exploration cadence/target,
-- demotion decay). Model-shape constants (shrinkage, priors, refresh cadence,
-- storage thresholds) stay in code deliberately — see docs/ranking-intelligence-system.md §11.

CREATE TABLE IF NOT EXISTS media_intelligence_configs (
    id bigserial PRIMARY KEY,
    tenant_id varchar(64) NOT NULL,

    -- Four rate/state signal weights (server-normalized to sum 1.0 on write).
    engagement_weight double precision NOT NULL DEFAULT 0.35,
    completion_weight double precision NOT NULL DEFAULT 0.25,
    quality_weight double precision NOT NULL DEFAULT 0.20,
    velocity_weight double precision NOT NULL DEFAULT 0.20,

    -- Exploration.
    exploration_slice_every integer NOT NULL DEFAULT 10,
    explore_impression_target integer NOT NULL DEFAULT 50,
    legacy_exposure_view_floor integer NOT NULL DEFAULT 25,

    -- Soft-eviction demotion decay.
    demotion_default_factor double precision NOT NULL DEFAULT 0.5,
    demotion_half_life_days integer NOT NULL DEFAULT 14,

    created_at timestamp NOT NULL DEFAULT now(),
    updated_at timestamp NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_media_intel_config_tenant
    ON media_intelligence_configs (tenant_id);
