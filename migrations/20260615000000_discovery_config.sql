-- Feeds Finding Slice 3 — discovery_config (scheduled sweeps + tunable knobs).
-- Single row per tenant; surfaces discovery tuning in the admin UI instead of
-- hardcoded consts. (Dev gets this via GORM AutoMigrate; this is for prod.)

CREATE TABLE IF NOT EXISTS discovery_configs (
    id                         BIGSERIAL PRIMARY KEY,
    tenant_id                  VARCHAR(64) NOT NULL DEFAULT 'default',
    automation_enabled         BOOLEAN NOT NULL DEFAULT FALSE,
    sweep_interval_hours       INTEGER NOT NULL DEFAULT 24,
    min_confidence             DOUBLE PRECISION NOT NULL DEFAULT 0.15,
    min_relevance              DOUBLE PRECISION NOT NULL DEFAULT 0.10,
    dup_threshold              DOUBLE PRECISION NOT NULL DEFAULT 0.92,
    dup_penalty                DOUBLE PRECISION NOT NULL DEFAULT 0.50,
    recency_window_days        INTEGER NOT NULL DEFAULT 30,
    max_candidates_per_profile INTEGER NOT NULL DEFAULT 15,
    search_provider            VARCHAR(16) NOT NULL DEFAULT 'auto',
    created_at                 TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at                 TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_discovery_config_tenant ON discovery_configs (tenant_id);
