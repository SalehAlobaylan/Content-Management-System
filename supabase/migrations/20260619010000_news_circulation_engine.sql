-- News Circulation Engine V1.

ALTER TABLE news_snapshots
    ADD COLUMN IF NOT EXISTS window VARCHAR(16) NOT NULL DEFAULT 'today';

DROP INDEX IF EXISTS idx_news_snapshot_tenant;
CREATE UNIQUE INDEX IF NOT EXISTS idx_news_snapshot_tenant_window
    ON news_snapshots (tenant_id, window);

CREATE TABLE IF NOT EXISTS news_circulation_policies (
    id BIGSERIAL PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL UNIQUE,
    preset VARCHAR(32) NOT NULL DEFAULT 'latest_plus',
    timezone VARCHAR(64) NOT NULL DEFAULT 'Asia/Riyadh',
    min_today_stories INTEGER NOT NULL DEFAULT 8,
    carryover_hours INTEGER NOT NULL DEFAULT 72,
    carryover_min_score DOUBLE PRECISION NOT NULL DEFAULT 0.25,
    breaking_max_age_minutes INTEGER NOT NULL DEFAULT 180,
    breaking_min_members INTEGER NOT NULL DEFAULT 3,
    recency_weight DOUBLE PRECISION NOT NULL DEFAULT 0.55,
    importance_weight DOUBLE PRECISION NOT NULL DEFAULT 0.15,
    momentum_weight DOUBLE PRECISION NOT NULL DEFAULT 0.10,
    coverage_weight DOUBLE PRECISION NOT NULL DEFAULT 0.30,
    source_quality_weight DOUBLE PRECISION NOT NULL DEFAULT 0.10,
    diversity_weight DOUBLE PRECISION NOT NULL DEFAULT 0.05,
    trending_weight DOUBLE PRECISION NOT NULL DEFAULT 0.05,
    source_cadence_mode VARCHAR(20) NOT NULL DEFAULT 'suggest',
    source_claim_interval_minutes INTEGER NOT NULL DEFAULT 15,
    source_min_interval_minutes INTEGER NOT NULL DEFAULT 10,
    source_max_interval_minutes INTEGER NOT NULL DEFAULT 360,
    source_max_change_percent INTEGER NOT NULL DEFAULT 50,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS news_story_overrides (
    id BIGSERIAL PRIMARY KEY,
    public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    tenant_id VARCHAR(64) NOT NULL,
    story_id UUID NOT NULL,
    pin_to_top BOOLEAN NOT NULL DEFAULT FALSE,
    suppress BOOLEAN NOT NULL DEFAULT FALSE,
    exclude_from_feed BOOLEAN NOT NULL DEFAULT FALSE,
    importance_boost DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    notes TEXT,
    set_by VARCHAR(255),
    expires_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT idx_news_story_overrides_topic_tenant UNIQUE (story_id, tenant_id)
);
CREATE INDEX IF NOT EXISTS idx_news_story_overrides_tenant ON news_story_overrides (tenant_id);
CREATE INDEX IF NOT EXISTS idx_news_story_overrides_story ON news_story_overrides (story_id);
CREATE INDEX IF NOT EXISTS idx_news_story_overrides_expires_at ON news_story_overrides (expires_at);

CREATE TABLE IF NOT EXISTS source_run_telemetry (
    id BIGSERIAL PRIMARY KEY,
    public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    tenant_id VARCHAR(64) NOT NULL,
    source_id UUID NOT NULL,
    job_id VARCHAR(128) NOT NULL UNIQUE,
    triggered_by VARCHAR(20) NOT NULL DEFAULT 'schedule',
    fetched INTEGER NOT NULL DEFAULT 0,
    accepted INTEGER NOT NULL DEFAULT 0,
    duplicates INTEGER NOT NULL DEFAULT 0,
    filtered INTEGER NOT NULL DEFAULT 0,
    failed INTEGER NOT NULL DEFAULT 0,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    duration_ms INTEGER NOT NULL DEFAULT 0,
    metadata JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_source_run_telemetry_tenant ON source_run_telemetry (tenant_id);
CREATE INDEX IF NOT EXISTS idx_source_run_telemetry_source ON source_run_telemetry (source_id);
CREATE INDEX IF NOT EXISTS idx_source_run_telemetry_finished_at ON source_run_telemetry (finished_at);

CREATE TABLE IF NOT EXISTS source_circulation_recommendations (
    id BIGSERIAL PRIMARY KEY,
    public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    tenant_id VARCHAR(64) NOT NULL,
    source_id UUID NOT NULL,
    source_name VARCHAR(255),
    source_type VARCHAR(20),
    current_interval_minutes INTEGER NOT NULL,
    recommended_interval_minutes INTEGER NOT NULL,
    score DOUBLE PRECISION NOT NULL DEFAULT 0,
    reason TEXT,
    mode VARCHAR(20) NOT NULL DEFAULT 'suggest',
    metrics JSONB,
    applied BOOLEAN NOT NULL DEFAULT FALSE,
    applied_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_source_circ_recs_tenant ON source_circulation_recommendations (tenant_id);
CREATE INDEX IF NOT EXISTS idx_source_circ_recs_source ON source_circulation_recommendations (source_id);
CREATE INDEX IF NOT EXISTS idx_source_circ_recs_applied ON source_circulation_recommendations (applied);
