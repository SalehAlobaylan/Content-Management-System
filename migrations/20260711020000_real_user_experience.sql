-- Real User Experience (RUX) System — browser-observed reliability telemetry.
-- Applied deliberately; never by startup patch. Ordinal follows
-- 20260711010000_feed_integrity_autopilot.sql so the stream stays ordered.
--
-- Doctrine: RUX events are operational, high-volume, short-lived, and separate
-- from user_interactions (durable product/ranking semantics). Observe-only V1:
-- these tables never drive content, ranking, playback, or deployment.

-- ── Raw evidence (append-only, short-lived: default 7-day retention) ──────────
CREATE TABLE IF NOT EXISTS experience_events (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL,
    tenant_id VARCHAR(64) NOT NULL DEFAULT 'default',
    schema_version INTEGER NOT NULL DEFAULT 1,
    event_type VARCHAR(40) NOT NULL,
    surface VARCHAR(16) NOT NULL,
    occurred_at TIMESTAMP NOT NULL,
    received_at TIMESTAMP NOT NULL DEFAULT NOW(),
    session_id VARCHAR(64) NOT NULL,
    page_load_id UUID NOT NULL,
    sequence INTEGER NOT NULL DEFAULT 0,
    journey_id UUID,
    release VARCHAR(80) NOT NULL,
    content_id UUID,
    story_id UUID,
    playback_type VARCHAR(16),
    locale VARCHAR(16),
    -- normalized cohort columns (from the client envelope's client{} block)
    browser_family VARCHAR(16) NOT NULL DEFAULT 'other',
    browser_major INTEGER NOT NULL DEFAULT 0,
    device_class VARCHAR(16) NOT NULL DEFAULT 'mobile',
    network_class VARCHAR(16) NOT NULL DEFAULT 'unknown',
    installed_pwa BOOLEAN NOT NULL DEFAULT FALSE,
    -- bounded numeric measurements (validated ranges)
    duration_ms INTEGER,
    media_error_code INTEGER,
    stall_duration_ms INTEGER,
    failure_class VARCHAR(24),
    visible BOOLEAN,
    measurements JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
-- Idempotency: duplicate event_id is a harmless no-op on insert.
CREATE UNIQUE INDEX IF NOT EXISTS idx_experience_events_event_id ON experience_events(event_id);
CREATE INDEX IF NOT EXISTS idx_experience_events_rollup ON experience_events(tenant_id, received_at, event_type, surface);
CREATE INDEX IF NOT EXISTS idx_experience_events_release ON experience_events(tenant_id, release, received_at);
CREATE INDEX IF NOT EXISTS idx_experience_events_content ON experience_events(tenant_id, content_id, received_at) WHERE content_id IS NOT NULL;

-- ── Aggregated rollups (one supported cohort dimension/value per row) ─────────
CREATE TABLE IF NOT EXISTS experience_metric_rollups (
    id BIGSERIAL PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL DEFAULT 'default',
    bucket_start TIMESTAMP NOT NULL,
    resolution VARCHAR(8) NOT NULL DEFAULT 'hour', -- 'minute' | 'hour'
    metric_key VARCHAR(64) NOT NULL,
    surface VARCHAR(16) NOT NULL,
    cohort_dim VARCHAR(24) NOT NULL DEFAULT 'global', -- global | release | playback_type | browser | device | network | locale
    cohort_val VARCHAR(80) NOT NULL DEFAULT 'all',
    numerator BIGINT NOT NULL DEFAULT 0,
    denominator BIGINT NOT NULL DEFAULT 0,
    sample_count BIGINT NOT NULL DEFAULT 0,
    latency_sum BIGINT NOT NULL DEFAULT 0,
    latency_buckets JSONB, -- compact histogram for p-estimates
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_experience_rollups_identity
    ON experience_metric_rollups(tenant_id, bucket_start, resolution, metric_key, surface, cohort_dim, cohort_val);
CREATE INDEX IF NOT EXISTS idx_experience_rollups_query
    ON experience_metric_rollups(tenant_id, surface, metric_key, bucket_start DESC);

-- ── Observe policy (singleton per tenant) ────────────────────────────────────
CREATE TABLE IF NOT EXISTS experience_policies (
    id BIGSERIAL PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL DEFAULT 'default',
    ingest_enabled BOOLEAN NOT NULL DEFAULT TRUE,     -- public-write kill-switch
    evaluation_enabled BOOLEAN NOT NULL DEFAULT FALSE, -- Observe scheduler on/off
    enabled_surfaces VARCHAR(64) NOT NULL DEFAULT 'foryou,news',
    min_sample_floor INTEGER NOT NULL DEFAULT 50,      -- per-verdict denominator floor
    confirm_windows INTEGER NOT NULL DEFAULT 2,        -- bad windows before degraded
    resolve_windows INTEGER NOT NULL DEFAULT 3,        -- clean windows before resolve
    telemetry_freshness_minutes INTEGER NOT NULL DEFAULT 15,
    rollup_max_buckets_per_pass INTEGER NOT NULL DEFAULT 180, -- outage catch-up cap
    raw_retention_days INTEGER NOT NULL DEFAULT 7,
    minute_rollup_retention_hours INTEGER NOT NULL DEFAULT 48,
    hour_rollup_retention_days INTEGER NOT NULL DEFAULT 400,
    max_release_cohorts INTEGER NOT NULL DEFAULT 4,    -- current + previous N, else 'other'
    thresholds JSONB,                                  -- per-SLI targets (code defaults if null)
    paused_until TIMESTAMP,
    last_evaluated_bucket TIMESTAMP,                   -- rollup checkpoint
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_experience_policies_tenant ON experience_policies(tenant_id);

-- ── Evaluation runs (one scheduled/manual verdict pass) ──────────────────────
CREATE TABLE IF NOT EXISTS experience_evaluation_runs (
    id BIGSERIAL PRIMARY KEY,
    public_id UUID NOT NULL DEFAULT gen_random_uuid(),
    tenant_id VARCHAR(64) NOT NULL DEFAULT 'default',
    trigger VARCHAR(24) NOT NULL,          -- manual | scheduled
    status VARCHAR(24) NOT NULL,           -- running | completed | partial | failed
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    telemetry_fresh BOOLEAN NOT NULL DEFAULT TRUE,
    surface_verdicts JSONB,                -- {foryou: {...}, news: {...}}
    summary TEXT,
    buckets_processed INTEGER NOT NULL DEFAULT 0,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,
    error TEXT,
    error_class VARCHAR(48) NOT NULL DEFAULT 'none',
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_experience_eval_runs_public_id ON experience_evaluation_runs(public_id);
CREATE INDEX IF NOT EXISTS idx_experience_eval_runs_tenant ON experience_evaluation_runs(tenant_id, started_at DESC);

-- ── Incidents (persistent unhealthy fingerprint) ─────────────────────────────
CREATE TABLE IF NOT EXISTS experience_incidents (
    id BIGSERIAL PRIMARY KEY,
    public_id UUID NOT NULL DEFAULT gen_random_uuid(),
    tenant_id VARCHAR(64) NOT NULL DEFAULT 'default',
    fingerprint VARCHAR(200) NOT NULL,     -- metric:surface:cohort_dim:cohort_val
    metric_key VARCHAR(64) NOT NULL,
    surface VARCHAR(16) NOT NULL,
    cohort_dim VARCHAR(24) NOT NULL DEFAULT 'global',
    cohort_val VARCHAR(80) NOT NULL DEFAULT 'all',
    severity VARCHAR(16) NOT NULL,         -- watching | degraded | critical
    status VARCHAR(24) NOT NULL,           -- open | recovering | resolved | closed
    summary TEXT,
    evidence JSONB,                        -- metric values, thresholds, denominator, trend
    recommendation TEXT,
    likely_owner VARCHAR(48),
    violation_streak INTEGER NOT NULL DEFAULT 0,
    clean_streak INTEGER NOT NULL DEFAULT 0,
    first_seen_at TIMESTAMP NOT NULL,
    last_seen_at TIMESTAMP NOT NULL,
    recovering_since TIMESTAMP,
    resolved_at TIMESTAMP,
    closed_by VARCHAR(255),
    close_reason_class VARCHAR(32),        -- resolved_externally | expected_behavior | false_positive | accepted_risk
    close_notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_experience_incidents_public_id ON experience_incidents(public_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_experience_incidents_open_fp
    ON experience_incidents(tenant_id, fingerprint) WHERE status IN ('open','recovering');
CREATE INDEX IF NOT EXISTS idx_experience_incidents_tenant ON experience_incidents(tenant_id, status, last_seen_at DESC);

-- ── Action ledger (human-readable audit of every evaluation decision) ────────
CREATE TABLE IF NOT EXISTS experience_actions (
    id BIGSERIAL PRIMARY KEY,
    public_id UUID NOT NULL DEFAULT gen_random_uuid(),
    tenant_id VARCHAR(64) NOT NULL DEFAULT 'default',
    run_id BIGINT REFERENCES experience_evaluation_runs(id) ON DELETE SET NULL,
    incident_id BIGINT REFERENCES experience_incidents(id) ON DELETE SET NULL,
    action_class VARCHAR(48) NOT NULL,     -- incident_opened | incident_updated | ... | diagnostic_recommended | withheld_* | error
    label TEXT NOT NULL,                   -- human sentence
    metric_key VARCHAR(64),
    surface VARCHAR(16),
    cohort_dim VARCHAR(24),
    cohort_val VARCHAR(80),
    guardrail VARCHAR(64),
    evidence JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_experience_actions_public_id ON experience_actions(public_id);
CREATE INDEX IF NOT EXISTS idx_experience_actions_tenant ON experience_actions(tenant_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_experience_actions_incident ON experience_actions(incident_id);

-- ── Suppressions (bounded TTL mutes, never silent) ───────────────────────────
CREATE TABLE IF NOT EXISTS experience_suppressions (
    id BIGSERIAL PRIMARY KEY,
    public_id UUID NOT NULL DEFAULT gen_random_uuid(),
    tenant_id VARCHAR(64) NOT NULL DEFAULT 'default',
    metric_key VARCHAR(64),
    surface VARCHAR(16),
    cohort_dim VARCHAR(24),
    cohort_val VARCHAR(80),
    reason TEXT NOT NULL,
    starts_at TIMESTAMP NOT NULL,
    expires_at TIMESTAMP NOT NULL,
    created_by VARCHAR(255),
    revoked_at TIMESTAMP,
    revoked_by VARCHAR(255),
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_experience_suppressions_public_id ON experience_suppressions(public_id);
CREATE INDEX IF NOT EXISTS idx_experience_suppressions_active ON experience_suppressions(tenant_id, metric_key, expires_at);

-- Seed the singleton Observe policy (ingest on, evaluation off until ratified).
INSERT INTO experience_policies (tenant_id) VALUES ('default') ON CONFLICT (tenant_id) DO NOTHING;
