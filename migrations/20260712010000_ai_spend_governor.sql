-- AI Spend & Economics Governor (stage 11). Applied deliberately.
BEGIN;

CREATE TABLE IF NOT EXISTS ai_spend_events (
    id bigserial PRIMARY KEY,
    event_id uuid NOT NULL UNIQUE,
    occurred_at timestamptz NOT NULL,
    spend_class varchar(32) NOT NULL,
    operation varchar(64) NOT NULL,
    provider varchar(64), model varchar(160), units jsonb NOT NULL DEFAULT '{}'::jsonb,
    cost_usd double precision NOT NULL DEFAULT 0, avoided_cost_usd double precision NOT NULL DEFAULT 0,
    cached boolean NOT NULL DEFAULT false, estimated boolean NOT NULL DEFAULT false,
    avoided_cost_estimated boolean NOT NULL DEFAULT false, unpriced boolean NOT NULL DEFAULT false,
    backfilled boolean NOT NULL DEFAULT false, price_row_id bigint, trigger_source varchar(64) NOT NULL DEFAULT 'unknown',
    system_run_id varchar(96), tenant_id varchar(64) NOT NULL DEFAULT 'default', source_service varchar(64) NOT NULL,
    over_cap_human boolean NOT NULL DEFAULT false, created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_ai_spend_events_occurred ON ai_spend_events (occurred_at);
CREATE INDEX IF NOT EXISTS idx_ai_spend_events_class_occurred ON ai_spend_events (spend_class, occurred_at);
CREATE INDEX IF NOT EXISTS idx_ai_spend_events_trigger_occurred ON ai_spend_events (trigger_source, occurred_at);

CREATE TABLE IF NOT EXISTS ai_price_book (
    id bigserial PRIMARY KEY, spend_class varchar(32) NOT NULL, provider varchar(64) NOT NULL,
    model_pattern varchar(160) NOT NULL DEFAULT '*', input_usd_per_1m double precision NOT NULL DEFAULT 0,
    output_usd_per_1m double precision NOT NULL DEFAULT 0, unit_usd double precision NOT NULL DEFAULT 0,
    effective_from timestamptz NOT NULL, note text, created_by varchar(120), created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_ai_price_book_match ON ai_price_book (spend_class, provider, effective_from DESC);

CREATE TABLE IF NOT EXISTS ai_spend_rollups (
    id bigserial PRIMARY KEY, day date NOT NULL, spend_class varchar(32) NOT NULL, operation varchar(64) NOT NULL,
    provider varchar(64) NOT NULL DEFAULT '', model varchar(160) NOT NULL DEFAULT '', trigger_source varchar(64) NOT NULL DEFAULT 'unknown',
    system_run_id varchar(96) NOT NULL DEFAULT '', events bigint NOT NULL DEFAULT 0, units jsonb NOT NULL DEFAULT '{}'::jsonb,
    cost_usd double precision NOT NULL DEFAULT 0, avoided_cost_usd double precision NOT NULL DEFAULT 0,
    cache_hits bigint NOT NULL DEFAULT 0, backfilled boolean NOT NULL DEFAULT false, updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (day, spend_class, operation, provider, model, trigger_source, system_run_id)
);

CREATE TABLE IF NOT EXISTS ai_spend_policies (
    id bigserial PRIMARY KEY, tenant_id varchar(64) NOT NULL UNIQUE, enabled boolean NOT NULL DEFAULT false,
    aggregation_interval_minutes integer NOT NULL DEFAULT 5, forecast_horizon_days integer NOT NULL DEFAULT 30,
    spike_multiplier double precision NOT NULL DEFAULT 4, retention_days integer NOT NULL DEFAULT 90,
    paused_until timestamptz, last_run_at timestamptz, created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ai_spend_budgets (
    id bigserial PRIMARY KEY, tenant_id varchar(64) NOT NULL DEFAULT 'default', scope varchar(96) NOT NULL,
    cap_usd double precision, warn_pct double precision NOT NULL DEFAULT 70, hard_pct double precision NOT NULL DEFAULT 100,
    window_started_at timestamptz NOT NULL DEFAULT now(), spend_usd double precision NOT NULL DEFAULT 0,
    reserved_usd double precision NOT NULL DEFAULT 0, paused_until timestamptz, updated_by varchar(120), updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, scope)
);

CREATE TABLE IF NOT EXISTS ai_spend_runs (
    id bigserial PRIMARY KEY, tenant_id varchar(64) NOT NULL DEFAULT 'default', trigger varchar(16) NOT NULL,
    status varchar(16) NOT NULL, headline varchar(64), watermarks_advanced jsonb, events_folded bigint NOT NULL DEFAULT 0,
    budget_verdicts jsonb, hygiene_counters jsonb, started_at timestamptz NOT NULL DEFAULT now(), completed_at timestamptz,
    duration_ms bigint NOT NULL DEFAULT 0, error text, error_class varchar(64)
);
CREATE INDEX IF NOT EXISTS idx_ai_spend_runs_tenant_started ON ai_spend_runs (tenant_id, started_at DESC);

CREATE TABLE IF NOT EXISTS ai_spend_episodes (
    id bigserial PRIMARY KEY, tenant_id varchar(64) NOT NULL DEFAULT 'default', kind varchar(48) NOT NULL, scope varchar(96),
    status varchar(24) NOT NULL DEFAULT 'open', first_seen_at timestamptz NOT NULL DEFAULT now(), last_seen_at timestamptz NOT NULL DEFAULT now(),
    evidence jsonb, attribution jsonb, close_reason text, false_positive boolean NOT NULL DEFAULT false, created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_ai_spend_episodes_open ON ai_spend_episodes (tenant_id, status, kind);

-- Seed the price book so metered events cost real money on day one instead of
-- pricing at 0/unpriced until an admin remembers to add rows. Pricing happens at
-- ingest, so these only affect events ingested after the migration runs — edit a
-- price by APPENDING a row with a later effective_from, never by mutating a row.
-- effective_from is intentionally epoch-early so it also covers backfilled events.
-- Rates below are the configured model's published/default prices at migration
-- authoring time; verify provider billing and append corrections. Gemini 3.5
-- Flash is $1.50 input / $9.00 output per 1M standard tokens (Google pricing).
-- DeepSeek's cache-hit differential is intentionally deferred: V1 records the
-- configured deepseek-chat cache-miss rate. unit_usd for stt_api mirrors
-- sttEstimatedCostPerHourUsd (0.26/hr) expressed per audio-second.
INSERT INTO ai_price_book (spend_class, provider, model_pattern, input_usd_per_1m, output_usd_per_1m, unit_usd, effective_from, note, created_by)
SELECT * FROM (VALUES
    ('llm',         'gemini',   'gemini-3.5-flash', 1.50, 9.00, 0.0,          TIMESTAMPTZ '2000-01-01', 'Google standard price — verify', 'migration_seed'),
    ('llm',         'deepseek', 'deepseek-chat',    0.27, 1.10, 0.0,          TIMESTAMPTZ '2000-01-01', 'approx list price — verify', 'migration_seed'),
    ('stt_api',     'deepgram', '*',                0.0,  0.0,  0.00007222,   TIMESTAMPTZ '2000-01-01', '0.26/audio-hour',            'migration_seed'),
    ('stt_local',   'local',    '*',                0.0,  0.0,  0.0,          TIMESTAMPTZ '2000-01-01', 'self-hosted compute only',   'migration_seed'),
    ('embedding',   'local',    '*',                0.0,  0.0,  0.0,          TIMESTAMPTZ '2000-01-01', 'self-hosted compute only',   'migration_seed'),
    ('rerank',      'local',    '*',                0.0,  0.0,  0.0,          TIMESTAMPTZ '2000-01-01', 'self-hosted compute only',   'migration_seed'),
    ('image_embed', 'local',    '*',                0.0,  0.0,  0.0,          TIMESTAMPTZ '2000-01-01', 'self-hosted compute only',   'migration_seed')
) AS seed(spend_class, provider, model_pattern, input_usd_per_1m, output_usd_per_1m, unit_usd, effective_from, note, created_by)
WHERE NOT EXISTS (
    SELECT 1 FROM ai_price_book existing
    WHERE existing.spend_class = seed.spend_class
      AND existing.provider = seed.provider
      AND existing.model_pattern = seed.model_pattern
      AND existing.effective_from = seed.effective_from
);

COMMIT;
