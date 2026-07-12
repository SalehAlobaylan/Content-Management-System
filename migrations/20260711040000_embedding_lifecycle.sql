-- Embedding & Model Lifecycle System (stage 10) — Migration 2 of 3.
-- Audit persistence (policy singleton, runs, findings). The campaign half
-- (campaigns/actions/exceptions) ships in a later migration with Slice 3.
--
-- Canonical location; applied DELIBERATELY (dev boots AutoMigrate from the Go
-- model tags). Ordered AFTER 20260711030000_embedding_space_provenance.sql.

BEGIN;

CREATE TABLE IF NOT EXISTS embedding_lifecycle_policies (
    id                            bigserial PRIMARY KEY,
    tenant_id                     varchar(64) NOT NULL,
    audit_enabled                 boolean NOT NULL DEFAULT false,
    audit_interval_minutes        integer NOT NULL DEFAULT 360,
    numeric_sample_size           integer NOT NULL DEFAULT 64,
    items_per_batch               integer NOT NULL DEFAULT 200,
    batches_per_run               integer NOT NULL DEFAULT 1,
    daily_item_cap                integer NOT NULL DEFAULT 5000,
    retry_ceiling                 integer NOT NULL DEFAULT 3,
    expected_descriptor_override  jsonb,
    override_reason               text,
    override_expires_at           timestamptz,
    campaigns_paused_until        timestamptz,
    last_audit_at                 timestamptz,
    created_at                    timestamptz NOT NULL DEFAULT now(),
    updated_at                    timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_embedding_lifecycle_policy_tenant
    ON embedding_lifecycle_policies (tenant_id);

CREATE TABLE IF NOT EXISTS embedding_lifecycle_runs (
    id               bigserial PRIMARY KEY,
    tenant_id        varchar(64) NOT NULL,
    trigger          varchar(16) NOT NULL,
    status           varchar(16) NOT NULL,
    headline         varchar(24),
    per_surface      jsonb,
    violations_major integer NOT NULL DEFAULT 0,
    violations_minor integer NOT NULL DEFAULT 0,
    check_errors     integer NOT NULL DEFAULT 0,
    started_at       timestamptz NOT NULL DEFAULT now(),
    completed_at     timestamptz,
    duration_ms      bigint NOT NULL DEFAULT 0,
    error            text,
    error_class      varchar(48)
);
CREATE INDEX IF NOT EXISTS idx_embedding_lifecycle_runs_tenant
    ON embedding_lifecycle_runs (tenant_id, started_at DESC);

CREATE TABLE IF NOT EXISTS embedding_lifecycle_findings (
    id           bigserial PRIMARY KEY,
    run_id       bigint NOT NULL,
    tenant_id    varchar(64) NOT NULL,
    surface_key  varchar(48) NOT NULL,
    check_key    varchar(48) NOT NULL,
    status       varchar(16) NOT NULL,
    severity     varchar(16),
    target_type  varchar(32),
    target_id    varchar(80),
    count        integer NOT NULL DEFAULT 0,
    evidence     jsonb,
    created_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_embedding_lifecycle_findings_run
    ON embedding_lifecycle_findings (run_id);
CREATE INDEX IF NOT EXISTS idx_embedding_lifecycle_findings_surface
    ON embedding_lifecycle_findings (surface_key, check_key);

COMMIT;
