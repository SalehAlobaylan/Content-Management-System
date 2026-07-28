-- Retention Autopilot: persisted, tenant-scoped observe/assist foundation.
-- This migration creates control/evidence state only. It does not mutate content.

CREATE TABLE IF NOT EXISTS retention_policies (
    id BIGSERIAL PRIMARY KEY,
    public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    tenant_id VARCHAR(64) NOT NULL UNIQUE,
    enabled BOOLEAN NOT NULL DEFAULT FALSE,
    mode VARCHAR(24) NOT NULL DEFAULT 'observe'
        CHECK (mode IN ('observe', 'assist', 'safe_auto')),
    schedule_interval_minutes INTEGER NOT NULL DEFAULT 360
        CHECK (schedule_interval_minutes BETWEEN 15 AND 10080),
    paused_until TIMESTAMPTZ,
    last_run_at TIMESTAMPTZ,
    policy_version INTEGER NOT NULL DEFAULT 1 CHECK (policy_version > 0),
    news_policy_version INTEGER,
    news_timezone VARCHAR(64) NOT NULL DEFAULT 'Asia/Riyadh',
    database_target_bytes BIGINT NOT NULL DEFAULT 419430400 CHECK (database_target_bytes > 0),
    database_warning_bytes BIGINT NOT NULL DEFAULT 419430400 CHECK (database_warning_bytes > 0),
    database_action_bytes BIGINT NOT NULL DEFAULT 461373440 CHECK (database_action_bytes > 0),
    database_critical_bytes BIGINT NOT NULL DEFAULT 503316480 CHECK (database_critical_bytes > 0),
    warning_forecast_days INTEGER NOT NULL DEFAULT 14 CHECK (warning_forecast_days BETWEEN 1 AND 90),
    action_forecast_days INTEGER NOT NULL DEFAULT 7 CHECK (action_forecast_days BETWEEN 1 AND 90),
    critical_forecast_hours INTEGER NOT NULL DEFAULT 48 CHECK (critical_forecast_hours BETWEEN 1 AND 720),
    max_rows_per_run INTEGER NOT NULL DEFAULT 500 CHECK (max_rows_per_run BETWEEN 1 AND 10000),
    max_bytes_per_run BIGINT NOT NULL DEFAULT 33554432 CHECK (max_bytes_per_run > 0),
    max_actions_per_run INTEGER NOT NULL DEFAULT 4 CHECK (max_actions_per_run BETWEEN 1 AND 50),
    action_modes JSONB NOT NULL DEFAULT '{}'::jsonb,
    trust_min_decisions INTEGER NOT NULL DEFAULT 20 CHECK (trust_min_decisions BETWEEN 1 AND 10000),
    trust_min_agreement_pct INTEGER NOT NULL DEFAULT 95 CHECK (trust_min_agreement_pct BETWEEN 50 AND 100),
    updated_by VARCHAR(255),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_retention_policy_bands CHECK (
        database_target_bytes <= database_warning_bytes
        AND database_warning_bytes < database_action_bytes
        AND database_action_bytes < database_critical_bytes
    )
);

CREATE TABLE IF NOT EXISTS retention_db_samples (
    id BIGSERIAL PRIMARY KEY,
    public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    tenant_id VARCHAR(64) NOT NULL,
    database_bytes BIGINT NOT NULL CHECK (database_bytes >= 0),
    provider_bytes BIGINT CHECK (provider_bytes IS NULL OR provider_bytes >= 0),
    provider_source VARCHAR(32) NOT NULL DEFAULT 'unavailable'
        CHECK (provider_source IN ('supabase_api', 'neon_api', 'operator_readback', 'unavailable')),
    provider_measured_at TIMESTAMPTZ,
    relation_bytes BIGINT NOT NULL DEFAULT 0 CHECK (relation_bytes >= 0),
    index_bytes BIGINT NOT NULL DEFAULT 0 CHECK (index_bytes >= 0),
    toast_bytes BIGINT NOT NULL DEFAULT 0 CHECK (toast_bytes >= 0),
    allocated_bytes BIGINT NOT NULL DEFAULT 0 CHECK (allocated_bytes >= 0),
    reusable_bytes BIGINT NOT NULL DEFAULT 0 CHECK (reusable_bytes >= 0),
    live_tuples BIGINT NOT NULL DEFAULT 0 CHECK (live_tuples >= 0),
    dead_tuples BIGINT NOT NULL DEFAULT 0 CHECK (dead_tuples >= 0),
    relation_breakdown JSONB NOT NULL DEFAULT '[]'::jsonb,
    forecast_inputs JSONB NOT NULL DEFAULT '{}'::jsonb,
    measured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_retention_db_samples_tenant_time
    ON retention_db_samples(tenant_id, measured_at DESC);

CREATE TABLE IF NOT EXISTS retention_months (
    id BIGSERIAL PRIMARY KEY,
    public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    tenant_id VARCHAR(64) NOT NULL,
    month_start DATE NOT NULL,
    state VARCHAR(32) NOT NULL DEFAULT 'open'
        CHECK (state IN (
            'open', 'compacting', 'review_building', 'review_verified',
            'purge_eligible', 'purging', 'archived', 'blocked', 'failed'
        )),
    state_reason TEXT,
    compacted_story_count INTEGER NOT NULL DEFAULT 0 CHECK (compacted_story_count >= 0),
    retained_content_count INTEGER NOT NULL DEFAULT 0 CHECK (retained_content_count >= 0),
    review_revision_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, month_start)
);

CREATE TABLE IF NOT EXISTS retention_runs (
    id BIGSERIAL PRIMARY KEY,
    public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    tenant_id VARCHAR(64) NOT NULL,
    lane VARCHAR(32) NOT NULL DEFAULT 'database',
    trigger VARCHAR(24) NOT NULL CHECK (trigger IN ('manual', 'scheduled', 'recovery', 'post_deploy')),
    mode VARCHAR(24) NOT NULL CHECK (mode IN ('observe', 'assist', 'safe_auto')),
    status VARCHAR(24) NOT NULL CHECK (status IN ('running', 'completed', 'partial', 'failed', 'blocked')),
    verdict VARCHAR(40) NOT NULL,
    policy_version INTEGER NOT NULL,
    correlation_id UUID NOT NULL DEFAULT gen_random_uuid(),
    before_evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    forecast_evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    after_evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    counts JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_by VARCHAR(255),
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    error_class VARCHAR(64) NOT NULL DEFAULT 'none',
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_retention_runs_tenant_started
    ON retention_runs(tenant_id, started_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_retention_runs_one_active_lane
    ON retention_runs(tenant_id, lane)
    WHERE status = 'running';

CREATE TABLE IF NOT EXISTS retention_actions (
    id BIGSERIAL PRIMARY KEY,
    public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    run_id BIGINT NOT NULL REFERENCES retention_runs(id) ON DELETE RESTRICT,
    tenant_id VARCHAR(64) NOT NULL,
    action_class VARCHAR(80) NOT NULL,
    owner_system VARCHAR(64) NOT NULL,
    target_scope TEXT NOT NULL,
    mode VARCHAR(24) NOT NULL CHECK (mode IN ('observe', 'assist', 'safe_auto')),
    decision VARCHAR(32) NOT NULL,
    outcome VARCHAR(32) NOT NULL CHECK (outcome IN (
        'would_execute', 'approval_required', 'approved', 'rejected', 'ready',
        'claimed', 'running', 'tool_succeeded', 'tool_failed', 'verifying',
        'verification_passed', 'verification_failed', 'skipped', 'expired'
    )),
    idempotency_key VARCHAR(255) NOT NULL,
    evidence_fingerprint CHAR(64) NOT NULL,
    manifest_hash CHAR(64),
    target_count INTEGER NOT NULL DEFAULT 0 CHECK (target_count >= 0),
    protected_count INTEGER NOT NULL DEFAULT 0 CHECK (protected_count >= 0),
    estimated_bytes BIGINT NOT NULL DEFAULT 0 CHECK (estimated_bytes >= 0),
    guardrail TEXT,
    evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    before_bytes BIGINT,
    forecast_after_bytes BIGINT,
    after_bytes BIGINT,
    claim_token UUID,
    claim_expires_at TIMESTAMPTZ,
    approved_at TIMESTAMPTZ,
    approved_by VARCHAR(255),
    rejected_at TIMESTAMPTZ,
    rejected_by VARCHAR(255),
    rejection_reason TEXT,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    verification JSONB NOT NULL DEFAULT '{}'::jsonb,
    recovery_ref TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, idempotency_key)
);
CREATE INDEX IF NOT EXISTS idx_retention_actions_run ON retention_actions(run_id, created_at);
CREATE INDEX IF NOT EXISTS idx_retention_actions_tenant_outcome
    ON retention_actions(tenant_id, outcome, created_at DESC);

CREATE TABLE IF NOT EXISTS retention_owner_requests (
    id BIGSERIAL PRIMARY KEY,
    public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    action_id BIGINT NOT NULL REFERENCES retention_actions(id) ON DELETE RESTRICT,
    tenant_id VARCHAR(64) NOT NULL,
    owner_system VARCHAR(64) NOT NULL,
    idempotency_key VARCHAR(255) NOT NULL,
    request_hash CHAR(64) NOT NULL,
    owner_run_id UUID,
    owner_action_id UUID,
    status VARCHAR(32) NOT NULL DEFAULT 'pending',
    result_hash CHAR(64),
    result JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, owner_system, idempotency_key)
);

CREATE TABLE IF NOT EXISTS retention_holds (
    id BIGSERIAL PRIMARY KEY,
    public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    tenant_id VARCHAR(64) NOT NULL,
    target_type VARCHAR(24) NOT NULL CHECK (target_type IN ('content', 'story', 'month')),
    target_id UUID NOT NULL,
    hold_class VARCHAR(24) NOT NULL CHECK (hold_class IN ('manual', 'editorial', 'legal', 'moderation', 'recovery')),
    reason TEXT NOT NULL,
    created_by VARCHAR(255) NOT NULL,
    expires_at TIMESTAMPTZ,
    released_at TIMESTAMPTZ,
    released_by VARCHAR(255),
    release_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_retention_holds_active_target
    ON retention_holds(tenant_id, target_type, target_id)
    WHERE released_at IS NULL;
