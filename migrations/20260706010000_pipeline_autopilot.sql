-- Pipeline Repair Autopilot — persistence foundation.
--
-- Stores the per-tenant policy, deterministic run records, and the item-level
-- action ledger used as retry attempt memory. Exhausted items stay FAILED; the
-- ledger prevents repeat autopilot retries and surfaces attention state.

CREATE TABLE IF NOT EXISTS pipeline_autopilot_policies (
    id bigserial PRIMARY KEY,
    tenant_id varchar(64) NOT NULL,
    enabled boolean NOT NULL DEFAULT false,
    mode varchar(24) NOT NULL DEFAULT 'observe',
    interval_minutes integer NOT NULL DEFAULT 180,
    max_items_per_run integer NOT NULL DEFAULT 200,
    max_batches_per_run integer NOT NULL DEFAULT 4,
    max_attempts integer NOT NULL DEFAULT 3,
    retry_backoff_hours integer NOT NULL DEFAULT 12,
    pending_age_floor_minutes integer NOT NULL DEFAULT 30,
    processing_stuck_hours integer NOT NULL DEFAULT 4,
    max_queue_depth integer NOT NULL DEFAULT 100,
    per_source_daily_retries integer NOT NULL DEFAULT 100,
    recovery_cooldown_minutes integer NOT NULL DEFAULT 60,
    trust_min_outcomes integer NOT NULL DEFAULT 20,
    trust_min_success_pct integer NOT NULL DEFAULT 40,
    paused_until timestamp,
    elevated_mode varchar(32),
    elevated_until timestamp,
    last_run_at timestamp,
    last_health_ok_at timestamp,
    created_at timestamp NOT NULL DEFAULT now(),
    updated_at timestamp NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_pipeline_autopilot_policy_tenant
    ON pipeline_autopilot_policies (tenant_id);

CREATE TABLE IF NOT EXISTS pipeline_autopilot_runs (
    id bigserial PRIMARY KEY,
    public_id uuid NOT NULL DEFAULT gen_random_uuid(),
    tenant_id varchar(64) NOT NULL,
    trigger varchar(24) NOT NULL,
    mode varchar(24) NOT NULL,
    elevated_mode varchar(32),
    status varchar(24) NOT NULL,
    headline varchar(32),
    started_at timestamp NOT NULL,
    finished_at timestamp,
    summary text,
    health_before jsonb,
    health_after jsonb,
    created_by varchar(255),
    error text,
    error_class varchar(48) NOT NULL DEFAULT 'none',
    created_at timestamp NOT NULL DEFAULT now(),
    updated_at timestamp NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_pipeline_autopilot_runs_public_id
    ON pipeline_autopilot_runs (public_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_autopilot_runs_tenant
    ON pipeline_autopilot_runs (tenant_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_autopilot_runs_status
    ON pipeline_autopilot_runs (status);
CREATE INDEX IF NOT EXISTS idx_pipeline_autopilot_runs_started_at
    ON pipeline_autopilot_runs (started_at);
CREATE INDEX IF NOT EXISTS idx_pipeline_autopilot_runs_error_class
    ON pipeline_autopilot_runs (error_class);

CREATE TABLE IF NOT EXISTS pipeline_autopilot_actions (
    id bigserial PRIMARY KEY,
    public_id uuid NOT NULL DEFAULT gen_random_uuid(),
    run_id bigint NOT NULL,
    tenant_id varchar(64) NOT NULL,
    lane varchar(32) NOT NULL,
    verdict varchar(32) NOT NULL,
    source_filter varchar(255),
    target_queue varchar(32),
    content_item_id uuid,
    status varchar(32) NOT NULL,
    outcome varchar(24),
    reason text,
    guardrail varchar(64),
    requested_count integer NOT NULL DEFAULT 0,
    enqueued_count integer NOT NULL DEFAULT 0,
    error_count integer NOT NULL DEFAULT 0,
    output jsonb,
    started_at timestamp NOT NULL,
    finished_at timestamp,
    created_at timestamp NOT NULL DEFAULT now(),
    updated_at timestamp NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_pipeline_autopilot_actions_public_id
    ON pipeline_autopilot_actions (public_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_autopilot_actions_run_id
    ON pipeline_autopilot_actions (run_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_autopilot_actions_tenant
    ON pipeline_autopilot_actions (tenant_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_autopilot_actions_status
    ON pipeline_autopilot_actions (status);
CREATE INDEX IF NOT EXISTS idx_pipeline_autopilot_actions_lane
    ON pipeline_autopilot_actions (lane);
CREATE INDEX IF NOT EXISTS idx_pipeline_autopilot_actions_content
    ON pipeline_autopilot_actions (content_item_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_autopilot_actions_outcome
    ON pipeline_autopilot_actions (outcome);
CREATE INDEX IF NOT EXISTS idx_pipeline_autopilot_actions_source_created
    ON pipeline_autopilot_actions (tenant_id, source_filter, created_at);
