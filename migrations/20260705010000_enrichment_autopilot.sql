-- Enrichment Coverage Autopilot — Slice 1 (persistence foundation).
--
-- Unlike Media/News Circulation there is no pre-existing enrichment policy table
-- (coverage was driven by the in-memory bulk-run state in
-- adminEnrichmentController.go), so this migration creates the whole per-tenant
-- policy + the two ledger tables (runs / actions) from scratch. Modeled on the
-- media_circulation_runs / media_circulation_actions pair.
-- See docs/enrichment-autopilot-plan.md.

-- ---------------------------------------------------------------
-- Policy: per-tenant autopilot knobs (defaults per plan §11)
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS enrichment_autopilot_policies (
    id bigserial PRIMARY KEY,
    tenant_id varchar(64) NOT NULL,
    enabled boolean NOT NULL DEFAULT false,
    mode varchar(24) NOT NULL DEFAULT 'observe',
    interval_minutes integer NOT NULL DEFAULT 360,
    max_items_per_run integer NOT NULL DEFAULT 200,
    max_items_per_class integer NOT NULL DEFAULT 100,
    max_transcripts_per_run integer NOT NULL DEFAULT 10,
    max_queue_depth integer NOT NULL DEFAULT 100,
    failure_breaker_pct integer NOT NULL DEFAULT 30,
    stall_window_runs integer NOT NULL DEFAULT 2,
    age_floor_minutes integer NOT NULL DEFAULT 10,
    trust_min_attempts integer NOT NULL DEFAULT 50,
    trust_max_failure_pct integer NOT NULL DEFAULT 15,
    paused_until timestamp,
    elevated_mode varchar(32),
    elevated_until timestamp,
    last_run_at timestamp,
    created_at timestamp NOT NULL DEFAULT now(),
    updated_at timestamp NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_enrichment_autopilot_policy_tenant
    ON enrichment_autopilot_policies (tenant_id);

-- ---------------------------------------------------------------
-- Runs: one deterministic Autopilot pass. stats_before/stats_after are the
-- exact enrichment stats read-model at two timestamps (plan G10 analog), so the
-- coverage delta is directly comparable to what the admin sees on the page.
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS enrichment_autopilot_runs (
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
    stats_before jsonb,
    stats_after jsonb,
    created_by varchar(255),
    error text,
    created_at timestamp NOT NULL DEFAULT now(),
    updated_at timestamp NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_enrichment_autopilot_runs_public_id
    ON enrichment_autopilot_runs (public_id);
CREATE INDEX IF NOT EXISTS idx_enrichment_autopilot_runs_tenant
    ON enrichment_autopilot_runs (tenant_id);
CREATE INDEX IF NOT EXISTS idx_enrichment_autopilot_runs_status
    ON enrichment_autopilot_runs (status);
CREATE INDEX IF NOT EXISTS idx_enrichment_autopilot_runs_started_at
    ON enrichment_autopilot_runs (started_at);

-- ---------------------------------------------------------------
-- Actions: audit-grade ledger, one row per considered (item × artifact).
-- The skip/would-skip reason taxonomy lives in the guardrail column; status
-- stays a clean enum. transcription_job_id cross-links autopilot STT triggers
-- into the transcription-jobs / Media Studio surfaces.
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS enrichment_autopilot_actions (
    id bigserial PRIMARY KEY,
    public_id uuid NOT NULL DEFAULT gen_random_uuid(),
    run_id bigint NOT NULL,
    tenant_id varchar(64) NOT NULL,
    content_id uuid,
    artifact varchar(24) NOT NULL,
    status varchar(24) NOT NULL,
    reason text,
    guardrail varchar(64),
    transcription_job_id uuid,
    duration_ms integer NOT NULL DEFAULT 0,
    started_at timestamp NOT NULL,
    finished_at timestamp,
    created_at timestamp NOT NULL DEFAULT now(),
    updated_at timestamp NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_enrichment_autopilot_actions_public_id
    ON enrichment_autopilot_actions (public_id);
CREATE INDEX IF NOT EXISTS idx_enrichment_autopilot_actions_run_id
    ON enrichment_autopilot_actions (run_id);
CREATE INDEX IF NOT EXISTS idx_enrichment_autopilot_actions_tenant
    ON enrichment_autopilot_actions (tenant_id);
CREATE INDEX IF NOT EXISTS idx_enrichment_autopilot_actions_status
    ON enrichment_autopilot_actions (status);
CREATE INDEX IF NOT EXISTS idx_enrichment_autopilot_actions_artifact
    ON enrichment_autopilot_actions (artifact);
