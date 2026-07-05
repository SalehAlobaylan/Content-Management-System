-- Media Circulation Autopilot — stage 5, Slice 1 (persistence foundation).
--
-- Adds the per-tenant autopilot knobs to media_circulation_policies (grill
-- decisions G1–G13; see docs/media-autopilot-stage5-plan.md) and the two
-- ledger tables modeled on the News Autopilot pair (news_autopilot_runs /
-- news_autopilot_actions) plus the V1 §12 audit trio: the guardrail that
-- allowed/blocked each action and its byte/queue/feed impact.

-- ---------------------------------------------------------------
-- Policy: autopilot knobs (defaults per plan §11)
-- ---------------------------------------------------------------
ALTER TABLE media_circulation_policies
    ADD COLUMN IF NOT EXISTS autopilot_enabled boolean NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS autopilot_mode varchar(24) NOT NULL DEFAULT 'observe',
    ADD COLUMN IF NOT EXISTS autopilot_interval_minutes integer NOT NULL DEFAULT 360,
    ADD COLUMN IF NOT EXISTS autopilot_max_actions_per_run integer NOT NULL DEFAULT 8,
    ADD COLUMN IF NOT EXISTS autopilot_max_atomize_per_run integer NOT NULL DEFAULT 3,
    ADD COLUMN IF NOT EXISTS autopilot_max_queue_depth integer NOT NULL DEFAULT 100,
    ADD COLUMN IF NOT EXISTS autopilot_max_bytes_per_run bigint NOT NULL DEFAULT 1073741824,
    ADD COLUMN IF NOT EXISTS autopilot_evict_confidence_floor double precision NOT NULL DEFAULT 0.5,
    ADD COLUMN IF NOT EXISTS autopilot_trust_min_decisions integer NOT NULL DEFAULT 20,
    ADD COLUMN IF NOT EXISTS autopilot_trust_max_revert_pct integer NOT NULL DEFAULT 10,
    ADD COLUMN IF NOT EXISTS autopilot_paused_until timestamp,
    ADD COLUMN IF NOT EXISTS autopilot_elevated_mode varchar(32),
    ADD COLUMN IF NOT EXISTS autopilot_elevated_until timestamp,
    ADD COLUMN IF NOT EXISTS autopilot_last_run_at timestamp;

-- ---------------------------------------------------------------
-- Runs: one deterministic Autopilot pass (News pattern + elevated mode)
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS media_circulation_runs (
    id bigserial PRIMARY KEY,
    public_id uuid NOT NULL DEFAULT gen_random_uuid(),
    tenant_id varchar(64) NOT NULL,
    trigger varchar(24) NOT NULL,
    mode varchar(24) NOT NULL,
    elevated_mode varchar(32),
    status varchar(24) NOT NULL,
    started_at timestamp NOT NULL,
    finished_at timestamp,
    summary text,
    health_before jsonb,
    health_after jsonb,
    created_by varchar(255),
    error text,
    created_at timestamp NOT NULL DEFAULT now(),
    updated_at timestamp NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_media_circ_runs_public_id
    ON media_circulation_runs (public_id);
CREATE INDEX IF NOT EXISTS idx_media_circ_runs_tenant
    ON media_circulation_runs (tenant_id);
CREATE INDEX IF NOT EXISTS idx_media_circ_runs_status
    ON media_circulation_runs (status);
CREATE INDEX IF NOT EXISTS idx_media_circ_runs_started_at
    ON media_circulation_runs (started_at);

-- ---------------------------------------------------------------
-- Actions: audit-grade ledger (News pattern + guardrail + impact trio
-- + recommendation link). The skip/would-skip reason taxonomy lives in
-- the guardrail column; status stays a clean enum.
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS media_circulation_actions (
    id bigserial PRIMARY KEY,
    public_id uuid NOT NULL DEFAULT gen_random_uuid(),
    run_id bigint NOT NULL,
    tenant_id varchar(64) NOT NULL,
    recommendation_id uuid,
    tool_name varchar(80) NOT NULL,
    status varchar(24) NOT NULL,
    reason text,
    guardrail varchar(64),
    input jsonb,
    output jsonb,
    error text,
    byte_impact bigint NOT NULL DEFAULT 0,
    queue_impact integer NOT NULL DEFAULT 0,
    feed_impact integer NOT NULL DEFAULT 0,
    started_at timestamp NOT NULL,
    finished_at timestamp,
    created_at timestamp NOT NULL DEFAULT now(),
    updated_at timestamp NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_media_circ_actions_public_id
    ON media_circulation_actions (public_id);
CREATE INDEX IF NOT EXISTS idx_media_circ_actions_run_id
    ON media_circulation_actions (run_id);
CREATE INDEX IF NOT EXISTS idx_media_circ_actions_tenant
    ON media_circulation_actions (tenant_id);
CREATE INDEX IF NOT EXISTS idx_media_circ_actions_status
    ON media_circulation_actions (status);
CREATE INDEX IF NOT EXISTS idx_media_circ_actions_tool
    ON media_circulation_actions (tool_name);
CREATE INDEX IF NOT EXISTS idx_media_circ_actions_recommendation
    ON media_circulation_actions (recommendation_id);
