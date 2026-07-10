-- Preferences Autopilot — Stage 7.
-- Bounded, ledgered supervisor for the topic-catalog maintenance loop + proposal
-- advisor. Adds policy/run/action tables, a durable affinity-recompute queue, the
-- explicit topics.needs_remap dirty flag, and additive proposal advisor columns.
-- All additive: no destructive change to the live preference system.

CREATE TABLE IF NOT EXISTS preference_autopilot_policies (
    id BIGSERIAL PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL DEFAULT 'default',
    enabled BOOLEAN NOT NULL DEFAULT FALSE,
    mode VARCHAR(24) NOT NULL DEFAULT 'observe',
    interval_minutes INTEGER NOT NULL DEFAULT 15,

    max_item_candidates INTEGER NOT NULL DEFAULT 250,
    max_story_candidates INTEGER NOT NULL DEFAULT 100,
    max_dirty_topics INTEGER NOT NULL DEFAULT 3,
    max_users_recompute INTEGER NOT NULL DEFAULT 100,
    max_proposals_enriched INTEGER NOT NULL DEFAULT 20,
    max_embedding_calls INTEGER NOT NULL DEFAULT 20,
    max_translation_calls INTEGER NOT NULL DEFAULT 10,
    max_mined_proposals INTEGER NOT NULL DEFAULT 25,
    max_centroid_refresh INTEGER NOT NULL DEFAULT 3,
    max_pending_proposals INTEGER NOT NULL DEFAULT 100,

    coverage_floor_foryou_pct INTEGER NOT NULL DEFAULT 70,
    coverage_floor_news_pct INTEGER NOT NULL DEFAULT 60,
    coverage_floor_story_pct INTEGER NOT NULL DEFAULT 50,

    high_confidence DOUBLE PRECISION NOT NULL DEFAULT 0.80,
    advisory_reject_floor DOUBLE PRECISION NOT NULL DEFAULT 0.35,
    duplicate_cosine DOUBLE PRECISION NOT NULL DEFAULT 0.90,

    failure_breaker_pct INTEGER NOT NULL DEFAULT 25,
    dead_topic_days INTEGER NOT NULL DEFAULT 14,

    trust_min_decisions INTEGER NOT NULL DEFAULT 30,
    trust_min_agreement_pct INTEGER NOT NULL DEFAULT 90,

    item_map_cursor BIGINT NOT NULL DEFAULT 0,
    story_map_cursor BIGINT NOT NULL DEFAULT 0,
    dirty_item_cursor BIGINT NOT NULL DEFAULT 0,
    dirty_story_cursor BIGINT NOT NULL DEFAULT 0,

    paused_until TIMESTAMP,
    last_run_at TIMESTAMP,
    last_mine_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_preference_autopilot_policy_tenant
    ON preference_autopilot_policies (tenant_id);

-- Keep this migration rerunnable if an earlier development draft created the
-- policy table before dirty-sweep checkpoints were introduced.
ALTER TABLE preference_autopilot_policies
    ADD COLUMN IF NOT EXISTS dirty_item_cursor BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS dirty_story_cursor BIGINT NOT NULL DEFAULT 0;

-- Earned auto-approve tier (§15 later slice, now implemented). Human-flipped +
-- server-gated on trust eligibility; its own stricter confidence threshold
-- (0.85-floored) — never inherits the 0.80 high-confidence default.
ALTER TABLE preference_autopilot_policies
    ADD COLUMN IF NOT EXISTS auto_approve_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS auto_approve_min_confidence DOUBLE PRECISION NOT NULL DEFAULT 0.92,
    ADD COLUMN IF NOT EXISTS max_auto_approvals INTEGER NOT NULL DEFAULT 3;

CREATE TABLE IF NOT EXISTS preference_autopilot_runs (
    id BIGSERIAL PRIMARY KEY,
    public_id UUID NOT NULL DEFAULT gen_random_uuid(),
    tenant_id VARCHAR(64) NOT NULL DEFAULT 'default',
    trigger VARCHAR(24) NOT NULL,
    mode VARCHAR(24) NOT NULL,
    status VARCHAR(24) NOT NULL,
    headline VARCHAR(32),
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,
    summary TEXT,
    recommended_action TEXT,
    stats_before JSONB,
    stats_after JSONB,
    created_by VARCHAR(255),
    error TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_preference_autopilot_runs_public_id
    ON preference_autopilot_runs (public_id);
CREATE INDEX IF NOT EXISTS idx_preference_autopilot_runs_tenant
    ON preference_autopilot_runs (tenant_id);
CREATE INDEX IF NOT EXISTS idx_preference_autopilot_runs_status
    ON preference_autopilot_runs (status);
CREATE INDEX IF NOT EXISTS idx_preference_autopilot_runs_started_at
    ON preference_autopilot_runs (started_at DESC);

CREATE TABLE IF NOT EXISTS preference_autopilot_actions (
    id BIGSERIAL PRIMARY KEY,
    public_id UUID NOT NULL DEFAULT gen_random_uuid(),
    run_id BIGINT NOT NULL REFERENCES preference_autopilot_runs(id) ON DELETE CASCADE,
    tenant_id VARCHAR(64) NOT NULL DEFAULT 'default',
    action_class VARCHAR(32) NOT NULL,
    subject_type VARCHAR(16) NOT NULL,
    subject_ref TEXT,
    status VARCHAR(24) NOT NULL,
    guardrail VARCHAR(48),
    reason TEXT,
    duration_ms INTEGER NOT NULL DEFAULT 0,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_preference_autopilot_actions_public_id
    ON preference_autopilot_actions (public_id);
CREATE INDEX IF NOT EXISTS idx_preference_autopilot_actions_run_id
    ON preference_autopilot_actions (run_id);
CREATE INDEX IF NOT EXISTS idx_preference_autopilot_actions_tenant
    ON preference_autopilot_actions (tenant_id);
CREATE INDEX IF NOT EXISTS idx_preference_autopilot_actions_class
    ON preference_autopilot_actions (action_class);
CREATE INDEX IF NOT EXISTS idx_preference_autopilot_actions_status
    ON preference_autopilot_actions (status);
-- Cross-run ledger explorer filters (cockpit deep logging).
CREATE INDEX IF NOT EXISTS idx_pref_actions_tenant_started
    ON preference_autopilot_actions (tenant_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_pref_actions_tenant_class_started
    ON preference_autopilot_actions (tenant_id, action_class, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_pref_actions_tenant_status_started
    ON preference_autopilot_actions (tenant_id, status, started_at DESC);

CREATE TABLE IF NOT EXISTS preference_affinity_recompute_queue (
    id BIGSERIAL PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL DEFAULT 'default',
    user_id UUID NOT NULL,
    reason VARCHAR(24) NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_pref_recompute_queue_tenant_user
    ON preference_affinity_recompute_queue (tenant_id, user_id);

-- Explicit dirty-state boundary (§0.1.2). Human catalog intent sets it; the
-- autopilot's dirty sweep clears it after a successful limited remap. Derived
-- member-count/centroid writes must NOT set it.
ALTER TABLE topics
    ADD COLUMN IF NOT EXISTS needs_remap BOOLEAN NOT NULL DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS idx_topics_needs_remap
    ON topics (tenant_id, active, needs_remap);

-- Proposal advisor columns (§11). Scored state lives on the proposal so the queue
-- can sort by it directly and trust comparisons cannot be silently rewritten.
ALTER TABLE topic_proposals
    ADD COLUMN IF NOT EXISTS confidence DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS autopilot_flags JSONB,
    ADD COLUMN IF NOT EXISTS embedding vector(1024),
    ADD COLUMN IF NOT EXISTS embedding_input_hash VARCHAR(64),
    ADD COLUMN IF NOT EXISTS embedded_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS enriched_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS predicted_verdict VARCHAR(24),
    ADD COLUMN IF NOT EXISTS predicted_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS prediction_version VARCHAR(24);

INSERT INTO preference_autopilot_policies (tenant_id)
VALUES ('default')
ON CONFLICT (tenant_id) DO NOTHING;
