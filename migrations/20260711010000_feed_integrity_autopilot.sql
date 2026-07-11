-- Feed Integrity Autopilot. Requires the base Feed Integrity migration.
DO $$
BEGIN
    IF to_regclass('public.feed_integrity_policies') IS NULL
       OR to_regclass('public.feed_integrity_runs') IS NULL
       OR to_regclass('public.feed_integrity_episodes') IS NULL THEN
        RAISE EXCEPTION 'feed integrity base migration must be applied first';
    END IF;
END $$;

ALTER TABLE feed_integrity_policies
    ADD COLUMN IF NOT EXISTS autopilot_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS autopilot_mode VARCHAR(24) NOT NULL DEFAULT 'observe',
    ADD COLUMN IF NOT EXISTS autopilot_paused_until TIMESTAMP,
    ADD COLUMN IF NOT EXISTS autopilot_action_modes JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS autopilot_action_hourly_cap INTEGER NOT NULL DEFAULT 2,
    ADD COLUMN IF NOT EXISTS autopilot_diagnostic_hourly_cap INTEGER NOT NULL DEFAULT 4,
    ADD COLUMN IF NOT EXISTS autopilot_cooldown_minutes INTEGER NOT NULL DEFAULT 60,
    ADD COLUMN IF NOT EXISTS autopilot_evidence_max_age_minutes INTEGER NOT NULL DEFAULT 10,
    ADD COLUMN IF NOT EXISTS autopilot_retry_limit INTEGER NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS autopilot_trust_min_decisions INTEGER NOT NULL DEFAULT 20,
    ADD COLUMN IF NOT EXISTS autopilot_trust_min_agreement_pct INTEGER NOT NULL DEFAULT 95;

ALTER TABLE feed_integrity_runs
    ADD COLUMN IF NOT EXISTS lane_results JSONB,
    ADD COLUMN IF NOT EXISTS autopilot_evaluated_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS autopilot_decision VARCHAR(32),
    ADD COLUMN IF NOT EXISTS autopilot_counts JSONB,
    ADD COLUMN IF NOT EXISTS autopilot_error_class VARCHAR(48) NOT NULL DEFAULT 'none';

ALTER TABLE feed_integrity_findings
    ADD COLUMN IF NOT EXISTS affected_count INTEGER NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS sample_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE feed_integrity_episodes
    ADD COLUMN IF NOT EXISTS violation_streak INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS clean_streak INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS flap_count_24h INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS attribution_version VARCHAR(32),
    ADD COLUMN IF NOT EXISTS recommended_action VARCHAR(80),
    ADD COLUMN IF NOT EXISTS latest_action_id BIGINT;

CREATE UNIQUE INDEX IF NOT EXISTS idx_feed_integrity_runs_id_tenant ON feed_integrity_runs(id, tenant_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_feed_integrity_episodes_id_tenant ON feed_integrity_episodes(id, tenant_id);

CREATE TABLE IF NOT EXISTS feed_integrity_actions (
    id BIGSERIAL PRIMARY KEY,
    public_id UUID NOT NULL DEFAULT gen_random_uuid(),
    tenant_id VARCHAR(64) NOT NULL,
    run_id BIGINT NOT NULL,
    episode_id BIGINT NOT NULL,
    retry_of_action_id BIGINT REFERENCES feed_integrity_actions(id) ON DELETE SET NULL,
    action_class VARCHAR(80) NOT NULL,
    owner_system VARCHAR(64) NOT NULL,
    target_scope TEXT NOT NULL,
    mode VARCHAR(24) NOT NULL,
    outcome VARCHAR(32) NOT NULL,
    decision VARCHAR(32) NOT NULL,
    guardrail VARCHAR(64),
    reason TEXT,
    idempotency_key VARCHAR(160) NOT NULL,
    evidence_fingerprint VARCHAR(64) NOT NULL,
    registry_version VARCHAR(32) NOT NULL,
    tool_version VARCHAR(32),
    verification_contract_version VARCHAR(32),
    input JSONB,
    output JSONB,
    verification JSONB,
    actor VARCHAR(255),
    correlation_id VARCHAR(80),
    claim_token VARCHAR(80),
    claimed_at TIMESTAMP,
    claim_expires_at TIMESTAMP,
    approved_at TIMESTAMP,
    executed_at TIMESTAMP,
    verification_due_at TIMESTAMP,
    finished_at TIMESTAMP,
    duration_ms BIGINT NOT NULL DEFAULT 0,
    error_class VARCHAR(48) NOT NULL DEFAULT 'none',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_feed_integrity_action_tenant_run
        FOREIGN KEY (run_id, tenant_id) REFERENCES feed_integrity_runs(id, tenant_id) ON DELETE CASCADE,
    CONSTRAINT fk_feed_integrity_action_tenant_episode
        FOREIGN KEY (episode_id, tenant_id) REFERENCES feed_integrity_episodes(id, tenant_id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_feed_integrity_actions_public_id ON feed_integrity_actions(public_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_feed_integrity_actions_idempotency ON feed_integrity_actions(idempotency_key);
CREATE INDEX IF NOT EXISTS idx_feed_integrity_actions_tenant_created ON feed_integrity_actions(tenant_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_feed_integrity_actions_run ON feed_integrity_actions(run_id);
CREATE INDEX IF NOT EXISTS idx_feed_integrity_actions_episode ON feed_integrity_actions(episode_id);
CREATE INDEX IF NOT EXISTS idx_feed_integrity_actions_class ON feed_integrity_actions(action_class);
CREATE INDEX IF NOT EXISTS idx_feed_integrity_actions_outcome ON feed_integrity_actions(outcome);
CREATE UNIQUE INDEX IF NOT EXISTS idx_feed_integrity_actions_one_active_episode
    ON feed_integrity_actions(episode_id)
    WHERE outcome IN ('approval_required','approved','ready','claimed','running','tool_succeeded','verifying');
CREATE INDEX IF NOT EXISTS idx_feed_integrity_runs_autopilot_pending
    ON feed_integrity_runs(tenant_id, started_at)
    WHERE status = 'completed' AND autopilot_evaluated_at IS NULL;

ALTER TABLE feed_integrity_episodes
    DROP CONSTRAINT IF EXISTS fk_feed_integrity_episode_latest_action;
ALTER TABLE feed_integrity_episodes
    ADD CONSTRAINT fk_feed_integrity_episode_latest_action
    FOREIGN KEY (latest_action_id) REFERENCES feed_integrity_actions(id) ON DELETE SET NULL;
