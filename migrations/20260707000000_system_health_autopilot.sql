-- System Health / Incident Autopilot
-- CMS-owned probe runner, incident ledger, and bounded containment audit trail.

CREATE TABLE IF NOT EXISTS system_autopilot_policies (
    id BIGSERIAL PRIMARY KEY,
    scope VARCHAR(32) NOT NULL DEFAULT 'platform',
    enabled BOOLEAN NOT NULL DEFAULT FALSE,
    mode VARCHAR(24) NOT NULL DEFAULT 'observe',
    interval_minutes INTEGER NOT NULL DEFAULT 10,
    confirm_probes INTEGER NOT NULL DEFAULT 2,
    resolve_probes INTEGER NOT NULL DEFAULT 3,
    flap_cycles_24h INTEGER NOT NULL DEFAULT 3,
    containment_ttl_minutes INTEGER NOT NULL DEFAULT 60,
    containment_disabled_for JSONB DEFAULT '["news_circulation","media_circulation","media_studio"]'::jsonb,
    containment_paused_until TIMESTAMP,
    last_run_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_system_autopilot_policy_scope
    ON system_autopilot_policies (scope);

CREATE TABLE IF NOT EXISTS system_incident_episodes (
    id BIGSERIAL PRIMARY KEY,
    public_id UUID NOT NULL DEFAULT gen_random_uuid(),
    root_service VARCHAR(48) NOT NULL,
    verdict VARCHAR(48) NOT NULL,
    status VARCHAR(32) NOT NULL,
    severity VARCHAR(24) NOT NULL DEFAULT 'warning',
    shadow BOOLEAN NOT NULL DEFAULT FALSE,
    summary TEXT,
    root_cause_hint TEXT,
    evidence JSONB,
    timeline JSONB,
    containment JSONB,
    first_detected_at TIMESTAMP NOT NULL,
    last_seen_at TIMESTAMP NOT NULL,
    recovering_since TIMESTAMP,
    resolved_at TIMESTAMP,
    closed_by VARCHAR(255),
    close_reason TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_system_incident_episodes_public_id
    ON system_incident_episodes (public_id);
CREATE INDEX IF NOT EXISTS idx_system_incident_episodes_root_service
    ON system_incident_episodes (root_service);
CREATE INDEX IF NOT EXISTS idx_system_incident_episodes_verdict
    ON system_incident_episodes (verdict);
CREATE INDEX IF NOT EXISTS idx_system_incident_episodes_status
    ON system_incident_episodes (status);
CREATE INDEX IF NOT EXISTS idx_system_incident_episodes_shadow
    ON system_incident_episodes (shadow);
CREATE INDEX IF NOT EXISTS idx_system_incident_episodes_first_detected
    ON system_incident_episodes (first_detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_system_incident_episodes_last_seen
    ON system_incident_episodes (last_seen_at DESC);

CREATE TABLE IF NOT EXISTS system_autopilot_runs (
    id BIGSERIAL PRIMARY KEY,
    public_id UUID NOT NULL DEFAULT gen_random_uuid(),
    trigger VARCHAR(24) NOT NULL,
    mode VARCHAR(24) NOT NULL,
    status VARCHAR(24) NOT NULL,
    headline VARCHAR(32) NOT NULL,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,
    summary TEXT,
    probe_results JSONB,
    created_by VARCHAR(255),
    error TEXT,
    error_class VARCHAR(48) NOT NULL DEFAULT 'none',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_system_autopilot_runs_public_id
    ON system_autopilot_runs (public_id);
CREATE INDEX IF NOT EXISTS idx_system_autopilot_runs_status
    ON system_autopilot_runs (status);
CREATE INDEX IF NOT EXISTS idx_system_autopilot_runs_started_at
    ON system_autopilot_runs (started_at DESC);
CREATE INDEX IF NOT EXISTS idx_system_autopilot_runs_error_class
    ON system_autopilot_runs (error_class);

CREATE TABLE IF NOT EXISTS system_autopilot_actions (
    id BIGSERIAL PRIMARY KEY,
    public_id UUID NOT NULL DEFAULT gen_random_uuid(),
    run_id BIGINT NOT NULL REFERENCES system_autopilot_runs(id) ON DELETE CASCADE,
    episode_id BIGINT REFERENCES system_incident_episodes(id) ON DELETE SET NULL,
    target VARCHAR(64) NOT NULL,
    action VARCHAR(48) NOT NULL,
    verdict VARCHAR(48),
    status VARCHAR(32) NOT NULL,
    guardrail VARCHAR(64),
    reason TEXT,
    output JSONB,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_system_autopilot_actions_public_id
    ON system_autopilot_actions (public_id);
CREATE INDEX IF NOT EXISTS idx_system_autopilot_actions_run_id
    ON system_autopilot_actions (run_id);
CREATE INDEX IF NOT EXISTS idx_system_autopilot_actions_episode_id
    ON system_autopilot_actions (episode_id);
CREATE INDEX IF NOT EXISTS idx_system_autopilot_actions_target
    ON system_autopilot_actions (target);
CREATE INDEX IF NOT EXISTS idx_system_autopilot_actions_action
    ON system_autopilot_actions (action);
CREATE INDEX IF NOT EXISTS idx_system_autopilot_actions_status
    ON system_autopilot_actions (status);

INSERT INTO system_autopilot_policies (scope)
VALUES ('platform')
ON CONFLICT (scope) DO NOTHING;
