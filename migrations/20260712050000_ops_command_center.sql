-- Operations Command Center: human attention state, briefing cursor, and
-- audited, reversible fleet pause commands. Applied deliberately.
BEGIN;

CREATE TABLE IF NOT EXISTS ops_attention_state (
    id BIGSERIAL PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL DEFAULT 'default',
    attention_key VARCHAR(255) NOT NULL,
    state VARCHAR(16) NOT NULL CHECK (state IN ('acked', 'snoozed')),
    snoozed_until TIMESTAMPTZ,
    baseline_fingerprint VARCHAR(255) NOT NULL DEFAULT '',
    baseline_severity VARCHAR(16) NOT NULL DEFAULT 'info',
    baseline_count INTEGER NOT NULL DEFAULT 0,
    actor_id VARCHAR(128) NOT NULL DEFAULT '',
    actor_email VARCHAR(255) NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, attention_key)
);
CREATE INDEX IF NOT EXISTS idx_ops_attention_state_updated ON ops_attention_state (updated_at);

CREATE TABLE IF NOT EXISTS ops_briefing_cursors (
    id BIGSERIAL PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL DEFAULT 'default',
    admin_id VARCHAR(128) NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT to_timestamp(0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, admin_id)
);

CREATE TABLE IF NOT EXISTS ops_fleet_commands (
    id BIGSERIAL PRIMARY KEY,
    public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    tenant_id VARCHAR(64) NOT NULL DEFAULT 'default',
    command VARCHAR(24) NOT NULL CHECK (command IN ('pause_member', 'pause_all', 'resume')),
    scope VARCHAR(96) NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    ttl_minutes INTEGER,
    idempotency_key VARCHAR(128) NOT NULL,
    source_command_id UUID REFERENCES ops_fleet_commands(public_id),
    actor_id VARCHAR(128) NOT NULL DEFAULT '',
    actor_email VARCHAR(255) NOT NULL DEFAULT '',
    status VARCHAR(16) NOT NULL DEFAULT 'succeeded' CHECK (status IN ('succeeded', 'partial', 'failed')),
    counts JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, idempotency_key)
);
CREATE INDEX IF NOT EXISTS idx_ops_fleet_commands_created ON ops_fleet_commands (created_at DESC);

CREATE TABLE IF NOT EXISTS ops_fleet_command_actions (
    id BIGSERIAL PRIMARY KEY,
    public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    command_id BIGINT NOT NULL REFERENCES ops_fleet_commands(id) ON DELETE CASCADE,
    source_action_id BIGINT REFERENCES ops_fleet_command_actions(id),
    member_key VARCHAR(64) NOT NULL,
    lane_key VARCHAR(64) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL DEFAULT 'default',
    prior_paused_until TIMESTAMPTZ,
    written_paused_until TIMESTAMPTZ,
    outcome VARCHAR(24) NOT NULL,
    guardrail VARCHAR(64),
    reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_ops_fleet_command_actions_command ON ops_fleet_command_actions (command_id, id);
CREATE INDEX IF NOT EXISTS idx_ops_fleet_command_actions_target ON ops_fleet_command_actions (member_key, lane_key, tenant_id);

COMMIT;
