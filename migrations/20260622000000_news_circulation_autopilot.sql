-- News Circulation Autopilot V1.
--
-- Autopilot is deterministic CMS orchestration around the existing circulation
-- engine: health checks, safe source tuning, snapshot refresh, and boosted
-- freshness tool access. It does not mutate story structure or editorial
-- overrides.

ALTER TABLE news_circulation_policies
    ADD COLUMN IF NOT EXISTS autopilot_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS autopilot_mode VARCHAR(24) NOT NULL DEFAULT 'safe_auto',
    ADD COLUMN IF NOT EXISTS autopilot_interval_minutes INTEGER NOT NULL DEFAULT 60,
    ADD COLUMN IF NOT EXISTS autopilot_boost_until TIMESTAMP,
    ADD COLUMN IF NOT EXISTS autopilot_paused_until TIMESTAMP,
    ADD COLUMN IF NOT EXISTS autopilot_last_run_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS autopilot_max_queue_depth INTEGER NOT NULL DEFAULT 100,
    ADD COLUMN IF NOT EXISTS autopilot_max_actions_per_run INTEGER NOT NULL DEFAULT 8;

CREATE TABLE IF NOT EXISTS news_autopilot_runs (
    id BIGSERIAL PRIMARY KEY,
    public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    tenant_id VARCHAR(64) NOT NULL,
    trigger VARCHAR(24) NOT NULL,
    mode VARCHAR(24) NOT NULL,
    tool_scope VARCHAR(24) NOT NULL,
    status VARCHAR(24) NOT NULL,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,
    summary TEXT,
    health_before JSONB,
    health_after JSONB,
    created_by VARCHAR(255),
    error TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_news_autopilot_runs_tenant
    ON news_autopilot_runs (tenant_id);
CREATE INDEX IF NOT EXISTS idx_news_autopilot_runs_status
    ON news_autopilot_runs (status);
CREATE INDEX IF NOT EXISTS idx_news_autopilot_runs_started_at
    ON news_autopilot_runs (started_at);

CREATE TABLE IF NOT EXISTS news_autopilot_actions (
    id BIGSERIAL PRIMARY KEY,
    public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    run_id BIGINT NOT NULL REFERENCES news_autopilot_runs(id) ON DELETE CASCADE,
    tenant_id VARCHAR(64) NOT NULL,
    tool_name VARCHAR(80) NOT NULL,
    status VARCHAR(24) NOT NULL,
    reason TEXT,
    input JSONB,
    output JSONB,
    error TEXT,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_news_autopilot_actions_run_id
    ON news_autopilot_actions (run_id);
CREATE INDEX IF NOT EXISTS idx_news_autopilot_actions_tenant
    ON news_autopilot_actions (tenant_id);
CREATE INDEX IF NOT EXISTS idx_news_autopilot_actions_tool
    ON news_autopilot_actions (tool_name);
CREATE INDEX IF NOT EXISTS idx_news_autopilot_actions_status
    ON news_autopilot_actions (status);
