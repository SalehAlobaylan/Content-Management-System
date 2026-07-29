-- Safety freeze: destructive Retention/Recovery capabilities remain disabled
-- until an administrator explicitly enables each persisted rollout gate.
CREATE TABLE IF NOT EXISTS retention_execution_controls (
    id BIGSERIAL PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL UNIQUE,
    canonical_compaction_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    historical_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    owner_runs_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    feed_recovery_rotate_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    feed_recovery_purge_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    updated_by VARCHAR(255),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO retention_execution_controls (tenant_id)
VALUES ('default')
ON CONFLICT (tenant_id) DO NOTHING;
