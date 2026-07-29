-- Exact normalized destructive scope. Reseed limits remain in the recovery
-- request; these rows are the complete frozen purge target set.
ALTER TABLE feed_recovery_plans
    ADD COLUMN IF NOT EXISTS target_root_hash CHAR(64),
    ADD COLUMN IF NOT EXISTS target_scope VARCHAR(48),
    ADD COLUMN IF NOT EXISTS target_segments INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS target_byte_size BIGINT NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS feed_recovery_plan_targets (
    id BIGSERIAL PRIMARY KEY,
    plan_id BIGINT NOT NULL REFERENCES feed_recovery_plans(id) ON DELETE RESTRICT,
    tenant_id VARCHAR(64) NOT NULL,
    lane VARCHAR(16) NOT NULL CHECK (lane IN ('news', 'media', 'both')),
    target_type VARCHAR(32) NOT NULL CHECK (target_type IN ('news_content', 'media_content')),
    target_id UUID NOT NULL,
    ordinal BIGINT NOT NULL CHECK (ordinal > 0),
    protected BOOLEAN NOT NULL DEFAULT FALSE,
    protection_reason TEXT,
    evidence_hash CHAR(64) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (plan_id, target_type, target_id),
    UNIQUE (plan_id, target_type, ordinal)
);
CREATE INDEX IF NOT EXISTS idx_feed_recovery_plan_targets_scope
    ON feed_recovery_plan_targets(plan_id, lane, target_type, ordinal);
