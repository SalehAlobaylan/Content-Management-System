-- Immutable, approval-bound plan for a bounded News compaction batch.
-- The manifest is evidence and an execution prerequisite; this migration does
-- not install a content deletion path.

CREATE TABLE IF NOT EXISTS retention_compaction_manifests (
    id BIGSERIAL PRIMARY KEY,
    public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    run_id BIGINT NOT NULL REFERENCES retention_runs(id) ON DELETE RESTRICT,
    action_id BIGINT UNIQUE REFERENCES retention_actions(id) ON DELETE RESTRICT,
    tenant_id VARCHAR(64) NOT NULL,
    policy_version INTEGER NOT NULL,
    timezone VARCHAR(64) NOT NULL,
    manifest_hash CHAR(64) NOT NULL UNIQUE,
    state VARCHAR(24) NOT NULL DEFAULT 'prepared'
        CHECK (state IN ('prepared', 'approved', 'expired', 'executed', 'blocked')),
    story_ids JSONB NOT NULL,
    anchor_content_ids JSONB NOT NULL,
    protected_content_ids JSONB NOT NULL,
    retire_content_ids JSONB NOT NULL,
    evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    story_count INTEGER NOT NULL CHECK (story_count >= 0),
    anchor_count INTEGER NOT NULL CHECK (anchor_count >= 0),
    protected_count INTEGER NOT NULL CHECK (protected_count >= 0),
    retire_count INTEGER NOT NULL CHECK (retire_count >= 0),
    estimated_bytes BIGINT NOT NULL DEFAULT 0 CHECK (estimated_bytes >= 0),
    expires_at TIMESTAMPTZ NOT NULL,
    approved_at TIMESTAMPTZ,
    approved_by VARCHAR(255),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_retention_compaction_manifests_tenant_state
    ON retention_compaction_manifests(tenant_id, state, created_at DESC);
