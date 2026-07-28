-- CMS-controlled lifecycle ledger for short-lived external recovery artifacts.
-- Payload bytes remain in Aggregation-owned object storage and never duplicate
-- back into PostgreSQL.

CREATE TABLE IF NOT EXISTS retention_recovery_artifacts (
    id BIGSERIAL PRIMARY KEY,
    public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    action_id BIGINT NOT NULL UNIQUE REFERENCES retention_actions(id) ON DELETE RESTRICT,
    manifest_id BIGINT NOT NULL UNIQUE REFERENCES retention_compaction_manifests(id) ON DELETE RESTRICT,
    tenant_id VARCHAR(64) NOT NULL,
    artifact_key TEXT NOT NULL UNIQUE,
    sha256 CHAR(64) NOT NULL,
    compressed_bytes BIGINT NOT NULL CHECK (compressed_bytes >= 0),
    uncompressed_bytes BIGINT NOT NULL CHECK (uncompressed_bytes >= 0),
    state VARCHAR(24) NOT NULL CHECK (state IN ('verified', 'expired', 'deleted', 'delete_failed')),
    expires_at TIMESTAMPTZ NOT NULL,
    verified_at TIMESTAMPTZ,
    deleted_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_retention_recovery_artifacts_expiry
    ON retention_recovery_artifacts(state, expires_at ASC);
