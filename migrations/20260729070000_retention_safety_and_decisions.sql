-- Retention remediation: immutable decision evidence and provider-aware
-- maintenance readiness. This migration does not mutate canonical content.

CREATE TABLE IF NOT EXISTS retention_action_decisions (
    id BIGSERIAL PRIMARY KEY,
    public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    action_id BIGINT NOT NULL REFERENCES retention_actions(id) ON DELETE RESTRICT,
    tenant_id VARCHAR(64) NOT NULL,
    action_class VARCHAR(80) NOT NULL,
    mode VARCHAR(24) NOT NULL CHECK (mode IN ('observe', 'assist', 'safe_auto')),
    decision VARCHAR(16) NOT NULL CHECK (decision IN ('approved', 'rejected')),
    actor VARCHAR(255) NOT NULL,
    reason TEXT,
    manifest_hash CHAR(64),
    evidence_fingerprint CHAR(64) NOT NULL,
    decided_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (action_id)
);
CREATE INDEX IF NOT EXISTS idx_retention_action_decisions_trust
    ON retention_action_decisions(tenant_id, action_class, mode, decided_at DESC);

ALTER TABLE retention_maintenance_reports
    ADD COLUMN IF NOT EXISTS provider_bytes BIGINT,
    ADD COLUMN IF NOT EXISTS provider_source VARCHAR(32) NOT NULL DEFAULT 'unavailable',
    ADD COLUMN IF NOT EXISTS provider_measured_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS provider_fresh BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS postgres_ready BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS provider_ready BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS blocking_reasons JSONB NOT NULL DEFAULT '[]'::jsonb;
