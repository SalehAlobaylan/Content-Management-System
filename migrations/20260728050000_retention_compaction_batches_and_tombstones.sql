-- Durable execution evidence for one bounded News compaction batch, plus
-- ingest tombstones so retirement cannot silently reappear on the next crawl.

CREATE TABLE IF NOT EXISTS retention_compaction_batches (
    id BIGSERIAL PRIMARY KEY,
    public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    action_id BIGINT NOT NULL REFERENCES retention_actions(id) ON DELETE RESTRICT,
    manifest_id BIGINT NOT NULL REFERENCES retention_compaction_manifests(id) ON DELETE RESTRICT,
    tenant_id VARCHAR(64) NOT NULL,
    batch_index INTEGER NOT NULL CHECK (batch_index >= 0),
    state VARCHAR(32) NOT NULL DEFAULT 'planned'
        CHECK (state IN ('planned', 'running', 'tool_succeeded', 'verifying', 'verification_passed', 'verification_failed', 'blocked')),
    target_hash CHAR(64) NOT NULL,
    target_ids JSONB NOT NULL,
    target_count INTEGER NOT NULL CHECK (target_count >= 0),
    estimated_bytes BIGINT NOT NULL DEFAULT 0 CHECK (estimated_bytes >= 0),
    before_evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    after_evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    error TEXT,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(action_id, batch_index)
);

CREATE INDEX IF NOT EXISTS idx_retention_compaction_batches_tenant_state
    ON retention_compaction_batches(tenant_id, state, created_at DESC);

CREATE TABLE IF NOT EXISTS news_ingest_tombstones (
    id BIGSERIAL PRIMARY KEY,
    public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    tenant_id VARCHAR(64) NOT NULL,
    identity_hash CHAR(64) NOT NULL,
    source_identity_hash CHAR(64) NOT NULL,
    original_url_hash CHAR(64) NOT NULL,
    original_content_id UUID NOT NULL,
    manifest_hash CHAR(64) NOT NULL,
    retirement_action_id BIGINT NOT NULL REFERENCES retention_actions(id) ON DELETE RESTRICT,
    reason VARCHAR(64) NOT NULL DEFAULT 'retention_compaction',
    replay_consumed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(tenant_id, identity_hash)
);

CREATE INDEX IF NOT EXISTS idx_news_ingest_tombstones_tenant_url
    ON news_ingest_tombstones(tenant_id, original_url_hash);
