-- Feed Recovery remediation: durable per-item media object/CMS deletion saga.

CREATE TABLE IF NOT EXISTS feed_recovery_media_purge_items (
    id BIGSERIAL PRIMARY KEY,
    public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    run_id BIGINT NOT NULL REFERENCES feed_recovery_runs(id) ON DELETE RESTRICT,
    plan_id BIGINT NOT NULL REFERENCES feed_recovery_plans(id) ON DELETE RESTRICT,
    tenant_id VARCHAR(64) NOT NULL,
    content_item_id UUID NOT NULL,
    manifest_hash CHAR(64) NOT NULL,
    item_hash CHAR(64) NOT NULL,
    provider_objects JSONB NOT NULL DEFAULT '[]'::jsonb,
    recovery_map_present BOOLEAN NOT NULL DEFAULT FALSE,
    no_full_rollback BOOLEAN NOT NULL DEFAULT FALSE,
    state VARCHAR(32) NOT NULL CHECK (state IN ('prepared','object_delete_requested','object_deleted','cms_delete_pending','cms_deleted','verified','blocked')),
    provider_request_id TEXT,
    provider_result_hash CHAR(64),
    provider_idempotency_key TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    object_deleted_at TIMESTAMPTZ,
    cms_deleted_at TIMESTAMPTZ,
    verified_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (run_id, content_item_id)
);
CREATE INDEX IF NOT EXISTS idx_feed_recovery_media_saga_state
    ON feed_recovery_media_purge_items(tenant_id, state, updated_at);
