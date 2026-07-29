-- Compatibility foundation for the Retention owner bridge.
--
-- Older installations created storage_sweep_runs through GORM rather than a
-- canonical migration. Create the minimum durable table before the owner
-- integration migration adds its correlation/idempotency columns. Existing
-- tables are left untouched; this is additive and safe for already-migrated
-- databases.
CREATE TABLE IF NOT EXISTS storage_sweep_runs (
    id BIGSERIAL PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    deleted_count INTEGER NOT NULL DEFAULT 0,
    moved_to_cold_count INTEGER NOT NULL DEFAULT 0,
    re_encoded_count INTEGER NOT NULL DEFAULT 0,
    freed_bytes BIGINT NOT NULL DEFAULT 0,
    trigger VARCHAR(20) NOT NULL DEFAULT 'auto',
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_storage_sweep_runs_tenant
    ON storage_sweep_runs (tenant_id, started_at DESC);
