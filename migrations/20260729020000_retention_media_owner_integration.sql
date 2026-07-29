-- Slice 6: durable Storage object/CMS saga evidence. Retention owner-request
-- correlation already has its canonical table in the foundation migration.
CREATE TABLE IF NOT EXISTS storage_operation_sagas (
  id BIGSERIAL PRIMARY KEY, public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id VARCHAR(64) NOT NULL, content_item_id UUID NOT NULL REFERENCES content_items(public_id) ON DELETE RESTRICT,
  owner_request_id BIGINT REFERENCES retention_owner_requests(id) ON DELETE RESTRICT,
  operation VARCHAR(32) NOT NULL, idempotency_key VARCHAR(255) NOT NULL,
  manifest_hash CHAR(64), correlation_id UUID, state VARCHAR(32) NOT NULL,
  object_evidence JSONB NOT NULL DEFAULT '{}'::jsonb, cms_evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  error TEXT, started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), completed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, content_item_id, operation, idempotency_key)
);
CREATE INDEX IF NOT EXISTS idx_storage_operation_sagas_reconcile
  ON storage_operation_sagas(tenant_id, state, updated_at DESC);

ALTER TABLE storage_sweep_runs ADD COLUMN IF NOT EXISTS public_id UUID NOT NULL DEFAULT gen_random_uuid();
ALTER TABLE storage_sweep_runs ADD COLUMN IF NOT EXISTS correlation_id UUID;
ALTER TABLE storage_sweep_runs ADD COLUMN IF NOT EXISTS owner_request_id BIGINT REFERENCES retention_owner_requests(id) ON DELETE SET NULL;
ALTER TABLE storage_sweep_runs ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR(255);
ALTER TABLE storage_sweep_runs ADD COLUMN IF NOT EXISTS manifest_hash CHAR(64);
CREATE UNIQUE INDEX IF NOT EXISTS idx_storage_sweep_runs_public_id ON storage_sweep_runs(public_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_storage_sweep_runs_owner_idempotency
  ON storage_sweep_runs(tenant_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
