-- Canonical tenant scope for source configuration. Older databases received
-- this column from the former startup compatibility patch, but a fresh schema
-- replay must establish it before source-run reliability indexes reference it.

ALTER TABLE content_sources
    ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(64);

UPDATE content_sources
SET tenant_id = 'default'
WHERE tenant_id IS NULL OR tenant_id = '';

ALTER TABLE content_sources
    ALTER COLUMN tenant_id SET DEFAULT 'default',
    ALTER COLUMN tenant_id SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_content_sources_tenant_id
    ON content_sources (tenant_id);
