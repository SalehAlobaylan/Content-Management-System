DROP INDEX IF EXISTS idx_content_items_tenant_id;
DROP INDEX IF EXISTS idx_content_sources_tenant_id;
DROP INDEX IF EXISTS idx_admin_users_tenant_id;

ALTER TABLE content_items DROP COLUMN IF EXISTS tenant_id;
ALTER TABLE content_sources DROP COLUMN IF EXISTS tenant_id;
ALTER TABLE admin_users DROP COLUMN IF EXISTS tenant_id;
