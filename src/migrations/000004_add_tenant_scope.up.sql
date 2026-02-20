ALTER TABLE admin_users
ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(64);

ALTER TABLE content_sources
ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(64);

ALTER TABLE content_items
ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(64);

UPDATE admin_users
SET tenant_id = 'default'
WHERE tenant_id IS NULL OR tenant_id = '';

UPDATE content_sources
SET tenant_id = 'default'
WHERE tenant_id IS NULL OR tenant_id = '';

UPDATE content_items
SET tenant_id = 'default'
WHERE tenant_id IS NULL OR tenant_id = '';

ALTER TABLE admin_users
ALTER COLUMN tenant_id SET DEFAULT 'default';
ALTER TABLE content_sources
ALTER COLUMN tenant_id SET DEFAULT 'default';
ALTER TABLE content_items
ALTER COLUMN tenant_id SET DEFAULT 'default';

ALTER TABLE admin_users
ALTER COLUMN tenant_id SET NOT NULL;
ALTER TABLE content_sources
ALTER COLUMN tenant_id SET NOT NULL;
ALTER TABLE content_items
ALTER COLUMN tenant_id SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_admin_users_tenant_id ON admin_users(tenant_id);
CREATE INDEX IF NOT EXISTS idx_content_sources_tenant_id ON content_sources(tenant_id);
CREATE INDEX IF NOT EXISTS idx_content_items_tenant_id ON content_items(tenant_id);
