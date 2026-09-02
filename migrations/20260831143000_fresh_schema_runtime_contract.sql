-- Restore runtime schema objects that older development databases received
-- from GORM AutoMigrate but fresh canonical migration replays did not.

-- Discovery imports upsert per tenant/profile/canonical source. The original
-- two-column index no longer matches the controller's ON CONFLICT target.
DROP INDEX IF EXISTS idx_source_suggestions_tenant_canonical;
CREATE UNIQUE INDEX IF NOT EXISTS idx_ss_tenant_profile_canonical
    ON source_suggestions (tenant_id, profile_id, canonical_key);

CREATE TABLE IF NOT EXISTS audit_logs (
    id BIGSERIAL PRIMARY KEY,
    public_id UUID NOT NULL DEFAULT gen_random_uuid(),
    tenant_id VARCHAR(64) NOT NULL DEFAULT 'default',
    user_id VARCHAR(128),
    user_email VARCHAR(255),
    action VARCHAR(64) NOT NULL,
    target_service VARCHAR(32) NOT NULL,
    target_resource VARCHAR(255),
    status VARCHAR(16) NOT NULL,
    error_message TEXT,
    payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_audit_logs_public_id ON audit_logs (public_id);
CREATE INDEX IF NOT EXISTS idx_audit_logs_tenant_id ON audit_logs (tenant_id);
CREATE INDEX IF NOT EXISTS idx_audit_logs_user_id ON audit_logs (user_id);
CREATE INDEX IF NOT EXISTS idx_audit_logs_action ON audit_logs (action);
CREATE INDEX IF NOT EXISTS idx_audit_logs_target_service ON audit_logs (target_service);
CREATE INDEX IF NOT EXISTS idx_audit_logs_created_at ON audit_logs (created_at);

CREATE TABLE IF NOT EXISTS content_flags (
    id BIGSERIAL PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    public_id UUID NOT NULL DEFAULT gen_random_uuid(),
    content_item_id UUID NOT NULL REFERENCES content_items(public_id) ON DELETE CASCADE,
    boost BOOLEAN NOT NULL DEFAULT FALSE,
    suppress BOOLEAN NOT NULL DEFAULT FALSE,
    pin_to_top BOOLEAN NOT NULL DEFAULT FALSE,
    exclude_from_feed BOOLEAN NOT NULL DEFAULT FALSE,
    boost_multiplier DOUBLE PRECISION NOT NULL DEFAULT 1.5,
    notes TEXT,
    set_by VARCHAR(255),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_content_flags_public_id ON content_flags (public_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_content_flags_content_tenant
    ON content_flags (content_item_id, tenant_id);
CREATE INDEX IF NOT EXISTS idx_content_flags_tenant ON content_flags (tenant_id);

CREATE TABLE IF NOT EXISTS storage_op_metrics (
    id BIGSERIAL PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    date DATE NOT NULL,
    tier VARCHAR(16) NOT NULL,
    op_class VARCHAR(2) NOT NULL,
    op_type VARCHAR(32) NOT NULL,
    source VARCHAR(32) NOT NULL,
    count BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_op_metrics_unique
    ON storage_op_metrics (tenant_id, date, tier, op_class, op_type, source);
CREATE INDEX IF NOT EXISTS idx_op_metrics_lookup
    ON storage_op_metrics (tenant_id, date);

ALTER TABLE content_items
    ADD COLUMN IF NOT EXISTS author_id UUID,
    ADD COLUMN IF NOT EXISTS file_size_bytes BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS archived_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS last_storage_check TIMESTAMP,
    ADD COLUMN IF NOT EXISTS last_restored_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS storage_tier VARCHAR(16),
    ADD COLUMN IF NOT EXISTS current_quality_profile_id BIGINT REFERENCES quality_profiles(id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS original_size_bytes BIGINT,
    ADD COLUMN IF NOT EXISTS original_bitrate_kbps INTEGER,
    ADD COLUMN IF NOT EXISTS current_bitrate_kbps INTEGER,
    ADD COLUMN IF NOT EXISTS media_version INTEGER NOT NULL DEFAULT 1;

CREATE INDEX IF NOT EXISTS idx_content_items_author_id ON content_items (author_id);
CREATE INDEX IF NOT EXISTS idx_content_items_current_quality_profile_id
    ON content_items (current_quality_profile_id);
