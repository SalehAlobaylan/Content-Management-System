CREATE TABLE IF NOT EXISTS user_source_prefs (
    tenant_id VARCHAR(64) NOT NULL,
    user_id UUID NOT NULL,
    source_key VARCHAR(320) NOT NULL,
    state VARCHAR(16) NOT NULL CHECK (state IN ('muted')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, user_id, source_key)
);

CREATE INDEX IF NOT EXISTS idx_user_source_prefs_user
    ON user_source_prefs (tenant_id, user_id);
