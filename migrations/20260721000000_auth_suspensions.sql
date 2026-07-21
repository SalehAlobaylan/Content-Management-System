-- IAM owns account lifecycle. This is only CMS's synchronous enforcement
-- mirror for rejecting access JWTs minted before the IAM suspension.
CREATE TABLE IF NOT EXISTS auth_suspensions (
    user_id UUID PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL DEFAULT 'default',
    suspended_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_auth_suspensions_tenant
    ON auth_suspensions(tenant_id);
