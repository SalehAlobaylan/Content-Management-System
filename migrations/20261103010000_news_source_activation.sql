-- Explicit operations only: no source activation or admission is seeded here.
CREATE TABLE news_collection_controls (
    tenant_id varchar(64) PRIMARY KEY,
    paused boolean NOT NULL DEFAULT false,
    version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
    updated_at timestamptz NOT NULL,
    updated_by varchar(128) NOT NULL
);
CREATE TABLE news_source_operations (
    public_id uuid PRIMARY KEY,
    tenant_id varchar(64) NOT NULL,
    actor varchar(128) NOT NULL,
    action varchar(16) NOT NULL CHECK (action IN ('enable','disable','pause','resume')),
    snapshot jsonb NOT NULL,
    result jsonb,
    created_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    applied_at timestamptz
);
CREATE INDEX news_source_operations_tenant_history ON news_source_operations(tenant_id, created_at DESC);
