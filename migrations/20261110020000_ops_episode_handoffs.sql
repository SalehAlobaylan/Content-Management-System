CREATE TABLE ops_handoffs (
    tenant_id varchar(64) NOT NULL,
    episode_id uuid NOT NULL,
    owner_id text NOT NULL,
    owner_name text NOT NULL,
    next_step text NOT NULL,
    next_check timestamptz,
    revision bigint NOT NULL CHECK (revision > 0),
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, episode_id)
);
