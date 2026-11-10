CREATE TABLE news_delete_reviews (
    id uuid PRIMARY KEY,
    tenant_id varchar(64) NOT NULL,
    actor text NOT NULL,
    selection jsonb NOT NULL,
    targets jsonb NOT NULL,
    expires_at timestamptz NOT NULL,
    deleted_count bigint CHECK (deleted_count >= 0)
);
