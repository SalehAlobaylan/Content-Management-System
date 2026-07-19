-- Consumer mutations can be retried by mobile after process death or network
-- loss. This ledger makes an Idempotency-Key replay return the original
-- interaction instead of incrementing a non-toggle counter twice.
CREATE TABLE IF NOT EXISTS consumer_request_idempotency (
    identity_scope VARCHAR(320) NOT NULL,
    endpoint VARCHAR(120) NOT NULL,
    idempotency_key VARCHAR(160) NOT NULL,
    request_digest CHAR(64) NOT NULL,
    interaction_public_id UUID NOT NULL REFERENCES user_interactions(public_id) ON DELETE RESTRICT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (identity_scope, endpoint, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_consumer_request_idempotency_created_at
    ON consumer_request_idempotency(created_at);
