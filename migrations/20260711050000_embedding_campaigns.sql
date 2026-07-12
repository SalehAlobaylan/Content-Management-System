-- Embedding & Model Lifecycle System (stage 10) — Migration 3 of 3.
-- Campaign execution: campaigns, action ledger, exceptions. Ordered AFTER
-- 20260711040000_embedding_lifecycle.sql. Applied DELIBERATELY.

BEGIN;

CREATE TABLE IF NOT EXISTS embedding_campaigns (
    id                  bigserial PRIMARY KEY,
    tenant_id           varchar(64) NOT NULL,
    space               varchar(16) NOT NULL,
    state               varchar(24) NOT NULL,
    target_space_id     char(64) NOT NULL,
    target_model        varchar(80),
    target_revision     varchar(80),
    from_space_id       char(64),
    surface_scope       jsonb,
    descriptor_snapshot jsonb,
    items_per_batch     integer NOT NULL DEFAULT 200,
    batches_per_run     integer NOT NULL DEFAULT 1,
    daily_item_cap      integer NOT NULL DEFAULT 5000,
    retry_ceiling       integer NOT NULL DEFAULT 3,
    completed_count     integer NOT NULL DEFAULT 0,
    failed_count        integer NOT NULL DEFAULT 0,
    skipped_count       integer NOT NULL DEFAULT 0,
    started_by          varchar(120),
    approval_reason     text,
    blocked_reason      text,
    lease_expires_at    timestamptz,
    created_at          timestamptz NOT NULL DEFAULT now(),
    started_at          timestamptz,
    completed_at        timestamptz,
    updated_at          timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_embedding_campaigns_tenant ON embedding_campaigns (tenant_id);
CREATE INDEX IF NOT EXISTS idx_embedding_campaigns_state ON embedding_campaigns (state);
-- Only one non-terminal campaign per space, enforced in the DB.
CREATE UNIQUE INDEX IF NOT EXISTS idx_embedding_campaign_active_space
    ON embedding_campaigns (space)
    WHERE state IN ('draft','running','paused','blocked','verifying');

CREATE TABLE IF NOT EXISTS embedding_campaign_actions (
    id                   bigserial PRIMARY KEY,
    campaign_id          bigint NOT NULL,
    tenant_id            varchar(64) NOT NULL,
    batch_id             varchar(48),
    surface_key          varchar(48) NOT NULL,
    tool                 varchar(32) NOT NULL,
    target_id            varchar(80) NOT NULL,
    status               varchar(16) NOT NULL,
    guardrail            varchar(48),
    reason               text,
    expected_producer_id char(64) NOT NULL,
    observed_producer_id char(64),
    request_id           varchar(64),
    latency_ms           bigint NOT NULL DEFAULT 0,
    retry_number         integer NOT NULL DEFAULT 0,
    evidence             jsonb,
    created_at           timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_embedding_actions_campaign ON embedding_campaign_actions (campaign_id);
CREATE INDEX IF NOT EXISTS idx_embedding_actions_batch ON embedding_campaign_actions (batch_id);
-- One ownership per attempt. Concurrent runners calculate the same retry number,
-- so only one can claim it; later retries remain possible and auditable.
CREATE UNIQUE INDEX IF NOT EXISTS idx_embedding_action_ownership
    ON embedding_campaign_actions (campaign_id, surface_key, target_id, expected_producer_id, retry_number);

CREATE TABLE IF NOT EXISTS embedding_campaign_exceptions (
    id              bigserial PRIMARY KEY,
    campaign_id     bigint NOT NULL,
    tenant_id       varchar(64) NOT NULL,
    surface_key     varchar(48) NOT NULL,
    target_id       varchar(80) NOT NULL,
    failure_class   varchar(48),
    attempts        integer NOT NULL DEFAULT 0,
    status          varchar(16) NOT NULL DEFAULT 'open',
    waived_by       varchar(120),
    waiver_reason   text,
    waiver_expires  timestamptz,
    latest_evidence jsonb,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_embedding_exceptions_campaign ON embedding_campaign_exceptions (campaign_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_embedding_exception_target
    ON embedding_campaign_exceptions (campaign_id, surface_key, target_id);

COMMIT;
