-- Durable normal-operation stage ledger for independently scheduled News and
-- Pods processing. BullMQ/ARQ are delivery mechanisms; these rows are the
-- authoritative desired state, claim, effect correlation, and verification
-- record. This is deliberately separate from approval-required recovery.

ALTER TABLE content_items
  ADD COLUMN IF NOT EXISTS processing_generation bigint NOT NULL DEFAULT 1,
  ADD COLUMN IF NOT EXISTS processing_input_digest varchar(64);

-- Provider identities are tenant-scoped. The historical single-column index
-- made an unrelated tenant's key a global conflict and encouraged unscoped
-- duplicate reads.
DROP INDEX IF EXISTS idx_content_items_idempotency_key;
CREATE UNIQUE INDEX IF NOT EXISTS idx_content_items_tenant_idempotency
  ON content_items(tenant_id, idempotency_key)
  WHERE idempotency_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_content_items_processing_generation
  ON content_items(tenant_id, public_id, processing_generation);
CREATE UNIQUE INDEX IF NOT EXISTS uq_content_items_tenant_public_id
  ON content_items(tenant_id, public_id);

CREATE TABLE IF NOT EXISTS content_stage_requests (
  id bigserial PRIMARY KEY,
  public_id uuid NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id varchar(64) NOT NULL,
  content_item_id uuid NOT NULL,
  processing_generation bigint NOT NULL CHECK (processing_generation > 0),
  lane varchar(16) NOT NULL CHECK (lane IN ('news', 'pods')),
  stage varchar(48) NOT NULL CHECK (stage IN (
    'news_text_embedding', 'news_story_classification', 'news_asset', 'news_llm_metadata',
    'pods_media_artifacts', 'pods_text_embedding', 'pods_transcript',
    'pods_atomization', 'pods_caption_reembedding', 'pods_image_embedding',
    'pods_llm_metadata'
  )),
  owner varchar(32) NOT NULL CHECK (owner IN ('cms', 'aggregation_news', 'aggregation_pods', 'enrichment', 'media')),
  blocking_scope varchar(24) NOT NULL CHECK (blocking_scope IN ('content_ready', 'feed_delivery', 'optional')),
  state varchar(24) NOT NULL CHECK (state IN (
    'queued', 'claimed', 'running', 'verifying', 'verified', 'deferred',
    'uncertain', 'reconciling', 'failed', 'cancelled', 'superseded'
  )),
  input_fingerprint varchar(64) NOT NULL,
  policy_version varchar(64) NOT NULL DEFAULT 'v1',
  model_recipe varchar(128) NOT NULL DEFAULT '',
  idempotency_key varchar(255) NOT NULL,
  dependency_manifest jsonb NOT NULL DEFAULT '[]'::jsonb,
  workload_estimate jsonb NOT NULL DEFAULT '{}'::jsonb,
  not_before_at timestamptz,
  deadline_at timestamptz,
  claim_owner varchar(128),
  claim_token uuid,
  claim_epoch bigint NOT NULL DEFAULT 0,
  claim_expires_at timestamptz,
  cancellation_requested_at timestamptz,
  cancellation_reason text,
  accepted_at timestamptz,
  verified_at timestamptz,
  finished_at timestamptz,
  failure_class varchar(64),
  terminal_proof jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  FOREIGN KEY (tenant_id, content_item_id) REFERENCES content_items(tenant_id, public_id) ON DELETE RESTRICT,
  UNIQUE (tenant_id, public_id),
  UNIQUE (tenant_id, content_item_id, processing_generation, stage),
  UNIQUE (tenant_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_content_stage_requests_claim
  ON content_stage_requests(tenant_id, lane, owner, state, not_before_at, created_at);
CREATE INDEX IF NOT EXISTS idx_content_stage_requests_item
  ON content_stage_requests(tenant_id, content_item_id, processing_generation, blocking_scope, state);
CREATE UNIQUE INDEX IF NOT EXISTS uq_content_stage_request_active_input
  ON content_stage_requests(tenant_id, content_item_id, stage, input_fingerprint)
  WHERE state NOT IN ('verified', 'failed', 'cancelled', 'superseded');

CREATE TABLE IF NOT EXISTS content_stage_attempts (
  id bigserial PRIMARY KEY,
  public_id uuid NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id varchar(64) NOT NULL,
  request_id uuid NOT NULL,
  attempt_number integer NOT NULL CHECK (attempt_number > 0),
  lane varchar(16) NOT NULL CHECK (lane IN ('news', 'pods')),
  stage varchar(48) NOT NULL,
  owner varchar(32) NOT NULL,
  input_fingerprint varchar(64) NOT NULL,
  state varchar(24) NOT NULL CHECK (state IN (
    'claimed', 'running', 'verifying', 'verified', 'deferred', 'uncertain',
    'reconciling', 'failed', 'cancelled', 'superseded'
  )),
  claim_token uuid NOT NULL,
  fence_token uuid NOT NULL,
  lease_epoch bigint NOT NULL DEFAULT 1,
  deterministic_job_id varchar(255) NOT NULL,
  lease_expires_at timestamptz NOT NULL,
  heartbeat_at timestamptz NOT NULL,
  effect_started_at timestamptz,
  accepted_at timestamptz,
  finished_at timestamptz,
  failure_class varchar(64),
  failure_summary varchar(512),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  FOREIGN KEY (tenant_id, request_id) REFERENCES content_stage_requests(tenant_id, public_id) ON DELETE RESTRICT,
  UNIQUE (tenant_id, public_id),
  UNIQUE (tenant_id, request_id, attempt_number),
  UNIQUE (tenant_id, deterministic_job_id),
  UNIQUE (tenant_id, fence_token)
);

CREATE INDEX IF NOT EXISTS idx_content_stage_attempts_request
  ON content_stage_attempts(tenant_id, request_id, attempt_number DESC);
CREATE INDEX IF NOT EXISTS idx_content_stage_attempts_lease
  ON content_stage_attempts(state, lease_expires_at);

CREATE TABLE IF NOT EXISTS content_stage_receipts (
  id bigserial PRIMARY KEY,
  public_id uuid NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id varchar(64) NOT NULL,
  request_id uuid NOT NULL,
  attempt_id uuid NOT NULL,
  content_item_id uuid NOT NULL,
  processing_generation bigint NOT NULL,
  lane varchar(16) NOT NULL,
  stage varchar(48) NOT NULL,
  owner varchar(32) NOT NULL,
  producer_event_id uuid NOT NULL,
  fence_token uuid NOT NULL,
  input_fingerprint varchar(64) NOT NULL,
  outcome varchar(24) NOT NULL CHECK (outcome IN ('accepted', 'persisted', 'absent', 'failed', 'cancelled', 'unknown')),
  payload_digest varchar(64) NOT NULL,
  artifact_digest varchar(64) NOT NULL DEFAULT '',
  observed_at timestamptz NOT NULL,
  payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  FOREIGN KEY (tenant_id, request_id) REFERENCES content_stage_requests(tenant_id, public_id) ON DELETE RESTRICT,
  FOREIGN KEY (tenant_id, attempt_id) REFERENCES content_stage_attempts(tenant_id, public_id) ON DELETE RESTRICT,
  FOREIGN KEY (tenant_id, content_item_id) REFERENCES content_items(tenant_id, public_id) ON DELETE RESTRICT,
  UNIQUE (tenant_id, owner, producer_event_id)
);

CREATE INDEX IF NOT EXISTS idx_content_stage_receipts_request
  ON content_stage_receipts(tenant_id, request_id, attempt_id, fence_token);

CREATE TABLE IF NOT EXISTS content_stage_events (
  id bigserial PRIMARY KEY,
  public_id uuid NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id varchar(64) NOT NULL,
  request_id uuid NOT NULL,
  attempt_id uuid,
  sequence bigint NOT NULL,
  event_type varchar(48) NOT NULL,
  payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  occurred_at timestamptz NOT NULL DEFAULT now(),
  FOREIGN KEY (tenant_id, request_id) REFERENCES content_stage_requests(tenant_id, public_id) ON DELETE RESTRICT,
  FOREIGN KEY (tenant_id, attempt_id) REFERENCES content_stage_attempts(tenant_id, public_id) ON DELETE RESTRICT,
  UNIQUE (tenant_id, request_id, sequence)
);

CREATE OR REPLACE FUNCTION reject_content_stage_event_mutation() RETURNS trigger AS $$
BEGIN
  RAISE EXCEPTION 'content stage events are append-only';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS content_stage_events_append_only ON content_stage_events;
CREATE TRIGGER content_stage_events_append_only
  BEFORE UPDATE OR DELETE ON content_stage_events
  FOR EACH ROW EXECUTE FUNCTION reject_content_stage_event_mutation();

CREATE TABLE IF NOT EXISTS content_stage_cutovers (
  id bigserial PRIMARY KEY,
  tenant_id varchar(64) NOT NULL,
  lane varchar(16) NOT NULL CHECK (lane IN ('news', 'pods')),
  mode varchar(24) NOT NULL DEFAULT 'legacy' CHECK (mode IN ('legacy', 'shadow', 'durable_required')),
  protocol_version varchar(32) NOT NULL DEFAULT 'content-stage/v1',
  promoted_by varchar(128),
  promoted_at timestamptz,
  verification_digest varchar(64),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, lane)
);

CREATE TABLE IF NOT EXISTS content_stage_controls (
  id bigserial PRIMARY KEY,
  tenant_id varchar(64) NOT NULL,
  lane varchar(16) NOT NULL CHECK (lane IN ('news', 'pods')),
  scheduling_enabled boolean NOT NULL DEFAULT true,
  execution_enabled boolean NOT NULL DEFAULT true,
  optional_metadata_enabled boolean NOT NULL DEFAULT true,
  transcript_execution_enabled boolean NOT NULL DEFAULT true,
  reason varchar(255) NOT NULL DEFAULT '',
  updated_by varchar(128) NOT NULL DEFAULT 'system',
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, lane)
);

INSERT INTO content_stage_cutovers (tenant_id, lane, mode)
VALUES ('default', 'news', 'legacy'), ('default', 'pods', 'legacy')
ON CONFLICT (tenant_id, lane) DO NOTHING;

INSERT INTO content_stage_controls (tenant_id, lane)
VALUES ('default', 'news'), ('default', 'pods')
ON CONFLICT (tenant_id, lane) DO NOTHING;
