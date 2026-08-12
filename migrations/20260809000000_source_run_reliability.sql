-- Pods Supply Continuity M1: durable, fenced source-run evidence.
--
-- This migration deliberately introduces no scheduler, queue access, or
-- provider side effect. It is the canonical persistence substrate required
-- before CMS can safely become the source-work admission authority.

ALTER TABLE source_run_requests
  ADD COLUMN IF NOT EXISTS lane VARCHAR(32) NOT NULL DEFAULT 'legacy',
  ADD COLUMN IF NOT EXISTS purpose VARCHAR(48) NOT NULL DEFAULT 'legacy',
  ADD COLUMN IF NOT EXISTS parent_request_id UUID,
  ADD COLUMN IF NOT EXISTS failed_scope JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS policy_fingerprint VARCHAR(128),
  ADD COLUMN IF NOT EXISTS evidence_fingerprint VARCHAR(128),
  ADD COLUMN IF NOT EXISTS argument_fingerprint VARCHAR(128),
  ADD COLUMN IF NOT EXISTS cadence_window_start TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS not_before_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS deadline_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS next_dispatch_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS root_execution_unit_id UUID,
  ADD COLUMN IF NOT EXISTS manifest_state VARCHAR(16) NOT NULL DEFAULT 'open',
  ADD COLUMN IF NOT EXISTS manifest_version BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS manifest_sealed_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS expected_unit_count INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS completed_unit_count INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS expected_page_count INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS completed_page_count INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS expected_batch_count INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS completed_batch_count INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS finalized_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS verified_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS evidence_state VARCHAR(32) NOT NULL DEFAULT 'legacy_unknown';

ALTER TABLE content_sources
  ADD COLUMN IF NOT EXISTS last_claimed_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS last_attempted_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS last_provider_success_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS last_new_item_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS last_delivery_verified_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS next_due_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS failure_streak INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS intake_circuit_until TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS source_config_version BIGINT NOT NULL DEFAULT 1;

-- Legacy last_fetched_at records a prior claim/enqueue behavior. Preserve it
-- only as a claim observation; it is never promoted to provider success.
UPDATE content_sources
SET last_claimed_at = last_fetched_at
WHERE last_fetched_at IS NOT NULL
  AND last_claimed_at IS NULL;

ALTER TABLE source_run_requests
  DROP CONSTRAINT IF EXISTS source_run_requests_state_check;
ALTER TABLE source_run_requests
  ADD CONSTRAINT source_run_requests_state_check CHECK (state IN (
    'requested','accepted','running','verification_required','completed',
    'succeeded','partial','blocked','failed','cancelled','expired'
  ));
ALTER TABLE source_run_requests
  DROP CONSTRAINT IF EXISTS source_run_requests_manifest_state_check;
ALTER TABLE source_run_requests
  ADD CONSTRAINT source_run_requests_manifest_state_check
  CHECK (manifest_state IN ('open','sealing','sealed'));

CREATE INDEX IF NOT EXISTS idx_source_run_requests_due
  ON source_run_requests(tenant_id, lane, next_dispatch_at)
  WHERE state IN ('requested','accepted','running','verification_required');
CREATE INDEX IF NOT EXISTS idx_source_run_requests_expiry
  ON source_run_requests(expires_at, deadline_at)
  WHERE state IN ('requested','accepted','running','verification_required');
CREATE INDEX IF NOT EXISTS idx_source_run_requests_source_history
  ON source_run_requests(tenant_id, content_source_id, requested_at DESC);
CREATE INDEX IF NOT EXISTS idx_content_sources_supply_due
  ON content_sources(tenant_id, category, next_due_at)
  WHERE is_active = true AND next_due_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS source_run_attempts (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id VARCHAR(64) NOT NULL,
  source_run_request_id UUID NOT NULL,
  content_source_id UUID NOT NULL,
  attempt_number INTEGER NOT NULL CHECK (attempt_number > 0),
  state VARCHAR(32) NOT NULL CHECK (state IN (
    'authorized','claimed','running','verification_required','succeeded',
    'partial','blocked','failed','cancelled','expired'
  )),
  fence_token UUID NOT NULL,
  dispatcher_owner VARCHAR(128),
  dispatcher_token UUID,
  dispatcher_epoch BIGINT NOT NULL DEFAULT 0,
  dispatcher_lease_expires_at TIMESTAMPTZ,
  heartbeat_at TIMESTAMPTZ,
  root_execution_unit_id UUID,
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  verification_required_at TIMESTAMPTZ,
  failure_class VARCHAR(100),
  failure_summary VARCHAR(1000),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, source_run_request_id, attempt_number),
  UNIQUE (tenant_id, fence_token)
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_source_run_active_provider_attempt
  ON source_run_attempts(tenant_id, content_source_id)
  WHERE state IN ('authorized','claimed','running','verification_required');
CREATE INDEX IF NOT EXISTS idx_source_run_attempt_dispatch_lease
  ON source_run_attempts(dispatcher_lease_expires_at)
  WHERE state IN ('authorized','claimed','running','verification_required');

CREATE TABLE IF NOT EXISTS source_run_execution_units (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id VARCHAR(64) NOT NULL,
  source_run_request_id UUID NOT NULL,
  source_run_attempt_id UUID NOT NULL,
  content_source_id UUID NOT NULL,
  parent_unit_id UUID,
  unit_type VARCHAR(32) NOT NULL CHECK (unit_type IN ('coordinator','fetch_page','normalize_batch')),
  unit_key VARCHAR(255) NOT NULL,
  page_id VARCHAR(128),
  batch_id VARCHAR(128),
  job_id VARCHAR(255) NOT NULL,
  attempt_fence_token UUID NOT NULL,
  state VARCHAR(32) NOT NULL CHECK (state IN (
    'authorized','accepted','running','verification_required','succeeded',
    'failed','cancelled','expired'
  )),
  execution_owner VARCHAR(128),
  execution_lease_token UUID,
  execution_lease_epoch BIGINT NOT NULL DEFAULT 0,
  execution_lease_expires_at TIMESTAMPTZ,
  heartbeat_at TIMESTAMPTZ,
  effect_started_at TIMESTAMPTZ,
  cancellation_requested_at TIMESTAMPTZ,
  verification_required BOOLEAN NOT NULL DEFAULT false,
  declared_child_count INTEGER NOT NULL DEFAULT 0,
  declared_child_digest VARCHAR(128),
  terminal_outcome VARCHAR(48),
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, source_run_attempt_id, unit_key),
  UNIQUE (tenant_id, job_id)
);
CREATE INDEX IF NOT EXISTS idx_source_run_execution_units_attempt
  ON source_run_execution_units(tenant_id, source_run_attempt_id, state);
CREATE INDEX IF NOT EXISTS idx_source_run_execution_units_lease
  ON source_run_execution_units(execution_lease_expires_at)
  WHERE state IN ('authorized','accepted','running','verification_required');

CREATE TABLE IF NOT EXISTS source_run_receipts (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id VARCHAR(64) NOT NULL,
  producer_event_key VARCHAR(255) NOT NULL,
  source_run_request_id UUID NOT NULL,
  source_run_attempt_id UUID NOT NULL,
  execution_unit_id UUID NOT NULL,
  content_source_id UUID NOT NULL,
  content_item_id UUID,
  unit_job_id VARCHAR(255) NOT NULL,
  attempt_fence_token UUID NOT NULL,
  execution_lease_token UUID NOT NULL,
  schema_version VARCHAR(64) NOT NULL,
  producer VARCHAR(32) NOT NULL CHECK (producer IN ('aggregation','enrichment','media')),
  stage VARCHAR(48) NOT NULL,
  event_type VARCHAR(64) NOT NULL,
  outcome VARCHAR(48) NOT NULL,
  sequence BIGINT NOT NULL DEFAULT 0,
  page_id VARCHAR(128),
  batch_id VARCHAR(128),
  final_page BOOLEAN NOT NULL DEFAULT false,
  causation_id VARCHAR(255),
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  payload_digest VARCHAR(128) NOT NULL,
  produced_at TIMESTAMPTZ NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, producer_event_key)
);
CREATE INDEX IF NOT EXISTS idx_source_run_receipts_unit_sequence
  ON source_run_receipts(tenant_id, execution_unit_id, sequence, observed_at);
CREATE INDEX IF NOT EXISTS idx_source_run_receipts_request
  ON source_run_receipts(tenant_id, source_run_request_id, observed_at DESC);

CREATE TABLE IF NOT EXISTS source_run_verification_tasks (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id VARCHAR(64) NOT NULL,
  task_key VARCHAR(255) NOT NULL,
  source_run_request_id UUID NOT NULL,
  source_run_attempt_id UUID,
  execution_unit_id UUID,
  content_source_id UUID NOT NULL,
  effect_identity VARCHAR(255) NOT NULL,
  scope_type VARCHAR(48) NOT NULL,
  scope_id VARCHAR(255) NOT NULL,
  stage VARCHAR(48) NOT NULL,
  evidence_boundary VARCHAR(255) NOT NULL,
  causation_id VARCHAR(255) NOT NULL,
  verifier_name VARCHAR(100) NOT NULL,
  verifier_schema_version VARCHAR(64) NOT NULL,
  state VARCHAR(24) NOT NULL DEFAULT 'queued' CHECK (state IN ('queued','claimed','running','terminal')),
  claim_owner VARCHAR(128),
  claim_token UUID,
  claim_epoch BIGINT NOT NULL DEFAULT 0,
  claim_expires_at TIMESTAMPTZ,
  heartbeat_at TIMESTAMPTZ,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  not_before_at TIMESTAMPTZ,
  deadline_at TIMESTAMPTZ,
  terminal_verdict VARCHAR(16) CHECK (terminal_verdict IS NULL OR terminal_verdict IN ('present','absent','unknown')),
  terminal_event_id UUID,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, task_key)
);
CREATE INDEX IF NOT EXISTS idx_source_run_verification_tasks_claim
  ON source_run_verification_tasks(tenant_id, state, not_before_at, claim_expires_at)
  WHERE state IN ('queued','claimed','running');

CREATE TABLE IF NOT EXISTS source_run_reconciliation_events (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id VARCHAR(64) NOT NULL,
  event_key VARCHAR(255) NOT NULL,
  source_run_request_id UUID NOT NULL,
  source_run_attempt_id UUID,
  execution_unit_id UUID,
  content_source_id UUID NOT NULL,
  attempt_fence_token UUID,
  effect_identity VARCHAR(255) NOT NULL,
  scope_type VARCHAR(48) NOT NULL,
  scope_id VARCHAR(255) NOT NULL,
  stage VARCHAR(48) NOT NULL,
  verdict VARCHAR(16) NOT NULL CHECK (verdict IN ('present','absent','unknown')),
  evidence_snapshot VARCHAR(255) NOT NULL,
  verifier_schema_version VARCHAR(64) NOT NULL,
  verification_task_id UUID NOT NULL,
  verifier_lease_token UUID NOT NULL,
  causation_id VARCHAR(255) NOT NULL,
  provenance_digest VARCHAR(128) NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  observed_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, event_key)
);
CREATE INDEX IF NOT EXISTS idx_source_run_reconciliation_events_target
  ON source_run_reconciliation_events(tenant_id, source_run_request_id, execution_unit_id, observed_at DESC);

CREATE TABLE IF NOT EXISTS source_run_projection_work (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id VARCHAR(64) NOT NULL,
  evidence_kind VARCHAR(48) NOT NULL CHECK (evidence_kind IN ('receipt','reconciliation_event','upstream_observation_event')),
  evidence_id UUID NOT NULL,
  reducer_version VARCHAR(64) NOT NULL,
  state VARCHAR(24) NOT NULL DEFAULT 'queued' CHECK (state IN ('queued','claimed','succeeded','failed')),
  claim_owner VARCHAR(128),
  claim_token UUID,
  claim_expires_at TIMESTAMPTZ,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  projected_at TIMESTAMPTZ,
  error_summary VARCHAR(1000),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, evidence_kind, evidence_id, reducer_version)
);
CREATE INDEX IF NOT EXISTS idx_source_run_projection_work_claim
  ON source_run_projection_work(state, claim_expires_at, created_at)
  WHERE state IN ('queued','claimed','failed');

CREATE TABLE IF NOT EXISTS source_run_receipt_quarantine (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id VARCHAR(64) NOT NULL,
  producer VARCHAR(32) NOT NULL,
  producer_event_key VARCHAR(255),
  reason VARCHAR(100) NOT NULL,
  payload JSONB NOT NULL,
  payload_digest VARCHAR(128) NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_source_run_receipt_quarantine_expiry
  ON source_run_receipt_quarantine(expires_at);

CREATE TABLE IF NOT EXISTS source_run_receipt_rejection_audits (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  reason VARCHAR(100) NOT NULL,
  remote_class VARCHAR(64),
  payload_bytes BIGINT NOT NULL DEFAULT 0,
  observed_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS source_upstream_observations (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id VARCHAR(64) NOT NULL,
  content_source_id UUID NOT NULL,
  source_run_request_id UUID,
  provider_capability VARCHAR(64) NOT NULL,
  provider_version VARCHAR(64) NOT NULL,
  upstream_item_id VARCHAR(255) NOT NULL,
  upstream_fingerprint VARCHAR(128) NOT NULL,
  replay_locator JSONB NOT NULL DEFAULT '{}'::jsonb,
  replay_until TIMESTAMPTZ,
  provider_cursor VARCHAR(255),
  provider_page_id VARCHAR(128),
  observed_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, content_source_id, provider_version, upstream_item_id)
);
CREATE INDEX IF NOT EXISTS idx_source_upstream_observations_replay
  ON source_upstream_observations(tenant_id, replay_until)
  WHERE replay_until IS NOT NULL;

CREATE TABLE IF NOT EXISTS source_upstream_observation_events (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id VARCHAR(64) NOT NULL,
  event_key VARCHAR(255) NOT NULL,
  observation_id UUID NOT NULL,
  event_type VARCHAR(64) NOT NULL CHECK (event_type IN (
    'deferred','materialization_reserved','materialization_requested',
    'materialized','filtered','replay_expiring','replay_expired',
    'unrecoverable','authorized_abandonment'
  )),
  causation_id VARCHAR(255),
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  occurred_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, event_key)
);
CREATE INDEX IF NOT EXISTS idx_source_upstream_observation_events_observation
  ON source_upstream_observation_events(tenant_id, observation_id, occurred_at);

CREATE OR REPLACE FUNCTION reject_source_run_reliability_mutation()
RETURNS TRIGGER AS $$
BEGIN
  RAISE EXCEPTION 'source-run reliability evidence is append-only';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS source_run_receipts_append_only ON source_run_receipts;
CREATE TRIGGER source_run_receipts_append_only
BEFORE UPDATE OR DELETE ON source_run_receipts
FOR EACH ROW EXECUTE FUNCTION reject_source_run_reliability_mutation();

DROP TRIGGER IF EXISTS source_run_reconciliation_events_append_only ON source_run_reconciliation_events;
CREATE TRIGGER source_run_reconciliation_events_append_only
BEFORE UPDATE OR DELETE ON source_run_reconciliation_events
FOR EACH ROW EXECUTE FUNCTION reject_source_run_reliability_mutation();

DROP TRIGGER IF EXISTS source_upstream_observations_append_only ON source_upstream_observations;
CREATE TRIGGER source_upstream_observations_append_only
BEFORE UPDATE OR DELETE ON source_upstream_observations
FOR EACH ROW EXECUTE FUNCTION reject_source_run_reliability_mutation();

DROP TRIGGER IF EXISTS source_upstream_observation_events_append_only ON source_upstream_observation_events;
CREATE TRIGGER source_upstream_observation_events_append_only
BEFORE UPDATE OR DELETE ON source_upstream_observation_events
FOR EACH ROW EXECUTE FUNCTION reject_source_run_reliability_mutation();
