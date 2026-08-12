-- M3: exact, tenant-scoped pipeline repair.  This is deliberately separate
-- from the old Pipeline Autopilot audit rows: a repair is an immutable command
-- with a fenced owner lease and independently-verifiable terminal proof.
CREATE TABLE IF NOT EXISTS pipeline_repair_requests (
  id bigserial PRIMARY KEY,
  public_id uuid NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id varchar(64) NOT NULL,
  content_item_id uuid NOT NULL,
  expected_item_updated_at timestamptz NOT NULL,
  expected_status varchar(20) NOT NULL,
  stage varchar(48) NOT NULL CHECK (stage IN ('media_download','media_transcode','media_thumbnail','text_embedding')),
  source_run_request_id bigint NULL,
  prior_stage_evidence_digest varchar(128) NOT NULL,
  repair_class varchar(32) NOT NULL DEFAULT 'approval_required',
  idempotency_key varchar(255) NOT NULL,
  deterministic_job_id varchar(255) NOT NULL,
  state varchar(32) NOT NULL CHECK (state IN ('awaiting_approval','queued','claimed','running','verifying','succeeded','failed','cancelled','uncertain')),
  approved_by varchar(128), approved_at timestamptz,
  claim_owner varchar(128), claim_token uuid, claim_epoch bigint NOT NULL DEFAULT 0,
  claim_expires_at timestamptz, cancellation_requested_at timestamptz,
  before_effects jsonb NOT NULL DEFAULT '{}'::jsonb,
  planned_effects jsonb NOT NULL DEFAULT '{}'::jsonb,
  after_effects jsonb NOT NULL DEFAULT '{}'::jsonb,
  verified_effects jsonb NOT NULL DEFAULT '{}'::jsonb,
  affected_subjects jsonb NOT NULL DEFAULT '[]'::jsonb,
  deep_links jsonb NOT NULL DEFAULT '[]'::jsonb,
  failure_class varchar(64), terminal_proof jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(), finished_at timestamptz
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_pipeline_repair_active_exact_stage
  ON pipeline_repair_requests(tenant_id, content_item_id, expected_item_updated_at, stage)
  WHERE state IN ('awaiting_approval','queued','claimed','running','verifying','uncertain');
CREATE UNIQUE INDEX IF NOT EXISTS uq_pipeline_repair_idempotency
  ON pipeline_repair_requests(tenant_id, idempotency_key);
CREATE INDEX IF NOT EXISTS idx_pipeline_repair_claim
  ON pipeline_repair_requests(state, claim_expires_at, created_at);

CREATE TABLE IF NOT EXISTS pipeline_repair_attempts (
  id bigserial PRIMARY KEY,
  public_id uuid NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id varchar(64) NOT NULL,
  repair_request_id uuid NOT NULL,
  attempt_number integer NOT NULL,
  state varchar(32) NOT NULL,
  fence_token uuid NOT NULL,
  owner_protocol varchar(64) NOT NULL,
  started_at timestamptz, effect_started_at timestamptz, finished_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, repair_request_id, attempt_number)
);

CREATE TABLE IF NOT EXISTS pipeline_repair_events (
  id bigserial PRIMARY KEY,
  public_id uuid NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id varchar(64) NOT NULL,
  repair_request_id uuid NOT NULL,
  attempt_id uuid,
  sequence bigint NOT NULL,
  event_key varchar(255) NOT NULL,
  event_type varchar(48) NOT NULL,
  payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  occurred_at timestamptz NOT NULL DEFAULT now(), created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, repair_request_id, sequence)
);
CREATE INDEX IF NOT EXISTS idx_pipeline_repair_events_cursor ON pipeline_repair_events(tenant_id, repair_request_id, sequence);

CREATE TABLE IF NOT EXISTS pipeline_stage_leases (
  id bigserial PRIMARY KEY,
  public_id uuid NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id varchar(64) NOT NULL,
  content_item_id uuid NOT NULL,
  item_updated_at timestamptz NOT NULL,
  stage varchar(48) NOT NULL CHECK (stage IN ('media_download','media_transcode','media_thumbnail','text_embedding')),
  execution_owner varchar(64) NOT NULL DEFAULT 'aggregation',
  repair_request_id uuid,
  deterministic_job_id varchar(255) NOT NULL,
  state varchar(24) NOT NULL CHECK (state IN ('claimed','running','verifying','terminal','cancelled','unknown')),
  lease_token uuid NOT NULL, fence_token uuid NOT NULL, lease_epoch bigint NOT NULL DEFAULT 1,
  lease_expires_at timestamptz NOT NULL, heartbeat_at timestamptz NOT NULL,
  effect_started_at timestamptz, terminal_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_pipeline_stage_live_lease
  ON pipeline_stage_leases(tenant_id, content_item_id, item_updated_at, stage)
  WHERE state IN ('claimed','running','verifying');
CREATE INDEX IF NOT EXISTS idx_pipeline_stage_lease_expiry ON pipeline_stage_leases(state, lease_expires_at);

CREATE OR REPLACE FUNCTION reject_pipeline_repair_event_mutation() RETURNS trigger AS $$
BEGIN RAISE EXCEPTION 'pipeline repair events are append-only'; END; $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS pipeline_repair_events_append_only ON pipeline_repair_events;
CREATE TRIGGER pipeline_repair_events_append_only BEFORE UPDATE OR DELETE ON pipeline_repair_events
FOR EACH ROW EXECUTE FUNCTION reject_pipeline_repair_event_mutation();

-- The new action is a static member of the existing subtractive-control set.
ALTER TABLE media_supply_controls DROP CONSTRAINT IF EXISTS media_supply_controls_control_key_check;
ALTER TABLE media_supply_controls ADD CONSTRAINT media_supply_controls_control_key_check CHECK (control_key IN (
  'supply_read_evaluation','normal_intake_scheduling','exceptional_recovery_execution','intake_admission_circuit',
  'supply_action:source_run.repair_missed_admission','supply_action:source_run.reclaim_dispatch_claim',
  'supply_action:source_run.transfer_execution_unit_lease','supply_action:source_run.adopt_unit_job',
  'supply_action:source_run.redeliver_receipt','supply_action:source_run.verify_effect',
  'supply_action:source_run.finalize_verified_no_change','supply_action:source_run.cancel_unstarted',
  'supply_action:pipeline.resume_exact_stage'
));
