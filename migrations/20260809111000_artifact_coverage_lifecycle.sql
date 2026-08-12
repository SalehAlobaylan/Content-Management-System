-- M4 forward completion: approval correlation, fenced attempts, receipts,
-- immutable events, and independent per-artifact disable controls.
ALTER TABLE artifact_coverage_requests
  ADD COLUMN IF NOT EXISTS action_request_id uuid,
  ADD COLUMN IF NOT EXISTS approved_by varchar(128),
  ADD COLUMN IF NOT EXISTS approved_at timestamptz,
  ADD COLUMN IF NOT EXISTS claim_owner varchar(128),
  ADD COLUMN IF NOT EXISTS claim_epoch bigint NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS effect_started_at timestamptz,
  ADD COLUMN IF NOT EXISTS acceptance_proof jsonb NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS affected_subjects jsonb NOT NULL DEFAULT '[]'::jsonb,
  ADD COLUMN IF NOT EXISTS deep_links jsonb NOT NULL DEFAULT '[]'::jsonb,
  ADD COLUMN IF NOT EXISTS failure_class varchar(64),
  ADD CONSTRAINT artifact_coverage_action_unique UNIQUE (tenant_id, action_request_id);

CREATE TABLE IF NOT EXISTS artifact_coverage_attempts (
  id bigserial PRIMARY KEY,
  public_id uuid NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id varchar(64) NOT NULL,
  request_id uuid NOT NULL,
  attempt_number integer NOT NULL,
  owner varchar(16) NOT NULL CHECK (owner IN ('media','enrichment')),
  state varchar(24) NOT NULL CHECK (state IN ('claimed','running','verifying','succeeded','failed','cancelled','uncertain')),
  claim_token uuid NOT NULL,
  fence_token uuid NOT NULL,
  input_digest varchar(128) NOT NULL,
  deterministic_job_id varchar(255) NOT NULL,
  lease_expires_at timestamptz NOT NULL,
  heartbeat_at timestamptz NOT NULL,
  effect_started_at timestamptz,
  accepted_at timestamptz,
  finished_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, request_id, attempt_number)
);

CREATE TABLE IF NOT EXISTS artifact_coverage_receipts (
  id bigserial PRIMARY KEY,
  public_id uuid NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id varchar(64) NOT NULL,
  request_id uuid NOT NULL,
  attempt_id uuid NOT NULL,
  owner varchar(16) NOT NULL,
  artifact varchar(48) NOT NULL,
  producer_event_id varchar(255) NOT NULL,
  fence_token uuid NOT NULL,
  outcome varchar(16) NOT NULL CHECK (outcome IN ('accepted','persisted','failed','cancelled','unknown')),
  payload_digest varchar(128) NOT NULL,
  observed_at timestamptz NOT NULL,
  payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, owner, producer_event_id)
);
CREATE INDEX IF NOT EXISTS idx_artifact_coverage_verification
  ON artifact_coverage_requests(state, updated_at) WHERE state IN ('verifying','uncertain');

ALTER TABLE media_supply_controls DROP CONSTRAINT IF EXISTS media_supply_controls_control_key_check;
ALTER TABLE media_supply_controls ADD CONSTRAINT media_supply_controls_control_key_check CHECK (control_key IN (
  'supply_read_evaluation','normal_intake_scheduling','exceptional_recovery_execution','intake_admission_circuit',
  'supply_action:source_run.repair_missed_admission','supply_action:source_run.reclaim_dispatch_claim',
  'supply_action:source_run.transfer_execution_unit_lease','supply_action:source_run.adopt_unit_job',
  'supply_action:source_run.redeliver_receipt','supply_action:source_run.verify_effect',
  'supply_action:source_run.finalize_verified_no_change','supply_action:source_run.cancel_unstarted',
  'supply_action:pipeline.resume_exact_stage','supply_action:artifact.request_transcript',
  'supply_action:artifact.request_image_embedding','supply_action:artifact.request_text_embedding',
  'supply_action:artifact.request_llm_metadata'
));
