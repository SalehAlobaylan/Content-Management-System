CREATE TABLE IF NOT EXISTS atomization_work_requests (
    id BIGSERIAL PRIMARY KEY,
    public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    tenant_id VARCHAR(64) NOT NULL,
    parent_content_item_id UUID NOT NULL,
    parent_updated_at TIMESTAMPTZ NOT NULL,
    transcript_id UUID NOT NULL,
    transcript_fingerprint VARCHAR(128) NOT NULL,
    policy_hash VARCHAR(128) NOT NULL,
    input_fingerprint VARCHAR(128) NOT NULL,
    action_request_id UUID NULL REFERENCES media_supply_action_requests(public_id),
    state VARCHAR(24) NOT NULL CHECK (state IN ('queued','claimed','running','verifying','uncertain','succeeded','failed','cancelled')),
    claim_owner VARCHAR(128), claim_token UUID, fence_token UUID,
    claim_epoch BIGINT NOT NULL DEFAULT 0, claim_expires_at TIMESTAMPTZ,
    effect_started_at TIMESTAMPTZ, cancellation_requested_at TIMESTAMPTZ,
    checkpoints JSONB NOT NULL DEFAULT '{}'::jsonb,
    terminal_proof JSONB NOT NULL DEFAULT '{}'::jsonb,
    failure_class VARCHAR(64), approved_by VARCHAR(128) NOT NULL,
    approved_at TIMESTAMPTZ NOT NULL, finished_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_atomization_work_active_input
    ON atomization_work_requests(tenant_id, parent_content_item_id, input_fingerprint)
    WHERE state IN ('queued','claimed','running','verifying','uncertain');
CREATE INDEX IF NOT EXISTS idx_atomization_work_claim ON atomization_work_requests(state, created_at);

CREATE TABLE IF NOT EXISTS atomization_work_attempts (
    id BIGSERIAL PRIMARY KEY, public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    tenant_id VARCHAR(64) NOT NULL, request_id UUID NOT NULL REFERENCES atomization_work_requests(public_id),
    attempt_number INTEGER NOT NULL CHECK (attempt_number BETWEEN 1 AND 2),
    state VARCHAR(24) NOT NULL, claim_token UUID NOT NULL, fence_token UUID NOT NULL,
    deterministic_job_id VARCHAR(255) NOT NULL, lease_expires_at TIMESTAMPTZ NOT NULL,
    heartbeat_at TIMESTAMPTZ NOT NULL, effect_started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(tenant_id, request_id, attempt_number), UNIQUE(tenant_id, deterministic_job_id)
);

CREATE TABLE IF NOT EXISTS atomization_work_events (
    id BIGSERIAL PRIMARY KEY, public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    tenant_id VARCHAR(64) NOT NULL, request_id UUID NOT NULL REFERENCES atomization_work_requests(public_id),
    sequence BIGINT NOT NULL, event_type VARCHAR(48) NOT NULL, payload JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(tenant_id, request_id, sequence)
);

CREATE TABLE IF NOT EXISTS studio_clearance_requests (
    id BIGSERIAL PRIMARY KEY, public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    tenant_id VARCHAR(64) NOT NULL, atomization_request_id UUID NOT NULL REFERENCES atomization_work_requests(public_id),
    child_ids JSONB NOT NULL, child_set_digest VARCHAR(128) NOT NULL,
    state VARCHAR(24) NOT NULL CHECK (state IN ('queued','claimed','running','verifying','uncertain','succeeded','failed','cancelled')),
    claim_token UUID, fence_token UUID, claim_epoch BIGINT NOT NULL DEFAULT 0, claim_expires_at TIMESTAMPTZ,
    effect_started_at TIMESTAMPTZ, terminal_proof JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), finished_at TIMESTAMPTZ,
    UNIQUE(tenant_id, atomization_request_id, child_set_digest)
);

CREATE OR REPLACE FUNCTION reject_atomization_event_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'atomization work events are append-only'; END $$;
DROP TRIGGER IF EXISTS trg_atomization_work_events_immutable ON atomization_work_events;
CREATE TRIGGER trg_atomization_work_events_immutable BEFORE UPDATE OR DELETE ON atomization_work_events
FOR EACH ROW EXECUTE FUNCTION reject_atomization_event_mutation();

ALTER TABLE media_supply_controls DROP CONSTRAINT IF EXISTS media_supply_controls_control_key_check;
ALTER TABLE media_supply_controls ADD CONSTRAINT media_supply_controls_control_key_check CHECK (control_key IN (
  'supply_read_evaluation','normal_intake_scheduling','exceptional_recovery_execution','intake_admission_circuit',
  'supply_action:source_run.repair_missed_admission','supply_action:source_run.reclaim_dispatch_claim',
  'supply_action:source_run.transfer_execution_unit_lease','supply_action:source_run.adopt_unit_job',
  'supply_action:source_run.redeliver_receipt','supply_action:source_run.verify_effect',
  'supply_action:source_run.finalize_verified_no_change','supply_action:source_run.cancel_unstarted',
  'supply_action:pipeline.resume_exact_stage','supply_action:artifact.request_transcript',
  'supply_action:artifact.request_image_embedding','supply_action:artifact.request_text_embedding',
  'supply_action:artifact.request_llm_metadata','supply_action:atomization.execute_exact_parent',
  'supply_action:studio.clear_exact_children'
));
