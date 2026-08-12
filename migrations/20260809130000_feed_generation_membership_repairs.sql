CREATE TABLE IF NOT EXISTS feed_generation_membership_repairs (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id VARCHAR(64) NOT NULL,
  action_request_id UUID NOT NULL REFERENCES media_supply_action_requests(public_id),
  content_item_id UUID NOT NULL,
  expected_generation_id UUID NOT NULL REFERENCES feed_generations(public_id),
  expected_head_version BIGINT NOT NULL CHECK (expected_head_version > 0),
  expected_item_updated_at TIMESTAMPTZ NOT NULL,
  state VARCHAR(24) NOT NULL CHECK (state IN ('queued','running','verifying','succeeded','failed','cancelled','uncertain')),
  before_effects JSONB NOT NULL DEFAULT '{}'::jsonb,
  after_effects JSONB NOT NULL DEFAULT '{}'::jsonb,
  verified_effects JSONB NOT NULL DEFAULT '{}'::jsonb,
  failure_class VARCHAR(64),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ,
  UNIQUE(tenant_id, action_request_id),
  UNIQUE(tenant_id, content_item_id, expected_generation_id)
);

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
  'supply_action:studio.clear_exact_children','supply_action:feed_generation.attach_verified_member'
));
