-- WP-05/WP-09: a pipeline repair can be verified only by the exact owner
-- receipt that carries its request, attempt, item-version and fence identity.
-- Historical processing evidence remains readable, but is deliberately not
-- eligible to complete a fenced repair.
ALTER TABLE content_processing_events
  ADD COLUMN IF NOT EXISTS pipeline_repair_request_id uuid,
  ADD COLUMN IF NOT EXISTS pipeline_repair_attempt_id uuid,
  ADD COLUMN IF NOT EXISTS pipeline_repair_fence_token uuid,
  ADD COLUMN IF NOT EXISTS expected_item_updated_at timestamptz,
  ADD COLUMN IF NOT EXISTS producer_event_id uuid,
  ADD COLUMN IF NOT EXISTS effect_input_digest varchar(128),
  ADD COLUMN IF NOT EXISTS execution_owner varchar(64);

CREATE UNIQUE INDEX IF NOT EXISTS uq_content_processing_event_producer_identity
  ON content_processing_events(tenant_id, producer_event_id)
  WHERE producer_event_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_content_processing_event_pipeline_repair
  ON content_processing_events(
    tenant_id, pipeline_repair_request_id, pipeline_repair_attempt_id,
    pipeline_repair_fence_token, content_item_id, stage, state
  )
  WHERE pipeline_repair_request_id IS NOT NULL;

ALTER TABLE pipeline_repair_requests
  ADD COLUMN IF NOT EXISTS effect_input_digest varchar(128),
  ADD COLUMN IF NOT EXISTS effect_producer_event_id uuid;

CREATE UNIQUE INDEX IF NOT EXISTS uq_pipeline_repair_effect_producer_identity
  ON pipeline_repair_requests(tenant_id, effect_producer_event_id)
  WHERE effect_producer_event_id IS NOT NULL;
