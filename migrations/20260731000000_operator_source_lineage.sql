-- Authoritative source-run lineage for Wahb Operator. Existing
-- source_run_telemetry remains a compatibility/metrics projection; these
-- records establish the request-to-worker correlation without querying BullMQ.

CREATE TABLE IF NOT EXISTS source_run_requests (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id VARCHAR(64) NOT NULL,
  content_source_id UUID NOT NULL,
  source_suggestion_id UUID,
  requested_by VARCHAR(24) NOT NULL CHECK (requested_by IN ('approval_handoff','manual','schedule','system')),
  requested_by_actor_id VARCHAR(255),
  state VARCHAR(24) NOT NULL CHECK (state IN ('requested','accepted','running','completed','failed')),
  aggregation_job_id VARCHAR(128),
  correlation_id VARCHAR(255) NOT NULL,
  idempotency_key VARCHAR(255) NOT NULL,
  requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  accepted_at TIMESTAMPTZ,
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  failure_class VARCHAR(100),
  failure_summary VARCHAR(1000),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(tenant_id, idempotency_key)
);
CREATE INDEX IF NOT EXISTS idx_source_run_requests_source_requested
  ON source_run_requests(tenant_id, content_source_id, requested_at DESC);
CREATE INDEX IF NOT EXISTS idx_source_run_requests_job
  ON source_run_requests(aggregation_job_id) WHERE aggregation_job_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_source_run_requests_suggestion
  ON source_run_requests(tenant_id, source_suggestion_id) WHERE source_suggestion_id IS NOT NULL;

ALTER TABLE content_items
  ADD COLUMN IF NOT EXISTS content_source_id UUID,
  ADD COLUMN IF NOT EXISTS source_run_request_id BIGINT;
CREATE INDEX IF NOT EXISTS idx_content_items_source_lineage
  ON content_items(tenant_id, content_source_id, source_run_request_id)
  WHERE content_source_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS content_processing_events (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id VARCHAR(64) NOT NULL,
  content_source_id UUID,
  source_run_request_id BIGINT,
  content_item_id UUID,
  stage VARCHAR(48) NOT NULL,
  state VARCHAR(24) NOT NULL CHECK (state IN ('requested','accepted','running','completed','failed','skipped')),
  producer VARCHAR(24) NOT NULL CHECK (producer IN ('cms','aggregation','enrichment','media')),
  job_id VARCHAR(128),
  correlation_id VARCHAR(255),
  idempotency_key VARCHAR(512),
  event_class VARCHAR(100) NOT NULL,
  error_class VARCHAR(100),
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_content_processing_events_lookup
  ON content_processing_events(tenant_id, content_source_id, source_run_request_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_content_processing_events_item
  ON content_processing_events(tenant_id, content_item_id, occurred_at DESC)
  WHERE content_item_id IS NOT NULL;

CREATE OR REPLACE FUNCTION reject_content_processing_event_mutation()
RETURNS TRIGGER AS $$
BEGIN
  RAISE EXCEPTION 'content_processing_events are append-only';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS content_processing_events_append_only ON content_processing_events;
CREATE TRIGGER content_processing_events_append_only
BEFORE UPDATE OR DELETE ON content_processing_events
FOR EACH ROW EXECUTE FUNCTION reject_content_processing_event_mutation();
