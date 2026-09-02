-- Feed reliability projections and bounded operational evidence. This
-- migration is additive: it does not approve sources, rewrite history, or
-- delete unmatched objects. It is intentionally not applied by this change.

CREATE TABLE IF NOT EXISTS media_source_yield_daily (
  tenant_id varchar(64) NOT NULL,
  content_source_id uuid NOT NULL,
  yield_date date NOT NULL,
  fetched_candidates integer NOT NULL DEFAULT 0,
  legal_duration_candidates integer NOT NULL DEFAULT 0,
  filtered_candidates integer NOT NULL DEFAULT 0,
  materialized_items integer NOT NULL DEFAULT 0,
  verified_media integer NOT NULL DEFAULT 0,
  ready_visible_units integer NOT NULL DEFAULT 0,
  public_returns integer NOT NULL DEFAULT 0,
  first_page_returns integer NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, content_source_id, yield_date)
);
CREATE INDEX IF NOT EXISTS idx_media_source_yield_daily_source_time
  ON media_source_yield_daily (tenant_id, content_source_id, yield_date DESC);
CREATE INDEX IF NOT EXISTS idx_media_source_yield_daily_time
  ON media_source_yield_daily (tenant_id, yield_date DESC);

CREATE TABLE IF NOT EXISTS pipeline_lane_health_snapshots (
  id bigserial PRIMARY KEY,
  tenant_id varchar(64) NOT NULL,
  lane varchar(32) NOT NULL,
  owner_principal varchar(128) NOT NULL,
  required_queue_depth integer NOT NULL DEFAULT 0,
  optional_queue_depth integer NOT NULL DEFAULT 0,
  required_oldest_age_seconds double precision NOT NULL DEFAULT 0,
  optional_oldest_age_seconds double precision NOT NULL DEFAULT 0,
  dlq_delta integer NOT NULL DEFAULT 0,
  failure_classes jsonb NOT NULL DEFAULT '{}'::jsonb,
  stage_counts jsonb NOT NULL DEFAULT '{}'::jsonb,
  enrichment_counts jsonb NOT NULL DEFAULT '{}'::jsonb,
  process_metrics jsonb NOT NULL DEFAULT '{}'::jsonb,
  resource_metrics jsonb NOT NULL DEFAULT '{}'::jsonb,
  captured_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, lane, owner_principal)
);
CREATE INDEX IF NOT EXISTS idx_pipeline_lane_health_snapshots_lookup
  ON pipeline_lane_health_snapshots (tenant_id, lane, captured_at DESC);

CREATE INDEX IF NOT EXISTS idx_content_stage_attempts_tenant_lane_owner_time
  ON content_stage_attempts (tenant_id, lane, owner, created_at DESC);

CREATE TABLE IF NOT EXISTS source_run_cutover_audit_events (
  id bigserial PRIMARY KEY,
  tenant_id varchar(64),
  lane varchar(32),
  event_type varchar(64) NOT NULL,
  from_epoch varchar(32),
  to_epoch varchar(32),
  verification_digest varchar(128),
  actor varchar(128) NOT NULL,
  payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  occurred_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_source_run_cutover_audit_lookup
  ON source_run_cutover_audit_events (tenant_id, lane, occurred_at DESC);

CREATE OR REPLACE FUNCTION reject_source_run_cutover_audit_mutation() RETURNS trigger AS $$
BEGIN
  RAISE EXCEPTION 'source_run_cutover_audit_events are append-only';
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS source_run_cutover_audit_events_append_only ON source_run_cutover_audit_events;
CREATE TRIGGER source_run_cutover_audit_events_append_only
  BEFORE UPDATE OR DELETE ON source_run_cutover_audit_events
  FOR EACH ROW EXECUTE FUNCTION reject_source_run_cutover_audit_mutation();
