CREATE TABLE IF NOT EXISTS source_upstream_observation_dispositions (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id VARCHAR(64) NOT NULL,
  observation_id UUID NOT NULL,
  disposition VARCHAR(64) NOT NULL CHECK (disposition IN (
    'deferred','materialization_reserved','materialization_requested','materialized',
    'filtered','replay_expiring','replay_expired','unrecoverable','authorized_abandonment'
  )),
  latest_event_id UUID NOT NULL,
  latest_event_at TIMESTAMPTZ NOT NULL,
  replay_until TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, observation_id)
);

CREATE INDEX IF NOT EXISTS idx_source_upstream_dispositions_due
  ON source_upstream_observation_dispositions(tenant_id, disposition, replay_until)
  WHERE disposition IN ('deferred', 'replay_expiring');

ALTER TABLE source_run_projection_work
  DROP CONSTRAINT IF EXISTS source_run_projection_work_evidence_kind_check;
ALTER TABLE source_run_projection_work
  ADD CONSTRAINT source_run_projection_work_evidence_kind_check
  CHECK (evidence_kind IN ('receipt','reconciliation_event','upstream_observation_event')) NOT VALID;
ALTER TABLE source_run_projection_work
  VALIDATE CONSTRAINT source_run_projection_work_evidence_kind_check;
