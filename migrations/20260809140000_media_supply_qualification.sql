CREATE TABLE IF NOT EXISTS media_supply_qualification_cases (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  case_key VARCHAR(200) NOT NULL UNIQUE,
  tenant_id VARCHAR(64) NOT NULL,
  action_key VARCHAR(120) NOT NULL,
  action_version VARCHAR(32) NOT NULL,
  adapter_version VARCHAR(64) NOT NULL,
  verifier_version VARCHAR(64) NOT NULL,
  rubric_version VARCHAR(64) NOT NULL CHECK (rubric_version = 'media-supply-qualification/v1'),
  schema_version VARCHAR(64) NOT NULL,
  policy_version VARCHAR(64) NOT NULL,
  environment_identity VARCHAR(64) NOT NULL,
  build_identity VARCHAR(128) NOT NULL,
  cohort VARCHAR(24) NOT NULL CHECK (cohort IN ('terminal','human_decision','fault','tri_state')),
  fault_case VARCHAR(80),
  origin VARCHAR(32) NOT NULL CHECK (origin IN ('cms_observe','owner_verifier','isolated_fault_harness')),
  origin_principal VARCHAR(128) NOT NULL,
  recommendation VARCHAR(24) NOT NULL CHECK (recommendation IN ('would_request','would_skip')),
  verified_success BOOLEAN NOT NULL,
  independent_verifier BOOLEAN NOT NULL,
  effect_verdict VARCHAR(16) NOT NULL CHECK (effect_verdict IN ('present','absent','unknown')),
  reversal_or_conflict BOOLEAN NOT NULL DEFAULT FALSE,
  violations JSONB NOT NULL DEFAULT '[]'::jsonb,
  correlation_digest CHAR(64) NOT NULL,
  payload_digest CHAR(64) NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK ((cohort = 'fault') = (fault_case IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS media_supply_qualification_human_decisions (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  case_id BIGINT NOT NULL UNIQUE REFERENCES media_supply_qualification_cases(id) ON DELETE RESTRICT,
  decision VARCHAR(24) NOT NULL CHECK (decision IN ('agreed','disagreed','not_required')),
  actor_id VARCHAR(255) NOT NULL,
  access_version VARCHAR(255) NOT NULL,
  decided_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS media_supply_qualification_reports (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  rubric_version VARCHAR(64) NOT NULL,
  action_key VARCHAR(120) NOT NULL,
  action_version VARCHAR(32) NOT NULL,
  adapter_version VARCHAR(64) NOT NULL,
  verifier_version VARCHAR(64) NOT NULL,
  schema_version VARCHAR(64) NOT NULL,
  policy_version VARCHAR(64) NOT NULL,
  environment_identity VARCHAR(64) NOT NULL,
  build_identity VARCHAR(128) NOT NULL,
  state VARCHAR(16) NOT NULL CHECK (state IN ('draft','sealed','superseded')),
  payload JSONB NOT NULL,
  report_digest CHAR(64) NOT NULL UNIQUE,
  seal CHAR(64),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  sealed_at TIMESTAMPTZ,
  CHECK ((state = 'draft' AND seal IS NULL AND sealed_at IS NULL) OR state IN ('sealed','superseded'))
);

CREATE TABLE IF NOT EXISTS media_supply_qualification_report_cases (
  report_id BIGINT NOT NULL REFERENCES media_supply_qualification_reports(id) ON DELETE RESTRICT,
  case_id BIGINT NOT NULL REFERENCES media_supply_qualification_cases(id) ON DELETE RESTRICT,
  PRIMARY KEY(report_id, case_id)
);

CREATE TABLE IF NOT EXISTS media_supply_qualification_signoffs (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  report_id BIGINT NOT NULL REFERENCES media_supply_qualification_reports(id) ON DELETE RESTRICT,
  role VARCHAR(32) NOT NULL CHECK (role IN ('product','engineering','operations','security')),
  actor_id VARCHAR(255) NOT NULL,
  access_version VARCHAR(255) NOT NULL,
  report_digest CHAR(64) NOT NULL,
  signed_at TIMESTAMPTZ NOT NULL,
  UNIQUE(report_id, role)
);

CREATE TABLE IF NOT EXISTS media_supply_action_promotions (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id VARCHAR(64) NOT NULL,
  action_key VARCHAR(120) NOT NULL,
  action_version VARCHAR(32) NOT NULL,
  adapter_version VARCHAR(64) NOT NULL,
  verifier_version VARCHAR(64) NOT NULL,
  schema_version VARCHAR(64) NOT NULL,
  policy_version VARCHAR(64) NOT NULL,
  report_id BIGINT NOT NULL REFERENCES media_supply_qualification_reports(id) ON DELETE RESTRICT,
  report_digest CHAR(64) NOT NULL,
  state VARCHAR(16) NOT NULL CHECK (state IN ('active','demoted','superseded')),
  promotion_epoch BIGINT NOT NULL DEFAULT 1 CHECK (promotion_epoch > 0),
  promoted_by VARCHAR(255) NOT NULL,
  promoted_access_version VARCHAR(255) NOT NULL,
  promoted_at TIMESTAMPTZ NOT NULL,
  demoted_at TIMESTAMPTZ,
  demotion_reason VARCHAR(120),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_media_supply_active_promotion
  ON media_supply_action_promotions(tenant_id, action_key) WHERE state='active';

CREATE TABLE IF NOT EXISTS media_supply_promotion_events (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id VARCHAR(64) NOT NULL,
  action_key VARCHAR(120) NOT NULL,
  promotion_id UUID NOT NULL REFERENCES media_supply_action_promotions(public_id),
  event_type VARCHAR(32) NOT NULL CHECK (event_type IN ('promoted','demoted','trust_reset')),
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  occurred_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION reject_media_supply_qualification_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'media supply qualification evidence is immutable'; END $$;
DROP TRIGGER IF EXISTS trg_media_supply_qualification_cases_immutable ON media_supply_qualification_cases;
CREATE TRIGGER trg_media_supply_qualification_cases_immutable BEFORE UPDATE OR DELETE ON media_supply_qualification_cases
FOR EACH ROW EXECUTE FUNCTION reject_media_supply_qualification_mutation();
DROP TRIGGER IF EXISTS trg_media_supply_promotion_events_immutable ON media_supply_promotion_events;
CREATE TRIGGER trg_media_supply_promotion_events_immutable BEFORE UPDATE OR DELETE ON media_supply_promotion_events
FOR EACH ROW EXECUTE FUNCTION reject_media_supply_qualification_mutation();
DROP TRIGGER IF EXISTS trg_media_supply_qualification_decisions_immutable ON media_supply_qualification_human_decisions;
CREATE TRIGGER trg_media_supply_qualification_decisions_immutable BEFORE UPDATE OR DELETE ON media_supply_qualification_human_decisions
FOR EACH ROW EXECUTE FUNCTION reject_media_supply_qualification_mutation();
