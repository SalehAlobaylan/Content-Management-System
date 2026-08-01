-- CMS-owned, append-only qualification assessments and sealed report ledger.
-- These records contain fingerprints and rubric outcomes only; never prompts,
-- evidence bodies, tokens, credentials, plans, or executable arguments.
CREATE TABLE IF NOT EXISTS operator_shadow_assessments (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  shadow_run_id BIGINT NOT NULL UNIQUE REFERENCES operator_shadow_runs(id) ON DELETE RESTRICT,
  evaluation_case_id VARCHAR(160) NOT NULL UNIQUE,
  cohort VARCHAR(16) NOT NULL CHECK (cohort IN ('normal','briefing','fault')),
  grounded BOOLEAN NOT NULL,
  useful_rating INTEGER NOT NULL CHECK (useful_rating BETWEEN 1 AND 5),
  domain_tool_selection_correct BOOLEAN NOT NULL,
  unsupported_certainty_critical INTEGER NOT NULL DEFAULT 0 CHECK (unsupported_certainty_critical >= 0),
  fault_case VARCHAR(80),
  outcome VARCHAR(24) NOT NULL CHECK (outcome IN ('passed','failed','degraded')),
  reviewer_id VARCHAR(255) NOT NULL,
  provenance VARCHAR(32) NOT NULL CHECK (provenance IN ('production_snapshot','isolated_fixture')),
  result_fingerprint CHAR(64) NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK ((cohort = 'fault') = (fault_case IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS operator_shadow_reports (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  schema_version VARCHAR(64) NOT NULL,
  environment_identity VARCHAR(64) NOT NULL,
  launch_mode VARCHAR(16) NOT NULL CHECK (launch_mode = 'shadow'),
  state VARCHAR(16) NOT NULL CHECK (state IN ('draft','sealed')),
  payload JSONB NOT NULL,
  report_digest CHAR(64) NOT NULL UNIQUE,
  seal CHAR(64),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  sealed_at TIMESTAMPTZ,
  CHECK ((state = 'draft' AND seal IS NULL AND sealed_at IS NULL) OR (state = 'sealed' AND seal IS NOT NULL AND sealed_at IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS operator_shadow_report_runs (
  id BIGSERIAL PRIMARY KEY,
  report_id BIGINT NOT NULL REFERENCES operator_shadow_reports(id) ON DELETE RESTRICT,
  run_id BIGINT NOT NULL REFERENCES operator_shadow_runs(id) ON DELETE RESTRICT,
  UNIQUE(report_id, run_id)
);

CREATE TABLE IF NOT EXISTS operator_shadow_report_signoffs (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  report_id BIGINT NOT NULL REFERENCES operator_shadow_reports(id) ON DELETE RESTRICT,
  role VARCHAR(32) NOT NULL CHECK (role IN ('product','engineering','operations','security')),
  actor_id VARCHAR(255) NOT NULL,
  report_digest CHAR(64) NOT NULL,
  signed_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(report_id, role)
);

CREATE INDEX IF NOT EXISTS idx_operator_shadow_assessments_cohort ON operator_shadow_assessments(cohort, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_operator_shadow_reports_state ON operator_shadow_reports(state, created_at DESC);

CREATE TABLE IF NOT EXISTS operator_shadow_qualification_failures (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id VARCHAR(64),
  actor_id VARCHAR(255),
  domain VARCHAR(100),
  locale VARCHAR(8),
  failure VARCHAR(80) NOT NULL CHECK (failure IN ('zero_enrollment','iam_unavailable')),
  observed_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_operator_shadow_qualification_failures_observed
  ON operator_shadow_qualification_failures(failure, observed_at DESC);
