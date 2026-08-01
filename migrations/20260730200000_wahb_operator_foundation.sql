-- Wahb Operator foundation. These tables persist only CMS-owned authority and
-- read-model history; they do not expose or enable any Operator endpoint.

CREATE TABLE IF NOT EXISTS operator_policies (
  tenant_id VARCHAR(64) PRIMARY KEY,
  launch_mode VARCHAR(16) NOT NULL DEFAULT 'off' CHECK (launch_mode IN ('off','shadow','public')),
  read_enabled BOOLEAN NOT NULL DEFAULT true,
  llm_enabled BOOLEAN NOT NULL DEFAULT true,
  execution_enabled BOOLEAN NOT NULL DEFAULT false,
  schedules_enabled BOOLEAN NOT NULL DEFAULT false,
  interactive_soft_spend_limit BIGINT NOT NULL DEFAULT 0 CHECK (interactive_soft_spend_limit >= 0),
  deep_hard_spend_limit BIGINT NOT NULL DEFAULT 0 CHECK (deep_hard_spend_limit >= 0),
  updated_by VARCHAR(255),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS operator_capability_controls (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id VARCHAR(64) NOT NULL,
  capability_kind VARCHAR(16) NOT NULL CHECK (capability_kind IN ('adapter','tool')),
  capability_key VARCHAR(160) NOT NULL,
  disabled BOOLEAN NOT NULL DEFAULT true,
  reason TEXT NOT NULL,
  expires_at TIMESTAMPTZ,
  actor_id VARCHAR(255) NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(tenant_id, capability_kind, capability_key)
);
CREATE INDEX IF NOT EXISTS idx_operator_capability_controls_active
  ON operator_capability_controls(tenant_id, capability_kind, disabled, expires_at);

CREATE TABLE IF NOT EXISTS operator_threads (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id VARCHAR(64) NOT NULL,
  creator_id VARCHAR(255) NOT NULL,
  title VARCHAR(300),
  locale VARCHAR(8) NOT NULL DEFAULT 'en' CHECK (locale IN ('ar','en')),
  last_activity_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  expires_at TIMESTAMPTZ NOT NULL,
  deleted_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_operator_threads_tenant_activity
  ON operator_threads(tenant_id, creator_id, last_activity_at DESC);

CREATE TABLE IF NOT EXISTS operator_messages (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  thread_id BIGINT NOT NULL REFERENCES operator_threads(id) ON DELETE CASCADE,
  tenant_id VARCHAR(64) NOT NULL,
  actor_type VARCHAR(16) NOT NULL CHECK (actor_type IN ('admin','operator','system')),
  actor_id VARCHAR(255),
  content JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_operator_messages_thread_created
  ON operator_messages(thread_id, created_at);

CREATE TABLE IF NOT EXISTS operator_investigations (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id VARCHAR(64) NOT NULL,
  thread_id BIGINT REFERENCES operator_threads(id) ON DELETE SET NULL,
  actor_id VARCHAR(255) NOT NULL,
  state VARCHAR(24) NOT NULL CHECK (state IN ('accepted','running','backgrounded','completed','failed','cancelled')),
  visible_context JSONB NOT NULL,
  packet_fingerprint CHAR(64),
  locale VARCHAR(8) NOT NULL CHECK (locale IN ('ar','en')),
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  finished_at TIMESTAMPTZ,
  expires_at TIMESTAMPTZ NOT NULL,
  error_class VARCHAR(100),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_operator_investigations_tenant_state
  ON operator_investigations(tenant_id, state, updated_at DESC);

CREATE TABLE IF NOT EXISTS operator_investigation_events (
  id BIGSERIAL PRIMARY KEY,
  investigation_id BIGINT NOT NULL REFERENCES operator_investigations(id) ON DELETE CASCADE,
  tenant_id VARCHAR(64) NOT NULL,
  sequence BIGINT NOT NULL CHECK (sequence > 0),
  event_type VARCHAR(40) NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(investigation_id, sequence)
);

CREATE TABLE IF NOT EXISTS operator_evidence (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  investigation_id BIGINT NOT NULL REFERENCES operator_investigations(id) ON DELETE CASCADE,
  tenant_id VARCHAR(64) NOT NULL,
  evidence_id VARCHAR(200) NOT NULL,
  authority VARCHAR(16) NOT NULL CHECK (authority IN ('live','derived','temporal','retrieved','memory')),
  domain VARCHAR(100) NOT NULL,
  adapter_key VARCHAR(150) NOT NULL,
  adapter_version VARCHAR(50) NOT NULL,
  required_permission VARCHAR(120) NOT NULL,
  record_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
  deep_link TEXT NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL,
  fetched_at TIMESTAMPTZ NOT NULL,
  max_age_seconds INTEGER NOT NULL CHECK (max_age_seconds > 0),
  expires_at TIMESTAMPTZ NOT NULL,
  content_hash CHAR(64) NOT NULL,
  source_version VARCHAR(100) NOT NULL,
  availability VARCHAR(16) NOT NULL CHECK (availability IN ('available','partial','stale','unavailable','conflicting')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(investigation_id, evidence_id)
);
CREATE INDEX IF NOT EXISTS idx_operator_evidence_tenant_expiry
  ON operator_evidence(tenant_id, expires_at);

CREATE TABLE IF NOT EXISTS operator_recommendations (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  investigation_id BIGINT REFERENCES operator_investigations(id) ON DELETE SET NULL,
  tenant_id VARCHAR(64) NOT NULL,
  recommendation_key VARCHAR(150) NOT NULL,
  subject_type VARCHAR(80) NOT NULL,
  subject_id VARCHAR(200) NOT NULL,
  state VARCHAR(24) NOT NULL DEFAULT 'eligible' CHECK (state IN ('eligible','snoozed','dismissed','expired','blocked')),
  evidence_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  expires_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_operator_recommendations_tenant_subject
  ON operator_recommendations(tenant_id, subject_type, subject_id, state);

CREATE TABLE IF NOT EXISTS operator_recommendation_feedback (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  recommendation_id BIGINT NOT NULL REFERENCES operator_recommendations(id) ON DELETE RESTRICT,
  tenant_id VARCHAR(64) NOT NULL,
  actor_id VARCHAR(255) NOT NULL,
  feedback_kind VARCHAR(24) NOT NULL CHECK (feedback_kind IN ('snooze','dismiss','subject_override')),
  reason VARCHAR(1000),
  expires_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS operator_schedules (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id VARCHAR(64) NOT NULL,
  creator_id VARCHAR(255) NOT NULL,
  owner_id VARCHAR(255) NOT NULL,
  state VARCHAR(24) NOT NULL DEFAULT 'active' CHECK (state IN ('active','paused','disabled')),
  scope JSONB NOT NULL,
  template JSONB NOT NULL,
  locale VARCHAR(8) NOT NULL CHECK (locale IN ('ar','en')),
  cadence VARCHAR(100) NOT NULL,
  required_permissions JSONB NOT NULL DEFAULT '[]'::jsonb,
  access_version VARCHAR(200) NOT NULL,
  next_run_at TIMESTAMPTZ,
  paused_reason VARCHAR(200),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_operator_schedules_due
  ON operator_schedules(state, next_run_at) WHERE state = 'active';

CREATE TABLE IF NOT EXISTS operator_schedule_runs (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  schedule_id BIGINT NOT NULL REFERENCES operator_schedules(id) ON DELETE RESTRICT,
  tenant_id VARCHAR(64) NOT NULL,
  state VARCHAR(24) NOT NULL CHECK (state IN ('accepted','running','completed','failed','paused')),
  claim_token UUID,
  claim_expires_at TIMESTAMPTZ,
  result_investigation_id BIGINT REFERENCES operator_investigations(id) ON DELETE SET NULL,
  pause_reason VARCHAR(200),
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  finished_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_operator_schedule_runs_claim
  ON operator_schedule_runs(state, claim_expires_at);
