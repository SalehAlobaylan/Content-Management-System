-- Media Redundancy Hygiene: deterministic evidence, human-confirmed families.
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE IF NOT EXISTS redundancy_policies (
  id BIGSERIAL PRIMARY KEY, tenant_id VARCHAR(64) NOT NULL UNIQUE,
  enabled BOOLEAN NOT NULL DEFAULT FALSE, collapse_enabled BOOLEAN NOT NULL DEFAULT TRUE,
  sweep_interval_minutes INTEGER NOT NULL DEFAULT 360,
  max_frontier_items INTEGER NOT NULL DEFAULT 500, max_pairs_scored INTEGER NOT NULL DEFAULT 2000,
  proposal_floor DOUBLE PRECISION NOT NULL DEFAULT 0.75,
  emit_circulation_recs BOOLEAN NOT NULL DEFAULT FALSE, confirm_rules JSONB NOT NULL DEFAULT '{}'::jsonb,
  paused_until TIMESTAMP, last_swept_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(), updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS redundancy_runs (
  id BIGSERIAL PRIMARY KEY, public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id VARCHAR(64) NOT NULL DEFAULT 'default', trigger VARCHAR(24) NOT NULL,
  status VARCHAR(24) NOT NULL, summary TEXT, counts JSONB, error TEXT, error_class VARCHAR(48),
  started_at TIMESTAMP NOT NULL DEFAULT NOW(), finished_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(), updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_redundancy_runs_tenant_started ON redundancy_runs(tenant_id, started_at DESC);
CREATE TABLE IF NOT EXISTS redundancy_families (
  id BIGSERIAL PRIMARY KEY, public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id VARCHAR(64) NOT NULL DEFAULT 'default', status VARCHAR(24) NOT NULL DEFAULT 'active',
  canonical_content_item_id UUID NOT NULL REFERENCES content_items(public_id),
  canonical_locked_by VARCHAR(255), canonical_reasons JSONB,
  first_confirmed_at TIMESTAMP NOT NULL DEFAULT NOW(), last_confirmed_at TIMESTAMP NOT NULL DEFAULT NOW(),
  dissolved_at TIMESTAMP, dissolved_by VARCHAR(255), dissolve_reason TEXT,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(), updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_redundancy_families_active ON redundancy_families(tenant_id, status);
CREATE TABLE IF NOT EXISTS redundancy_pairs (
  id BIGSERIAL PRIMARY KEY, public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id VARCHAR(64) NOT NULL DEFAULT 'default', item_a_id UUID NOT NULL REFERENCES content_items(public_id),
  item_b_id UUID NOT NULL REFERENCES content_items(public_id), family_id BIGINT REFERENCES redundancy_families(id),
  latest_evaluation_id BIGINT, confidence DOUBLE PRECISION NOT NULL DEFAULT 0,
  verdict VARCHAR(48) NOT NULL, tombstoned BOOLEAN NOT NULL DEFAULT FALSE,
  reviewed_by VARCHAR(255), reviewed_at TIMESTAMP, reject_reason TEXT,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(), updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
  CONSTRAINT redundancy_pairs_ordered CHECK (item_a_id < item_b_id),
  CONSTRAINT redundancy_pairs_unique UNIQUE (tenant_id, item_a_id, item_b_id)
);
CREATE INDEX IF NOT EXISTS idx_redundancy_pairs_review ON redundancy_pairs(tenant_id, verdict, tombstoned, confidence DESC);
CREATE TABLE IF NOT EXISTS redundancy_pair_evaluations (
  id BIGSERIAL PRIMARY KEY, pair_id BIGINT NOT NULL REFERENCES redundancy_pairs(id) ON DELETE CASCADE,
  run_id BIGINT REFERENCES redundancy_runs(id) ON DELETE SET NULL, input_fingerprint VARCHAR(128), evaluator_version VARCHAR(32) NOT NULL DEFAULT 'v1',
  lane_scores JSONB NOT NULL, confidence DOUBLE PRECISION NOT NULL, machine_verdict VARCHAR(48) NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
ALTER TABLE redundancy_pairs ADD CONSTRAINT fk_redundancy_pair_latest_evaluation FOREIGN KEY (latest_evaluation_id) REFERENCES redundancy_pair_evaluations(id) DEFERRABLE INITIALLY DEFERRED;
CREATE TABLE IF NOT EXISTS redundancy_family_members (
  id BIGSERIAL PRIMARY KEY, family_id BIGINT NOT NULL REFERENCES redundancy_families(id) ON DELETE CASCADE,
  tenant_id VARCHAR(64) NOT NULL DEFAULT 'default', content_item_id UUID NOT NULL REFERENCES content_items(public_id),
  role VARCHAR(24) NOT NULL, since TIMESTAMP NOT NULL DEFAULT NOW(), ended_at TIMESTAMP,
  CONSTRAINT redundancy_member_role CHECK (role IN ('canonical','redundant'))
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_redundancy_member_active_item ON redundancy_family_members(tenant_id, content_item_id) WHERE ended_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_redundancy_member_feed ON redundancy_family_members(tenant_id, role, content_item_id) WHERE ended_at IS NULL;
CREATE TABLE IF NOT EXISTS redundancy_actions (
  id BIGSERIAL PRIMARY KEY, public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id VARCHAR(64) NOT NULL DEFAULT 'default', run_id BIGINT REFERENCES redundancy_runs(id) ON DELETE SET NULL,
  pair_id BIGINT REFERENCES redundancy_pairs(id) ON DELETE SET NULL, family_id BIGINT REFERENCES redundancy_families(id) ON DELETE SET NULL,
  action_kind VARCHAR(64) NOT NULL, actor VARCHAR(255) NOT NULL, outcome VARCHAR(24) NOT NULL,
  reason TEXT, metadata JSONB, idempotency_key VARCHAR(160), created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_redundancy_action_idempotency ON redundancy_actions(tenant_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_redundancy_actions_tenant_created ON redundancy_actions(tenant_id, created_at DESC);
CREATE TABLE IF NOT EXISTS redundancy_fingerprints (
  id BIGSERIAL PRIMARY KEY, tenant_id VARCHAR(64) NOT NULL DEFAULT 'default', content_item_id UUID NOT NULL REFERENCES content_items(public_id) ON DELETE CASCADE,
  transcript_checksum VARCHAR(64) NOT NULL, body_hash VARCHAR(64) NOT NULL, shingle_count INTEGER NOT NULL DEFAULT 0,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(), updated_at TIMESTAMP NOT NULL DEFAULT NOW(), UNIQUE(tenant_id, content_item_id, transcript_checksum)
);
CREATE INDEX IF NOT EXISTS idx_redundancy_fingerprint_item ON redundancy_fingerprints(tenant_id, content_item_id, updated_at DESC);
INSERT INTO redundancy_policies (tenant_id) VALUES ('default') ON CONFLICT (tenant_id) DO NOTHING;
