-- Redacted CMS-owned shadow qualification ledger. No prompt, evidence body,
-- plan, approval, credential, or action result is persisted here.
CREATE TABLE IF NOT EXISTS operator_shadow_runs (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id VARCHAR(64) NOT NULL,
  domain VARCHAR(100) NOT NULL,
  locale VARCHAR(8) NOT NULL CHECK (locale IN ('en','ar')),
  state VARCHAR(24) NOT NULL CHECK (state IN ('completed','failed')),
  packet_fingerprint CHAR(64),
  evidence_count INTEGER NOT NULL DEFAULT 0,
  unknown_count INTEGER NOT NULL DEFAULT 0,
  conflict_count INTEGER NOT NULL DEFAULT 0,
  latency_ms BIGINT NOT NULL DEFAULT 0,
  error_class VARCHAR(120),
  started_at TIMESTAMPTZ NOT NULL,
  finished_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_operator_shadow_runs_tenant_domain_created
  ON operator_shadow_runs(tenant_id, domain, created_at DESC);
