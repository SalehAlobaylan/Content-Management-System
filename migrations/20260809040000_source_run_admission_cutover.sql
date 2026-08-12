-- Durable source-run cutover authority.  This is forward-only: M1 is already
-- applied and must not be edited.  The global epoch changes only after every
-- active tenant/lane has a validated durable record.
CREATE TABLE IF NOT EXISTS source_run_admission_protocol (
  id BIGSERIAL PRIMARY KEY,
  protocol_key VARCHAR(64) NOT NULL UNIQUE,
  epoch VARCHAR(32) NOT NULL CHECK (epoch IN ('compatibility', 'durable_required')),
  version BIGINT NOT NULL DEFAULT 0,
  activated_at TIMESTAMPTZ,
  activated_by VARCHAR(128),
  build VARCHAR(128),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS source_run_admission_cutovers (
  id BIGSERIAL PRIMARY KEY,
  tenant_id VARCHAR(64) NOT NULL,
  lane VARCHAR(32) NOT NULL CHECK (lane IN ('news', 'media')),
  mode VARCHAR(32) NOT NULL CHECK (mode IN ('legacy', 'durable')),
  protocol VARCHAR(64) NOT NULL,
  version BIGINT NOT NULL DEFAULT 0,
  activated_at TIMESTAMPTZ,
  activated_by VARCHAR(128),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, lane)
);

INSERT INTO source_run_admission_protocol (protocol_key, epoch)
VALUES ('source-run/v1', 'compatibility')
ON CONFLICT (protocol_key) DO NOTHING;
