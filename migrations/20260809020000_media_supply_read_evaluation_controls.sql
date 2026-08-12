-- Pods Supply Continuity: durable, subtractive control for the bounded
-- evidence evaluator. A row can only disable its one fixed evaluator; it is
-- not a source-admission, dispatch, queue, provider, or feed-visibility flag.
--
-- The Console has no direct mutation route for this table. Any future control
-- write must be admitted as a fixed, signed CMS action rather than accepting a
-- caller-supplied control key or scope.

CREATE TABLE IF NOT EXISTS media_supply_controls (
  id BIGSERIAL PRIMARY KEY,
  tenant_id VARCHAR(64) NOT NULL,
  control_key VARCHAR(64) NOT NULL CHECK (control_key = 'supply_read_evaluation'),
  scope_type VARCHAR(24) NOT NULL CHECK (scope_type = 'tenant'),
  scope_id VARCHAR(64) NOT NULL CHECK (scope_id = 'all'),
  disabled_at TIMESTAMPTZ NOT NULL,
  disabled_by VARCHAR(128) NOT NULL,
  reason TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT uq_media_supply_controls_scope UNIQUE (tenant_id, control_key, scope_type, scope_id)
);

CREATE INDEX IF NOT EXISTS idx_media_supply_controls_tenant
  ON media_supply_controls(tenant_id, control_key);
