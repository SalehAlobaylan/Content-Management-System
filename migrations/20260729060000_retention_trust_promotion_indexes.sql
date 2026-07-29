-- Retention Autopilot Slice 10: trust-promotion read paths.
-- Evidence remains derived from immutable action/audit ledgers; this migration
-- adds only bounded indexes and does not mutate content or enable Safe Auto.

CREATE INDEX IF NOT EXISTS idx_retention_actions_trust_window
    ON retention_actions(tenant_id, action_class, mode, outcome, updated_at DESC);

DO $$
BEGIN
    -- Some older installations provision audit_logs outside the canonical
    -- migration set. Keep this optimization harmless on a fresh database.
    IF to_regclass('public.audit_logs') IS NOT NULL THEN
        CREATE INDEX IF NOT EXISTS idx_retention_audit_breaker_resets
            ON audit_logs(tenant_id, action, target_resource, created_at DESC);
    END IF;
END $$;
