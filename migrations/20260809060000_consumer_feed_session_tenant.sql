-- Bind frozen Pods sessions to the same explicit server-owned tenant used by
-- direct feed assembly. Existing sessions are short-lived; backfill uses the
-- deliberately configured deployment tenant at migration time.
ALTER TABLE consumer_feed_sessions
  ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(64);

UPDATE consumer_feed_sessions
SET tenant_id = current_setting('wahb.public_tenant_id', true)
WHERE tenant_id IS NULL
  AND NULLIF(BTRIM(current_setting('wahb.public_tenant_id', true)), '') IS NOT NULL;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM consumer_feed_sessions WHERE tenant_id IS NULL OR BTRIM(tenant_id) = '') THEN
    RAISE EXCEPTION 'consumer_feed_sessions tenant backfill requires SET wahb.public_tenant_id';
  END IF;
END $$;

ALTER TABLE consumer_feed_sessions ALTER COLUMN tenant_id SET NOT NULL;
CREATE INDEX IF NOT EXISTS idx_consumer_feed_sessions_tenant_identity_expiry
  ON consumer_feed_sessions(tenant_id, identity_scope, feed_type, expires_at DESC);
