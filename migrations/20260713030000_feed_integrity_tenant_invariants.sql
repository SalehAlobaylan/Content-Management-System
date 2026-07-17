-- Feed Integrity tenant and policy invariants. Applied deliberately; never by startup patch.
-- Refuse unsafe legacy data rather than silently rewriting ownership or episode history.
DO $$
DECLARE
    duplicate_active_identities BIGINT;
    cross_tenant_retries BIGINT;
    cross_tenant_latest_actions BIGINT;
BEGIN
    IF to_regclass('public.feed_integrity_actions') IS NULL
       OR to_regclass('public.feed_integrity_episodes') IS NULL
       OR to_regclass('public.feed_integrity_runs') IS NULL THEN
        RAISE EXCEPTION 'feed integrity base and autopilot migrations must be applied first';
    END IF;

    SELECT COUNT(*) INTO duplicate_active_identities
    FROM (
        SELECT 1
        FROM feed_integrity_episodes
        WHERE status IN ('open', 'recovering')
        GROUP BY tenant_id, check_key, feed, variant, scope
        HAVING COUNT(*) > 1
    ) duplicates;

    SELECT COUNT(*) INTO cross_tenant_retries
    FROM feed_integrity_actions child
    JOIN feed_integrity_actions parent ON parent.id = child.retry_of_action_id
    WHERE child.retry_of_action_id IS NOT NULL
      AND child.tenant_id <> parent.tenant_id;

    SELECT COUNT(*) INTO cross_tenant_latest_actions
    FROM feed_integrity_episodes episode
    JOIN feed_integrity_actions action ON action.id = episode.latest_action_id
    WHERE episode.latest_action_id IS NOT NULL
      AND episode.tenant_id <> action.tenant_id;

    IF duplicate_active_identities > 0
       OR cross_tenant_retries > 0
       OR cross_tenant_latest_actions > 0 THEN
        RAISE EXCEPTION
            'feed integrity invariant preflight failed: duplicate active identities %, cross-tenant retries %, cross-tenant latest actions %; reconcile these rows before applying this migration',
            duplicate_active_identities, cross_tenant_retries, cross_tenant_latest_actions;
    END IF;
END $$;

DROP INDEX IF EXISTS idx_feed_integrity_episodes_open_identity;
CREATE UNIQUE INDEX IF NOT EXISTS idx_feed_integrity_episodes_active_identity
    ON feed_integrity_episodes(tenant_id, check_key, feed, variant, scope)
    WHERE status IN ('open', 'recovering');

CREATE UNIQUE INDEX IF NOT EXISTS idx_feed_integrity_actions_id_tenant
    ON feed_integrity_actions(id, tenant_id);

ALTER TABLE feed_integrity_actions
    DROP CONSTRAINT IF EXISTS feed_integrity_actions_retry_of_action_id_fkey;
ALTER TABLE feed_integrity_actions
    ADD CONSTRAINT fk_feed_integrity_action_retry_tenant
    FOREIGN KEY (retry_of_action_id, tenant_id)
    REFERENCES feed_integrity_actions(id, tenant_id)
    ON DELETE SET NULL (retry_of_action_id);

ALTER TABLE feed_integrity_episodes
    DROP CONSTRAINT IF EXISTS fk_feed_integrity_episode_latest_action;
ALTER TABLE feed_integrity_episodes
    ADD CONSTRAINT fk_feed_integrity_episode_latest_action_tenant
    FOREIGN KEY (latest_action_id, tenant_id)
    REFERENCES feed_integrity_actions(id, tenant_id)
    ON DELETE SET NULL (latest_action_id);

DROP INDEX IF EXISTS idx_feed_integrity_runs_autopilot_pending;
CREATE INDEX IF NOT EXISTS idx_feed_integrity_runs_autopilot_pending
    ON feed_integrity_runs(tenant_id, started_at)
    WHERE status = 'completed' AND autopilot_evaluated_at IS NULL;
