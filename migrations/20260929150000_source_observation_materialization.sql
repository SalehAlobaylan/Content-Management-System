-- Preserve source identities for every materialized durable source-run item.
-- Unresolved observed items remain drainable after their original request is
-- terminal; materialized and filtered items are terminal evidence.

ALTER TABLE source_upstream_observation_events
    DROP CONSTRAINT IF EXISTS source_upstream_observation_events_event_type_check;

ALTER TABLE source_upstream_observation_events
    ADD CONSTRAINT source_upstream_observation_events_event_type_check
    CHECK (event_type IN (
        'observed','deferred','materialization_reserved','materialization_requested',
        'materialized','filtered','replay_expiring','replay_expired',
        'unrecoverable','authorized_abandonment'
    )) NOT VALID;

ALTER TABLE source_upstream_observation_events
    VALIDATE CONSTRAINT source_upstream_observation_events_event_type_check;

ALTER TABLE source_upstream_observation_dispositions
    DROP CONSTRAINT IF EXISTS source_upstream_observation_dispositions_disposition_check;

ALTER TABLE source_upstream_observation_dispositions
    ADD CONSTRAINT source_upstream_observation_dispositions_disposition_check
    CHECK (disposition IN (
        'observed','deferred','materialization_reserved','materialization_requested',
        'materialized','filtered','replay_expiring','replay_expired',
        'unrecoverable','authorized_abandonment'
    )) NOT VALID;

ALTER TABLE source_upstream_observation_dispositions
    VALIDATE CONSTRAINT source_upstream_observation_dispositions_disposition_check;

CREATE INDEX IF NOT EXISTS idx_source_upstream_observations_pending_materialization
    ON source_upstream_observation_dispositions(tenant_id, disposition, replay_until)
    WHERE disposition IN ('observed','deferred','replay_expiring');
