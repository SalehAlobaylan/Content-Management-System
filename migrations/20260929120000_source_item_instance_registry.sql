-- Durable provider identity and append-only materialization instances for
-- campaign-authorized reconstruction. This migration creates no grants and
-- enables no reset execution.
-- wahb:large-table-backfill: operator-maintenance

CREATE UNIQUE INDEX IF NOT EXISTS uq_source_run_units_tenant_public_id
    ON source_run_execution_units(tenant_id, public_id);

CREATE TABLE IF NOT EXISTS source_item_identities (
    id BIGSERIAL PRIMARY KEY,
    public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    tenant_id VARCHAR(64) NOT NULL,
    content_source_id UUID NOT NULL,
    upstream_item_id VARCHAR(255) NOT NULL CHECK (length(btrim(upstream_item_id)) > 0),
    current_instance_generation INTEGER NOT NULL DEFAULT 0 CHECK (current_instance_generation >= 0),
    current_content_item_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, content_source_id, upstream_item_id),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, public_id),
    CHECK ((current_instance_generation = 0) = (current_content_item_id IS NULL))
);
CREATE INDEX IF NOT EXISTS idx_source_item_identities_content_head
    ON source_item_identities(tenant_id, current_content_item_id)
    WHERE current_content_item_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS source_item_instances (
    id BIGSERIAL PRIMARY KEY,
    public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    tenant_id VARCHAR(64) NOT NULL,
    identity_id BIGINT NOT NULL,
    instance_generation INTEGER NOT NULL CHECK (instance_generation > 0),
    content_item_id UUID NOT NULL,
    source_observation_id UUID NOT NULL,
    upstream_fingerprint CHAR(64) NOT NULL CHECK (upstream_fingerprint ~ '^[0-9a-f]{64}$'),
    provider_version VARCHAR(64) NOT NULL,
    campaign_id BIGINT,
    state VARCHAR(16) NOT NULL CHECK (state IN ('active','superseded','retired')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    retired_at TIMESTAMPTZ,
    FOREIGN KEY (tenant_id, identity_id) REFERENCES source_item_identities(tenant_id, id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, campaign_id) REFERENCES content_reset_campaigns(tenant_id, id) ON DELETE RESTRICT,
    UNIQUE (identity_id, instance_generation),
    UNIQUE (tenant_id, id),
    CHECK ((state = 'retired') = (retired_at IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS idx_source_item_instances_identity_state
    ON source_item_instances(tenant_id, identity_id, state, instance_generation DESC);
CREATE UNIQUE INDEX IF NOT EXISTS uq_source_item_instance_active_identity
    ON source_item_instances(tenant_id, identity_id)
    WHERE state = 'active';

CREATE TABLE IF NOT EXISTS content_reset_reconstruction_grants (
    id BIGSERIAL PRIMARY KEY,
    public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    tenant_id VARCHAR(64) NOT NULL,
    campaign_id BIGINT NOT NULL,
    revision_id BIGINT NOT NULL,
    identity_id BIGINT NOT NULL,
    grant_kind VARCHAR(24) NOT NULL CHECK (grant_kind IN ('replacement','new_identity')),
    target_content_item_id UUID,
    source_run_request_id UUID NOT NULL,
    source_run_attempt_id UUID NOT NULL,
    execution_unit_id UUID NOT NULL,
    execution_fence_token UUID NOT NULL,
    page_id VARCHAR(128) NOT NULL,
    batch_id VARCHAR(128) NOT NULL,
    source_observation_id UUID NOT NULL,
    replacement_instance_generation INTEGER NOT NULL CHECK (replacement_instance_generation >= 1),
    expected_fingerprint CHAR(64) NOT NULL CHECK (expected_fingerprint ~ '^[0-9a-f]{64}$'),
    provider_version VARCHAR(64) NOT NULL,
    grant_token_hash CHAR(64) NOT NULL UNIQUE CHECK (grant_token_hash ~ '^[0-9a-f]{64}$'),
    state VARCHAR(16) NOT NULL CHECK (state IN ('issued','consumed','revoked','expired')),
    expires_at TIMESTAMPTZ NOT NULL,
    consumed_at TIMESTAMPTZ,
    replacement_content_item_id UUID,
    created_by VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (tenant_id, campaign_id) REFERENCES content_reset_campaigns(tenant_id, id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, revision_id) REFERENCES content_reset_revisions(tenant_id, id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, identity_id) REFERENCES source_item_identities(tenant_id, id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, execution_unit_id) REFERENCES source_run_execution_units(tenant_id, public_id) ON DELETE RESTRICT,
    UNIQUE (tenant_id, id),
    CHECK ((state = 'consumed') = (consumed_at IS NOT NULL)),
    CHECK ((state = 'consumed') = (replacement_content_item_id IS NOT NULL)),
    CHECK ((grant_kind = 'replacement') = (target_content_item_id IS NOT NULL)),
    CHECK ((grant_kind = 'new_identity') = (replacement_instance_generation = 1))
);
CREATE INDEX IF NOT EXISTS idx_content_reset_reconstruction_grants_state
    ON content_reset_reconstruction_grants(tenant_id, campaign_id, state, expires_at);
CREATE UNIQUE INDEX IF NOT EXISTS uq_content_reset_reconstruction_grant_active_instance
    ON content_reset_reconstruction_grants(campaign_id, identity_id, replacement_instance_generation)
    WHERE state = 'issued';

-- Backfill only source identities with persisted materialization receipts whose
-- content rows still exist. Deleted content remains outside this first registry
-- backfill until it can be reconciled from a frozen reset manifest.
WITH observed_materializations AS (
    SELECT DISTINCT ON (observation.tenant_id, observation.content_source_id, observation.upstream_item_id)
           observation.tenant_id,
           observation.content_source_id,
           observation.upstream_item_id,
           observation.public_id AS observation_id,
           observation.upstream_fingerprint,
           observation.provider_version,
           item.public_id AS content_item_id,
           event.occurred_at
      FROM source_upstream_observations observation
      JOIN source_upstream_observation_events event
        ON event.tenant_id = observation.tenant_id
       AND event.observation_id = observation.public_id
       AND event.event_type = 'materialized'
       AND event.payload->>'content_item_id' ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      JOIN content_items item
        ON item.tenant_id = observation.tenant_id
       AND item.public_id = CASE
               WHEN event.payload->>'content_item_id' ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
               THEN (event.payload->>'content_item_id')::uuid
               ELSE NULL::uuid
           END
       AND item.content_source_id = observation.content_source_id
     WHERE observation.upstream_fingerprint ~* '^[0-9a-f]{64}$'
     ORDER BY observation.tenant_id, observation.content_source_id, observation.upstream_item_id,
              event.occurred_at DESC, event.id DESC
), inserted_identities AS (
    INSERT INTO source_item_identities (
        tenant_id, content_source_id, upstream_item_id, current_instance_generation, current_content_item_id
    )
    SELECT tenant_id, content_source_id, upstream_item_id, 1, content_item_id
      FROM observed_materializations
    ON CONFLICT (tenant_id, content_source_id, upstream_item_id) DO NOTHING
    RETURNING id, tenant_id, content_source_id, upstream_item_id,
              current_instance_generation, current_content_item_id
), identity_rows AS (
    SELECT id, tenant_id, content_source_id, upstream_item_id,
           current_instance_generation, current_content_item_id
      FROM inserted_identities
    UNION ALL
    SELECT registry.id, registry.tenant_id, registry.content_source_id, registry.upstream_item_id,
           registry.current_instance_generation, registry.current_content_item_id
      FROM source_item_identities registry
     WHERE NOT EXISTS (
         SELECT 1 FROM inserted_identities inserted
          WHERE inserted.tenant_id = registry.tenant_id
            AND inserted.content_source_id = registry.content_source_id
            AND inserted.upstream_item_id = registry.upstream_item_id
     )
)
INSERT INTO source_item_instances (
    tenant_id, identity_id, instance_generation, content_item_id, source_observation_id,
    upstream_fingerprint, provider_version, state
)
SELECT materialized.tenant_id, identity.id, 1, materialized.content_item_id,
       materialized.observation_id, lower(materialized.upstream_fingerprint),
       materialized.provider_version, 'active'
  FROM observed_materializations materialized
  JOIN identity_rows identity
    ON identity.tenant_id = materialized.tenant_id
   AND identity.content_source_id = materialized.content_source_id
   AND identity.upstream_item_id = materialized.upstream_item_id
   AND identity.current_instance_generation = 1
   AND identity.current_content_item_id = materialized.content_item_id
ON CONFLICT (identity_id, instance_generation) DO NOTHING;

CREATE OR REPLACE FUNCTION retire_source_item_instance_before_content_delete()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE source_item_instances
       SET state = 'retired', retired_at = COALESCE(retired_at, NOW())
     WHERE tenant_id = OLD.tenant_id
       AND content_item_id = OLD.public_id
       AND state = 'active';
    RETURN OLD;
END;
$$;

DROP TRIGGER IF EXISTS content_item_source_instance_retirement ON content_items;
CREATE TRIGGER content_item_source_instance_retirement
BEFORE DELETE ON content_items
FOR EACH ROW EXECUTE FUNCTION retire_source_item_instance_before_content_delete();
