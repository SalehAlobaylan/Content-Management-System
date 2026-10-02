-- Durable, tenant-scoped planning foundation for Content Fresh Start.
-- This migration does not enable or perform destructive execution.
-- wahb:large-table-backfill: operator-maintenance
-- Assigning stable inventory sequence values requires one ordered pass over
-- existing content_items. Schedule it as explicit database maintenance and
-- verify write latency before relying on the preview boundary.

-- Give paginated previews a stable insertion boundary. A per-tenant counter
-- update is transactional: planning locks one tenant row briefly, and inserts
-- that commit afterward receive a sequence above the frozen high-water mark.
CREATE TABLE IF NOT EXISTS tenant_content_inventory_counters (
    tenant_id VARCHAR(64) PRIMARY KEY,
    current_sequence BIGINT NOT NULL DEFAULT 0 CHECK (current_sequence >= 0),
    last_deletion_prune_at TIMESTAMPTZ NOT NULL DEFAULT 'epoch',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
ALTER TABLE tenant_content_inventory_counters
    ADD COLUMN IF NOT EXISTS last_deletion_prune_at TIMESTAMPTZ NOT NULL DEFAULT 'epoch';

ALTER TABLE content_items
    ADD COLUMN IF NOT EXISTS inventory_sequence BIGINT;

WITH current_sequences AS (
    SELECT tenant_id, COALESCE(MAX(inventory_sequence), 0) AS current_sequence
    FROM content_items
    GROUP BY tenant_id
), ranked_content AS (
    SELECT content.id,
           current_sequences.current_sequence
               + ROW_NUMBER() OVER (PARTITION BY content.tenant_id ORDER BY content.id) AS sequence
    FROM content_items AS content
    JOIN current_sequences ON current_sequences.tenant_id = content.tenant_id
    WHERE content.inventory_sequence IS NULL
)
UPDATE content_items AS content
SET inventory_sequence = ranked_content.sequence
FROM ranked_content
WHERE content.id = ranked_content.id;

INSERT INTO tenant_content_inventory_counters (tenant_id, current_sequence)
SELECT tenant_id, COALESCE(MAX(inventory_sequence), 0)
FROM content_items
GROUP BY tenant_id
ON CONFLICT (tenant_id) DO UPDATE
SET current_sequence = GREATEST(tenant_content_inventory_counters.current_sequence, EXCLUDED.current_sequence),
    updated_at = NOW();

ALTER TABLE content_items
    ALTER COLUMN inventory_sequence SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_content_items_tenant_inventory_sequence
    ON content_items(tenant_id, inventory_sequence);

CREATE TABLE IF NOT EXISTS content_reset_inventory_deletions (
    id BIGSERIAL PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    inventory_sequence BIGINT NOT NULL CHECK (inventory_sequence > 0),
    content_item_id UUID NOT NULL,
    type VARCHAR(20) NOT NULL,
    status VARCHAR(20) NOT NULL,
    content_source_id UUID,
    processing_generation BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ,
    deleted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, inventory_sequence)
);
CREATE INDEX IF NOT EXISTS idx_content_reset_inventory_deletions_boundary
    ON content_reset_inventory_deletions(tenant_id, inventory_sequence);
CREATE INDEX IF NOT EXISTS idx_content_reset_inventory_deletions_retention
    ON content_reset_inventory_deletions(tenant_id, deleted_at);

CREATE OR REPLACE FUNCTION protect_tenant_content_inventory_counter()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'tenant content inventory counters cannot be deleted';
    END IF;
    IF NEW.tenant_id IS DISTINCT FROM OLD.tenant_id THEN
        RAISE EXCEPTION 'tenant content inventory counter identity is immutable';
    END IF;
    IF NEW.current_sequence < OLD.current_sequence THEN
        RAISE EXCEPTION 'tenant content inventory counter cannot move backward';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS tenant_content_inventory_counter_guard ON tenant_content_inventory_counters;
CREATE TRIGGER tenant_content_inventory_counter_guard
BEFORE UPDATE OR DELETE ON tenant_content_inventory_counters
FOR EACH ROW EXECUTE FUNCTION protect_tenant_content_inventory_counter();

CREATE UNIQUE INDEX IF NOT EXISTS uq_content_items_tenant_public_id
    ON content_items(tenant_id, public_id);

CREATE TABLE IF NOT EXISTS content_reset_campaigns (
    id BIGSERIAL PRIMARY KEY,
    public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    tenant_id VARCHAR(64) NOT NULL,
    operation VARCHAR(24) NOT NULL CHECK (operation IN ('clear','fresh_start','empty')),
    lane VARCHAR(16) NOT NULL CHECK (lane IN ('news','pods','both')),
    state VARCHAR(24) NOT NULL CHECK (state IN ('planning','previewed','blocked','approved','executing','partial','published','cleanup_pending','complete','cancelled')),
    current_revision INTEGER NOT NULL DEFAULT 1 CHECK (current_revision > 0),
    idempotency_key VARCHAR(128) NOT NULL,
    request_hash CHAR(64) NOT NULL,
    created_by VARCHAR(255) NOT NULL,
    cancelled_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, public_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_content_reset_campaign_idempotency
    ON content_reset_campaigns(tenant_id, idempotency_key);
CREATE INDEX IF NOT EXISTS idx_content_reset_campaigns_tenant_created
    ON content_reset_campaigns(tenant_id, created_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS uq_content_reset_one_planning_or_active_tenant
    ON content_reset_campaigns(tenant_id)
    WHERE state IN ('planning','approved','executing','partial','published','cleanup_pending');

CREATE TABLE IF NOT EXISTS content_reset_revisions (
    id BIGSERIAL PRIMARY KEY,
    public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    campaign_id BIGINT NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    revision INTEGER NOT NULL CHECK (revision > 0),
    request JSONB NOT NULL,
    state VARCHAR(24) NOT NULL CHECK (state IN ('planning','previewed','blocked','superseded')),
    selection_highwater BIGINT NOT NULL DEFAULT 0,
    inventory_highwater BIGINT NOT NULL DEFAULT 0 CHECK (inventory_highwater >= 0),
    scan_cursor BIGINT NOT NULL DEFAULT 0,
    target_count BIGINT NOT NULL DEFAULT 0 CHECK (target_count >= 0),
    protected_count BIGINT NOT NULL DEFAULT 0 CHECK (protected_count >= 0),
    unknown_date_count BIGINT NOT NULL DEFAULT 0 CHECK (unknown_date_count >= 0),
    unattributed_count BIGINT NOT NULL DEFAULT 0 CHECK (unattributed_count >= 0),
    replay_source_snapshot JSONB NOT NULL DEFAULT '[]'::jsonb,
    replay_source_hash CHAR(64) NOT NULL DEFAULT repeat('0',64),
    missing_explicit_count INTEGER NOT NULL DEFAULT 0 CHECK (missing_explicit_count >= 0),
    manifest_chain_hash CHAR(64) NOT NULL DEFAULT repeat('0',64),
    manifest_hash CHAR(64),
    blockers JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_by VARCHAR(255) NOT NULL,
    planning_started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    planning_completed_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (tenant_id, campaign_id) REFERENCES content_reset_campaigns(tenant_id, id) ON DELETE RESTRICT,
    UNIQUE (campaign_id, revision),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, public_id)
);
CREATE INDEX IF NOT EXISTS idx_content_reset_revisions_tenant_campaign
    ON content_reset_revisions(tenant_id, campaign_id, revision DESC);

CREATE TABLE IF NOT EXISTS content_reset_targets (
    id BIGSERIAL PRIMARY KEY,
    revision_id BIGINT NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    content_item_id UUID NOT NULL,
    item_ordinal BIGINT NOT NULL,
    lane VARCHAR(8) NOT NULL CHECK (lane IN ('news','pods')),
    disposition VARCHAR(16) NOT NULL CHECK (disposition IN ('selected','preserve','blocked')),
    protected BOOLEAN NOT NULL DEFAULT FALSE,
    protection_reason VARCHAR(64),
    protection_evidence JSONB NOT NULL DEFAULT '[]'::jsonb,
    snapshot_hash CHAR(64) NOT NULL,
    snapshot JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (tenant_id, revision_id) REFERENCES content_reset_revisions(tenant_id, id) ON DELETE RESTRICT,
    UNIQUE (revision_id, content_item_id),
    UNIQUE (revision_id, item_ordinal)
);
CREATE INDEX IF NOT EXISTS idx_content_reset_targets_page
    ON content_reset_targets(revision_id, item_ordinal);
CREATE INDEX IF NOT EXISTS idx_content_reset_targets_disposition
    ON content_reset_targets(revision_id, disposition, item_ordinal);

CREATE TABLE IF NOT EXISTS content_reset_steps (
    id BIGSERIAL PRIMARY KEY,
    public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    campaign_id BIGINT NOT NULL,
    revision_id BIGINT NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    step_key VARCHAR(255) NOT NULL,
    owner VARCHAR(32) NOT NULL,
    target_type VARCHAR(32) NOT NULL,
    target_id VARCHAR(255) NOT NULL,
    effect_type VARCHAR(48) NOT NULL,
    state VARCHAR(24) NOT NULL CHECK (state IN ('pending','claimed','outcome_unknown','succeeded','blocked','failed','cancelled')),
    command JSONB NOT NULL,
    receipt JSONB,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    lease_token UUID,
    lease_until TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (tenant_id, campaign_id) REFERENCES content_reset_campaigns(tenant_id, id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, revision_id) REFERENCES content_reset_revisions(tenant_id, id) ON DELETE RESTRICT,
    UNIQUE (campaign_id, step_key)
);
CREATE INDEX IF NOT EXISTS idx_content_reset_steps_state
    ON content_reset_steps(tenant_id, state, updated_at);

CREATE TABLE IF NOT EXISTS content_reset_evidence (
    id BIGSERIAL PRIMARY KEY,
    public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    campaign_id BIGINT NOT NULL,
    revision_id BIGINT,
    tenant_id VARCHAR(64) NOT NULL,
    evidence_key VARCHAR(255) NOT NULL,
    evidence_type VARCHAR(48) NOT NULL,
    owner VARCHAR(32) NOT NULL,
    payload JSONB NOT NULL,
    payload_hash CHAR(64) NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (tenant_id, campaign_id) REFERENCES content_reset_campaigns(tenant_id, id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, revision_id) REFERENCES content_reset_revisions(tenant_id, id) ON DELETE RESTRICT,
    UNIQUE (campaign_id, evidence_key)
);
CREATE INDEX IF NOT EXISTS idx_content_reset_evidence_tenant_created
    ON content_reset_evidence(tenant_id, campaign_id, observed_at);

-- Durable conflict intent. Owners must check these records at admission and
-- immediately before mutation; a row by itself does not integrate an owner.
CREATE TABLE IF NOT EXISTS lifecycle_operation_claims (
    id BIGSERIAL PRIMARY KEY,
    public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    tenant_id VARCHAR(64) NOT NULL,
    campaign_id BIGINT,
    owner VARCHAR(32) NOT NULL CHECK (length(btrim(owner)) > 0),
    resource_type VARCHAR(32) NOT NULL CHECK (resource_type IN ('tenant','lane','source','item','artifact')),
    resource_key VARCHAR(255) NOT NULL CHECK (length(btrim(resource_key)) > 0),
    phase VARCHAR(32) NOT NULL CHECK (length(btrim(phase)) > 0),
    state VARCHAR(16) NOT NULL CHECK (state IN ('active','released','transferred','blocked')),
    fencing_token UUID NOT NULL,
    generation BIGINT NOT NULL DEFAULT 1 CHECK (generation > 0),
    lease_until TIMESTAMPTZ,
    released_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (tenant_id, campaign_id) REFERENCES content_reset_campaigns(tenant_id, id) ON DELETE RESTRICT,
    CHECK ((state = 'released') = (released_at IS NOT NULL))
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_lifecycle_operation_active_resource
    ON lifecycle_operation_claims(tenant_id, resource_type, resource_key)
    WHERE state = 'active';
CREATE INDEX IF NOT EXISTS idx_lifecycle_operation_campaign
    ON lifecycle_operation_claims(tenant_id, campaign_id, state);

CREATE TABLE IF NOT EXISTS content_reset_intake_pauses (
    id BIGSERIAL PRIMARY KEY,
    public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    tenant_id VARCHAR(64) NOT NULL,
    campaign_id BIGINT NOT NULL,
    lane VARCHAR(8) NOT NULL CHECK (lane IN ('news','pods')),
    content_source_id UUID,
    owner VARCHAR(32) NOT NULL CHECK (length(btrim(owner)) > 0),
    reason VARCHAR(64) NOT NULL CHECK (length(btrim(reason)) > 0),
    state VARCHAR(16) NOT NULL CHECK (state IN ('active','released')),
    fencing_token UUID NOT NULL,
    generation BIGINT NOT NULL CHECK (generation > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    released_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (tenant_id, campaign_id) REFERENCES content_reset_campaigns(tenant_id, id) ON DELETE RESTRICT,
    CHECK ((state = 'released') = (released_at IS NOT NULL))
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_content_reset_active_intake_pause
    ON content_reset_intake_pauses(tenant_id, campaign_id, lane, COALESCE(content_source_id, '00000000-0000-0000-0000-000000000000'::uuid))
    WHERE state = 'active';
CREATE INDEX IF NOT EXISTS idx_content_reset_intake_pause_admission
    ON content_reset_intake_pauses(tenant_id, lane, content_source_id)
    WHERE state = 'active';

CREATE OR REPLACE FUNCTION reject_content_reset_evidence_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'Content Reset evidence is append-only';
END;
$$;
DROP TRIGGER IF EXISTS content_reset_evidence_append_only ON content_reset_evidence;
CREATE TRIGGER content_reset_evidence_append_only
BEFORE UPDATE OR DELETE ON content_reset_evidence
FOR EACH ROW EXECUTE FUNCTION reject_content_reset_evidence_mutation();

CREATE OR REPLACE FUNCTION reject_content_reset_target_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'Content Reset revision targets are immutable';
END;
$$;
DROP TRIGGER IF EXISTS content_reset_targets_immutable ON content_reset_targets;
CREATE TRIGGER content_reset_targets_immutable
BEFORE UPDATE OR DELETE ON content_reset_targets
FOR EACH ROW EXECUTE FUNCTION reject_content_reset_target_mutation();

CREATE OR REPLACE FUNCTION enforce_content_reset_campaign_identity()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'Content Reset campaign history cannot be deleted';
    END IF;
    IF NEW.public_id IS DISTINCT FROM OLD.public_id
       OR NEW.tenant_id IS DISTINCT FROM OLD.tenant_id
       OR NEW.operation IS DISTINCT FROM OLD.operation
       OR NEW.lane IS DISTINCT FROM OLD.lane
       OR NEW.idempotency_key IS DISTINCT FROM OLD.idempotency_key
       OR NEW.request_hash IS DISTINCT FROM OLD.request_hash
       OR NEW.created_by IS DISTINCT FROM OLD.created_by
       OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION 'Content Reset campaign intent is immutable';
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS content_reset_campaign_identity_guard ON content_reset_campaigns;
CREATE TRIGGER content_reset_campaign_identity_guard
BEFORE UPDATE OR DELETE ON content_reset_campaigns
FOR EACH ROW EXECUTE FUNCTION enforce_content_reset_campaign_identity();

CREATE OR REPLACE FUNCTION enforce_content_reset_revision_identity()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'Content Reset revision history cannot be deleted';
    END IF;
    IF NEW.public_id IS DISTINCT FROM OLD.public_id
       OR NEW.campaign_id IS DISTINCT FROM OLD.campaign_id
       OR NEW.tenant_id IS DISTINCT FROM OLD.tenant_id
       OR NEW.revision IS DISTINCT FROM OLD.revision
       OR NEW.request IS DISTINCT FROM OLD.request
       OR NEW.created_by IS DISTINCT FROM OLD.created_by
       OR NEW.selection_highwater IS DISTINCT FROM OLD.selection_highwater
       OR NEW.inventory_highwater IS DISTINCT FROM OLD.inventory_highwater
       OR NEW.unknown_date_count IS DISTINCT FROM OLD.unknown_date_count
       OR NEW.unattributed_count IS DISTINCT FROM OLD.unattributed_count
       OR NEW.replay_source_snapshot IS DISTINCT FROM OLD.replay_source_snapshot
       OR NEW.replay_source_hash IS DISTINCT FROM OLD.replay_source_hash
       OR NEW.planning_started_at IS DISTINCT FROM OLD.planning_started_at
       OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION 'Content Reset revision intent is immutable';
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS content_reset_revision_identity_guard ON content_reset_revisions;
CREATE TRIGGER content_reset_revision_identity_guard
BEFORE UPDATE OR DELETE ON content_reset_revisions
FOR EACH ROW EXECUTE FUNCTION enforce_content_reset_revision_identity();

CREATE OR REPLACE FUNCTION enforce_content_reset_step_identity()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'Content Reset owner steps cannot be deleted';
    END IF;
    IF NEW.public_id IS DISTINCT FROM OLD.public_id
       OR NEW.campaign_id IS DISTINCT FROM OLD.campaign_id
       OR NEW.revision_id IS DISTINCT FROM OLD.revision_id
       OR NEW.tenant_id IS DISTINCT FROM OLD.tenant_id
       OR NEW.step_key IS DISTINCT FROM OLD.step_key
       OR NEW.owner IS DISTINCT FROM OLD.owner
       OR NEW.target_type IS DISTINCT FROM OLD.target_type
       OR NEW.target_id IS DISTINCT FROM OLD.target_id
       OR NEW.effect_type IS DISTINCT FROM OLD.effect_type
       OR NEW.command IS DISTINCT FROM OLD.command
       OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION 'Content Reset owner command identity is immutable';
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS content_reset_step_identity_guard ON content_reset_steps;
CREATE TRIGGER content_reset_step_identity_guard
BEFORE UPDATE OR DELETE ON content_reset_steps
FOR EACH ROW EXECUTE FUNCTION enforce_content_reset_step_identity();

CREATE OR REPLACE FUNCTION enforce_content_reset_intake_pause_identity()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'Content Reset intake pause history cannot be deleted';
    END IF;
    IF NEW.public_id IS DISTINCT FROM OLD.public_id
       OR NEW.tenant_id IS DISTINCT FROM OLD.tenant_id
       OR NEW.campaign_id IS DISTINCT FROM OLD.campaign_id
       OR NEW.lane IS DISTINCT FROM OLD.lane
       OR NEW.content_source_id IS DISTINCT FROM OLD.content_source_id
       OR NEW.owner IS DISTINCT FROM OLD.owner
       OR NEW.reason IS DISTINCT FROM OLD.reason
       OR NEW.fencing_token IS DISTINCT FROM OLD.fencing_token
       OR NEW.generation IS DISTINCT FROM OLD.generation
       OR NEW.created_at IS DISTINCT FROM OLD.created_at
       OR (OLD.released_at IS NOT NULL AND NEW.released_at IS DISTINCT FROM OLD.released_at) THEN
        RAISE EXCEPTION 'Content Reset intake pause identity is immutable';
    END IF;
    IF OLD.state = 'released' AND NEW.state IS DISTINCT FROM OLD.state THEN
        RAISE EXCEPTION 'released Content Reset intake pause is terminal';
    END IF;
    IF OLD.state = 'active' AND NEW.state NOT IN ('active','released') THEN
        RAISE EXCEPTION 'invalid Content Reset intake pause transition';
    END IF;
    IF OLD.state = 'active' AND NEW.state = 'released' AND NEW.released_at IS NULL THEN
        RAISE EXCEPTION 'releasing a Content Reset intake pause requires a receipt time';
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS content_reset_intake_pause_identity_guard ON content_reset_intake_pauses;
CREATE TRIGGER content_reset_intake_pause_identity_guard
BEFORE UPDATE OR DELETE ON content_reset_intake_pauses
FOR EACH ROW EXECUTE FUNCTION enforce_content_reset_intake_pause_identity();

-- Serialize database-level content writes with the same advisory-lock keys
-- used by src/lifecycle. Application checks remain useful for typed 409
-- responses; these triggers close bypass and check-to-write races.
CREATE OR REPLACE FUNCTION content_reset_lifecycle_lane(p_type VARCHAR)
RETURNS TEXT LANGUAGE SQL IMMUTABLE AS $$
    SELECT CASE WHEN p_type IN ('VIDEO', 'PODCAST') THEN 'pods' ELSE 'news' END;
$$;

CREATE OR REPLACE FUNCTION content_reset_lifecycle_item_lock_keys(
    p_tenant_id TEXT,
    p_lane TEXT,
    p_source_id UUID,
    p_item_id UUID
)
RETURNS SETOF TEXT LANGUAGE SQL IMMUTABLE AS $$
    SELECT DISTINCT lock_key
    FROM unnest(
        ARRAY[
            'content-lifecycle/v1/' || p_tenant_id || '/tenant/*',
            'content-lifecycle/v1/' || p_tenant_id || '/lane/' || p_lane,
            'content-lifecycle/v1/' || p_tenant_id || '/item-id/' || p_lane || '/' || p_item_id::TEXT,
            'content-lifecycle/v1/' || p_tenant_id || '/item/' || p_lane || '/' || COALESCE(p_source_id::TEXT, '-') || '/' || p_item_id::TEXT
        ] || CASE WHEN p_source_id IS NULL THEN ARRAY[]::TEXT[] ELSE ARRAY[
            'content-lifecycle/v1/' || p_tenant_id || '/source/' || p_lane || '/' || p_source_id::TEXT
        ] END
    ) AS locks(lock_key)
$$;

CREATE OR REPLACE FUNCTION assert_content_reset_item_allowed(
    p_tenant_id TEXT,
    p_lane TEXT,
    p_source_id UUID,
    p_item_id UUID,
    p_phase TEXT
)
RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE
    lock_key TEXT;
BEGIN
    FOR lock_key IN
        SELECT content_reset_lifecycle_item_lock_keys(p_tenant_id, p_lane, p_source_id, p_item_id)
        ORDER BY 1
    LOOP
        PERFORM pg_advisory_xact_lock_shared(hashtextextended(lock_key, 0));
    END LOOP;

    IF EXISTS (
        SELECT 1
        FROM lifecycle_operation_claims claim
        WHERE claim.tenant_id = p_tenant_id
          AND claim.state = 'active'
          AND claim.phase IN ('all', p_phase)
          AND (
              claim.resource_type = 'tenant' AND claim.resource_key = '*'
              OR claim.resource_type = 'lane' AND claim.resource_key = p_lane
              OR claim.resource_type = 'source' AND p_source_id IS NOT NULL
                 AND claim.resource_key = p_lane || '/' || p_source_id::TEXT
              OR claim.resource_type IN ('item', 'artifact')
                 AND split_part(claim.resource_key, '/', 1) = p_lane
                 AND split_part(claim.resource_key, '/', 3) = p_item_id::TEXT
          )
    ) THEN
        RAISE EXCEPTION 'lifecycle_operation_conflict: content item is claimed by an active campaign'
            USING ERRCODE = 'P0001';
    END IF;

    IF p_phase = 'content_create' AND EXISTS (
        SELECT 1
        FROM content_reset_intake_pauses pause
        WHERE pause.tenant_id = p_tenant_id
          AND pause.lane = p_lane
          AND pause.state = 'active'
          AND (pause.content_source_id IS NULL OR pause.content_source_id IS NOT DISTINCT FROM p_source_id)
    ) THEN
        RAISE EXCEPTION 'content_reset_intake_paused: source intake is held by an active campaign'
            USING ERRCODE = 'P0001';
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION enforce_content_reset_lifecycle_item_fence()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    lock_key TEXT;
    old_lane TEXT;
    new_lane TEXT;
BEGIN
    IF TG_OP IN ('UPDATE', 'DELETE') THEN
        old_lane := content_reset_lifecycle_lane(OLD.type);
    END IF;
    IF TG_OP IN ('INSERT', 'UPDATE') THEN
        new_lane := content_reset_lifecycle_lane(NEW.type);
    END IF;

    IF TG_OP = 'INSERT' THEN
        FOR lock_key IN
            SELECT content_reset_lifecycle_item_lock_keys(NEW.tenant_id, new_lane, NEW.content_source_id, NEW.public_id)
            ORDER BY 1
        LOOP
            PERFORM pg_advisory_xact_lock_shared(hashtextextended(lock_key, 0));
        END LOOP;
        PERFORM assert_content_reset_item_allowed(NEW.tenant_id, new_lane, NEW.content_source_id, NEW.public_id, 'content_create');
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        FOR lock_key IN
            SELECT content_reset_lifecycle_item_lock_keys(OLD.tenant_id, old_lane, OLD.content_source_id, OLD.public_id)
            ORDER BY 1
        LOOP
            PERFORM pg_advisory_xact_lock_shared(hashtextextended(lock_key, 0));
        END LOOP;
        PERFORM assert_content_reset_item_allowed(OLD.tenant_id, old_lane, OLD.content_source_id, OLD.public_id, 'content_write');
        RETURN OLD;
    END IF;

    IF NEW.type IS DISTINCT FROM OLD.type THEN
        RAISE EXCEPTION 'content item type is immutable after creation'
            USING ERRCODE = 'P0001';
    END IF;

    FOR lock_key IN
        SELECT DISTINCT scopes.lock_key
        FROM (
            SELECT content_reset_lifecycle_item_lock_keys(OLD.tenant_id, old_lane, OLD.content_source_id, OLD.public_id) AS lock_key
            UNION ALL
            SELECT content_reset_lifecycle_item_lock_keys(NEW.tenant_id, new_lane, NEW.content_source_id, NEW.public_id) AS lock_key
        ) AS scopes
        ORDER BY scopes.lock_key
    LOOP
        PERFORM pg_advisory_xact_lock_shared(hashtextextended(lock_key, 0));
    END LOOP;
    PERFORM assert_content_reset_item_allowed(OLD.tenant_id, old_lane, OLD.content_source_id, OLD.public_id, 'content_write');
    PERFORM assert_content_reset_item_allowed(NEW.tenant_id, new_lane, NEW.content_source_id, NEW.public_id, 'content_write');
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS content_reset_lifecycle_item_fence ON content_items;
CREATE TRIGGER content_reset_lifecycle_item_fence
BEFORE INSERT OR UPDATE OR DELETE ON content_items
FOR EACH ROW EXECUTE FUNCTION enforce_content_reset_lifecycle_item_fence();

CREATE OR REPLACE FUNCTION assert_content_reset_lane_membership_allowed(
    p_tenant_id TEXT,
    p_lane TEXT
)
RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE
    lock_key TEXT;
BEGIN
    FOR lock_key IN
        SELECT lock_key FROM unnest(ARRAY[
            'content-lifecycle/v1/' || p_tenant_id || '/tenant/*',
            'content-lifecycle/v1/' || p_tenant_id || '/lane/' || p_lane
        ]) AS locks(lock_key)
        ORDER BY lock_key
    LOOP
        PERFORM pg_advisory_xact_lock_shared(hashtextextended(lock_key, 0));
    END LOOP;
    IF EXISTS (
        SELECT 1 FROM lifecycle_operation_claims claim
        WHERE claim.tenant_id = p_tenant_id
          AND claim.state = 'active'
          AND claim.phase IN ('all', 'feed_membership')
          AND (
              claim.resource_type = 'tenant' AND claim.resource_key = '*'
              OR claim.resource_type = 'lane' AND claim.resource_key = p_lane
          )
    ) THEN
        RAISE EXCEPTION 'lifecycle_operation_conflict: feed membership is claimed by an active campaign'
            USING ERRCODE = 'P0001';
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION enforce_content_reset_lifecycle_membership_fence()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    generation_tenant TEXT;
    generation_lane TEXT;
    lifecycle_lane TEXT;
    membership_type TEXT;
    membership_id UUID;
    generation_id UUID;
    lock_key TEXT;
    item RECORD;
    item_found BOOLEAN := FALSE;
BEGIN
    IF TG_OP = 'UPDATE' AND (
        OLD.generation_id IS DISTINCT FROM NEW.generation_id
        OR OLD.member_type IS DISTINCT FROM NEW.member_type
        OR OLD.member_id IS DISTINCT FROM NEW.member_id
    ) THEN
        RAISE EXCEPTION 'feed generation membership identity is immutable; delete and reinsert through the feed owner'
            USING ERRCODE = 'P0001';
    END IF;

    IF TG_OP = 'DELETE' THEN
        membership_type := OLD.member_type;
        membership_id := OLD.member_id;
        generation_id := OLD.generation_id;
    ELSE
        membership_type := NEW.member_type;
        membership_id := NEW.member_id;
        generation_id := NEW.generation_id;
    END IF;

    SELECT generation.tenant_id,
           CASE WHEN generation.lane = 'media' THEN 'pods' ELSE 'news' END
      INTO generation_tenant, lifecycle_lane
      FROM feed_generations generation
     WHERE generation.public_id = generation_id;
    IF NOT FOUND THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;

    IF membership_type = 'feed_unit' THEN
        SELECT content.public_id, content.tenant_id, content.content_source_id, content.type
          INTO item
          FROM content_items content
         WHERE content.tenant_id = generation_tenant AND content.public_id = membership_id;
        IF NOT FOUND THEN
            IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
            RETURN NEW;
        END IF;
        PERFORM assert_content_reset_item_allowed(
            item.tenant_id,
            content_reset_lifecycle_lane(item.type),
            item.content_source_id,
            item.public_id,
            'feed_membership'
        );
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;

    IF membership_type <> 'story' THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;

    FOR lock_key IN
        SELECT DISTINCT scopes.lock_key
        FROM content_items content
        CROSS JOIN LATERAL content_reset_lifecycle_item_lock_keys(
            content.tenant_id,
            content_reset_lifecycle_lane(content.type),
            content.content_source_id,
            content.public_id
        ) AS scopes(lock_key)
        WHERE content.tenant_id = generation_tenant
          AND content.story_id = membership_id
          AND content.type = 'NEWS'
        ORDER BY scopes.lock_key
    LOOP
        PERFORM pg_advisory_xact_lock_shared(hashtextextended(lock_key, 0));
    END LOOP;

    FOR item IN
        SELECT content.public_id, content.tenant_id, content.content_source_id, content.type
          FROM content_items content
         WHERE content.tenant_id = generation_tenant
           AND content.story_id = membership_id
           AND content.type = 'NEWS'
         ORDER BY content.public_id
    LOOP
        item_found := TRUE;
        PERFORM assert_content_reset_item_allowed(
            item.tenant_id,
            content_reset_lifecycle_lane(item.type),
            item.content_source_id,
            item.public_id,
            'feed_membership'
        );
    END LOOP;
    IF NOT item_found THEN
        PERFORM assert_content_reset_lane_membership_allowed(generation_tenant, lifecycle_lane);
    END IF;
    IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS content_reset_lifecycle_membership_fence ON feed_generation_memberships;
CREATE TRIGGER content_reset_lifecycle_membership_fence
BEFORE INSERT OR UPDATE OR DELETE ON feed_generation_memberships
FOR EACH ROW EXECUTE FUNCTION enforce_content_reset_lifecycle_membership_fence();

-- Source identity/configuration is part of replay authority. Runtime telemetry
-- updates remain independent, while configuration changes are versioned and
-- fenced against a campaign's frozen source snapshot.
CREATE OR REPLACE FUNCTION content_reset_lifecycle_source_lane(p_category TEXT)
RETURNS TEXT LANGUAGE SQL IMMUTABLE AS $$
    SELECT CASE WHEN p_category = 'media' THEN 'pods' ELSE 'news' END;
$$;

CREATE OR REPLACE FUNCTION content_reset_lifecycle_source_lock_keys(
    p_tenant_id TEXT,
    p_lane TEXT,
    p_source_id UUID
)
RETURNS SETOF TEXT LANGUAGE SQL IMMUTABLE AS $$
    SELECT DISTINCT lock_key
    FROM unnest(ARRAY[
        'content-lifecycle/v1/' || p_tenant_id || '/tenant/*',
        'content-lifecycle/v1/' || p_tenant_id || '/lane/' || p_lane,
        'content-lifecycle/v1/' || p_tenant_id || '/source/' || p_lane || '/' || p_source_id::TEXT
    ]) AS locks(lock_key)
$$;

CREATE OR REPLACE FUNCTION assert_content_reset_source_allowed(
    p_tenant_id TEXT,
    p_lane TEXT,
    p_source_id UUID
)
RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE
    lock_key TEXT;
BEGIN
    FOR lock_key IN
        SELECT content_reset_lifecycle_source_lock_keys(p_tenant_id, p_lane, p_source_id)
        ORDER BY 1
    LOOP
        PERFORM pg_advisory_xact_lock_shared(hashtextextended(lock_key, 0));
    END LOOP;
    IF EXISTS (
        SELECT 1 FROM lifecycle_operation_claims claim
        WHERE claim.tenant_id = p_tenant_id
          AND claim.state = 'active'
          AND claim.phase IN ('all', 'source_admission', 'source_dispatch')
          AND (
              claim.resource_type = 'tenant' AND claim.resource_key = '*'
              OR claim.resource_type = 'lane' AND claim.resource_key = p_lane
              OR claim.resource_type = 'source' AND claim.resource_key = p_lane || '/' || p_source_id::TEXT
          )
    ) THEN
        RAISE EXCEPTION 'lifecycle_operation_conflict: source configuration is claimed by an active campaign'
            USING ERRCODE = 'P0001';
    END IF;
    IF EXISTS (
        SELECT 1 FROM content_reset_intake_pauses pause
        WHERE pause.tenant_id = p_tenant_id
          AND pause.lane = p_lane
          AND pause.state = 'active'
          AND (pause.content_source_id IS NULL OR pause.content_source_id = p_source_id)
    ) THEN
        RAISE EXCEPTION 'content_reset_intake_paused: source configuration is held by an active campaign'
            USING ERRCODE = 'P0001';
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION enforce_content_reset_source_lifecycle_fence()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    old_lane TEXT;
    new_lane TEXT;
    lock_key TEXT;
BEGIN
    IF TG_OP = 'INSERT' THEN
        new_lane := content_reset_lifecycle_source_lane(NEW.category);
        PERFORM assert_content_reset_source_allowed(NEW.tenant_id, new_lane, NEW.public_id);
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        old_lane := content_reset_lifecycle_source_lane(OLD.category);
        PERFORM assert_content_reset_source_allowed(OLD.tenant_id, old_lane, OLD.public_id);
        RETURN OLD;
    END IF;

    IF NEW.public_id IS DISTINCT FROM OLD.public_id OR NEW.tenant_id IS DISTINCT FROM OLD.tenant_id THEN
        RAISE EXCEPTION 'content source tenant and public identity are immutable';
    END IF;
    IF NEW.name IS DISTINCT FROM OLD.name
       OR NEW.type IS DISTINCT FROM OLD.type
       OR NEW.category IS DISTINCT FROM OLD.category
       OR NEW.feed_url IS DISTINCT FROM OLD.feed_url
       OR NEW.image_url IS DISTINCT FROM OLD.image_url
       OR NEW.api_config IS DISTINCT FROM OLD.api_config
       OR NEW.media_acquisition_mode IS DISTINCT FROM OLD.media_acquisition_mode
       OR NEW.metadata IS DISTINCT FROM OLD.metadata
       OR NEW.discovery_profile_id IS DISTINCT FROM OLD.discovery_profile_id
       OR NEW.is_active IS DISTINCT FROM OLD.is_active
       OR NEW.fetch_interval_minutes IS DISTINCT FROM OLD.fetch_interval_minutes THEN
        old_lane := content_reset_lifecycle_source_lane(OLD.category);
        new_lane := content_reset_lifecycle_source_lane(NEW.category);
        FOR lock_key IN
            SELECT DISTINCT scope.lock_key
            FROM (
                SELECT content_reset_lifecycle_source_lock_keys(OLD.tenant_id, old_lane, OLD.public_id) AS lock_key
                UNION ALL
                SELECT content_reset_lifecycle_source_lock_keys(NEW.tenant_id, new_lane, NEW.public_id) AS lock_key
            ) AS scope
            ORDER BY scope.lock_key
        LOOP
            PERFORM pg_advisory_xact_lock_shared(hashtextextended(lock_key, 0));
        END LOOP;
        PERFORM assert_content_reset_source_allowed(OLD.tenant_id, old_lane, OLD.public_id);
        PERFORM assert_content_reset_source_allowed(NEW.tenant_id, new_lane, NEW.public_id);
        NEW.source_config_version := OLD.source_config_version + 1;
    ELSE
        NEW.source_config_version := OLD.source_config_version;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS content_reset_source_lifecycle_fence ON content_sources;
CREATE TRIGGER content_reset_source_lifecycle_fence
BEFORE INSERT OR UPDATE OR DELETE ON content_sources
FOR EACH ROW EXECUTE FUNCTION enforce_content_reset_source_lifecycle_fence();

CREATE OR REPLACE FUNCTION assign_tenant_content_inventory_sequence()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    assigned_sequence BIGINT;
BEGIN
    IF TG_OP = 'UPDATE' THEN
        IF NEW.tenant_id IS DISTINCT FROM OLD.tenant_id THEN
            RAISE EXCEPTION 'content item tenant identity is immutable';
        END IF;
        IF NEW.inventory_sequence IS DISTINCT FROM OLD.inventory_sequence THEN
            RAISE EXCEPTION 'content item inventory sequence is immutable';
        END IF;
        RETURN NEW;
    END IF;

    INSERT INTO tenant_content_inventory_counters (tenant_id, current_sequence)
    VALUES (NEW.tenant_id, 1)
    ON CONFLICT (tenant_id) DO UPDATE
    SET current_sequence = tenant_content_inventory_counters.current_sequence + 1,
        updated_at = NOW()
    RETURNING current_sequence INTO assigned_sequence;

    NEW.inventory_sequence := assigned_sequence;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS tenant_content_inventory_sequence ON content_items;
DROP TRIGGER IF EXISTS tenant_content_inventory_sequence_insert ON content_items;
CREATE TRIGGER tenant_content_inventory_sequence_insert
BEFORE INSERT ON content_items
FOR EACH ROW EXECUTE FUNCTION assign_tenant_content_inventory_sequence();

DROP TRIGGER IF EXISTS tenant_content_inventory_sequence_update ON content_items;
CREATE TRIGGER tenant_content_inventory_sequence_update
BEFORE UPDATE OF tenant_id, inventory_sequence ON content_items
FOR EACH ROW EXECUTE FUNCTION assign_tenant_content_inventory_sequence();

CREATE OR REPLACE FUNCTION reject_content_reset_inventory_deletion_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'UPDATE' THEN
        RAISE EXCEPTION 'Content Reset inventory deletion evidence is immutable';
    END IF;
    -- This is an operational preview-invalidation ledger, not the campaign
    -- audit trail. Keep it longer than the maximum preview lifetime, then let
    -- the bounded owner pruning below remove expired rows.
    IF OLD.deleted_at >= clock_timestamp() - INTERVAL '25 hours' THEN
        RAISE EXCEPTION 'recent Content Reset inventory deletion evidence cannot be removed';
    END IF;
    RETURN OLD;
END;
$$;
DROP TRIGGER IF EXISTS content_reset_inventory_deletions_append_only ON content_reset_inventory_deletions;
CREATE TRIGGER content_reset_inventory_deletions_append_only
BEFORE UPDATE OR DELETE ON content_reset_inventory_deletions
FOR EACH ROW EXECUTE FUNCTION reject_content_reset_inventory_deletion_mutation();

CREATE OR REPLACE FUNCTION record_content_reset_inventory_deletion()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    deletion_sequence BIGINT;
    last_prune_at TIMESTAMPTZ;
BEGIN
    INSERT INTO tenant_content_inventory_counters (tenant_id, current_sequence, last_deletion_prune_at)
    VALUES (OLD.tenant_id, 1, 'epoch')
    ON CONFLICT (tenant_id) DO UPDATE
    SET current_sequence = tenant_content_inventory_counters.current_sequence + 1,
        updated_at = NOW()
    RETURNING current_sequence, last_deletion_prune_at INTO deletion_sequence, last_prune_at;

    INSERT INTO content_reset_inventory_deletions (
        tenant_id, inventory_sequence, content_item_id, type, status,
        content_source_id, processing_generation, created_at, published_at, deleted_at
    ) VALUES (
        OLD.tenant_id, deletion_sequence, OLD.public_id, OLD.type, OLD.status,
        OLD.content_source_id, OLD.processing_generation, OLD.created_at,
        OLD.published_at, clock_timestamp()
    );
    -- Prune at most once per hour of deletion activity. Keep facts older than
    -- the 24-hour planning TTL plus one safety hour; the retention index bounds
    -- the scan to expired rows for the current tenant.
    IF last_prune_at < clock_timestamp() - INTERVAL '1 hour' THEN
        UPDATE tenant_content_inventory_counters
        SET last_deletion_prune_at = clock_timestamp(), updated_at = NOW()
        WHERE tenant_id = OLD.tenant_id;
        DELETE FROM content_reset_inventory_deletions
        WHERE tenant_id = OLD.tenant_id
          AND deleted_at < clock_timestamp() - INTERVAL '25 hours';
    END IF;
    RETURN OLD;
END;
$$;

DROP TRIGGER IF EXISTS content_reset_inventory_delete_capture ON content_items;
CREATE TRIGGER content_reset_inventory_delete_capture
AFTER DELETE ON content_items
FOR EACH ROW EXECUTE FUNCTION record_content_reset_inventory_deletion();
