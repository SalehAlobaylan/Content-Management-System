-- Explicit, item-scoped Pods reset. Reset execution stays disabled until the
-- operator has qualified the deployed runtime and deliberately enables it.
ALTER TABLE retention_execution_controls
    ADD COLUMN IF NOT EXISTS pods_reset_enabled BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE content_items
    ADD COLUMN IF NOT EXISTS retired_payload_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS pods_reset_runs (
    public_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id VARCHAR(64) NOT NULL,
    state VARCHAR(24) NOT NULL CHECK (state IN ('preview','approved','executing','partial','complete','cancelled')),
    phase VARCHAR(32) NOT NULL DEFAULT 'preview',
    manifest_hash CHAR(64) NOT NULL,
    schema_fingerprint CHAR(64) NOT NULL,
    policy_version INTEGER NOT NULL DEFAULT 1,
    manifest JSONB NOT NULL,
    created_by VARCHAR(255) NOT NULL,
    approved_by VARCHAR(255),
    approval_phrase_hash CHAR(64),
    approved_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ NOT NULL,
    fencing_token UUID,
    execution_token UUID,
    execution_lease_until TIMESTAMPTZ,
    execution_epoch BIGINT NOT NULL DEFAULT 0,
    pause_requested BOOLEAN NOT NULL DEFAULT FALSE,
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_pods_reset_runs_tenant_created
    ON pods_reset_runs(tenant_id, created_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS uq_pods_reset_run_active_tenant
    ON pods_reset_runs(tenant_id)
    WHERE state IN ('approved','executing','partial');

CREATE TABLE IF NOT EXISTS pods_reset_items (
    id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES pods_reset_runs(public_id) ON DELETE RESTRICT,
    tenant_id VARCHAR(64) NOT NULL,
    content_item_id UUID NOT NULL,
    ordinal INTEGER NOT NULL,
    snapshot_hash CHAR(64) NOT NULL,
    decision JSONB NOT NULL,
    state VARCHAR(24) NOT NULL CHECK (state IN ('planned','blocked','fenced','verification_pending','objects_deleted','complete')),
    blocked_reasons JSONB NOT NULL DEFAULT '[]'::jsonb,
    last_error TEXT,
    verification_probe_count SMALLINT NOT NULL DEFAULT 0 CHECK (verification_probe_count BETWEEN 0 AND 2),
    verification_not_before TIMESTAMPTZ,
    verification_evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(run_id, content_item_id),
    UNIQUE(run_id, ordinal)
);
CREATE INDEX IF NOT EXISTS idx_pods_reset_items_state
    ON pods_reset_items(run_id, state, ordinal);

CREATE TABLE IF NOT EXISTS pods_reset_objects (
    id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES pods_reset_runs(public_id) ON DELETE RESTRICT,
    content_item_id UUID NOT NULL,
    storage_tier VARCHAR(16) NOT NULL CHECK (storage_tier IN ('primary','cold')),
    bucket VARCHAR(255) NOT NULL,
    object_key TEXT NOT NULL,
    etag TEXT NOT NULL DEFAULT '',
    size_bytes BIGINT NOT NULL DEFAULT 0 CHECK (size_bytes >= 0),
    state VARCHAR(24) NOT NULL CHECK (state IN ('planned','deleting','deleted','blocked')),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    deleted_at TIMESTAMPTZ,
    deleted_by_run BOOLEAN NOT NULL DEFAULT FALSE,
    freed_bytes BIGINT NOT NULL DEFAULT 0 CHECK (freed_bytes >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(run_id, storage_tier, bucket, object_key)
);
CREATE INDEX IF NOT EXISTS idx_pods_reset_objects_state
    ON pods_reset_objects(run_id, content_item_id, state);

-- This permanent identity fence both preserves restrictive FK targets and
-- prevents idempotent ingest from bringing retired content back to life.
CREATE TABLE IF NOT EXISTS pods_reset_retirements (
    tenant_id VARCHAR(64) NOT NULL,
    content_item_id UUID NOT NULL REFERENCES content_items(public_id) ON DELETE RESTRICT,
    run_public_id UUID NOT NULL REFERENCES pods_reset_runs(public_id) ON DELETE RESTRICT,
    manifest_hash CHAR(64) NOT NULL,
    fencing_token UUID NOT NULL,
    identity_hash CHAR(64) NOT NULL,
    state VARCHAR(16) NOT NULL CHECK (state IN ('retiring','retired')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    retired_at TIMESTAMPTZ,
    PRIMARY KEY(tenant_id, content_item_id),
    UNIQUE(tenant_id, identity_hash)
);
CREATE INDEX IF NOT EXISTS idx_pods_reset_retirements_run
    ON pods_reset_retirements(run_public_id, state);

-- One transactionally committed per-item completion record acts as the durable
-- reset outbox/action ledger. It is immutable and keyed for idempotent resume.
CREATE TABLE IF NOT EXISTS pods_reset_actions (
    public_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL REFERENCES pods_reset_runs(public_id) ON DELETE RESTRICT,
    tenant_id VARCHAR(64) NOT NULL,
    content_item_id UUID NOT NULL REFERENCES content_items(public_id) ON DELETE RESTRICT,
    action VARCHAR(32) NOT NULL CHECK (action IN ('payload_retired')),
    manifest_hash CHAR(64) NOT NULL,
    result JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(run_id, content_item_id, action)
);
CREATE INDEX IF NOT EXISTS idx_pods_reset_actions_tenant_created
    ON pods_reset_actions(tenant_id, created_at DESC);

CREATE OR REPLACE FUNCTION reject_pods_reset_action_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'Pods reset action evidence is append-only';
END;
$$;
DROP TRIGGER IF EXISTS pods_reset_actions_append_only ON pods_reset_actions;
CREATE TRIGGER pods_reset_actions_append_only
BEFORE UPDATE OR DELETE ON pods_reset_actions
FOR EACH ROW EXECUTE FUNCTION reject_pods_reset_action_mutation();

-- Enforce retirement at the database write boundary too. This catches stale
-- service callbacks that passed a preflight check before the permanent fence
-- was installed. Only the owning reset transaction may update retired rows.
CREATE OR REPLACE FUNCTION enforce_pods_reset_retirement_fence()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    target_id UUID;
    run_id_value UUID;
    fence_value UUID;
    permitted BOOLEAN := FALSE;
    column_name TEXT;
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF TG_TABLE_NAME = 'content_items' THEN
            IF EXISTS (SELECT 1 FROM pods_reset_retirements r WHERE r.tenant_id=OLD.tenant_id AND r.content_item_id=OLD.public_id) THEN
                RAISE EXCEPTION 'retired content identity cannot be hard-deleted';
            END IF;
        END IF;
        RETURN OLD;
    END IF;

    IF TG_TABLE_NAME = 'content_items' AND TG_OP = 'INSERT' THEN
        IF EXISTS (SELECT 1 FROM pods_reset_retirements r WHERE r.content_item_id=NEW.public_id)
           OR (NEW.idempotency_key IS NOT NULL AND btrim(NEW.idempotency_key) <> '' AND EXISTS (
                SELECT 1 FROM pods_reset_retirements r
                WHERE r.tenant_id = NEW.tenant_id
                  AND r.identity_hash = encode(digest(NEW.tenant_id || E'\n' || btrim(NEW.idempotency_key), 'sha256'), 'hex')
           )) THEN
            RAISE EXCEPTION 'content identity is permanently retired';
        END IF;
    END IF;

    FOREACH column_name IN ARRAY TG_ARGV LOOP
        target_id := NULLIF(to_jsonb(NEW)->>column_name, '')::UUID;
        IF target_id IS NULL THEN
            CONTINUE;
        END IF;
        -- Serialize relationship creation with the reset's content-row fence.
        PERFORM 1 FROM content_items c WHERE c.public_id=target_id FOR UPDATE;
        IF EXISTS (
            SELECT 1 FROM pods_reset_retirements r
            WHERE r.content_item_id = target_id AND r.state IN ('retiring','retired')
        ) THEN
            run_id_value := NULLIF(current_setting('wahb.pods_reset.run_id', true), '')::UUID;
            fence_value := NULLIF(current_setting('wahb.pods_reset.fencing_token', true), '')::UUID;
            SELECT EXISTS (
                SELECT 1 FROM pods_reset_retirements r
                JOIN pods_reset_runs p ON p.public_id = r.run_public_id
                WHERE r.content_item_id = target_id
                  AND r.run_public_id = run_id_value
                  AND r.fencing_token = fence_value
                  AND p.fencing_token = fence_value
                  AND p.state IN ('executing','partial')
            ) INTO permitted;
            IF NOT permitted THEN
                RAISE EXCEPTION 'write rejected for permanently retired content identity';
            END IF;
        END IF;
    END LOOP;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS pods_reset_content_identity_fence ON content_items;
CREATE TRIGGER pods_reset_content_identity_fence
BEFORE INSERT OR UPDATE OR DELETE ON content_items
FOR EACH ROW EXECUTE FUNCTION enforce_pods_reset_retirement_fence('public_id', 'parent_content_item_id');

-- Moderation reports target content polymorphically rather than through an
-- FK. Serialize new content reports against retirement so a report created
-- after preflight cannot race the destructive boundary.
CREATE OR REPLACE FUNCTION enforce_pods_reset_moderation_report_fence()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'UPDATE'
       AND NEW.target_type IS NOT DISTINCT FROM OLD.target_type
       AND NEW.target_id IS NOT DISTINCT FROM OLD.target_id THEN
        RETURN NEW;
    END IF;
    IF NEW.target_type = 'content' THEN
        PERFORM 1 FROM content_items c WHERE c.tenant_id=NEW.tenant_id AND c.public_id=NEW.target_id FOR UPDATE;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'moderation report content target is unavailable';
        END IF;
        IF EXISTS (
            SELECT 1 FROM pods_reset_retirements r
            WHERE r.tenant_id=NEW.tenant_id AND r.content_item_id=NEW.target_id AND r.state IN ('retiring','retired')
        ) THEN
            RAISE EXCEPTION 'moderation report rejected for permanently retired content';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS pods_reset_moderation_report_fence ON moderation_reports;
CREATE TRIGGER pods_reset_moderation_report_fence
BEFORE INSERT OR UPDATE ON moderation_reports
FOR EACH ROW EXECUTE FUNCTION enforce_pods_reset_moderation_report_fence();

-- Install equivalent guards on all current direct item-owned relationship
-- tables. This list is the migration-time producer/writeback registry; the
-- CMS schema fingerprint and policy tests must be updated with new classes.
DO $$
DECLARE
    target_table RECORD;
    trigger_name TEXT;
    columns_to_guard TEXT[];
BEGIN
    FOR target_table IN
        SELECT reference_tables.table_name
        FROM (
            SELECT local_table.relname AS table_name
            FROM pg_class local_table
            JOIN pg_namespace local_schema ON local_schema.oid = local_table.relnamespace
            JOIN pg_attribute local_column ON local_column.attrelid = local_table.oid AND local_column.attnum > 0 AND NOT local_column.attisdropped
            WHERE local_schema.nspname = current_schema()
              AND local_table.relkind IN ('r', 'p')
              AND NOT EXISTS (SELECT 1 FROM pg_inherits partition_link WHERE partition_link.inhrelid = local_table.oid)
              AND local_column.attname IN ('content_item_id', 'parent_content_item_id')
            UNION
            SELECT local_table.relname AS table_name
            FROM pg_constraint fk
            JOIN pg_class local_table ON local_table.oid = fk.conrelid
            JOIN pg_namespace local_schema ON local_schema.oid = local_table.relnamespace
            JOIN generate_subscripts(fk.conkey, 1) AS key_position(position) ON TRUE
            JOIN pg_attribute local_column ON local_column.attrelid = fk.conrelid AND local_column.attnum = fk.conkey[key_position.position]
            JOIN pg_attribute referenced_column ON referenced_column.attrelid = fk.confrelid AND referenced_column.attnum = fk.confkey[key_position.position]
            WHERE fk.contype = 'f' AND fk.confrelid = 'content_items'::regclass
              AND referenced_column.attname = 'public_id'
              AND local_schema.nspname = current_schema()
              AND local_table.relkind IN ('r', 'p')
              AND NOT EXISTS (SELECT 1 FROM pg_inherits partition_link WHERE partition_link.inhrelid = local_table.oid)
        ) AS reference_tables
        -- Reset control/audit tables keep the retired content identity as
        -- their audit key; they are guarded by their own state constraints.
        WHERE reference_tables.table_name NOT IN ('content_items', 'pods_reset_runs', 'pods_reset_items', 'pods_reset_objects', 'pods_reset_actions', 'pods_reset_retirements')
    LOOP
        SELECT array_agg(DISTINCT reference_column.column_name ORDER BY reference_column.column_name) INTO columns_to_guard
        FROM (
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = target_table.table_name
              AND column_name IN ('content_item_id', 'parent_content_item_id')
            UNION
            SELECT local_column.attname AS column_name
            FROM pg_constraint fk
            JOIN pg_class local_table ON local_table.oid = fk.conrelid
            JOIN pg_namespace local_schema ON local_schema.oid = local_table.relnamespace
            JOIN generate_subscripts(fk.conkey, 1) AS key_position(position) ON TRUE
            JOIN pg_attribute local_column ON local_column.attrelid = fk.conrelid AND local_column.attnum = fk.conkey[key_position.position]
            JOIN pg_attribute referenced_column ON referenced_column.attrelid = fk.confrelid AND referenced_column.attnum = fk.confkey[key_position.position]
            WHERE fk.contype = 'f' AND fk.confrelid = 'content_items'::regclass
              AND referenced_column.attname = 'public_id'
              AND local_schema.nspname = current_schema()
              AND local_table.relkind IN ('r', 'p')
              AND NOT EXISTS (SELECT 1 FROM pg_inherits partition_link WHERE partition_link.inhrelid = local_table.oid)
              AND local_table.relname = target_table.table_name
        ) AS reference_column;
        trigger_name := 'pods_reset_fence_' || target_table.table_name;
        IF octet_length(trigger_name) > 63 THEN
            trigger_name := 'pods_reset_fence_' || md5(target_table.table_name);
        END IF;
        EXECUTE format('DROP TRIGGER IF EXISTS %I ON %I', trigger_name, target_table.table_name);
        EXECUTE format('CREATE TRIGGER %I BEFORE INSERT OR UPDATE ON %I FOR EACH ROW EXECUTE FUNCTION enforce_pods_reset_retirement_fence(%s)',
            trigger_name, target_table.table_name,
            (SELECT string_agg(quote_literal(column_name), ',') FROM unnest(columns_to_guard) AS guarded_column(column_name)));
    END LOOP;
END;
$$;

-- Supply actions store their target in a polymorphic target_id column rather
-- than content_item_id. Fence content-item actions explicitly so a queued or
-- concurrently claimed action cannot admit new work after retirement.
CREATE OR REPLACE FUNCTION enforce_pods_reset_supply_action_fence()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    run_id_value UUID;
    fence_value UUID;
    permitted BOOLEAN := FALSE;
BEGIN
    IF NEW.target_type <> 'content_item' THEN
        RETURN NEW;
    END IF;
    PERFORM 1 FROM content_items c WHERE c.tenant_id=NEW.tenant_id AND c.public_id=NEW.target_id FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'supply action content target is unavailable';
    END IF;
    IF EXISTS (
        SELECT 1 FROM pods_reset_retirements r
        WHERE r.tenant_id=NEW.tenant_id AND r.content_item_id=NEW.target_id AND r.state IN ('retiring','retired')
    ) THEN
        run_id_value := NULLIF(current_setting('wahb.pods_reset.run_id', true), '')::UUID;
        fence_value := NULLIF(current_setting('wahb.pods_reset.fencing_token', true), '')::UUID;
        SELECT EXISTS (
            SELECT 1 FROM pods_reset_retirements r
            JOIN pods_reset_runs p ON p.public_id=r.run_public_id
            WHERE r.tenant_id=NEW.tenant_id AND r.content_item_id=NEW.target_id
              AND r.run_public_id=run_id_value AND r.fencing_token=fence_value
              AND p.fencing_token=fence_value AND p.state IN ('executing','partial')
        ) INTO permitted;
        IF NOT permitted THEN
            RAISE EXCEPTION 'supply action rejected for permanently retired content identity';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS pods_reset_supply_action_fence ON media_supply_action_requests;
CREATE TRIGGER pods_reset_supply_action_fence
BEFORE INSERT OR UPDATE ON media_supply_action_requests
FOR EACH ROW EXECUTE FUNCTION enforce_pods_reset_supply_action_fence();

COMMENT ON TABLE pods_reset_retirements IS
    'Permanent CMS-owned fence and minimal identity for explicitly reset Pods items.';
