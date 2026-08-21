-- Database identity and application writer authority are explicit, additive,
-- and adopted only by an operator through the canonical migration workflow.
CREATE TABLE IF NOT EXISTS wahb_database_identity (
    singleton boolean PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    database_id uuid NOT NULL UNIQUE,
    lineage_root_id uuid NOT NULL,
    database_epoch bigint NOT NULL DEFAULT 0 CHECK (database_epoch >= 0),
    schema_contract_version varchar(64) NOT NULL DEFAULT 'cms/v1',
    predecessor_database_id uuid,
    migration_program_id uuid,
    adopted_at timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS wahb_database_identity_events (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    database_id uuid NOT NULL,
    lineage_root_id uuid NOT NULL,
    database_epoch bigint NOT NULL,
    event_type varchar(64) NOT NULL,
    migration_program_id uuid,
    predecessor_database_id uuid,
    recorded_at timestamptz NOT NULL DEFAULT now(),
    evidence jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS wahb_database_writer_fence (
    singleton boolean PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    state varchar(32) NOT NULL DEFAULT 'open' CHECK (state IN ('open', 'quiescing', 'sealed', 'successor_open')),
    epoch bigint NOT NULL DEFAULT 0 CHECK (epoch >= 0),
    fence_token uuid NOT NULL DEFAULT gen_random_uuid(),
    migration_program_id uuid,
    changed_at timestamptz NOT NULL DEFAULT now(),
    changed_by varchar(255) NOT NULL DEFAULT 'canonical_migration'
);

INSERT INTO wahb_database_writer_fence (singleton)
VALUES (TRUE)
ON CONFLICT (singleton) DO NOTHING;

-- This protects direct SQL and worker writes that do not pass through the
-- application HTTP/GORM fences. The coordinator/migration owner changes the
-- fence state through the explicitly exempt singleton table.
CREATE OR REPLACE FUNCTION wahb_enforce_writer_fence()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    current_state varchar(32);
BEGIN
    SELECT state INTO current_state FROM wahb_database_writer_fence WHERE singleton = TRUE;
    IF current_state IS DISTINCT FROM 'open' AND current_state IS DISTINCT FROM 'successor_open' THEN
        RAISE EXCEPTION 'wahb database writes are fenced (%)', COALESCE(current_state, 'missing') USING ERRCODE = '55000';
    END IF;
    RETURN COALESCE(NEW, OLD);
END;
$$;

DO $$
DECLARE
    entry record;
BEGIN
    FOR entry IN
        SELECT quote_ident(schemaname) AS schema_name, quote_ident(tablename) AS table_name
        FROM pg_tables
        WHERE schemaname = 'public'
          AND tablename NOT IN ('cms_schema_migrations', 'wahb_database_identity', 'wahb_database_identity_events', 'wahb_database_writer_fence')
    LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS wahb_writer_fence_guard ON %s.%s', entry.schema_name, entry.table_name);
        EXECUTE format('CREATE TRIGGER wahb_writer_fence_guard BEFORE INSERT OR UPDATE OR DELETE ON %s.%s FOR EACH ROW EXECUTE FUNCTION wahb_enforce_writer_fence()', entry.schema_name, entry.table_name);
    END LOOP;
END;
$$;
