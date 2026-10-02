-- Allow the same provider identity to be observed again by a bounded replay
-- request while preserving idempotency within one request page.

DO $$
DECLARE
    constraint_name TEXT;
BEGIN
    FOR constraint_name IN
        SELECT constraint_row.conname
          FROM pg_constraint constraint_row
          JOIN pg_class table_row ON table_row.oid = constraint_row.conrelid
          JOIN pg_namespace schema_row ON schema_row.oid = table_row.relnamespace
         WHERE table_row.relname = 'source_upstream_observations'
           AND schema_row.nspname = current_schema()
           AND constraint_row.contype = 'u'
           AND (
               SELECT array_agg(attribute_row.attname::TEXT ORDER BY key_column.ordinality)
                 FROM unnest(constraint_row.conkey) WITH ORDINALITY AS key_column(attnum, ordinality)
                 JOIN pg_attribute attribute_row
                   ON attribute_row.attrelid = table_row.oid
                  AND attribute_row.attnum = key_column.attnum
           ) = ARRAY['tenant_id', 'content_source_id', 'provider_version', 'upstream_item_id']::TEXT[]
    LOOP
        EXECUTE format('ALTER TABLE source_upstream_observations DROP CONSTRAINT %I', constraint_name);
    END LOOP;
END;
$$;

CREATE UNIQUE INDEX IF NOT EXISTS uq_source_upstream_observations_request_page_identity
    ON source_upstream_observations(
        tenant_id, content_source_id, source_run_request_id, provider_version, provider_page_id, upstream_item_id
    );
