-- Destructive boundary: the runner requires --allow-destructive. Sparse
-- retrieval already returns 410 and Qwen write-back is dense-only. Record the
-- exact legacy payload count being retired in the immutable maintenance ledger,
-- then remove the dead index/column atomically. Final free-downgrade readiness
-- is measured after this logical retirement and any operator-owned physical
-- reclaim; it must not be a prerequisite for the reclaim-enabling migration.
DO $$
DECLARE
  sparse_rows BIGINT := 0;
  current_bytes BIGINT := 0;
  target_bytes BIGINT := 419430400;
  report_tenant VARCHAR(64) := 'default';
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='content_items' AND column_name='embedding_sparse') THEN
    SELECT COUNT(*) INTO sparse_rows
    FROM content_items
    WHERE embedding_sparse IS NOT NULL;

    SELECT pg_database_size(current_database()) INTO current_bytes;
    SELECT tenant_id, database_target_bytes
      INTO report_tenant, target_bytes
    FROM retention_policies
    ORDER BY updated_at DESC
    LIMIT 1;

    INSERT INTO retention_maintenance_reports (
      tenant_id, database_bytes, target_bytes, sparse_use_count, state, evidence
    ) VALUES (
      COALESCE(report_tenant, 'default'),
      current_bytes,
      COALESCE(target_bytes, 419430400),
      sparse_rows,
      'sparse_retired',
      jsonb_build_object(
        'dense_model', 'Qwen/Qwen3-Embedding-0.6B',
        'sparse_runtime', 'removed',
        'legacy_populated_rows_discarded', sparse_rows,
        'approval', 'explicit_destructive_migration',
        'physical_reclaim', 'operator_owned'
      )
    );

    DROP INDEX IF EXISTS content_items_embedding_sparse_idx;
    DROP INDEX IF EXISTS idx_content_items_ready_missing_sparse;
    ALTER TABLE content_items DROP COLUMN embedding_sparse;
  END IF;
END $$;
