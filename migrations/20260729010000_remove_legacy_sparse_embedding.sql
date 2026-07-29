-- Apply only after the deployed readiness report proves zero sparse use.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='content_items' AND column_name='embedding_sparse') THEN
    IF NOT EXISTS (SELECT 1 FROM retention_maintenance_reports WHERE state = 'free_downgrade_ready') THEN
      RAISE EXCEPTION 'cannot remove embedding_sparse without a recorded free-downgrade readiness report';
    END IF;
    IF EXISTS (SELECT 1 FROM content_items WHERE embedding_sparse IS NOT NULL) THEN
      RAISE EXCEPTION 'cannot remove embedding_sparse while populated rows remain';
    END IF;
    DROP INDEX IF EXISTS content_items_embedding_sparse_idx;
    DROP INDEX IF EXISTS idx_content_items_ready_missing_sparse;
    ALTER TABLE content_items DROP COLUMN embedding_sparse;
  END IF;
END $$;
