-- Restore Media source production after the durable source-run scheduler
-- cutover. The scheduler intentionally ignores NULL next_due_at, while the
-- retired legacy Media circulation loop was the only mechanism that had ever
-- polled sources created before the cutover. Mark active Media sources due for
-- a bounded first admission without claiming provider success or advancing any
-- evidence checkpoint.

UPDATE content_sources
SET next_due_at = NOW(),
    source_config_version = GREATEST(source_config_version, 1)
WHERE category = 'media'
  AND is_active = TRUE
  AND next_due_at IS NULL;
