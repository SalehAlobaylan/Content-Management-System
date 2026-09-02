-- Pods discovery is intentionally narrow: source runs create preview metadata
-- first, so a single poll must not flood the media library with long episodes.
-- The tenant policy remains adjustable through the existing media-acquisition
-- settings surface, but every tenant starts with the safe cap of ten.

ALTER TABLE media_acquisition_configs
  ADD COLUMN IF NOT EXISTS pods_source_run_item_limit integer NOT NULL DEFAULT 10;

ALTER TABLE media_acquisition_configs
  DROP CONSTRAINT IF EXISTS media_acquisition_configs_pods_source_run_item_limit_check;
ALTER TABLE media_acquisition_configs
  ADD CONSTRAINT media_acquisition_configs_pods_source_run_item_limit_check
  CHECK (pods_source_run_item_limit BETWEEN 1 AND 50);

UPDATE media_acquisition_configs
SET pods_source_run_item_limit = 10
WHERE pods_source_run_item_limit IS NULL
   OR pods_source_run_item_limit < 1
   OR pods_source_run_item_limit > 50;
