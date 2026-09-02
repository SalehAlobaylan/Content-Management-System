-- Media delivery repair was added as a CMS-fenced pipeline repair stage after
-- the original exact-stage substrate. Keep both the durable request and its
-- execution lease on the same closed stage vocabulary.

ALTER TABLE pipeline_repair_requests
  DROP CONSTRAINT IF EXISTS pipeline_repair_requests_stage_check;
ALTER TABLE pipeline_repair_requests
  ADD CONSTRAINT pipeline_repair_requests_stage_check CHECK (stage IN (
    'media_download', 'media_transcode', 'media_thumbnail',
    'media_delivery_generation', 'text_embedding'
  ));

ALTER TABLE pipeline_stage_leases
  DROP CONSTRAINT IF EXISTS pipeline_stage_leases_stage_check;
ALTER TABLE pipeline_stage_leases
  ADD CONSTRAINT pipeline_stage_leases_stage_check CHECK (stage IN (
    'media_download', 'media_transcode', 'media_thumbnail',
    'media_delivery_generation', 'text_embedding'
  ));
