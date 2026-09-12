-- Recover only stages rejected by the old discovery-time execution deadline.
-- No retries of attempted/uncertain effects, no media approval changes, and no
-- paid STT admission. Apply with workers stopped, then restart the application.
WITH recovered AS (
  UPDATE content_stage_requests r
  SET state = 'queued', deadline_at = NULL, not_before_at = NULL,
      finished_at = NULL, failure_class = '', terminal_proof = '{}'::jsonb,
      updated_at = NOW()
  FROM content_items i
  WHERE i.tenant_id = r.tenant_id AND i.public_id = r.content_item_id
    AND i.processing_generation = r.processing_generation
    AND i.status <> 'ARCHIVED'
    AND r.state = 'failed'
    AND r.failure_class = 'execution_budget_exhausted'
    AND r.deadline_at < NOW()
    AND r.cancellation_requested_at IS NULL
    AND r.stage IN ('pods_transcript', 'pods_image_embedding')
    AND r.terminal_proof->>'effect' = 'absent'
    AND NOT EXISTS (
      SELECT 1 FROM content_stage_attempts a
      WHERE a.tenant_id = r.tenant_id AND a.request_id = r.public_id
    )
    AND EXISTS (
      SELECT 1 FROM content_stage_requests media
      WHERE media.tenant_id = r.tenant_id
        AND media.content_item_id = r.content_item_id
        AND media.processing_generation = r.processing_generation
        AND media.stage = 'pods_media_artifacts' AND media.state = 'verified'
    )
    AND (r.stage = 'pods_image_embedding' OR (
      jsonb_typeof(i.metadata->'caption_artifact') = 'object'
      AND LENGTH(TRIM(i.metadata->'caption_artifact'->>'full_text')) > 0
      AND jsonb_typeof(i.metadata->'caption_artifact'->'segments') = 'array'
    ))
  RETURNING r.tenant_id, r.public_id
)
INSERT INTO content_stage_events
  (public_id, tenant_id, request_id, sequence, event_type, payload, occurred_at)
SELECT gen_random_uuid(), r.tenant_id, r.public_id,
       COALESCE((SELECT MAX(e.sequence) FROM content_stage_events e
                 WHERE e.tenant_id = r.tenant_id AND e.request_id = r.public_id), 0) + 1,
       'unattempted_deadline_recovered',
       '{"reason":"discovery_deadline_expired_before_first_attempt","previous_state":"failed","state":"queued","paid_stt_admitted":false}'::jsonb,
       NOW()
FROM recovered r;
