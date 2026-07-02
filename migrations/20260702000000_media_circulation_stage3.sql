-- Media Circulation Engine — stage 3 foundation.
--
-- Applies the pre-autopilot hardening layer:
-- - canonical duration buckets for media feed units
-- - generation timestamp for the cockpit
-- - human exception overrides consulted before recommendations are emitted

UPDATE content_items AS ci
SET duration_bucket = (
    SELECT (v.bucket::text || 'm')
    FROM (VALUES (5), (10), (15), (20), (30), (40)) AS v(bucket)
    ORDER BY ABS(ROUND(ci.duration_sec::numeric / 60.0) - v.bucket), v.bucket
    LIMIT 1
)
WHERE ci.duration_sec IS NOT NULL
  AND ci.is_feed_unit = TRUE
  AND ci.type IN ('VIDEO', 'PODCAST')
  AND (
      ci.duration_bucket IS NULL
      OR ci.duration_bucket NOT IN ('5m', '10m', '15m', '20m', '30m', '40m')
  );

ALTER TABLE media_circulation_policies
    ADD COLUMN IF NOT EXISTS last_generated_at timestamp;

CREATE TABLE IF NOT EXISTS media_circulation_overrides (
    id bigserial PRIMARY KEY,
    public_id uuid NOT NULL DEFAULT gen_random_uuid(),
    tenant_id varchar(64) NOT NULL,
    subject_kind varchar(24) NOT NULL,
    subject_id uuid NOT NULL,
    override_type varchar(32) NOT NULL,
    params jsonb,
    expires_at timestamp,
    set_by varchar(255),
    notes text,
    created_at timestamp NOT NULL DEFAULT now(),
    updated_at timestamp NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_media_circ_overrides_public_id
    ON media_circulation_overrides (public_id);
CREATE INDEX IF NOT EXISTS idx_media_circ_overrides_subject
    ON media_circulation_overrides (tenant_id, subject_kind, subject_id);
CREATE INDEX IF NOT EXISTS idx_media_circ_overrides_type
    ON media_circulation_overrides (tenant_id, override_type);
CREATE INDEX IF NOT EXISTS idx_media_circ_overrides_expires
    ON media_circulation_overrides (expires_at);
