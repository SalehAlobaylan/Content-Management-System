-- Media Atomization manual controls: tenant defaults, source/episode overrides,
-- and manual queue state.

CREATE TABLE IF NOT EXISTS media_atomization_policies (
    id bigserial PRIMARY KEY,
    tenant_id varchar(64) NOT NULL,
    chaptering_enabled boolean NOT NULL DEFAULT true,
    auto_publish_high_confidence boolean NOT NULL DEFAULT true,
    parent_feed_visible boolean NOT NULL DEFAULT false,
    preserve_video boolean NOT NULL DEFAULT true,
    remove_sponsor_segments boolean NOT NULL DEFAULT true,
    min_chapter_minutes integer NOT NULL DEFAULT 5,
    min_feed_unit_seconds integer NOT NULL DEFAULT 270,
    soft_max_chapter_minutes integer NOT NULL DEFAULT 30,
    hard_max_chapter_minutes integer NOT NULL DEFAULT 40,
    atomization_min_parent_seconds integer NOT NULL DEFAULT 2400,
    max_chapters_per_parent integer NOT NULL DEFAULT 5,
    chaptering_mode varchar(32) NOT NULL DEFAULT 'contextual',
    high_confidence_threshold double precision NOT NULL DEFAULT 0.82,
    preferred_playback_rendition varchar(16) NOT NULL DEFAULT 'hls',
    fallback_playback_rendition varchar(16) NOT NULL DEFAULT 'mp4',
    audio_only_allowed boolean NOT NULL DEFAULT true,
    created_at timestamp NOT NULL DEFAULT now(),
    updated_at timestamp NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_media_atomization_policy_tenant
    ON media_atomization_policies (tenant_id);

ALTER TABLE content_items
    ADD COLUMN IF NOT EXISTS atomization_override varchar(16),
    ADD COLUMN IF NOT EXISTS atomization_override_reason text,
    ADD COLUMN IF NOT EXISTS atomization_override_by uuid,
    ADD COLUMN IF NOT EXISTS atomization_override_at timestamp,
    ADD COLUMN IF NOT EXISTS manual_atomization_requested_at timestamp;

CREATE INDEX IF NOT EXISTS idx_content_items_atomization_override
    ON content_items (atomization_override);

ALTER TABLE media_atomization_runs
    ADD COLUMN IF NOT EXISTS trigger varchar(24),
    ADD COLUMN IF NOT EXISTS requested_by uuid;

CREATE INDEX IF NOT EXISTS idx_media_atomization_runs_trigger
    ON media_atomization_runs (trigger);
