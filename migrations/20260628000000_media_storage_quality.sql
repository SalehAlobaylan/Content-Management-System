-- Media Storage + Quality v1.
--
-- Separates artifact lifecycle from content/feed lifecycle. Content status
-- still controls public feed eligibility; storage_state describes where the
-- media bytes are and whether they can be recovered.

ALTER TABLE content_items
    ADD COLUMN IF NOT EXISTS storage_state varchar(32) NOT NULL DEFAULT 'hot',
    ADD COLUMN IF NOT EXISTS storage_state_reason text,
    ADD COLUMN IF NOT EXISTS storage_recovery_status varchar(32) NOT NULL DEFAULT 'recoverable',
    ADD COLUMN IF NOT EXISTS storage_deleted_at timestamp,
    ADD COLUMN IF NOT EXISTS storage_last_verified_at timestamp,
    ADD COLUMN IF NOT EXISTS media_suitability varchar(40) NOT NULL DEFAULT 'unknown',
    ADD COLUMN IF NOT EXISTS media_suitability_confidence double precision,
    ADD COLUMN IF NOT EXISTS media_suitability_reasons jsonb,
    ADD COLUMN IF NOT EXISTS media_suitability_reviewed_at timestamp,
    ADD COLUMN IF NOT EXISTS media_suitability_reviewed_by uuid;

CREATE INDEX IF NOT EXISTS idx_content_items_storage_state
    ON content_items (storage_state);
CREATE INDEX IF NOT EXISTS idx_content_items_storage_recovery_status
    ON content_items (storage_recovery_status);
CREATE INDEX IF NOT EXISTS idx_content_items_storage_deleted_at
    ON content_items (storage_deleted_at);
CREATE INDEX IF NOT EXISTS idx_content_items_media_suitability
    ON content_items (media_suitability);

ALTER TABLE storage_policies
    ADD COLUMN IF NOT EXISTS preset varchar(32) NOT NULL DEFAULT 'balanced';

ALTER TABLE storage_policies
    ALTER COLUMN archive_action SET DEFAULT 're_encode';

CREATE TABLE IF NOT EXISTS media_storage_artifact_events (
    id bigserial PRIMARY KEY,
    public_id uuid NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    tenant_id varchar(64) NOT NULL,
    content_item_id uuid NOT NULL,
    parent_content_item_id uuid,
    event_type varchar(48) NOT NULL,
    status varchar(24) NOT NULL,
    reason text,
    trigger varchar(32),
    source varchar(32),
    storage_tier varchar(16),
    old_storage_tier varchar(16),
    old_media_url text,
    new_media_url text,
    old_size_bytes bigint DEFAULT 0,
    new_size_bytes bigint DEFAULT 0,
    freed_bytes bigint DEFAULT 0,
    deleted_bytes bigint DEFAULT 0,
    quality_profile_id bigint,
    artifact_keys jsonb,
    recovery_payload jsonb,
    error text,
    created_by varchar(255),
    created_at timestamp NOT NULL DEFAULT now(),
    CONSTRAINT media_storage_events_content_item_id_fkey
        FOREIGN KEY (content_item_id) REFERENCES content_items (public_id) ON DELETE CASCADE,
    CONSTRAINT media_storage_events_parent_content_item_id_fkey
        FOREIGN KEY (parent_content_item_id) REFERENCES content_items (public_id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_media_storage_events_tenant_created
    ON media_storage_artifact_events (tenant_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_media_storage_events_content
    ON media_storage_artifact_events (content_item_id);
CREATE INDEX IF NOT EXISTS idx_media_storage_events_parent
    ON media_storage_artifact_events (parent_content_item_id);
CREATE INDEX IF NOT EXISTS idx_media_storage_events_type
    ON media_storage_artifact_events (event_type);
CREATE INDEX IF NOT EXISTS idx_media_storage_events_status
    ON media_storage_artifact_events (status);
