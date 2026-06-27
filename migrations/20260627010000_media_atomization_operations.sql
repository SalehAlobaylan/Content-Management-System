-- Media Atomization operations: dashboard run state and embedding-pending feed visibility.

CREATE TABLE IF NOT EXISTS media_atomization_runs (
    id bigserial PRIMARY KEY,
    public_id uuid NOT NULL DEFAULT gen_random_uuid(),
    tenant_id varchar(64) NOT NULL,
    parent_content_item_id uuid NOT NULL,
    status varchar(24) NOT NULL,
    phase varchar(32) NOT NULL,
    child_count integer NOT NULL DEFAULT 0,
    review_count integer NOT NULL DEFAULT 0,
    error_message text,
    started_at timestamp,
    completed_at timestamp,
    created_at timestamp NOT NULL DEFAULT now(),
    updated_at timestamp NOT NULL DEFAULT now(),
    CONSTRAINT media_atomization_runs_parent_content_item_id_fkey
        FOREIGN KEY (parent_content_item_id) REFERENCES content_items (public_id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_media_atomization_runs_public_id ON media_atomization_runs (public_id);
CREATE INDEX IF NOT EXISTS idx_media_atomization_runs_tenant ON media_atomization_runs (tenant_id);
CREATE INDEX IF NOT EXISTS idx_media_atomization_runs_parent ON media_atomization_runs (parent_content_item_id);
CREATE INDEX IF NOT EXISTS idx_media_atomization_runs_status ON media_atomization_runs (status);
CREATE INDEX IF NOT EXISTS idx_media_atomization_runs_phase ON media_atomization_runs (phase);
CREATE INDEX IF NOT EXISTS idx_media_atomization_runs_started_at ON media_atomization_runs (started_at);
