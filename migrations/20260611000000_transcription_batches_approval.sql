-- Durable transcription batches, advisory transcript approval, and richer STT job state.

ALTER TABLE transcription_jobs
    ADD COLUMN IF NOT EXISTS batch_id uuid,
    ADD COLUMN IF NOT EXISTS batch_item_id uuid,
    ADD COLUMN IF NOT EXISTS media_job_id varchar(128),
    ADD COLUMN IF NOT EXISTS writeback_status varchar(32),
    ADD COLUMN IF NOT EXISTS writeback_error text,
    ADD COLUMN IF NOT EXISTS provider_error_code varchar(64),
    ADD COLUMN IF NOT EXISTS canceled boolean DEFAULT false;

CREATE INDEX IF NOT EXISTS idx_transcription_jobs_batch ON transcription_jobs (batch_id);
CREATE INDEX IF NOT EXISTS idx_transcription_jobs_batch_item ON transcription_jobs (batch_item_id);
CREATE INDEX IF NOT EXISTS idx_transcription_jobs_media_job ON transcription_jobs (media_job_id);
CREATE INDEX IF NOT EXISTS idx_transcription_jobs_canceled ON transcription_jobs (canceled);

ALTER TABLE transcripts
    ADD COLUMN IF NOT EXISTS approved_at timestamp,
    ADD COLUMN IF NOT EXISTS approved_by varchar(255),
    ADD COLUMN IF NOT EXISTS approval_reason text;

ALTER TABLE transcript_versions
    ADD COLUMN IF NOT EXISTS actor varchar(255),
    ADD COLUMN IF NOT EXISTS embeddings_regenerated boolean DEFAULT false,
    ADD COLUMN IF NOT EXISTS approved_at timestamp,
    ADD COLUMN IF NOT EXISTS approved_by varchar(255),
    ADD COLUMN IF NOT EXISTS approval_reason text;

CREATE TABLE IF NOT EXISTS transcription_batches (
    id bigserial PRIMARY KEY,
    public_id uuid NOT NULL DEFAULT gen_random_uuid(),
    tenant_id varchar(64) NOT NULL DEFAULT 'default',
    status varchar(24) NOT NULL,
    force boolean DEFAULT true,
    actor varchar(255),
    total_count integer DEFAULT 0,
    accepted_count integer DEFAULT 0,
    skipped_count integer DEFAULT 0,
    failed_count integer DEFAULT 0,
    canceled_count integer DEFAULT 0,
    completed_count integer DEFAULT 0,
    latest_error text,
    metadata jsonb,
    canceled_at timestamp,
    completed_at timestamp,
    created_at timestamp DEFAULT now(),
    updated_at timestamp DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_transcription_batches_public_id ON transcription_batches (public_id);
CREATE INDEX IF NOT EXISTS idx_transcription_batches_tenant ON transcription_batches (tenant_id);
CREATE INDEX IF NOT EXISTS idx_transcription_batches_status ON transcription_batches (status);
CREATE INDEX IF NOT EXISTS idx_transcription_batches_created ON transcription_batches (created_at);

CREATE TABLE IF NOT EXISTS transcription_batch_items (
    id bigserial PRIMARY KEY,
    public_id uuid NOT NULL DEFAULT gen_random_uuid(),
    tenant_id varchar(64) NOT NULL DEFAULT 'default',
    batch_id uuid NOT NULL,
    content_item_id uuid NOT NULL,
    job_id uuid,
    status varchar(24) NOT NULL,
    reason text,
    error text,
    created_at timestamp DEFAULT now(),
    updated_at timestamp DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_transcription_batch_items_public_id ON transcription_batch_items (public_id);
CREATE INDEX IF NOT EXISTS idx_transcription_batch_items_tenant ON transcription_batch_items (tenant_id);
CREATE INDEX IF NOT EXISTS idx_transcription_batch_items_batch ON transcription_batch_items (batch_id);
CREATE INDEX IF NOT EXISTS idx_transcription_batch_items_content ON transcription_batch_items (content_item_id);
CREATE INDEX IF NOT EXISTS idx_transcription_batch_items_job ON transcription_batch_items (job_id);
CREATE INDEX IF NOT EXISTS idx_transcription_batch_items_status ON transcription_batch_items (status);
CREATE INDEX IF NOT EXISTS idx_transcription_batch_items_created ON transcription_batch_items (created_at);
