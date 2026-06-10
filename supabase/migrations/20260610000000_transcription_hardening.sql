-- Transcription hardening: lifecycle jobs, quality scoring, and STT replacement versions.

ALTER TABLE transcription_configs
    ADD COLUMN IF NOT EXISTS monthly_reserved_usd double precision DEFAULT 0,
    ADD COLUMN IF NOT EXISTS auto_repair_enabled boolean DEFAULT true,
    ADD COLUMN IF NOT EXISTS quality_review_threshold double precision DEFAULT 0.75,
    ADD COLUMN IF NOT EXISTS quality_auto_repair_threshold double precision DEFAULT 0.45;

CREATE TABLE IF NOT EXISTS transcription_jobs (
    id bigserial PRIMARY KEY,
    public_id uuid NOT NULL DEFAULT gen_random_uuid(),
    tenant_id varchar(64) NOT NULL DEFAULT 'default',
    content_item_id uuid NOT NULL,
    transcript_id uuid,
    trigger_source varchar(32) NOT NULL,
    status varchar(24) NOT NULL,
    provider varchar(64),
    model varchar(128),
    language varchar(16),
    skip_reason text,
    error_message text,
    retry_count integer DEFAULT 0,
    estimated_cost_usd double precision DEFAULT 0,
    reserved_cost_usd double precision DEFAULT 0,
    actual_cost_usd double precision DEFAULT 0,
    started_at timestamp,
    completed_at timestamp,
    metadata jsonb,
    created_at timestamp DEFAULT now(),
    updated_at timestamp DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_transcription_jobs_public_id ON transcription_jobs (public_id);
CREATE INDEX IF NOT EXISTS idx_transcription_jobs_tenant_id ON transcription_jobs (tenant_id);
CREATE INDEX IF NOT EXISTS idx_transcription_jobs_content ON transcription_jobs (content_item_id);
CREATE INDEX IF NOT EXISTS idx_transcription_jobs_transcript ON transcription_jobs (transcript_id);
CREATE INDEX IF NOT EXISTS idx_transcription_jobs_status ON transcription_jobs (status);
CREATE INDEX IF NOT EXISTS idx_transcription_jobs_trigger ON transcription_jobs (trigger_source);
CREATE INDEX IF NOT EXISTS idx_transcription_jobs_created_at ON transcription_jobs (created_at);

CREATE TABLE IF NOT EXISTS transcript_quality (
    id bigserial PRIMARY KEY,
    public_id uuid NOT NULL DEFAULT gen_random_uuid(),
    tenant_id varchar(64) NOT NULL DEFAULT 'default',
    content_item_id uuid NOT NULL,
    transcript_id uuid NOT NULL,
    score double precision NOT NULL DEFAULT 1,
    status varchar(24) NOT NULL,
    issue_codes text[],
    details jsonb,
    computed_at timestamp NOT NULL DEFAULT now(),
    created_at timestamp DEFAULT now(),
    updated_at timestamp DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_transcript_quality_public_id ON transcript_quality (public_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_transcript_quality_content ON transcript_quality (content_item_id);
CREATE INDEX IF NOT EXISTS idx_transcript_quality_tenant ON transcript_quality (tenant_id);
CREATE INDEX IF NOT EXISTS idx_transcript_quality_transcript ON transcript_quality (transcript_id);
CREATE INDEX IF NOT EXISTS idx_transcript_quality_status ON transcript_quality (status);
CREATE INDEX IF NOT EXISTS idx_transcript_quality_computed ON transcript_quality (computed_at);

CREATE TABLE IF NOT EXISTS transcript_versions (
    id bigserial PRIMARY KEY,
    public_id uuid NOT NULL DEFAULT gen_random_uuid(),
    tenant_id varchar(64) NOT NULL DEFAULT 'default',
    content_item_id uuid NOT NULL,
    transcript_id uuid NOT NULL,
    full_text text NOT NULL,
    summary text,
    word_timestamps jsonb,
    segments jsonb,
    chapters jsonb,
    language varchar(10),
    source varchar(32),
    provider varchar(64),
    checksum varchar(64) NOT NULL,
    reason varchar(64) NOT NULL DEFAULT 'stt_replacement',
    created_at timestamp DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_transcript_versions_public_id ON transcript_versions (public_id);
CREATE INDEX IF NOT EXISTS idx_transcript_versions_tenant ON transcript_versions (tenant_id);
CREATE INDEX IF NOT EXISTS idx_transcript_versions_content ON transcript_versions (content_item_id);
CREATE INDEX IF NOT EXISTS idx_transcript_versions_transcript ON transcript_versions (transcript_id);
CREATE INDEX IF NOT EXISTS idx_transcript_versions_checksum ON transcript_versions (checksum);
CREATE INDEX IF NOT EXISTS idx_transcript_versions_created ON transcript_versions (created_at);
