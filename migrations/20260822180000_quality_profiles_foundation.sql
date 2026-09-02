-- Canonical ingest quality-profile foundation. Older databases received this
-- table from GORM AutoMigrate; fresh canonical replays need it before the
-- audio-first delivery contract extends and references it.

CREATE TABLE IF NOT EXISTS quality_profiles (
    id BIGSERIAL PRIMARY KEY,
    tenant_id VARCHAR(64),
    source_type VARCHAR(20),
    name VARCHAR(64) NOT NULL,
    description TEXT,
    video_codec VARCHAR(16) NOT NULL DEFAULT 'h264',
    max_height INTEGER NOT NULL DEFAULT 720,
    target_bitrate_kbps INTEGER NOT NULL DEFAULT 0,
    crf INTEGER NOT NULL DEFAULT 23,
    preset VARCHAR(16) NOT NULL DEFAULT 'fast',
    audio_codec VARCHAR(16) NOT NULL DEFAULT 'aac',
    audio_bitrate_kbps INTEGER NOT NULL DEFAULT 128,
    output_container VARCHAR(8) NOT NULL DEFAULT 'mp4',
    thumbnail_offset_seconds INTEGER NOT NULL DEFAULT 2,
    thumbnail_max_height INTEGER NOT NULL DEFAULT 360,
    allowed_input_mime_types TEXT[],
    max_input_size_bytes BIGINT,
    max_input_duration_sec INTEGER,
    preset_key VARCHAR(32) NOT NULL DEFAULT '',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_quality_profile_scope
    ON quality_profiles (tenant_id, source_type);
