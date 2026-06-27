-- Repair databases that received media_atomization_runs.public_id as a
-- PostgreSQL unique constraint instead of the GORM-compatible unique index.

DO $$
BEGIN
    IF to_regclass('media_atomization_runs') IS NULL THEN
        RETURN;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'media_atomization_runs'::regclass
          AND conname = 'idx_media_atomization_runs_public_id'
    ) THEN
        ALTER TABLE media_atomization_runs DROP CONSTRAINT idx_media_atomization_runs_public_id;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'media_atomization_runs'::regclass
          AND conname = 'uni_media_atomization_runs_public_id'
    ) THEN
        ALTER TABLE media_atomization_runs DROP CONSTRAINT uni_media_atomization_runs_public_id;
    END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS idx_media_atomization_runs_public_id
    ON media_atomization_runs (public_id);
