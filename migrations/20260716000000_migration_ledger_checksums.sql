ALTER TABLE cms_schema_migrations
    ADD COLUMN IF NOT EXISTS checksum_sha256 VARCHAR(64),
    ADD COLUMN IF NOT EXISTS execution_mode VARCHAR(32) NOT NULL DEFAULT 'legacy';

ALTER TABLE cms_schema_migrations
    ADD CONSTRAINT cms_schema_migrations_checksum_length
    CHECK (checksum_sha256 IS NULL OR char_length(checksum_sha256) = 64) NOT VALID;
