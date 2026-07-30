-- Repair historical baseline drift where the redundancy hygiene migration was
-- adopted before its fingerprint table had actually been created.
CREATE TABLE IF NOT EXISTS redundancy_fingerprints (
  id BIGSERIAL PRIMARY KEY,
  tenant_id VARCHAR(64) NOT NULL DEFAULT 'default',
  content_item_id UUID NOT NULL REFERENCES content_items(public_id) ON DELETE CASCADE,
  transcript_checksum VARCHAR(64) NOT NULL,
  body_hash VARCHAR(64) NOT NULL,
  shingle_count INTEGER NOT NULL DEFAULT 0,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
  UNIQUE(tenant_id, content_item_id, transcript_checksum)
);

CREATE INDEX IF NOT EXISTS idx_redundancy_fingerprint_item
  ON redundancy_fingerprints(tenant_id, content_item_id, updated_at DESC);
