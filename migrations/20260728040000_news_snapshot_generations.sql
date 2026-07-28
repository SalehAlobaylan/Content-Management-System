-- A generation head is the cross-replica invalidation contract for News
-- snapshots. A snapshot from an earlier generation is never served.

CREATE TABLE IF NOT EXISTS news_snapshot_generations (
    tenant_id VARCHAR(64) NOT NULL,
    "window" VARCHAR(16) NOT NULL,
    generation BIGINT NOT NULL DEFAULT 1 CHECK (generation > 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, "window")
);

INSERT INTO news_snapshot_generations (tenant_id, "window", generation)
SELECT tenant_id, "window", 1
FROM news_snapshots
ON CONFLICT (tenant_id, "window") DO NOTHING;

ALTER TABLE news_snapshots
    ADD COLUMN IF NOT EXISTS generation BIGINT NOT NULL DEFAULT 1 CHECK (generation > 0);

UPDATE news_snapshots snapshot
SET generation = head.generation
FROM news_snapshot_generations head
WHERE head.tenant_id = snapshot.tenant_id
  AND head."window" = snapshot."window"
  AND snapshot.generation <> head.generation;
