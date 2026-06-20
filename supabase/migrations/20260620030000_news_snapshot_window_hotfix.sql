-- Hotfix for production databases that missed the circulation snapshot window
-- column while the application already writes tenant + window cache rows.

ALTER TABLE news_snapshots
    ADD COLUMN IF NOT EXISTS window VARCHAR(16) DEFAULT 'today';

UPDATE news_snapshots
SET window = 'today'
WHERE window IS NULL OR window = '';

ALTER TABLE news_snapshots
    ALTER COLUMN window SET DEFAULT 'today',
    ALTER COLUMN window SET NOT NULL;

DROP INDEX IF EXISTS idx_news_snapshot_tenant;

CREATE UNIQUE INDEX IF NOT EXISTS idx_news_snapshot_tenant_window
    ON news_snapshots (tenant_id, window);
