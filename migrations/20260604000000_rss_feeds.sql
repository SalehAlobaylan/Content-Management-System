-- Saved syndication feeds for the News manager.
--
-- Each row is a named, admin-defined RSS/Atom/JSON feed served publicly at a
-- stable slug (/api/v1/feed/saved/:slug). Filters reference first-class topics;
-- the FK clears (SET NULL) if a topic is dropped by a full re-cluster.

CREATE TABLE IF NOT EXISTS rss_feeds (
    id           SERIAL PRIMARY KEY,
    public_id    UUID NOT NULL DEFAULT gen_random_uuid(),
    tenant_id    VARCHAR(64) NOT NULL DEFAULT 'default',
    slug         VARCHAR(160) NOT NULL,
    name         VARCHAR(200) NOT NULL,
    title        TEXT,
    description  TEXT,
    topic_id     UUID,
    content_type VARCHAR(20),
    item_limit   INTEGER NOT NULL DEFAULT 50,
    enabled      BOOLEAN NOT NULL DEFAULT true,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_rss_feeds_public_id ON rss_feeds (public_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_rss_feeds_tenant_slug ON rss_feeds (tenant_id, slug);
CREATE INDEX IF NOT EXISTS idx_rss_feeds_tenant ON rss_feeds (tenant_id);
CREATE INDEX IF NOT EXISTS idx_rss_feeds_topic_id ON rss_feeds (topic_id);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'rss_feeds_topic_id_fkey'
    ) THEN
        ALTER TABLE rss_feeds
            ADD CONSTRAINT rss_feeds_topic_id_fkey
            FOREIGN KEY (topic_id) REFERENCES topics(public_id) ON DELETE SET NULL;
    END IF;
END $$;
