-- Live news feed: the News feed is assembled LIVE from current story state
-- ("write-time intelligence, read-time freshness" — see docs/PRD.md). The
-- snapshot becomes a freshness-bounded read-through cache, invalidated the
-- moment a story gains a member; related-story order moves to write time.
-- (Dev gets the columns via GORM AutoMigrate; this migration is for prod.)

-- Cache invalidation flag — set by classification when a story gains a member.
ALTER TABLE news_snapshots ADD COLUMN IF NOT EXISTS dirty boolean DEFAULT false;

-- Write-time-computed ordered related-story ids (JSON array of UUID strings),
-- cross-encoder reranked when news_rerank_enabled.
ALTER TABLE topics ADD COLUMN IF NOT EXISTS related_ids jsonb;

-- Live serving is the product default; legacy modes fold into it.
-- 'cached_only' remains as the emergency escape hatch.
ALTER TABLE ranking_configs ALTER COLUMN news_feed_mode SET DEFAULT 'live';
UPDATE ranking_configs SET news_feed_mode = 'live'
WHERE news_feed_mode IN ('precompute', 'on_demand');
