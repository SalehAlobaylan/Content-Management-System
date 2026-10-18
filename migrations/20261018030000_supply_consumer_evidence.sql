-- Capture consumer proof at the generation actually frozen by CMS. Historical
-- sessions/events remain uncorrelated; never backfill them from the current head.
ALTER TABLE consumer_feed_sessions ADD COLUMN IF NOT EXISTS active_generation_id UUID;
ALTER TABLE experience_events ADD COLUMN IF NOT EXISTS feed_session_id UUID;
ALTER TABLE pods_boundary_observations ADD COLUMN IF NOT EXISTS feed_session_id UUID;
ALTER TABLE media_supply_episode_events DROP CONSTRAINT media_supply_episode_events_event_type_check;
ALTER TABLE media_supply_episode_events ADD CONSTRAINT media_supply_episode_events_event_type_check
  CHECK (event_type IN ('opened','observed','recovering','resolved','resolution_clean','resolution_not_clean'));
CREATE INDEX IF NOT EXISTS pods_boundary_recent_session_idx
  ON pods_boundary_observations (tenant_id, generation_id, boundary, observed_at DESC)
  WHERE feed_session_id IS NOT NULL;
