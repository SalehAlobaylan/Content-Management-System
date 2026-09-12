-- Recovery records an independent clean observation before marking an episode
-- resolved. Keep the database ledger vocabulary aligned with that producer.
ALTER TABLE media_supply_episode_events
  DROP CONSTRAINT media_supply_episode_events_event_type_check;

ALTER TABLE media_supply_episode_events
  ADD CONSTRAINT media_supply_episode_events_event_type_check
  CHECK (event_type IN ('opened', 'observed', 'recovering', 'resolved', 'resolution_clean'));
