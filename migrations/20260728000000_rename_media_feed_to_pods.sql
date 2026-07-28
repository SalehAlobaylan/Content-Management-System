-- Canonical media-feed vocabulary: Pods. This upgrades databases that applied
-- earlier migrations before the terminology changed.

DO $$
DECLARE
    change_record RECORD;
BEGIN
    FOR change_record IN
        SELECT * FROM (VALUES
            ('preference_settings', 'foryou_enabled', 'pods_enabled'),
            ('preference_settings', 'w_foryou', 'w_pods'),
            ('preference_autopilot_policies', 'coverage_floor_foryou_pct', 'coverage_floor_pods_pct'),
            ('feed_integrity_policies', 'foryou_latency_budget_ms', 'pods_latency_budget_ms'),
            ('feed_integrity_policies', 'expected_min_foryou_units', 'expected_min_pods_units'),
            ('ranking_configs', 'foryou_completed_repeat_days', 'pods_completed_repeat_days'),
            ('ranking_configs', 'foryou_meaningful_repeat_days', 'pods_meaningful_repeat_days'),
            ('ranking_configs', 'foryou_sample_repeat_days', 'pods_sample_repeat_days')
        ) AS changes(table_name, old_column, new_column)
    LOOP
        IF EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = change_record.table_name
              AND column_name = change_record.old_column
        ) AND NOT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = change_record.table_name
              AND column_name = change_record.new_column
        ) THEN
            EXECUTE format(
                'ALTER TABLE %I RENAME COLUMN %I TO %I',
                change_record.table_name,
                change_record.old_column,
                change_record.new_column
            );
        END IF;
    END LOOP;
END $$;

ALTER TABLE consumer_feed_sessions
    DROP CONSTRAINT IF EXISTS consumer_feed_sessions_feed_type_check;
UPDATE consumer_feed_sessions SET feed_type = 'pods' WHERE feed_type = 'foryou';
ALTER TABLE consumer_feed_sessions
    ADD CONSTRAINT consumer_feed_sessions_feed_type_check CHECK (feed_type IN ('pods'));

UPDATE feed_integrity_findings SET feed = 'pods' WHERE feed = 'foryou';
UPDATE feed_integrity_episodes SET feed = 'pods' WHERE feed = 'foryou';
UPDATE feed_integrity_suppressions SET feed = 'pods' WHERE feed = 'foryou';
UPDATE feed_integrity_runs
SET feed_results = jsonb_set(feed_results - 'foryou', '{pods}', feed_results -> 'foryou', true)
WHERE feed_results ? 'foryou';

UPDATE experience_events SET surface = 'pods' WHERE surface = 'foryou';
UPDATE experience_metric_rollups SET surface = 'pods' WHERE surface = 'foryou';
UPDATE experience_incidents
SET surface = 'pods',
    fingerprint = replace(fingerprint, 'foryou', 'pods')
WHERE surface = 'foryou';
UPDATE experience_actions SET surface = 'pods' WHERE surface = 'foryou';
UPDATE experience_suppressions SET surface = 'pods' WHERE surface = 'foryou';
UPDATE experience_policies
SET enabled_surfaces = replace(enabled_surfaces, 'foryou', 'pods')
WHERE enabled_surfaces LIKE '%foryou%';
UPDATE experience_evaluation_runs
SET surface_verdicts = jsonb_set(
    surface_verdicts - 'foryou',
    '{pods}',
    surface_verdicts -> 'foryou',
    true
)
WHERE surface_verdicts ? 'foryou';
