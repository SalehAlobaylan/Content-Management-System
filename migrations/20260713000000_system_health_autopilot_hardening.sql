-- System Health incident identity and containment defaults.
CREATE UNIQUE INDEX IF NOT EXISTS idx_system_incident_episodes_active_identity
    ON system_incident_episodes (root_service, verdict)
    WHERE status IN ('open', 'recovering');

UPDATE system_autopilot_policies
SET containment_disabled_for = (
    SELECT jsonb_agg(value ORDER BY value)
    FROM (
        SELECT DISTINCT value
        FROM jsonb_array_elements_text(
            CASE
                WHEN jsonb_typeof(system_autopilot_policies.containment_disabled_for) = 'array'
                    THEN system_autopilot_policies.containment_disabled_for
                ELSE '[]'::jsonb
            END
        )
        UNION
        SELECT unnest(ARRAY['embedding_lifecycle', 'media_circulation', 'media_studio', 'news_circulation', 'redundancy'])
    ) AS containment_values(value)
)
WHERE scope = 'platform';
