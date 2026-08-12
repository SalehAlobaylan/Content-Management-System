-- Provision generation authority for tenants that existed before generation
-- membership became the serving boundary. The public default tenant is
-- included even when it currently has no content, so an empty feed remains a
-- valid empty feed rather than an infrastructure failure.
DO $$
DECLARE
    scope RECORD;
    generation_id UUID;
BEGIN
    FOR scope IN
        SELECT tenants.tenant_id, lanes.lane
        FROM (
            SELECT DISTINCT tenant_id
            FROM content_items
            WHERE tenant_id IS NOT NULL AND btrim(tenant_id) <> ''
            UNION
            SELECT 'default'::VARCHAR(64)
        ) AS tenants
        CROSS JOIN (VALUES ('news'::VARCHAR(16)), ('media'::VARCHAR(16))) AS lanes(lane)
    LOOP
        SELECT active_generation_id
        INTO generation_id
        FROM feed_generation_heads
        WHERE tenant_id = scope.tenant_id AND lane = scope.lane;

        IF generation_id IS NULL THEN
            INSERT INTO feed_generations (
                tenant_id,
                lane,
                state,
                build_watermark,
                caught_up_at,
                cutover_at,
                verification
            )
            VALUES (
                scope.tenant_id,
                scope.lane,
                'active',
                NOW(),
                NOW(),
                NOW(),
                jsonb_build_object(
                    'foundation', TRUE,
                    'source', '20260812100000_bootstrap_feed_generation_authority'
                )
            )
            RETURNING public_id INTO generation_id;

            INSERT INTO feed_generation_heads (
                tenant_id,
                lane,
                active_generation_id,
                generation,
                updated_at
            )
            VALUES (scope.tenant_id, scope.lane, generation_id, 1, NOW())
            ON CONFLICT (tenant_id, lane) DO UPDATE
            SET active_generation_id = EXCLUDED.active_generation_id,
                generation = GREATEST(feed_generation_heads.generation, 1),
                updated_at = NOW()
            WHERE feed_generation_heads.active_generation_id IS NULL;
        END IF;

        IF scope.lane = 'news' THEN
            INSERT INTO feed_generation_memberships (
                generation_id,
                member_type,
                member_id
            )
            SELECT generation_id, 'story', story_id
            FROM content_items
            WHERE tenant_id = scope.tenant_id
              AND type = 'NEWS'
              AND status = 'READY'
              AND story_id IS NOT NULL
            GROUP BY story_id
            ON CONFLICT DO NOTHING;
        ELSE
            INSERT INTO feed_generation_memberships (
                generation_id,
                member_type,
                member_id
            )
            SELECT generation_id, 'feed_unit', public_id
            FROM content_items
            WHERE tenant_id = scope.tenant_id
              AND type IN ('VIDEO', 'PODCAST')
              AND status = 'READY'
              AND is_feed_unit = TRUE
              AND feed_visibility = 'visible'
            ON CONFLICT DO NOTHING;
        END IF;
    END LOOP;
END $$;
