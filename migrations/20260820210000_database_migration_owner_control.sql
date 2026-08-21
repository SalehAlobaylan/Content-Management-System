-- Durable CMS owner-side quiescence. This table is deliberately exempt from
-- the application writer fence: only the capability-authenticated migration
-- coordinator route may update it while ordinary writers are sealed.
CREATE TABLE IF NOT EXISTS database_migration_owner_control (
    singleton boolean PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    state varchar(24) NOT NULL DEFAULT 'running' CHECK (state IN ('running', 'quiescing', 'quiesced')),
    migration_program_id uuid,
    fence_epoch bigint NOT NULL DEFAULT 0 CHECK (fence_epoch >= 0),
    changed_at timestamptz NOT NULL DEFAULT now(),
    changed_by varchar(255) NOT NULL DEFAULT 'canonical_migration'
);
INSERT INTO database_migration_owner_control (singleton) VALUES (TRUE) ON CONFLICT (singleton) DO NOTHING;
DROP TRIGGER IF EXISTS wahb_writer_fence_guard ON database_migration_owner_control;
