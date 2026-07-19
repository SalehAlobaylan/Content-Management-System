-- `legacy_event_topics` is the archived pre-story-cluster table from the
-- topic → story vocabulary split. Every archived row was reconciled into
-- `stories`; this duplicate retains a large, unused vector index footprint.
--
-- Apply deliberately through the CMS migration workflow. Do not mirror under
-- `supabase/migrations` or execute as a startup schema patch.
DROP TABLE IF EXISTS public.legacy_event_topics;
