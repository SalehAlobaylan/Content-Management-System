-- Close concurrent transcript-link and editorial-protection races at the DB
-- boundary. The reset continues to retain stable transcript identities; only
-- exclusively owned transcript payload is cleared during finalization.
ALTER TABLE transcripts
  ADD COLUMN IF NOT EXISTS retired_payload_at TIMESTAMPTZ;

CREATE OR REPLACE FUNCTION enforce_pods_reset_transcript_link_fence()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  payload_retired_at TIMESTAMPTZ;
  owner_retired_at TIMESTAMPTZ;
BEGIN
  IF NEW.transcript_id IS NULL THEN
    RETURN NEW;
  END IF;
  IF TG_OP = 'UPDATE' AND NEW.transcript_id IS NOT DISTINCT FROM OLD.transcript_id THEN
    RETURN NEW;
  END IF;

  SELECT tr.retired_payload_at, owner.retired_payload_at
    INTO payload_retired_at, owner_retired_at
    FROM transcripts tr
    JOIN content_items owner ON owner.public_id = tr.content_item_id
   WHERE tr.public_id = NEW.transcript_id
     AND owner.tenant_id = NEW.tenant_id
   FOR UPDATE OF tr;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'transcript link target is unavailable for this tenant';
  END IF;
  IF payload_retired_at IS NOT NULL THEN
    RAISE EXCEPTION 'transcript payload is permanently retired';
  END IF;
  IF owner_retired_at IS NOT NULL THEN
    RAISE EXCEPTION 'transcript owner identity is permanently retired';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS pods_reset_transcript_link_fence ON content_items;
CREATE TRIGGER pods_reset_transcript_link_fence
BEFORE INSERT OR UPDATE OF transcript_id ON content_items
FOR EACH ROW EXECUTE FUNCTION enforce_pods_reset_transcript_link_fence();

-- Item/family circulation protections are polymorphic rather than foreign-key
-- relationships. Serialize their creation with retirement on the subject row.
CREATE OR REPLACE FUNCTION enforce_pods_reset_circulation_override_fence()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.subject_kind NOT IN ('item', 'family')
     OR NEW.override_type NOT IN ('editorial_hold', 'never_archive', 'keep_latest_n_hot')
     OR (NEW.expires_at IS NOT NULL AND NEW.expires_at <= NOW()) THEN
    RETURN NEW;
  END IF;

  PERFORM 1
    FROM content_items c
   WHERE c.tenant_id = NEW.tenant_id AND c.public_id = NEW.subject_id
   FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'circulation protection subject is unavailable';
  END IF;
  IF EXISTS (
    SELECT 1 FROM pods_reset_retirements r
     WHERE r.tenant_id = NEW.tenant_id
       AND r.content_item_id = NEW.subject_id
       AND r.state IN ('retiring', 'retired')
  ) THEN
    RAISE EXCEPTION 'circulation protection cannot be added to retired content';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS pods_reset_circulation_override_fence ON media_circulation_overrides;
CREATE TRIGGER pods_reset_circulation_override_fence
BEFORE INSERT OR UPDATE ON media_circulation_overrides
FOR EACH ROW EXECUTE FUNCTION enforce_pods_reset_circulation_override_fence();

-- Item-family recommendations are actionable until their owner records an
-- explicit terminal disposition. Their subject is polymorphic, so serialize
-- actionable inserts/updates against the selected content identity.
CREATE OR REPLACE FUNCTION enforce_pods_reset_circulation_recommendation_fence()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.unit_type <> 'item_family'
     OR NEW.status IN ('applied', 'dismissed', 'superseded') THEN
    RETURN NEW;
  END IF;

  PERFORM 1 FROM content_items c
   WHERE c.tenant_id=NEW.tenant_id AND c.public_id=NEW.subject_id
   FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'circulation recommendation content subject is unavailable';
  END IF;
  IF EXISTS (
    SELECT 1 FROM pods_reset_retirements r
     WHERE r.tenant_id=NEW.tenant_id AND r.content_item_id=NEW.subject_id
       AND r.state IN ('retiring','retired')
  ) THEN
    RAISE EXCEPTION 'circulation recommendation rejected for permanently retired content';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS pods_reset_circulation_recommendation_fence ON media_circulation_recommendations;
CREATE TRIGGER pods_reset_circulation_recommendation_fence
BEFORE INSERT OR UPDATE ON media_circulation_recommendations
FOR EACH ROW EXECUTE FUNCTION enforce_pods_reset_circulation_recommendation_fence();

-- An active, unexpired supply preview may be approved into a durable request.
-- Fence preview creation/resurrection so it cannot race the reset's final
-- blocker check and retirement row lock.
CREATE OR REPLACE FUNCTION enforce_pods_reset_supply_preview_fence()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.target_type <> 'content_item'
     OR NEW.state <> 'active'
     OR NEW.expires_at <= NOW() THEN
    RETURN NEW;
  END IF;

  PERFORM 1 FROM content_items c
   WHERE c.tenant_id=NEW.tenant_id AND c.public_id=NEW.target_id
   FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'supply preview content target is unavailable';
  END IF;
  IF EXISTS (
    SELECT 1 FROM pods_reset_retirements r
     WHERE r.tenant_id=NEW.tenant_id AND r.content_item_id=NEW.target_id
       AND r.state IN ('retiring','retired')
  ) THEN
    RAISE EXCEPTION 'supply preview rejected for permanently retired content';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS pods_reset_supply_preview_fence ON media_supply_action_previews;
CREATE TRIGGER pods_reset_supply_preview_fence
BEFORE INSERT OR UPDATE ON media_supply_action_previews
FOR EACH ROW EXECUTE FUNCTION enforce_pods_reset_supply_preview_fence();

-- Content and story-level retention holds are also polymorphic. Lock every
-- affected content row so a new hold either blocks reset preflight or observes
-- that the retiring item has already detached from the story.
CREATE OR REPLACE FUNCTION enforce_pods_reset_retention_hold_fence()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.released_at IS NOT NULL
     OR (NEW.expires_at IS NOT NULL AND NEW.expires_at <= NOW()) THEN
    RETURN NEW;
  END IF;

  IF NEW.target_type = 'content' THEN
    PERFORM 1
      FROM content_items c
     WHERE c.tenant_id = NEW.tenant_id AND c.public_id = NEW.target_id
     FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'retention hold content target is unavailable';
    END IF;
    IF EXISTS (
      SELECT 1 FROM pods_reset_retirements r
       WHERE r.tenant_id = NEW.tenant_id
         AND r.content_item_id = NEW.target_id
         AND r.state IN ('retiring', 'retired')
    ) THEN
      RAISE EXCEPTION 'retention hold cannot be added to retired content';
    END IF;
  ELSIF NEW.target_type = 'story' THEN
    PERFORM 1
      FROM content_items c
     WHERE c.tenant_id = NEW.tenant_id AND c.story_id = NEW.target_id
     ORDER BY c.public_id
     FOR UPDATE;
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS pods_reset_retention_hold_fence ON retention_holds;
CREATE TRIGGER pods_reset_retention_hold_fence
BEFORE INSERT OR UPDATE ON retention_holds
FOR EACH ROW EXECUTE FUNCTION enforce_pods_reset_retention_hold_fence();

-- Feed memberships use member_type/member_id rather than an FK. An old media
-- reconciler must not attach a retired item after the reset detached it.
CREATE OR REPLACE FUNCTION enforce_pods_reset_feed_membership_fence()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.member_type <> 'feed_unit' THEN
    RETURN NEW;
  END IF;

  PERFORM 1 FROM content_items c WHERE c.public_id=NEW.member_id FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'feed membership content identity is unavailable';
  END IF;
  IF EXISTS (
    SELECT 1 FROM pods_reset_retirements r
     WHERE r.content_item_id=NEW.member_id
       AND r.state IN ('retiring','retired')
  ) THEN
    RAISE EXCEPTION 'feed membership cannot reference retired content';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS pods_reset_feed_membership_fence ON feed_generation_memberships;
CREATE TRIGGER pods_reset_feed_membership_fence
BEFORE INSERT OR UPDATE ON feed_generation_memberships
FOR EACH ROW EXECUTE FUNCTION enforce_pods_reset_feed_membership_fence();

-- Media recovery plans retain immutable target lists, but an unexpired plan or
-- unresolved run must not keep authority over an item being retired.
CREATE OR REPLACE FUNCTION enforce_pods_reset_feed_recovery_target_fence()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.target_type <> 'media_content' THEN
    RETURN NEW;
  END IF;

  PERFORM 1 FROM content_items c
   WHERE c.tenant_id=NEW.tenant_id AND c.public_id=NEW.target_id
   FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'feed recovery content target is unavailable';
  END IF;
  IF EXISTS (
    SELECT 1 FROM pods_reset_retirements r
     WHERE r.tenant_id=NEW.tenant_id AND r.content_item_id=NEW.target_id
       AND r.state IN ('retiring','retired')
  ) THEN
    RAISE EXCEPTION 'feed recovery target rejected for permanently retired content';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS pods_reset_feed_recovery_target_fence ON feed_recovery_plan_targets;
CREATE TRIGGER pods_reset_feed_recovery_target_fence
BEFORE INSERT OR UPDATE ON feed_recovery_plan_targets
FOR EACH ROW EXECUTE FUNCTION enforce_pods_reset_feed_recovery_target_fence();

-- These append-only ledgers may retain historical identity links, but late
-- producers cannot append fresh rows for an identity once retirement wins.
DROP TRIGGER IF EXISTS pods_reset_fence_experience_events ON experience_events;
CREATE TRIGGER pods_reset_fence_experience_events
BEFORE INSERT OR UPDATE ON experience_events
FOR EACH ROW EXECUTE FUNCTION enforce_pods_reset_retirement_fence('content_id');

DROP TRIGGER IF EXISTS pods_reset_fence_enrichment_autopilot_actions ON enrichment_autopilot_actions;
CREATE TRIGGER pods_reset_fence_enrichment_autopilot_actions
BEFORE INSERT OR UPDATE ON enrichment_autopilot_actions
FOR EACH ROW EXECUTE FUNCTION enforce_pods_reset_retirement_fence('content_id');

DROP TRIGGER IF EXISTS pods_reset_fence_news_month_archive_story_sources ON news_month_archive_story_sources;
CREATE TRIGGER pods_reset_fence_news_month_archive_story_sources
BEFORE INSERT OR UPDATE ON news_month_archive_story_sources
FOR EACH ROW EXECUTE FUNCTION enforce_pods_reset_retirement_fence('original_content_id');

DROP TRIGGER IF EXISTS pods_reset_fence_news_ingest_tombstones ON news_ingest_tombstones;
CREATE TRIGGER pods_reset_fence_news_ingest_tombstones
BEFORE INSERT OR UPDATE ON news_ingest_tombstones
FOR EACH ROW EXECUTE FUNCTION enforce_pods_reset_retirement_fence('original_content_id');

-- Retention compaction stores exact item IDs in JSON arrays rather than
-- relational columns. Treat those arrays as real relationships: lock all
-- extant targets in UUID order and reject newly actionable scopes containing
-- a permanently retired identity.
CREATE OR REPLACE FUNCTION enforce_pods_reset_retention_batch_fence()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  target_id UUID;
BEGIN
  IF NEW.state IN ('verification_passed', 'blocked') THEN
    RETURN NEW;
  END IF;

  IF jsonb_typeof(NEW.target_ids) <> 'array' THEN
    RAISE EXCEPTION 'retention batch target scope must be a JSON array';
  END IF;
  FOR target_id IN
    SELECT DISTINCT value::uuid
      FROM jsonb_array_elements_text(NEW.target_ids) target(value)
     ORDER BY value::uuid
  LOOP
    PERFORM 1 FROM content_items c
     WHERE c.tenant_id=NEW.tenant_id AND c.public_id=target_id
     FOR UPDATE;
    IF EXISTS (
      SELECT 1 FROM pods_reset_retirements r
       WHERE r.tenant_id=NEW.tenant_id AND r.content_item_id=target_id
         AND r.state IN ('retiring','retired')
    ) THEN
      RAISE EXCEPTION 'retention batch cannot target permanently retired content';
    END IF;
  END LOOP;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS pods_reset_retention_batch_fence ON retention_compaction_batches;
CREATE TRIGGER pods_reset_retention_batch_fence
BEFORE INSERT OR UPDATE ON retention_compaction_batches
FOR EACH ROW EXECUTE FUNCTION enforce_pods_reset_retention_batch_fence();

CREATE OR REPLACE FUNCTION enforce_pods_reset_retention_manifest_fence()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  target_id UUID;
BEGIN
  IF NEW.state IN ('expired','executed','blocked')
     OR (NEW.state IN ('prepared','approved') AND NEW.expires_at <= NOW()) THEN
    RETURN NEW;
  END IF;
  IF jsonb_typeof(NEW.anchor_content_ids) <> 'array'
     OR jsonb_typeof(NEW.protected_content_ids) <> 'array'
     OR jsonb_typeof(NEW.retire_content_ids) <> 'array' THEN
    RAISE EXCEPTION 'retention manifest content scopes must be JSON arrays';
  END IF;

  FOR target_id IN
    SELECT DISTINCT value::uuid
      FROM jsonb_array_elements_text(
        NEW.anchor_content_ids || NEW.protected_content_ids || NEW.retire_content_ids
      ) target(value)
     ORDER BY value::uuid
  LOOP
    PERFORM 1 FROM content_items c
     WHERE c.tenant_id=NEW.tenant_id AND c.public_id=target_id
     FOR UPDATE;
    IF EXISTS (
      SELECT 1 FROM pods_reset_retirements r
       WHERE r.tenant_id=NEW.tenant_id AND r.content_item_id=target_id
         AND r.state IN ('retiring','retired')
    ) THEN
      RAISE EXCEPTION 'retention manifest cannot reference permanently retired content';
    END IF;
  END LOOP;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS pods_reset_retention_manifest_fence ON retention_compaction_manifests;
CREATE TRIGGER pods_reset_retention_manifest_fence
BEFORE INSERT OR UPDATE ON retention_compaction_manifests
FOR EACH ROW EXECUTE FUNCTION enforce_pods_reset_retention_manifest_fence();
