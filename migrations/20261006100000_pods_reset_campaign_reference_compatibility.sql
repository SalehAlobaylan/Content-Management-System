-- Installed Fresh Start tables remain immutable history. No table/column is
-- dropped, and the NOT NULL inventory sequence keeps its existing insert owner.
-- This release removes campaign recovery, so rollout requires no old obligations.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM content_reset_campaigns)
       OR EXISTS (SELECT 1 FROM lifecycle_operation_claims WHERE state = 'active')
       OR EXISTS (SELECT 1 FROM content_reset_intake_pauses WHERE state = 'active')
       OR EXISTS (SELECT 1 FROM source_item_instances WHERE campaign_id IS NOT NULL OR state = 'staged')
       OR EXISTS (SELECT 1 FROM feed_generations WHERE purpose = 'content_reset' OR content_reset_campaign_id IS NOT NULL)
       OR EXISTS (SELECT 1 FROM pods_reset_runs WHERE content_reset_campaign_id IS NOT NULL)
       OR EXISTS (SELECT 1 FROM source_run_requests WHERE purpose = 'content_reset_replay') THEN
        RAISE EXCEPTION 'campaign obligations require the previous owner runtime; reset simplification cannot proceed';
    END IF;
END $$;

-- Exact relationships, never a wildcard exemption from unknown-reference checks.
CREATE TRIGGER pods_reset_fence_content_reset_inventory_deletions
BEFORE INSERT OR UPDATE ON content_reset_inventory_deletions
FOR EACH ROW EXECUTE FUNCTION enforce_pods_reset_retirement_fence('content_item_id');
CREATE TRIGGER pods_reset_fence_content_reset_targets
BEFORE INSERT OR UPDATE ON content_reset_targets
FOR EACH ROW EXECUTE FUNCTION enforce_pods_reset_retirement_fence('content_item_id');
CREATE TRIGGER pods_reset_fence_content_reset_replay_reuses
BEFORE INSERT OR UPDATE ON content_reset_replay_reuses
FOR EACH ROW EXECUTE FUNCTION enforce_pods_reset_retirement_fence('content_item_id');
CREATE TRIGGER pods_reset_fence_source_item_instances
BEFORE INSERT OR UPDATE ON source_item_instances
FOR EACH ROW EXECUTE FUNCTION enforce_pods_reset_retirement_fence('content_item_id');
