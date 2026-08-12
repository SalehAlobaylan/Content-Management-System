-- Terminal membership repair rows are immutable history, not permanent
-- admission locks. Only one non-terminal repair may own an exact target.
ALTER TABLE feed_generation_membership_repairs
  DROP CONSTRAINT IF EXISTS feed_generation_membership_repairs_tenant_id_content_item_id_expected_generation_id_key;
DROP INDEX IF EXISTS uq_feed_generation_membership_repair_active;
CREATE UNIQUE INDEX uq_feed_generation_membership_repair_active
  ON feed_generation_membership_repairs(tenant_id, content_item_id, expected_generation_id)
  WHERE state IN ('queued','running','verifying','uncertain');
