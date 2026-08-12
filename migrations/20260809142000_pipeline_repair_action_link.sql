-- Bind every new exact pipeline repair to the signed Supply action that
-- authorized it. Historical repair rows remain readable with a NULL link but
-- cannot gain new execution authority through this compatibility column.
ALTER TABLE pipeline_repair_requests
  ADD COLUMN IF NOT EXISTS action_request_id uuid;

CREATE UNIQUE INDEX IF NOT EXISTS uq_pipeline_repair_action_request
  ON pipeline_repair_requests(action_request_id)
  WHERE action_request_id IS NOT NULL;

ALTER TABLE pipeline_repair_requests
  DROP CONSTRAINT IF EXISTS pipeline_repair_requests_action_request_fk;
ALTER TABLE pipeline_repair_requests
  ADD CONSTRAINT pipeline_repair_requests_action_request_fk
  FOREIGN KEY (action_request_id) REFERENCES media_supply_action_requests(public_id)
  ON DELETE RESTRICT NOT VALID;
