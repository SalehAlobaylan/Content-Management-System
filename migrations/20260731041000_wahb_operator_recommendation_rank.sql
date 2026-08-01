ALTER TABLE operator_recommendations
  ADD COLUMN IF NOT EXISTS rank INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_operator_recommendations_investigation_rank
  ON operator_recommendations(investigation_id, rank)
  WHERE state = 'eligible';
