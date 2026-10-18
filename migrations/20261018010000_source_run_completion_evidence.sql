-- Additive exact Normalize input authority. Historical units keep unknown cohorts;
-- no content/history backfill or runtime migration is performed.
ALTER TABLE source_run_execution_units
  ADD COLUMN normalization_inputs JSONB NOT NULL DEFAULT '[]'::jsonb,
  ADD COLUMN normalization_input_digest VARCHAR(64) NOT NULL DEFAULT '';
ALTER TABLE source_run_execution_units ADD CONSTRAINT source_run_normalization_inputs_shape
  CHECK (jsonb_typeof(normalization_inputs) = 'array' AND jsonb_array_length(normalization_inputs) <= 100
    AND ((normalization_input_digest = '' AND normalization_inputs = '[]'::jsonb)
      OR (unit_type = 'normalize_batch' AND jsonb_array_length(normalization_inputs) BETWEEN 1 AND 100
        AND normalization_input_digest ~ '^[a-f0-9]{64}$')));
CREATE FUNCTION source_run_normalization_inputs_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.normalization_inputs IS DISTINCT FROM OLD.normalization_inputs
    OR NEW.normalization_input_digest IS DISTINCT FROM OLD.normalization_input_digest THEN
    RAISE EXCEPTION 'source-run normalization cohort is immutable';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER source_run_normalization_inputs_immutable
  BEFORE UPDATE ON source_run_execution_units FOR EACH ROW EXECUTE FUNCTION source_run_normalization_inputs_immutable();
