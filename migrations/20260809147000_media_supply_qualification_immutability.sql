-- Qualification cases were immutable from M2B. Reports are intentionally
-- mutable only for the one draft -> sealed transition; report membership and
-- signoffs are append-once evidence and never editable.
CREATE OR REPLACE FUNCTION guard_media_supply_qualification_report()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    RAISE EXCEPTION 'media supply qualification reports cannot be deleted';
  END IF;
  IF OLD.state <> 'draft' THEN
    RAISE EXCEPTION 'sealed media supply qualification reports are immutable';
  END IF;
  IF NEW.state = 'draft' THEN
    RAISE EXCEPTION 'media supply qualification report draft mutation is forbidden';
  END IF;
  IF NEW.state <> 'sealed'
     OR NEW.rubric_version <> OLD.rubric_version
     OR NEW.action_key <> OLD.action_key
     OR NEW.action_version <> OLD.action_version
     OR NEW.adapter_version <> OLD.adapter_version
     OR NEW.verifier_version <> OLD.verifier_version
     OR NEW.schema_version <> OLD.schema_version
     OR NEW.policy_version <> OLD.policy_version
     OR NEW.environment_identity <> OLD.environment_identity
     OR NEW.build_identity <> OLD.build_identity
     OR NEW.payload <> OLD.payload
     OR NEW.report_digest <> OLD.report_digest
     OR NEW.seal IS NULL
     OR NEW.sealed_at IS NULL THEN
    RAISE EXCEPTION 'invalid media supply qualification report seal transition';
  END IF;
  RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_media_supply_qualification_reports_guard ON media_supply_qualification_reports;
CREATE TRIGGER trg_media_supply_qualification_reports_guard
BEFORE UPDATE OR DELETE ON media_supply_qualification_reports
FOR EACH ROW EXECUTE FUNCTION guard_media_supply_qualification_report();

DROP TRIGGER IF EXISTS trg_media_supply_qualification_report_cases_immutable ON media_supply_qualification_report_cases;
CREATE TRIGGER trg_media_supply_qualification_report_cases_immutable
BEFORE UPDATE OR DELETE ON media_supply_qualification_report_cases
FOR EACH ROW EXECUTE FUNCTION reject_media_supply_qualification_mutation();

DROP TRIGGER IF EXISTS trg_media_supply_qualification_signoffs_immutable ON media_supply_qualification_signoffs;
CREATE TRIGGER trg_media_supply_qualification_signoffs_immutable
BEFORE UPDATE OR DELETE ON media_supply_qualification_signoffs
FOR EACH ROW EXECUTE FUNCTION reject_media_supply_qualification_mutation();
