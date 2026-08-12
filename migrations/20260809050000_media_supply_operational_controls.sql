-- M2A expansion: one existing subtractive control ledger now carries only
-- named Supply Continuity stops. This does not create an enable action,
-- modify policy, or grant a caller-selected control key.
ALTER TABLE media_supply_controls
  DROP CONSTRAINT IF EXISTS media_supply_controls_control_key_check;
ALTER TABLE media_supply_controls
  ADD CONSTRAINT media_supply_controls_control_key_check CHECK (control_key IN (
    'supply_read_evaluation',
    'normal_intake_scheduling',
    'exceptional_recovery_execution',
    'intake_admission_circuit'
  ));

CREATE INDEX IF NOT EXISTS idx_media_supply_controls_tenant_key
  ON media_supply_controls(tenant_id, control_key, scope_type, scope_id);
