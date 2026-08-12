-- M2B: static, subtractive controls for each registered Supply recovery
-- action. The code registry remains the authority for which names are legal;
-- this forward-only constraint only makes those durable control rows storable.
ALTER TABLE media_supply_controls
  DROP CONSTRAINT IF EXISTS media_supply_controls_control_key_check;

ALTER TABLE media_supply_controls
  ADD CONSTRAINT media_supply_controls_control_key_check CHECK (control_key IN (
    'supply_read_evaluation',
    'normal_intake_scheduling',
    'exceptional_recovery_execution',
    'intake_admission_circuit',
    'supply_action:source_run.repair_missed_admission',
    'supply_action:source_run.reclaim_dispatch_claim',
    'supply_action:source_run.transfer_execution_unit_lease',
    'supply_action:source_run.adopt_unit_job',
    'supply_action:source_run.redeliver_receipt',
    'supply_action:source_run.verify_effect',
    'supply_action:source_run.finalize_verified_no_change',
    'supply_action:source_run.cancel_unstarted'
  ));
