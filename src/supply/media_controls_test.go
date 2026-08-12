package supply

import (
	"testing"

	"content-management-system/src/models"
)

func TestMediaSupplyControlsUseOnlyStaticSubtractiveKeys(t *testing.T) {
	for _, key := range []string{
		models.MediaSupplyControlReadEvaluation,
		models.MediaSupplyControlNormalIntakeScheduling,
		models.MediaSupplyControlExceptionalRecovery,
		models.MediaSupplyControlIntakeCircuit,
		"supply_action:source_run.cancel_unstarted",
	} {
		if !models.IsKnownMediaSupplyControlKey(key) {
			t.Fatalf("registered control %q was rejected", key)
		}
	}
	if models.IsKnownMediaSupplyControlKey("enable_everything") {
		t.Fatal("unregistered control key was admitted")
	}
	if _, err := MayScheduleNormalIntake(nil, "tenant-a"); err == nil {
		t.Fatal("normal intake must fail closed when control evidence is unavailable")
	}
	if models.IsKnownMediaSupplyControlKey("supply_action:anything") {
		t.Fatal("unregistered action control must not be accepted")
	}
}

func TestSupplyActionControlKeysComeOnlyFromRegistry(t *testing.T) {
	key, err := SupplyActionControlKey(SupplyActionCancelUnstarted)
	if err != nil || key != "supply_action:source_run.cancel_unstarted" {
		t.Fatalf("unexpected registered action control: %q %v", key, err)
	}
	if _, err := SupplyActionControlKey("source_run.retry_all"); err == nil {
		t.Fatal("unregistered action must not receive a control key")
	}
}
