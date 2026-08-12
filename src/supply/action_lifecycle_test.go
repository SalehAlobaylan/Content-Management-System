package supply

import "testing"

func TestSupplyActionLifecycleDoesNotBlindRetryAfterEffectBoundary(t *testing.T) {
	for _, transition := range [][2]SupplyActionRequestState{
		{SupplyActionAwaitingApproval, SupplyActionQueued},
		{SupplyActionQueued, SupplyActionClaimed},
		{SupplyActionClaimed, SupplyActionRunning},
		{SupplyActionRunning, SupplyActionVerifying},
		{SupplyActionVerifying, SupplyActionSucceeded},
		{SupplyActionUncertain, SupplyActionVerifying},
	} {
		if err := ValidateSupplyActionTransition(transition[0], transition[1]); err != nil {
			t.Fatalf("expected transition %s -> %s: %v", transition[0], transition[1], err)
		}
	}
	if CanTransitionSupplyAction(SupplyActionUncertain, SupplyActionQueued) {
		t.Fatal("uncertain effect must verify before any later action decision")
	}
	if CanTransitionSupplyAction(SupplyActionRunning, SupplyActionQueued) {
		t.Fatal("running action must never be blindly requeued")
	}
}

func TestRequireSupplyActionDescriptorRejectsAliasesAndTargetMismatch(t *testing.T) {
	if _, err := RequireSupplyActionDescriptor(SupplyActionVerifyEffect, "source_run_execution_unit"); err != nil {
		t.Fatal(err)
	}
	if _, err := RequireSupplyActionDescriptor("source_run.retry_everything", "source_run_execution_unit"); err == nil {
		t.Fatal("unregistered alias must be rejected")
	}
	if _, err := RequireSupplyActionDescriptor(SupplyActionVerifyEffect, "content_source"); err == nil {
		t.Fatal("registered key cannot be rebound to another target type")
	}
}
