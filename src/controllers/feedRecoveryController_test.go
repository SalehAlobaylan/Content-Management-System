package controllers

import "testing"

func TestRecoveryLanesExpandsBothWithoutInvalidAvailabilityLane(t *testing.T) {
	if got := recoveryLanes("news"); len(got) != 1 || got[0] != "news" {
		t.Fatalf("news lanes = %#v", got)
	}
	got := recoveryLanes("both")
	if len(got) != 2 || got[0] != "news" || got[1] != "media" {
		t.Fatalf("both lanes = %#v, want news then media", got)
	}
}

func TestRecoveryRepairToolsAreRegisteredAndBounded(t *testing.T) {
	if len(registeredFeedRecoveryRepairTools) != 2 {
		t.Fatalf("registered repair tool count = %d, want 2", len(registeredFeedRecoveryRepairTools))
	}
	for _, tool := range registeredFeedRecoveryRepairTools {
		if tool.Name == "" || tool.Description == "" {
			t.Fatalf("repair tool is not operator-readable: %+v", tool)
		}
		if tool.Name == "content_delete" || tool.Name == "source_cleanup" {
			t.Fatalf("destructive/source tool was registered: %q", tool.Name)
		}
	}
}

func TestRecoveryPlanRejectsRollbackAcknowledgementOutsidePurge(t *testing.T) {
	_, err := buildRecoveryPlan(nil, "default", feedRecoveryPlanRequest{Lane: "news", Level: "repair", CapacityMode: "safe_cutover", NoFullRollback: true}, "admin@example.test")
	if err == nil {
		t.Fatal("repair must reject no_full_rollback acknowledgement")
	}
}
