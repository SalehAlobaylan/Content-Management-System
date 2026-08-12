package controllers

import (
	"testing"

	operatorpkg "content-management-system/src/operator"
)

func TestMediaSupplyEvaluatorDisablePlanRequiresFixedSignedArguments(t *testing.T) {
	canonical := operatorpkg.CanonicalPlan{
		ToolKey: "media_circulation.supply.disable_evaluator", TargetIDs: []string{"current"},
		NormalizedArguments: map[string]any{"control_key": "supply_read_evaluation", "scope_type": "tenant", "scope_id": "all"},
	}
	if !mediaSupplyEvaluatorDisableArgumentsValid(canonical) {
		t.Fatal("exact code-owned Supply evaluator disable arguments must be accepted")
	}
	canonical.NormalizedArguments["scope_id"] = "tenant-a"
	if mediaSupplyEvaluatorDisableArgumentsValid(canonical) {
		t.Fatal("a browser-derived control scope must never enter the signed executor")
	}
}
