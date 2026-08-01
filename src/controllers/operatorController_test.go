package controllers

import (
	"errors"
	"testing"

	"content-management-system/src/models"
	operatorpkg "content-management-system/src/operator"
)

func TestOperatorPlanConfirmationIsBoundToHighImpactDigest(t *testing.T) {
	routine := models.OperatorActionPlan{RiskTier: string(operatorpkg.RiskRoutine), Digest: "0123456789abcdef"}
	if !validOperatorConfirmationForPlan(routine, "APPROVE") || validOperatorConfirmationForPlan(routine, "APPROVE 01234567") {
		t.Fatal("routine plans must accept only the localized routine confirmation")
	}
	high := models.OperatorActionPlan{RiskTier: string(operatorpkg.RiskHigh), Digest: "0123456789abcdef"}
	if !validOperatorConfirmationForPlan(high, "APPROVE 01234567") || !validOperatorConfirmationForPlan(high, "أوافق 01234567") {
		t.Fatal("high-impact plans must accept their signed digest-bound phrase")
	}
	if validOperatorConfirmationForPlan(high, "APPROVE 89ABCDEF") {
		t.Fatal("a confirmation for another plan digest must fail closed")
	}
}

func TestOperatorPlanQueueSchemaErrorsAreReportedAsReadinessFailures(t *testing.T) {
	if !isOperatorPlanQueueSchemaError(errors.New(`ERROR: new row for relation "operator_action_plans" violates check constraint "operator_action_plans_state_check"`)) {
		t.Fatal("expected stale queued-state constraint to be recognized")
	}
	if isOperatorPlanQueueSchemaError(errors.New("operator plan approval preconditions changed")) {
		t.Fatal("ordinary approval races must remain precondition failures")
	}
}
