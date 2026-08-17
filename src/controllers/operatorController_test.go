package controllers

import (
	"errors"
	"testing"
	"time"

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

func TestOperatorCursorRoundTripsWithoutExposingDatabaseIDs(t *testing.T) {
	want := operatorCursor{Time: time.Date(2026, time.August, 12, 12, 0, 0, 0, time.UTC), ID: "701152a5-1659-4c16-ab77-712485ea4e09", Pinned: true, Kind: "plan"}
	encoded := encodeOperatorCursor(want)
	if encoded == "" || encoded == want.ID {
		t.Fatal("cursor must be opaque")
	}
	got, err := decodeOperatorCursor(encoded)
	if err != nil || got.ID != want.ID || got.Kind != want.Kind || !got.Time.Equal(want.Time) || !got.Pinned {
		t.Fatalf("cursor round trip = %+v, %v", got, err)
	}
	if _, err := decodeOperatorCursor("not-a-cursor"); err == nil {
		t.Fatal("malformed cursor must fail closed")
	}
}

func TestOperatorTaskGroupsCoverAllDurableKinds(t *testing.T) {
	for _, state := range []string{"accepted", "backgrounded", "running", "queued", "claimed", "verifying", "active"} {
		if !operatorTaskInGroup(state, "active") {
			t.Fatalf("%s should be active", state)
		}
	}
	if !operatorTaskInGroup("awaiting_approval", "needs_approval") || operatorTaskInGroup("queued", "needs_approval") {
		t.Fatal("approval grouping must contain only exact approval waits")
	}
	for _, state := range []string{"failed", "blocked", "paused"} {
		if !operatorTaskInGroup(state, "failed") {
			t.Fatalf("%s should require attention", state)
		}
	}
	for _, state := range []string{"completed", "done", "succeeded", "cancelled"} {
		if !operatorTaskInGroup(state, "completed") {
			t.Fatalf("%s should be terminal", state)
		}
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
