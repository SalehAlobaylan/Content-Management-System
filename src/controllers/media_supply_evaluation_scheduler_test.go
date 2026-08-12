package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/supply"
	"testing"
	"time"
)

func TestMediaSupplyEvaluationControlIsStrictlySubtractive(t *testing.T) {
	if !mediaSupplyEvaluationMayRecord(nil) {
		t.Fatal("absence of a durable disable row must retain the read-only evaluator default")
	}

	control := &models.MediaSupplyControl{
		TenantID: "tenant-a", ControlKey: models.MediaSupplyControlReadEvaluation,
		ScopeType: models.MediaSupplyControlScopeTenant, ScopeID: models.MediaSupplyControlScopeAll,
	}
	if mediaSupplyEvaluationMayRecord(control) {
		t.Fatal("a matching durable control row must only remove evaluator authority")
	}
}

func TestMediaSupplyEvaluationControlIdentityIsFixed(t *testing.T) {
	if models.MediaSupplyControlReadEvaluation != "supply_read_evaluation" {
		t.Fatalf("unexpected evaluator control identity %q", models.MediaSupplyControlReadEvaluation)
	}
	if models.MediaSupplyControlScopeTenant != "tenant" || models.MediaSupplyControlScopeAll != "all" {
		t.Fatal("evaluator control must remain tenant-scoped rather than accepting caller-defined scope")
	}
}

func TestMediaSupplyEvaluationCheckpointUsesOnlyRegisteredOutcomes(t *testing.T) {
	if !isMediaSupplyEvaluationTrigger(models.MediaSupplyEvaluationTriggerScheduled) ||
		!isMediaSupplyEvaluationTrigger(models.MediaSupplyEvaluationTriggerManual) {
		t.Fatal("scheduled and manual are the only evaluator checkpoint triggers")
	}
	if isMediaSupplyEvaluationTrigger("provider") {
		t.Fatal("checkpoint trigger must not admit a provider-owned path")
	}
	for _, outcome := range []string{
		models.MediaSupplyEvaluationOutcomeDisabled,
		models.MediaSupplyEvaluationOutcomeControlUnavailable,
		models.MediaSupplyEvaluationOutcomeRecordFailed,
	} {
		if !isMediaSupplyEvaluationFailureOutcome(outcome) {
			t.Fatalf("registered failure outcome %q was rejected", outcome)
		}
	}
	if isMediaSupplyEvaluationFailureOutcome("retry_requested") {
		t.Fatal("checkpoint outcome must not introduce a retry capability")
	}
}

func TestMediaSupplyEvaluationWorkerStatusRejectsMissingAndStaleHeartbeats(t *testing.T) {
	now := time.Date(2026, time.August, 9, 12, 0, 0, 0, time.UTC)
	restore := supply.SetMediaSupplyEvaluatorHeartbeatForTest(time.Time{})
	t.Cleanup(restore)
	if got := supply.MediaSupplyEvaluatorWorkerStatusAt(now); got.State != "not_started" {
		t.Fatalf("missing heartbeat = %#v", got)
	}

	supply.MarkMediaSupplyEvaluatorHeartbeat(now.Add(-supply.MediaSupplyEvaluatorHeartbeatGrace - time.Nanosecond))
	if got := supply.MediaSupplyEvaluatorWorkerStatusAt(now); got.State != "stale" || supply.MediaSupplyEvaluatorWorkerHealthy(now) {
		t.Fatalf("expired heartbeat must be stale, got %#v", got)
	}

	supply.MarkMediaSupplyEvaluatorHeartbeat(now.Add(-mediaSupplyEvaluationInterval))
	if got := supply.MediaSupplyEvaluatorWorkerStatusAt(now); got.State != "ready" || !supply.MediaSupplyEvaluatorWorkerHealthy(now) {
		t.Fatalf("recent heartbeat must be ready, got %#v", got)
	}
}
