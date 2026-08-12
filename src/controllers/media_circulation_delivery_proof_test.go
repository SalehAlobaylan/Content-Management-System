package controllers

import (
	"testing"

	"content-management-system/src/models"
	"content-management-system/src/supply"
)

func TestMediaDeliveryState(t *testing.T) {
	tests := []struct {
		name      string
		request   models.SourceRunRequest
		hasIngest bool
		ingest    models.SourceRunReconciliationEvent
		hasTask   bool
		task      models.SourceRunVerificationTask
		hasPods   bool
		pods      models.SourceRunReconciliationEvent
		wantState string
	}{
		{
			name:      "pods present is verified",
			hasPods:   true,
			pods:      models.SourceRunReconciliationEvent{Verdict: string(supply.VerdictPresent)},
			wantState: "verified",
		},
		{
			name:      "active observation stays pending",
			hasTask:   true,
			task:      models.SourceRunVerificationTask{State: models.SourceRunVerificationTaskQueued},
			wantState: "pending",
		},
		{
			name:      "exhausted unknown observation is degraded",
			hasPods:   true,
			pods:      models.SourceRunReconciliationEvent{Verdict: string(supply.VerdictUnknown)},
			hasTask:   true,
			task:      models.SourceRunVerificationTask{State: models.SourceRunVerificationTaskTerminal},
			wantState: "degraded",
		},
		{
			name:      "ingest verification required stays pending",
			request:   models.SourceRunRequest{State: models.SourceRunVerificationRequired},
			wantState: "pending",
		},
		{
			name:      "failed source run is not observed",
			request:   models.SourceRunRequest{State: models.SourceRunFailed},
			wantState: "not_observed",
		},
		{
			name:      "verified ingest awaits first pods observation",
			request:   models.SourceRunRequest{State: models.SourceRunSucceeded},
			hasIngest: true,
			ingest:    models.SourceRunReconciliationEvent{Verdict: string(supply.VerdictPresent)},
			wantState: "pending",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, _ := mediaDeliveryState(test.request, "", test.hasIngest, test.ingest, test.hasTask, test.task, test.hasPods, test.pods)
			if got != test.wantState {
				t.Fatalf("mediaDeliveryState() = %q, want %q", got, test.wantState)
			}
		})
	}
}
