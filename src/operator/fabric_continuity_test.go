package operator

import (
	"testing"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/supply"
)

func TestOperatorMediaSourceScheduleState(t *testing.T) {
	now := time.Date(2026, time.August, 9, 12, 0, 0, 0, time.UTC)
	past, future := now.Add(-time.Minute), now.Add(time.Minute)
	tests := []struct {
		name      string
		source    models.ContentSource
		hasLatest bool
		latest    models.SourceRunRequest
		want      string
	}{
		{name: "inactive", source: models.ContentSource{IsActive: false}, want: "paused"},
		{name: "missing due", source: models.ContentSource{IsActive: true}, want: "unknown"},
		{name: "active run", source: models.ContentSource{IsActive: true, NextDueAt: &past}, hasLatest: true, latest: models.SourceRunRequest{State: models.SourceRunRunning}, want: "in_flight"},
		{name: "future due", source: models.ContentSource{IsActive: true, NextDueAt: &future}, want: "scheduled"},
		{name: "due no active run", source: models.ContentSource{IsActive: true, NextDueAt: &past}, hasLatest: true, latest: models.SourceRunRequest{State: models.SourceRunSucceeded}, want: "due_unadmitted"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, _ := operatorMediaSourceScheduleState(test.source, test.hasLatest, test.latest, now)
			if got != test.want {
				t.Fatalf("operatorMediaSourceScheduleState() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestOperatorMediaDeliveryStatePreservesUnknown(t *testing.T) {
	request := models.SourceRunRequest{State: models.SourceRunSucceeded}
	state, _ := operatorMediaDeliveryState(request, false, models.SourceRunReconciliationEvent{}, false, models.SourceRunVerificationTask{}, false, models.SourceRunReconciliationEvent{})
	if state != "unknown" {
		t.Fatalf("missing delivery evidence = %q, want unknown", state)
	}
	state, _ = operatorMediaDeliveryState(request, false, models.SourceRunReconciliationEvent{}, true, models.SourceRunVerificationTask{State: models.SourceRunVerificationTaskTerminal}, true, models.SourceRunReconciliationEvent{Verdict: string(supply.VerdictPresent)})
	if state != "verified" {
		t.Fatalf("present pods evidence = %q, want verified", state)
	}
	state, _ = operatorMediaDeliveryState(request, false, models.SourceRunReconciliationEvent{}, true, models.SourceRunVerificationTask{State: models.SourceRunVerificationTaskTerminal}, true, models.SourceRunReconciliationEvent{Verdict: string(supply.VerdictUnknown)})
	if state != "degraded" {
		t.Fatalf("exhausted unknown pods evidence = %q, want degraded", state)
	}
}
