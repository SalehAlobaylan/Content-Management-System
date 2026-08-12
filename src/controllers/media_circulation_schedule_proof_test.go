package controllers

import (
	"testing"
	"time"

	"content-management-system/src/models"
)

func TestMediaSourceScheduleState(t *testing.T) {
	now := time.Date(2026, time.August, 9, 12, 0, 0, 0, time.UTC)
	past := now.Add(-time.Minute)
	future := now.Add(time.Minute)
	tests := []struct {
		name      string
		source    models.ContentSource
		hasLatest bool
		latest    models.SourceRunRequest
		wantState string
	}{
		{name: "inactive source is paused", source: models.ContentSource{IsActive: false}, wantState: "paused"},
		{name: "active source without next due is unknown", source: models.ContentSource{IsActive: true}, wantState: "unknown"},
		{name: "active request takes precedence over due", source: models.ContentSource{IsActive: true, NextDueAt: &past}, hasLatest: true, latest: models.SourceRunRequest{State: models.SourceRunVerificationRequired}, wantState: "in_flight"},
		{name: "future due source is scheduled", source: models.ContentSource{IsActive: true, NextDueAt: &future}, wantState: "scheduled"},
		{name: "due source without active request stays admission fact", source: models.ContentSource{IsActive: true, NextDueAt: &past}, hasLatest: true, latest: models.SourceRunRequest{State: models.SourceRunSucceeded}, wantState: "due_unadmitted"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, _ := mediaSourceScheduleState(test.source, test.hasLatest, test.latest, now)
			if got != test.wantState {
				t.Fatalf("mediaSourceScheduleState() = %q, want %q", got, test.wantState)
			}
		})
	}
}

func TestSourceRunRequestActive(t *testing.T) {
	for _, state := range []string{models.SourceRunRequested, models.SourceRunAccepted, models.SourceRunRunning, models.SourceRunVerificationRequired} {
		if !sourceRunRequestActive(state) {
			t.Fatalf("%s must remain active", state)
		}
	}
	for _, state := range []string{models.SourceRunSucceeded, models.SourceRunFailed, models.SourceRunCancelled, "unknown"} {
		if sourceRunRequestActive(state) {
			t.Fatalf("%s must not be active", state)
		}
	}
}
