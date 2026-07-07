package controllers

import (
	"content-management-system/src/models"
	"encoding/json"
	"testing"
	"time"
)

func TestDefaultSystemAutopilotPolicyMatchesPlanDefaults(t *testing.T) {
	p := models.DefaultSystemAutopilotPolicy()
	if p.Enabled {
		t.Fatalf("system health autopilot should default disabled")
	}
	if p.Mode != models.SystemAutopilotModeObserve {
		t.Fatalf("mode = %q, want observe", p.Mode)
	}
	if p.IntervalMinutes != 10 || p.ConfirmProbes != 2 || p.ResolveProbes != 3 {
		t.Fatalf("unexpected cadence defaults: interval=%d confirm=%d resolve=%d", p.IntervalMinutes, p.ConfirmProbes, p.ResolveProbes)
	}
	disabled := containmentDisabledSet(p)
	for _, key := range []string{"news_circulation", "media_circulation", "media_studio"} {
		if !disabled[key] {
			t.Fatalf("%s should be opted out by default", key)
		}
	}
	for _, key := range []string{"pipeline", "enrichment"} {
		if disabled[key] {
			t.Fatalf("%s should be containment-enabled by default", key)
		}
	}
}

func TestSanitizeSystemAutopilotPolicyClampsAndDefaults(t *testing.T) {
	p := models.SystemAutopilotPolicy{
		Mode:                   "reckless",
		IntervalMinutes:        1,
		ConfirmProbes:          0,
		ResolveProbes:          20,
		FlapCycles24h:          0,
		ContainmentTTLMinutes:  1,
		ContainmentDisabledFor: []byte(`not json`),
	}
	got := sanitizeSystemAutopilotPolicy(p)
	if got.Scope != systemAutopilotScope {
		t.Fatalf("scope = %q", got.Scope)
	}
	if got.Mode != models.SystemAutopilotModeObserve {
		t.Fatalf("mode = %q", got.Mode)
	}
	if got.IntervalMinutes != 10 || got.ConfirmProbes != 2 || got.ResolveProbes != 12 || got.FlapCycles24h != 3 || got.ContainmentTTLMinutes != 60 {
		t.Fatalf("unexpected sanitized values: %+v", got)
	}
	p.IntervalMinutes = 999
	got = sanitizeSystemAutopilotPolicy(p)
	if got.IntervalMinutes != 60 {
		t.Fatalf("high interval clamp = %d, want 60", got.IntervalMinutes)
	}
	var disabled []string
	if err := json.Unmarshal(got.ContainmentDisabledFor, &disabled); err != nil || len(disabled) == 0 {
		t.Fatalf("disabled containment defaults invalid: %v %v", disabled, err)
	}
}

func TestConfirmSystemAnomaliesRequiresConsecutiveProbes(t *testing.T) {
	current := []systemAnomaly{{Key: "aggregation:service_down", Service: "aggregation", Verdict: models.SystemVerdictServiceDown}}
	prev := []systemRunSnapshot{
		{Anomalies: []systemAnomaly{{Key: "aggregation:service_down"}}},
		{Anomalies: []systemAnomaly{{Key: "aggregation:service_down"}}},
	}
	if got := confirmSystemAnomalies(current, prev[:1], 3); len(got) != 0 {
		t.Fatalf("confirmed too early: %+v", got)
	}
	got := confirmSystemAnomalies(current, prev, 3)
	if len(got) != 1 || !got[0].Confirmed {
		t.Fatalf("expected confirmed anomaly, got %+v", got)
	}
}

func TestQueueBacklogAttentionRequiresThreeProbeStreak(t *testing.T) {
	current := systemAnomaly{Key: "aggregation:queue_backlog", Service: "aggregation", Verdict: models.SystemVerdictQueueBacklog}
	prev := []systemRunSnapshot{
		{Anomalies: []systemAnomaly{{Key: "aggregation:queue_backlog"}}},
		{Anomalies: []systemAnomaly{{Key: "aggregation:queue_backlog"}}},
	}
	if got := systemAnomalyStreak(current, prev[:1]); got != 2 {
		t.Fatalf("streak = %d, want 2", got)
	}
	if got := systemAnomalyStreak(current, prev); got != 3 {
		t.Fatalf("streak = %d, want 3", got)
	}
}

func TestSystemRootCauseHintAndTimeline(t *testing.T) {
	anomaly := systemAnomaly{
		Key:      "media:worker_stalled",
		Service:  "media",
		Verdict:  models.SystemVerdictWorkerStalled,
		Severity: "critical",
		Summary:  "Media worker is stalled",
	}
	if hint := systemRootCauseHint(anomaly); hint == "" {
		t.Fatalf("expected root cause hint")
	}
	timeline := appendSystemEpisodeTimeline(nil, "opened", testNow(), anomaly, systemHealthSnapshot{
		Overall: "degraded",
		Issues:  []systemHealthIssue{{Severity: "critical", Service: "media", Message: "worker stalled"}},
	})
	var entries []map[string]interface{}
	if err := json.Unmarshal(timeline, &entries); err != nil {
		t.Fatalf("timeline json invalid: %v", err)
	}
	if len(entries) != 1 || entries[0]["transition"] != "opened" || entries[0]["verdict"] != models.SystemVerdictWorkerStalled {
		t.Fatalf("unexpected timeline: %+v", entries)
	}
}

func testNow() time.Time {
	return time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
}
