package controllers

import (
	"content-management-system/src/models"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
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
	for _, key := range []string{"embedding_lifecycle", "news_circulation", "media_circulation", "media_studio", "redundancy"} {
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

func TestContainmentDisabledSetHonorsPersistedOptIn(t *testing.T) {
	p := models.DefaultSystemAutopilotPolicy()
	p.ContainmentDisabledFor = []byte(`[]`)
	if containmentDisabledSet(p)["media_studio"] {
		t.Fatal("removing a persisted opt-out must genuinely opt in the sibling")
	}
}

func TestSystemContainmentLedgerScopesOwnershipToSiblingTenant(t *testing.T) {
	ledger := systemContainmentLedger{Version: 2, Siblings: map[string]map[string]systemContainmentLedgerEntry{
		"pipeline": {
			"tenant-a": {WrittenUntil: "2026-07-13T12:00:00Z", Outcome: "paused"},
			"tenant-b": {Outcome: "skipped", Reason: "human_pause"},
		},
	}}
	raw := marshalAutopilotJSON(ledger)
	got, legacy := readSystemContainmentLedger(raw)
	if legacy {
		t.Fatal("v2 containment ledger must not be treated as legacy")
	}
	if entry, ok := containmentEntry(got, "pipeline", "tenant-a"); !ok || entry.WrittenUntil == "" {
		t.Fatalf("missing tenant-a ownership: %+v", got)
	}
	if entry, ok := containmentEntry(got, "pipeline", "tenant-b"); !ok || entry.Reason != "human_pause" || entry.WrittenUntil != "" {
		t.Fatalf("tenant-b human pause must remain unowned: %+v", got)
	}
}

func TestSystemContainmentLegacyLedgerNeverAuthorizesResume(t *testing.T) {
	_, legacy := readSystemContainmentLedger([]byte(`{"pipeline":"2026-07-13T12:00:00Z"}`))
	if !legacy {
		t.Fatal("v1 sibling-wide containment must be treated as legacy")
	}
}

func TestSystemContainmentCompareAndSetQueriesAreTenantExact(t *testing.T) {
	sibling, ok := systemSiblingByKey("pipeline")
	if !ok {
		t.Fatal("pipeline sibling missing")
	}
	pause := systemPauseCompareAndSetSQL(sibling, "= ?")
	resume := systemResumeCompareAndSetSQL(sibling)
	for _, query := range []string{pause, resume} {
		if !strings.Contains(query, "tenant_id = ?") {
			t.Fatalf("containment query must scope to one tenant: %s", query)
		}
		if strings.Contains(query, "<=") || strings.Contains(query, ">=") {
			t.Fatalf("containment query must use exact ownership, not a tolerance: %s", query)
		}
	}
	if !strings.Contains(resume, "paused_until = ?") {
		t.Fatalf("resume must compare the exact written timestamp: %s", resume)
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

func TestSystemDependencyHealthyStringUsesExactEnums(t *testing.T) {
	for _, value := range []string{"connected", "reachable", "configured", "ready", "ok", "true"} {
		if !systemDependencyHealthyString(value) {
			t.Fatalf("%q should be healthy", value)
		}
	}
	for _, value := range []string{"disconnected", "unreachable", "circuit_open", "not_ready", "", "unknown"} {
		if systemDependencyHealthyString(value) {
			t.Fatalf("%q must not be healthy", value)
		}
	}
}

func TestSystemHTTPProbeRejectsOversizedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("x", systemProbeBodyLimit+1)))
	}))
	defer server.Close()
	probe := systemHTTPProbe(context.Background(), server.URL, false)
	if probe.Error == "" || !strings.Contains(probe.Error, "exceeds") {
		t.Fatalf("oversized response must be rejected, got %+v", probe)
	}
}

func TestSystemCorrelationOnlyCollapsesDeclaredSharedRoots(t *testing.T) {
	got := correlateSystemAnomalies([]systemAnomaly{
		{Key: "aggregation:queue_backlog", Service: "aggregation", Verdict: models.SystemVerdictQueueBacklog},
		{Key: "media:worker_stalled", Service: "media", Verdict: models.SystemVerdictWorkerStalled},
		{Key: "iam:service_down", Service: "iam", Verdict: models.SystemVerdictServiceDown},
	})
	if len(got) != 2 {
		t.Fatalf("got %d incidents, want redis correlation plus independent IAM", len(got))
	}
	if got[0].Service != "iam" || got[1].Service != "redis" {
		t.Fatalf("unexpected correlations: %+v", got)
	}
}

func TestSystemEpisodeObservablyHealthyRequiresEveryCorrelatedMember(t *testing.T) {
	ep := models.SystemIncidentEpisode{RootService: "redis"}
	snapshot := systemHealthSnapshot{Services: []systemProbeResult{{Name: "aggregation", Status: "healthy"}, {Name: "media", Status: "unknown"}}}
	if systemEpisodeObservablyHealthy(ep, snapshot) {
		t.Fatal("unknown member must not advance correlated recovery")
	}
	snapshot.Services[1].Status = "healthy"
	if !systemEpisodeObservablyHealthy(ep, snapshot) {
		t.Fatal("all healthy members must allow recovery")
	}
}

func TestSystemIncidentListProjectionOmitsLargeDiagnosticJSON(t *testing.T) {
	ep := models.SystemIncidentEpisode{
		RootService: "aggregation", Verdict: models.SystemVerdictServiceDown, Evidence: []byte(`{"large":"diagnostic"}`), Timeline: []byte(`[{"transition":"opened"}]`),
	}
	payload, err := json.Marshal(systemIncidentEpisodeListProjection(ep))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(payload), "evidence") || strings.Contains(string(payload), "timeline") {
		t.Fatalf("list projection leaked diagnostic JSON: %s", payload)
	}
	if !strings.Contains(string(payload), `"kind":"system_health.inspect"`) || !strings.Contains(string(payload), `"href":"/platform/system-health"`) {
		t.Fatalf("missing deterministic human recommendation: %s", payload)
	}
}

func testNow() time.Time {
	return time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
}
