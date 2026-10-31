package controllers

import (
	"content-management-system/src/models"
	"testing"
	"time"
)

func TestOpsReadLivenessSeparatesActiveStuckDeepAndManual(t *testing.T) {
	now := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	old := now.Add(-48 * time.Hour)
	recent := now.Add(-5 * time.Minute)
	row := opsStatusRow{Enabled: true, CreatedAt: old, LastRunAt: &old, IntervalMinutes: 15}
	lane := opsFleetLane{Scheduled: true}
	base := opsMemberStatus{MemberKey: "pipeline"}
	active := opsLaneLiveness(base, lane, row, opsRunEvidence{Status: "running", StartedAt: recent}, true, now)
	if active.State != "running" {
		t.Fatalf("active run was mislabeled %s", active.State)
	}
	stuck := opsLaneLiveness(base, lane, row, opsRunEvidence{Status: "running", StartedAt: old}, true, now)
	if stuck.State != "run_stuck" {
		t.Fatalf("stuck run was mislabeled %s", stuck.State)
	}
	row.LastRunAt = &recent
	row.DeepLastRunAt = &old
	row.DeepIntervalHours = 24
	base.MemberKey = "feed_integrity"
	deep := opsLaneLiveness(base, lane, row, opsRunEvidence{}, false, now)
	if deep.State != "stalled" || deep.DeepDueAt == nil {
		t.Fatalf("fresh light hid stale deep: %+v", deep)
	}
	manual := opsLaneLiveness(base, opsFleetLane{ManualOnly: true}, row, opsRunEvidence{}, false, now)
	if manual.State != "idle" || manual.Liveness != "not_applicable" {
		t.Fatalf("manual lane received fictional cadence: %+v", manual)
	}
	pause := now.Add(time.Hour)
	row.PausedUntil = &pause
	paused := opsLaneLiveness(base, lane, row, opsRunEvidence{}, false, now)
	if paused.State != "paused" {
		t.Fatal("human pause lost")
	}
	base.MemberKey = "system_health"
	observing := opsLaneLiveness(base, lane, row, opsRunEvidence{Status: "running", StartedAt: recent}, true, now)
	if observing.State != "running" {
		t.Fatal("containment pause incorrectly stopped observation")
	}
}

func TestOpsReadHeadlineUnknownSuppressedAndIndependentMembers(t *testing.T) {
	cases := []struct {
		name  string
		items []opsAttentionItem
		fleet []opsMemberStatus
		want  string
	}{
		{name: "unknown scheduler", fleet: []opsMemberStatus{{State: "unknown", Liveness: "unknown"}}, want: "watching"},
		{name: "snoozed incident", items: []opsAttentionItem{{System: "system_health", Kind: "episode", Severity: "critical", Snoozed: true}}, want: "watching"},
		{name: "adapter failure", fleet: []opsMemberStatus{{State: "errored"}}, want: "attention"},
		{name: "confirmed degraded consumer", items: []opsAttentionItem{{System: "experience", Kind: "episode", Severity: "degraded"}}, want: "attention"},
		{name: "same member not fleet incident", fleet: []opsMemberStatus{{MemberKey: "feed_integrity", State: "stalled", Liveness: "observable"}, {MemberKey: "feed_integrity", State: "run_stuck", Liveness: "observable"}}, want: "attention"},
		{name: "three observable members", fleet: []opsMemberStatus{{MemberKey: "a", State: "stalled", Liveness: "observable"}, {MemberKey: "b", State: "stalled", Liveness: "observable"}, {MemberKey: "c", State: "stalled", Liveness: "observable"}}, want: "incident"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := opsHeadline(tt.items, tt.fleet)
			if got != tt.want {
				t.Fatalf("got %s want %s", got, tt.want)
			}
		})
	}
}

func TestOpsReadAttentionEscalatesAndExpiresWithoutWrites(t *testing.T) {
	now := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	until := now.Add(time.Hour)
	item := opsAttentionItem{Key: "a", Fingerprint: "evidence-a", Severity: "minor", Count: 2}
	state := models.OpsAttentionState{State: "snoozed", BaselineFingerprint: "evidence-a", BaselineSeverity: "minor", BaselineCount: 2, UpdatedAt: now, SnoozedUntil: &until}
	if got := opsApplyAttentionBaseline(item, state, now); !got.Snoozed || got.SnoozedUntil == nil {
		t.Fatal("active snooze not applied")
	}
	for _, changed := range []opsAttentionItem{{Fingerprint: "new", Severity: "minor", Count: 2}, {Fingerprint: "evidence-a", Severity: "major", Count: 2}, {Fingerprint: "evidence-a", Severity: "minor", Count: 3}} {
		if got := opsApplyAttentionBaseline(changed, state, now); got.Snoozed {
			t.Fatalf("escalation stayed hidden: %+v", changed)
		}
	}
	if got := opsApplyAttentionBaseline(item, state, until.Add(time.Second)); got.Snoozed || got.State != "" {
		t.Fatal("expired snooze remains applied")
	}
	state.State = "acked"
	if got := opsApplyAttentionBaseline(item, state, now); got.State != "acked" {
		t.Fatal("acknowledgment disappeared")
	}
	state.UpdatedAt = now.Add(-opsAttentionRetention - time.Second)
	if got := opsApplyAttentionBaseline(item, state, now); got.State != "" {
		t.Fatal("retained state applied after expiry")
	}
}

func TestOpsReadAttentionOrdersKindsThenAgeAndFiltersState(t *testing.T) {
	now := time.Now().UTC()
	items := []opsAttentionItem{{Key: "old-episode", Severity: "major", Kind: "episode", FirstSeen: now.Add(-time.Hour)}, {Key: "new-approval", Severity: "major", Kind: "approval", FirstSeen: now}, {Key: "acked", Severity: "minor", Kind: "attention", State: "acked"}, {Key: "hidden", Severity: "critical", Kind: "episode", State: "snoozed", Snoozed: true}}
	opsSortAttention(items)
	if items[0].Key != "hidden" || items[1].Key != "new-approval" {
		t.Fatalf("wrong priority: %+v", items)
	}
	got := opsFilterAttention(items, "", "", "", "open", false, false)
	if len(got) != 2 {
		t.Fatalf("open filter leaked suppressed state: %+v", got)
	}
	got = opsFilterAttention(items, "", "", "", "snoozed", true, true)
	if len(got) != 1 || got[0].Key != "hidden" {
		t.Fatal("snoozed restore view unavailable")
	}
}

func TestOpsReadBriefingProofBindsAdminAndHighWater(t *testing.T) {
	since := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	through := since.Add(time.Hour)
	secret := []byte("fixture-only-signing-material")
	token := opsBriefingToken(secret, "tenant", "admin", since, through)
	if token == opsBriefingToken(secret, "other", "admin", since, through) || token == opsBriefingToken(secret, "tenant", "other", since, through) || token == opsBriefingToken(secret, "tenant", "admin", since, through.Add(time.Second)) {
		t.Fatal("briefing proof can be replayed against another principal or high-water timestamp")
	}
	if token != opsBriefingToken(secret, "tenant", "admin", since, through) {
		t.Fatal("proof is not deterministic across replicas")
	}
}

func TestOpsReadCalendarShowsOverlapButNotAdjacentRuns(t *testing.T) {
	now := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	start := now.Add(-time.Hour)
	middle := start.Add(30 * time.Minute)
	end := middle.Add(10 * time.Minute)
	runs := []opsCalendarRun{{Key: "a", System: "feed_integrity", StartedAt: start, FinishedAt: &middle}, {Key: "b", System: "embedding_lifecycle", StartedAt: middle, FinishedAt: &end}}
	if got := opsCalendarOverlaps(runs, now); len(got) != 0 {
		t.Fatal("adjacent runs were called contention")
	}
	runs[1].StartedAt = middle.Add(-5 * time.Minute)
	overlaps := opsCalendarOverlaps(runs, now)
	if len(overlaps) != 1 || len(overlaps[0].Systems) != 2 || !overlaps[0].FinishedAt.Equal(middle) {
		t.Fatalf("missing actual overlap: %+v", overlaps)
	}
}

func TestOpsReadRegistryHasCompleteStaticRunContracts(t *testing.T) {
	seen := map[string]bool{}
	supply := false
	for _, m := range opsFleetRegistry {
		if seen[m.Key] {
			t.Fatalf("duplicate member %s", m.Key)
		}
		seen[m.Key] = true
		if m.Key == "supply_continuity" {
			supply = true
		}
		lanes := map[string]bool{}
		for _, lane := range m.Lanes {
			if lanes[lane.Key] {
				t.Fatalf("duplicate lane %s/%s", m.Key, lane.Key)
			}
			lanes[lane.Key] = true
			if lane.RunTable != "" && (lane.RunFinished == "" || lane.RunState == "") {
				t.Fatalf("incomplete run contract: %s/%s", m.Key, lane.Key)
			}
			if lane.ActionTable != "" && (lane.ActionKind == "" || lane.ActionOutcome == "") {
				t.Fatalf("incomplete action contract: %s/%s", m.Key, lane.Key)
			}
		}
	}
	if !supply {
		t.Fatal("Supply Continuity is invisible")
	}
}
