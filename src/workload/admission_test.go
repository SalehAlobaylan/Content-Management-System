package workload

import (
	"math"
	"strings"
	"testing"
	"time"

	"content-management-system/src/models"
)

func validRequest(class string, units int) Request {
	return Request{TenantID: "tenant-a", System: "test", Class: class, Units: units, RunID: "run-a", Origin: OriginAutomation, OwnerKind: "test", OwnerID: "owner-a"}
}
func normalSnapshots(class ActionClass, now time.Time) map[string]models.WorkloadCapacitySnapshot {
	rows := map[string]models.WorkloadCapacitySnapshot{}
	for _, key := range class.Resources {
		rows[key] = models.WorkloadCapacitySnapshot{Resource: key, Available: true, Healthy: true, State: "normal", ObservedAt: now}
	}
	return rows
}

func TestCapacityDecisionKeepsUnknownExplicitAndSlotsIndependentFromQueueDepth(t *testing.T) {
	now := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name, class string
		units       int
		modify      func(map[string]models.WorkloadCapacitySnapshot, map[string]int, *SpendSignal)
		verdict     string
		granted     int
		unknown     bool
	}{
		{"healthy", "source_pull", 3, nil, "admit", 3, false},
		{"queue_pressure_is_not_double_counted", "source_pull", 3, func(s map[string]models.WorkloadCapacitySnapshot, _ map[string]int, _ *SpendSignal) {
			row := s["agg_fetch_queue"]
			row.Pressure = 40
			s["agg_fetch_queue"] = row
		}, "admit", 3, false},
		{"overlapping_slots_shrink", "text_embedding", 4, func(_ map[string]models.WorkloadCapacitySnapshot, r map[string]int, _ *SpendSignal) {
			r["enrichment_svc"] = 2
		}, "admit_reduced", 2, false},
		{"reservation_fills_resource", "image_embedding", 1, func(_ map[string]models.WorkloadCapacitySnapshot, r map[string]int, _ *SpendSignal) {
			r["media_svc"] = 4
		}, "defer", 0, false},
		{"queue_saturation_defers", "source_pull", 1, func(s map[string]models.WorkloadCapacitySnapshot, _ map[string]int, _ *SpendSignal) {
			row := s["agg_fetch_queue"]
			row.State = "saturated"
			s["agg_fetch_queue"] = row
		}, "defer", 0, false},
		{"recovering_shrinks", "source_pull", 4, func(s map[string]models.WorkloadCapacitySnapshot, _ map[string]int, _ *SpendSignal) {
			row := s["aggregation_svc"]
			row.State = "recovering"
			s["aggregation_svc"] = row
		}, "admit_reduced", 2, false},
		{"stale_failopen_is_unknown", "image_embedding", 2, func(s map[string]models.WorkloadCapacitySnapshot, _ map[string]int, _ *SpendSignal) {
			row := s["media_svc"]
			row.ObservedAt = now.Add(-2 * time.Minute)
			s["media_svc"] = row
		}, "unknown", 2, true},
		{"future_observation_unknown", "image_embedding", 2, func(s map[string]models.WorkloadCapacitySnapshot, _ map[string]int, _ *SpendSignal) {
			row := s["media_svc"]
			row.ObservedAt = now.Add(time.Minute)
			s["media_svc"] = row
		}, "unknown", 2, true},
		{"missing_resource_unknown", "image_embedding", 2, func(s map[string]models.WorkloadCapacitySnapshot, _ map[string]int, _ *SpendSignal) {
			delete(s, "media_svc")
		}, "unknown", 2, true},
		{"tenant_cap_retains_owner", "stt_batch", 1, func(_ map[string]models.WorkloadCapacitySnapshot, _ map[string]int, s *SpendSignal) {
			s.State = "capped"
		}, "defer", 0, false},
		{"unknown_money_never_normal", "llm_batch", 1, func(_ map[string]models.WorkloadCapacitySnapshot, _ map[string]int, s *SpendSignal) {
			s.Available = false
		}, "unknown", 1, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			class, _ := Class(test.class)
			p := defaultPolicy()
			snapshots := normalSnapshots(class, now)
			reserved := map[string]int{}
			spend := SpendSignal{Available: true, State: "normal", ObservedAt: now}
			if test.modify != nil {
				test.modify(snapshots, reserved, &spend)
			}
			verdict, granted, _, unknown := evaluate(validRequest(test.class, test.units), class, p, snapshots, reserved, spend, now)
			if verdict != test.verdict || granted != test.granted || unknown != test.unknown {
				t.Fatalf("verdict=%s units=%d unknown=%v", verdict, granted, unknown)
			}
		})
	}
}

func TestSnapshotRecoveryRequiresObservableCooldownAndDetectsSustainedGrowth(t *testing.T) {
	p := defaultPolicy()
	now := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	previous := models.WorkloadCapacitySnapshot{}
	sample := Sample{Available: true, Healthy: true, ObservedAt: now, Pressure: 100}
	previous = snapshotFromSample("agg_fetch_queue", sample, previous, p, now)
	if previous.State != "saturated" {
		t.Fatal(previous.State)
	}
	sample.Pressure = 1
	sample.ObservedAt = now.Add(time.Second)
	previous = snapshotFromSample("agg_fetch_queue", sample, previous, p, now.Add(time.Second))
	if previous.State != "recovering" || previous.RecoverySince == nil {
		t.Fatalf("immediate recovery: %+v", previous)
	}
	sample.ObservedAt = now.Add(60 * time.Second)
	previous = snapshotFromSample("agg_fetch_queue", sample, previous, p, now.Add(60*time.Second))
	if previous.State != "recovering" {
		t.Fatal("cooldown flapped")
	}
	sample.ObservedAt = now.Add(122 * time.Second)
	previous = snapshotFromSample("agg_fetch_queue", sample, previous, p, now.Add(122*time.Second))
	if previous.State != "normal" {
		t.Fatal(previous.State)
	}
	previous = models.WorkloadCapacitySnapshot{}
	for i := 0; i < 4; i++ {
		sample.Pressure = float64(i * 6)
		sample.ObservedAt = now.Add(time.Duration(i) * time.Second)
		previous = snapshotFromSample("agg_fetch_queue", sample, previous, p, sample.ObservedAt)
	}
	if previous.State != "constrained" || previous.Reason != "sustained_queue_growth" {
		t.Fatalf("unseen growth: %+v", previous)
	}
	for _, bad := range []Sample{{Available: true, Healthy: true, ObservedAt: now, Pressure: math.NaN()}, {Available: true, Healthy: true, ObservedAt: now.Add(-time.Hour)}, {Healthy: true, ObservedAt: now}} {
		row := snapshotFromSample("agg_fetch_queue", bad, previous, p, now)
		if row.State != "unknown" || row.Available || row.Healthy {
			t.Fatalf("unknown reported healthy: %+v", row)
		}
	}
}

func TestOffAndUnavailableDoNotNeedSchemaAndInvalidIdentityDoesNotExecute(t *testing.T) {
	r := validRequest("text_embedding", 1)
	d := RequestPermit(nil, r)
	if !d.Allowed() || !d.Unavailable || d.WouldVerdict != "unknown" || d.PermitID != "" {
		t.Fatalf("unsafe unavailable result: %+v", d)
	}
	r.TenantID = ""
	if d = RequestPermit(nil, r); d.Allowed() || d.Verdict != "blocked" {
		t.Fatalf("tenant-less admitted: %+v", d)
	}
	r = validRequest("ai_enqueue", 1)
	if d = RequestPermit(nil, r); d.Allowed() {
		t.Fatal("obsolete diagnostic class became effect authority")
	}
}

func TestRegistryAndPolicyCannotChangeResourcesOrWeakenEvidenceThroughAPI(t *testing.T) {
	registry := Registry()
	seen := map[string]bool{}
	for _, class := range registry.Classes {
		if seen[class.Key] {
			t.Fatal("duplicate class")
		}
		seen[class.Key] = true
		for _, key := range class.Resources {
			if _, ok := resourceByKey(key); !ok {
				t.Fatalf("class %s unknown resource %s", class.Key, key)
			}
		}
	}
	registry.Classes[0].Resources[0] = "mutated"
	registry.Classes[0].Weights["aggregation_svc"] = 999
	if Class, _ := Class("source_pull"); Class.Resources[0] == "mutated" || Class.Weights["aggregation_svc"] != 1 {
		t.Fatal("registry caller mutated code-owned authority")
	}
	if err := validateStoredPolicy(defaultPolicy()); err != nil {
		t.Fatal(err)
	}
	p := defaultPolicy()
	p.ObservationHours = 0
	if err := validateStoredPolicy(p); err == nil {
		t.Fatal("evidence requirement weakened")
	}
	p = defaultPolicy()
	limits := limitsFor(p)
	limits["foreign"] = ResourceLimit{Capacity: 1}
	p.ResourceLimits = jsonValue(limits)
	if err := validateStoredPolicy(p); err == nil {
		t.Fatal("browser-defined resource accepted")
	}
	first := validRequest("text_embedding", 1)
	second := first
	second.TenantID = "tenant-b"
	if requestIdentity(first) == requestIdentity(second) || !strings.Contains(string(jsonValue(registry)), "pipeline_autopilot") {
		t.Fatal("identity isolation or diagnostic coverage missing")
	}
}
