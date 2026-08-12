package supply

import (
	"reflect"
	"testing"
	"time"
)

func TestEvaluateMediaSupplyPreservesObservedBoundaryAndUnknowns(t *testing.T) {
	now := time.Date(2026, time.August, 9, 12, 0, 0, 0, time.UTC)
	base := SupplyEvaluationInput{TenantID: "tenant-a", EvaluatedAt: now, ScheduleAvailable: true}
	tests := []struct {
		name     string
		input    SupplyEvaluationInput
		verdict  SupplyVerdict
		boundary string
		wantIDs  []string
		unknown  bool
	}{
		{
			name:    "due unadmitted remains CMS admission fact",
			input:   SupplyEvaluationInput{TenantID: base.TenantID, EvaluatedAt: now, ScheduleAvailable: true, Schedule: []SupplyScheduleObservation{{SourceID: "source-b", State: "due_unadmitted"}, {SourceID: "source-a", State: "due_unadmitted"}}},
			verdict: SupplyVerdictSourceDueNotAdmitted, boundary: "cms_admission", wantIDs: []string{"source-a", "source-b"},
		},
		{
			name:    "terminal source run without ingest is execution evidence",
			input:   SupplyEvaluationInput{TenantID: base.TenantID, EvaluatedAt: now, ScheduleAvailable: true, Delivery: []SupplyDeliveryObservation{{SourceID: "source-a", RequestID: "request-a", RequestState: "failed", State: "not_observed"}}},
			verdict: SupplyVerdictSourceRunWithoutIngest, boundary: "source_run_execution", wantIDs: []string{"source-a"},
		},
		{
			name:    "verified no change is healthy and does not await pods delivery",
			input:   SupplyEvaluationInput{TenantID: base.TenantID, EvaluatedAt: now, ScheduleAvailable: true, Schedule: []SupplyScheduleObservation{{SourceID: "source-a", State: "scheduled"}}, Delivery: []SupplyDeliveryObservation{{SourceID: "source-a", RequestID: "request-a", RequestState: "succeeded", State: "not_applicable", TerminalOutcome: string(OutcomeNoChange)}}},
			verdict: SupplyVerdictHealthyNoUpstreamChange, boundary: "provider_observation", wantIDs: []string{},
		},
		{
			name:    "blocked zero intake stays explicit",
			input:   SupplyEvaluationInput{TenantID: base.TenantID, EvaluatedAt: now, ScheduleAvailable: true, Delivery: []SupplyDeliveryObservation{{SourceID: "source-a", RequestID: "request-a", RequestState: "blocked", State: "blocked", TerminalOutcome: string(OutcomeObservationBlockedByIntake)}}},
			verdict: SupplyVerdictIntakeObservationBlocked, boundary: "intake_capacity", wantIDs: []string{"source-a"},
		},
		{
			name:    "exhausted pods observation stays degraded not a retry",
			input:   SupplyEvaluationInput{TenantID: base.TenantID, EvaluatedAt: now, ScheduleAvailable: true, Delivery: []SupplyDeliveryObservation{{SourceID: "source-a", RequestID: "request-a", State: "degraded"}}},
			verdict: SupplyVerdictPodsDeliveryDegraded, boundary: "pods_delivery_verification", wantIDs: []string{"source-a"},
		},
		{
			name:    "eligible generation omission is an explicit boundary",
			input:   SupplyEvaluationInput{TenantID: base.TenantID, EvaluatedAt: now, ScheduleAvailable: true, Schedule: []SupplyScheduleObservation{{SourceID: "source-a", State: "scheduled"}}, Exposure: &PodsExposureProof{EvidenceCompleteness: "complete", Verdict: "eligible_not_generation_reachable"}},
			verdict: SupplyVerdictGenerationOmission, boundary: "generation_membership", wantIDs: []string{},
		},
		{
			name:    "old reachable inventory cannot be healthy",
			input:   SupplyEvaluationInput{TenantID: base.TenantID, EvaluatedAt: now, ScheduleAvailable: true, Schedule: []SupplyScheduleObservation{{SourceID: "source-a", State: "scheduled"}}, Exposure: &PodsExposureProof{EvidenceCompleteness: "complete", Verdict: "pods_inventory_stale"}},
			verdict: SupplyVerdictPodsInventoryStale, boundary: "supply_freshness", wantIDs: []string{},
		},
		{
			name:    "unknown evidence blocks conclusion",
			input:   SupplyEvaluationInput{TenantID: base.TenantID, EvaluatedAt: now, ScheduleAvailable: true, Schedule: []SupplyScheduleObservation{{SourceID: "source-a", State: "unknown"}}},
			verdict: SupplyVerdictEvidenceUnavailable, boundary: "evidence", wantIDs: []string{"source-a"}, unknown: true,
		},
		{
			name:    "active observations are not terminal failure",
			input:   SupplyEvaluationInput{TenantID: base.TenantID, EvaluatedAt: now, ScheduleAvailable: true, Schedule: []SupplyScheduleObservation{{SourceID: "source-a", State: "in_flight"}}, Delivery: []SupplyDeliveryObservation{{SourceID: "source-a", RequestID: "request-a", State: "pending"}}},
			verdict: SupplyVerdictObservationPending, boundary: "observation", wantIDs: []string{},
		},
		{
			name:    "no active sources is configuration evidence rather than healthy supply",
			input:   base,
			verdict: SupplyVerdictNoActiveMediaSources, boundary: "media_source_configuration", wantIDs: []string{},
		},
		{
			name:    "quiet configured sample does not assert upstream no change",
			input:   SupplyEvaluationInput{TenantID: base.TenantID, EvaluatedAt: now, ScheduleAvailable: true, Schedule: []SupplyScheduleObservation{{SourceID: "source-a", State: "scheduled"}}},
			verdict: SupplyVerdictNoCurrentBreak, boundary: "none_observed", wantIDs: []string{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := EvaluateMediaSupply(test.input)
			if got.Verdict != test.verdict || got.HeadlineBoundary != test.boundary {
				t.Fatalf("evaluation = %#v, want verdict=%q boundary=%q", got, test.verdict, test.boundary)
			}
			if !reflect.DeepEqual(got.AffectedSourceIDs, test.wantIDs) {
				t.Fatalf("affected source IDs = %#v, want %#v", got.AffectedSourceIDs, test.wantIDs)
			}
			if (len(got.Unknowns) > 0) != test.unknown {
				t.Fatalf("unknowns = %#v, want unknown=%t", got.Unknowns, test.unknown)
			}
			if !got.ReadOnly || got.SchemaVersion != SupplyEvaluationSchemaVersion {
				t.Fatalf("evaluation must retain its read-only public contract: %#v", got)
			}
		})
	}
}

func TestEvaluateMediaSupplyFailsClosedForMissingTenantOrSchedule(t *testing.T) {
	for _, input := range []SupplyEvaluationInput{
		{ScheduleAvailable: true},
		{TenantID: "tenant-a", ScheduleAvailable: false},
	} {
		got := EvaluateMediaSupply(input)
		if got.Verdict != SupplyVerdictEvidenceUnavailable || got.EvidenceCompleteness != "unavailable" || len(got.Unknowns) != 1 {
			t.Fatalf("missing authority/evidence must fail closed: %#v", got)
		}
	}
}
