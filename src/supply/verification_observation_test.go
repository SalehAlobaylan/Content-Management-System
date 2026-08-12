package supply

import "testing"

func TestObservationReasonPreservesUnknownRatherThanInventingAbsence(t *testing.T) {
	if got := observationReason("normalize_batch", 1, 0, true, 0); got != "CMS has consumer-side content linked to this exact normalization unit" {
		t.Fatalf("present observation reason = %q", got)
	}
	if got := observationReason("normalize_batch", 0, 0, true, 0); got != "CMS cannot independently establish the effect from current evidence" {
		t.Fatalf("missing evidence must remain unknown, got %q", got)
	}
	if got := observationReason("fetch_page", 0, 0, true, 1); got == "" {
		t.Fatal("fetch observation must retain its bounded evidence reason")
	}
}

func TestCoordinatorDeliveryObservationRequiresExactCMSReadback(t *testing.T) {
	verdict, outcome := observationVerdict("coordinator", 3, 3, true)
	if verdict != VerdictPresent || outcome != OutcomeNewItems {
		t.Fatalf("matching downstream content must be verified new items, got %s/%s", verdict, outcome)
	}
	verdict, outcome = observationVerdict("coordinator", 0, 0, true)
	if verdict != VerdictPresent || outcome != OutcomeNoChange {
		t.Fatalf("zero expected and observed content must be verified no change, got %s/%s", verdict, outcome)
	}
	verdict, outcome = observationVerdict("coordinator", 2, 3, true)
	if verdict != VerdictUnknown || outcome != OutcomeUnknown {
		t.Fatalf("partial downstream readback must remain unknown, got %s/%s", verdict, outcome)
	}
	verdict, outcome = observationVerdict("coordinator", 0, 0, false)
	if verdict != VerdictUnknown || outcome != OutcomeUnknown {
		t.Fatalf("incomplete immutable evidence must remain unknown, got %s/%s", verdict, outcome)
	}
	verdict, outcome = observationVerdict("pods_delivery", 2, 2, true)
	if verdict != VerdictPresent || outcome != OutcomeNewItems {
		t.Fatalf("complete Pods readback must verify visible delivery, got %s/%s", verdict, outcome)
	}
	if _, ok := podsDeliveryIngestEventID("consumer_pods_delivery:not-a-uuid"); ok {
		t.Fatal("malformed Pods-delivery causation must not select evidence")
	}
	if podsDeliveryObservationItemLimit < 1 || podsDeliveryObservationUnitLimit < 1 {
		t.Fatal("Pods-delivery observer must have explicit positive bounds")
	}
}
