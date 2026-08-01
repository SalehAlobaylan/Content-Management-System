package operator

import "testing"

func TestScheduledTemplateRejectsResolveIntent(t *testing.T) {
	template := ScheduledTemplate{
		VisibleContext: VisibleContext{SchemaVersion: ContractVersion, Domain: "sources", View: "list", Filters: map[string]any{}, Subjects: []SubjectRef{{Type: "tenant", ID: "tenant-a"}}, AvailableIntents: []Intent{IntentExplain, IntentResolve}},
		Request:        InvestigationRequest{Intent: IntentResolve, Message: "run this source", Tier: "fast"},
		AdapterKeys:    []string{"context:sources"},
	}
	if err := template.Validate(); err == nil {
		t.Fatal("a schedule must reject a standing mutation intent")
	}
}

func TestScheduleCadenceIsBounded(t *testing.T) {
	if _, _, err := ScheduleCadence(14); err == nil {
		t.Fatal("cadence below the minimum must fail")
	}
	if cadence, duration, err := ScheduleCadence(60); err != nil || cadence != "60m" || duration.Minutes() != 60 {
		t.Fatalf("unexpected accepted cadence %q %v %v", cadence, duration, err)
	}
	if _, _, err := ScheduleCadence(7*24*60 + 1); err == nil {
		t.Fatal("cadence beyond the maximum must fail")
	}
}
