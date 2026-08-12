package supply

import "testing"

func TestSupplyMetricDimensionsAreBounded(t *testing.T) {
	valid := SupplyMetricSample{Name: "source_requests_open", Owner: "cms", Action: "source_run", Stage: "admission", Verdict: "open"}
	if !SupplyMetricDimensionsAreBounded(valid) {
		t.Fatal("registered fixed dimensions were rejected")
	}
	for _, invalid := range []SupplyMetricSample{
		{Name: "x", Owner: "tenant-123", Action: "source_run", Stage: "admission", Verdict: "open"},
		{Name: "x", Owner: "cms", Action: "request-id", Stage: "admission", Verdict: "open"},
		{Name: "x", Owner: "cms", Action: "source_run", Stage: "item-id", Verdict: "open"},
	} {
		if SupplyMetricDimensionsAreBounded(invalid) {
			t.Fatalf("unbounded dimensions admitted: %#v", invalid)
		}
	}
}
