package controllers

import "testing"

func TestOperationalAutopilotsRequireExplicitTenant(t *testing.T) {
	if _, _, err := runPipelineAutopilot(nil, "", pipelineAutopilotRunOptions{}); err == nil {
		t.Fatal("pipeline autopilot accepted an implicit tenant")
	}
	if _, _, err := runEnrichmentAutopilot(nil, "", enrichmentAutopilotRunOptions{}); err == nil {
		t.Fatal("enrichment autopilot accepted an implicit tenant")
	}
	if _, _, err := runMediaCirculationAutopilot(nil, "", mediaAutopilotRunOptions{}); err == nil {
		t.Fatal("media circulation autopilot accepted an implicit tenant")
	}
}
