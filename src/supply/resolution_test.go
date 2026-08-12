package supply

import "testing"

func TestAssessEpisodeResolutionRequiresBoundarySpecificPresentEvidence(t *testing.T) {
	tests := []struct {
		name  string
		input EpisodeResolutionInput
		state string
		kind  string
	}{
		{
			name:  "due admission resolves only after admission proof",
			input: EpisodeResolutionInput{Verdict: SupplyVerdictSourceDueNotAdmitted, SourceAdmissionRecovered: ResolutionObservationPresent},
			state: EpisodeResolutionResolved, kind: "source_admission_restored",
		},
		{
			name:  "delivery issue remains open without later verified delivery",
			input: EpisodeResolutionInput{Verdict: SupplyVerdictPodsDeliveryDegraded, SourceDeliveryRecovered: ResolutionObservationAbsent},
			state: EpisodeResolutionStillOpen, kind: "pods_delivery_verified_after_episode",
		},
		{
			name:  "unavailable evidence cannot close itself",
			input: EpisodeResolutionInput{Verdict: SupplyVerdictEvidenceUnavailable, FreshEvidenceAvailable: ResolutionObservationUnknown},
			state: EpisodeResolutionUnknown, kind: "cms_evidence_boundary_restored",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := AssessEpisodeResolution(test.input)
			if got.State != test.state || got.Kind != test.kind {
				t.Fatalf("resolution = %#v, want state=%q kind=%q", got, test.state, test.kind)
			}
		})
	}
}

func TestAssessEpisodeResolutionNeverUsesUnsupportedVerdictAsClosure(t *testing.T) {
	got := AssessEpisodeResolution(EpisodeResolutionInput{Verdict: SupplyVerdictObservationPending, SourceAdmissionRecovered: ResolutionObservationPresent})
	if got.State != EpisodeResolutionUnknown || got.Kind != "unsupported_verdict" {
		t.Fatalf("unsupported verdict must remain non-closable, got %#v", got)
	}
}
