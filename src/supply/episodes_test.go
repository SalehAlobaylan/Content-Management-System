package supply

import (
	"testing"
	"time"
)

func TestEpisodeFingerprintIsTenantScopedAndIgnoresObservationTime(t *testing.T) {
	evaluation := SupplyEvaluation{
		SchemaVersion: SupplyEvaluationSchemaVersion, Verdict: SupplyVerdictSourceDueNotAdmitted,
		HeadlineBoundary: "cms_admission", Owner: "CMS source-run scheduler",
		AffectedSourceIDs: []string{"source-b", "source-a"}, AffectedRequestIDs: []string{"request-a"},
		EvaluatedAt: time.Now().UTC(),
	}
	first, err := EpisodeFingerprint("tenant-a", evaluation)
	if err != nil {
		t.Fatal(err)
	}
	evaluation.EvaluatedAt = evaluation.EvaluatedAt.Add(time.Hour)
	evaluation.AffectedSourceIDs = []string{"source-a", "source-b"}
	second, err := EpisodeFingerprint("tenant-a", evaluation)
	if err != nil || first != second {
		t.Fatalf("episode fingerprint must be stable across evidence time/order: %q %q %v", first, second, err)
	}
	third, err := EpisodeFingerprint("tenant-b", evaluation)
	if err != nil || first == third {
		t.Fatalf("episode fingerprint must remain tenant scoped: %q %q %v", first, third, err)
	}
	if _, err := EpisodeFingerprint("", evaluation); err == nil {
		t.Fatal("missing tenant must be rejected")
	}
}

func TestSupplyEpisodeAdmissionAndSeverityRemainReadOnly(t *testing.T) {
	if IsEpisodeWorthy(SupplyVerdictObservationPending) || IsEpisodeWorthy(SupplyVerdictNoCurrentBreak) {
		t.Fatal("normal observations must not create supply episodes")
	}
	if !IsEpisodeWorthy(SupplyVerdictEvidenceUnavailable) || EpisodeSeverity(SupplyVerdictEvidenceUnavailable) != "warning" {
		t.Fatal("unknown evidence must remain visible as warning attention")
	}
	if !IsEpisodeWorthy(SupplyVerdictSourceDueNotAdmitted) || EpisodeSeverity(SupplyVerdictSourceDueNotAdmitted) != "major" {
		t.Fatal("observed missed admission must become a major attention episode")
	}
}
