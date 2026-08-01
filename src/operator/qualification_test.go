package operator

import (
	"strings"
	"testing"
)

func qualificationFingerprint() string { return strings.Repeat("a", 64) }

func TestQualificationAssessmentRejectsProductionFaultFixtures(t *testing.T) {
	input := QualificationAssessmentInput{ShadowRunPublicID: "f9c01bfe-14e5-4168-bb7a-0b0e7702e7bf", EvaluationCaseID: "fault-iam", Cohort: "fault", Grounded: true, UsefulRating: 5, DomainToolSelectionCorrect: true, FaultCase: "iam_outage", Outcome: "passed", ReviewerID: "reviewer", Provenance: "isolated_fixture", ResultFingerprint: qualificationFingerprint()}
	if err := validateQualificationAssessment(input, "production"); err == nil {
		t.Fatal("production must reject injected fault assessment")
	}
	if err := validateQualificationAssessment(input, "staging"); err != nil {
		t.Fatalf("isolated fixture should be valid outside production: %v", err)
	}
}

func TestQualificationCoverageRequiresEveryDomainLocaleAndFault(t *testing.T) {
	run := QualificationRun{RealSnapshotID: "run", EvaluationCaseID: "case", Domain: ShadowDomains[0], Locale: "en", Kind: "normal", Grounded: true, UsefulRating: 5, DomainToolSelectionCorrect: true, Outcome: "passed", AccessVersionHash: qualificationFingerprint(), PacketFingerprint: qualificationFingerprint(), ResultFingerprint: qualificationFingerprint()}
	if err := requiredCoverage([]QualificationRun{run}); err == nil {
		t.Fatal("incomplete report must not qualify")
	}
}

func TestShadowReportSealIsDigestBound(t *testing.T) {
	key := []byte("this-is-a-test-only-shadow-signing-key-123")
	seal, err := shadowReportSeal(key, strings.Repeat("b", 64))
	if err != nil {
		t.Fatal(err)
	}
	other, err := shadowReportSeal(key, strings.Repeat("c", 64))
	if err != nil {
		t.Fatal(err)
	}
	if seal == other {
		t.Fatal("different report digests must have different seals")
	}
}
