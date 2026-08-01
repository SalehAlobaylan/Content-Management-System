package operator

import "testing"

func TestDetectAuthorityConflictsIsDeterministic(t *testing.T) {
	conflicts, err := DetectAuthorityConflicts([]AuthorityClaim{{Key: "feed.window", Value: "dirty", EvidenceID: "ev-1"}, {Key: "feed.window", Value: "clean", EvidenceID: "ev-2"}, {Key: "policy.mode", Value: "observe", EvidenceID: "ev-3"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(conflicts) != 1 || conflicts[0] != "Conflicting authoritative values for feed.window; no action can be planned until the source records agree." {
		t.Fatalf("unexpected conflicts: %#v", conflicts)
	}
}

func TestDetectAuthorityConflictsRejectsUnevidencedClaim(t *testing.T) {
	if _, err := DetectAuthorityConflicts([]AuthorityClaim{{Key: "feed.window", Value: "dirty"}}); err == nil {
		t.Fatal("conflict claims must remain evidence-bound")
	}
}

func TestResolveAuthorityClaimsPrefersLiveAndBlocksEqualPrecedenceDisagreement(t *testing.T) {
	resolved, conflicts, err := ResolveAuthorityClaims([]AuthorityClaim{{Key: "feed.window", Value: "clean", EvidenceID: "memory", Authority: EvidenceMemory}, {Key: "feed.window", Value: "dirty", EvidenceID: "live-a", Authority: EvidenceLive}, {Key: "policy.mode", Value: "observe", EvidenceID: "live-b", Authority: EvidenceLive}, {Key: "policy.mode", Value: "apply", EvidenceID: "live-c", Authority: EvidenceLive}})
	if err != nil {
		t.Fatal(err)
	}
	if resolved["feed.window"].EvidenceID != "live-a" || len(conflicts) != 1 {
		t.Fatalf("unexpected precedence resolution=%#v conflicts=%#v", resolved, conflicts)
	}
}
