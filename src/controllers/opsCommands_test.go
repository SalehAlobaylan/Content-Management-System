package controllers

import (
	"testing"
	"time"
)

func TestOpsCommandLockOrderMatchesContainment(t *testing.T) {
	targets := []opsPauseTarget{
		{Member: opsFleetMember{Key: "pipeline"}, Lane: opsFleetLane{Key: "autopilot"}, TenantID: "b"},
		{Member: opsFleetMember{Key: "enrichment"}, Lane: opsFleetLane{Key: "autopilot"}, TenantID: "a"},
		{Member: opsFleetMember{Key: "system_health"}, Lane: opsFleetLane{Key: "containment"}, TenantID: "platform"},
		{Member: opsFleetMember{Key: "pipeline"}, Lane: opsFleetLane{Key: "autopilot"}, TenantID: "a"},
	}
	opsSortTargets(targets)
	if targets[0].Member.Key != "system_health" || targets[1].Member.Key != "enrichment" || targets[2].TenantID != "a" || targets[3].TenantID != "b" {
		t.Fatalf("lock order risks containment conflict: %+v", targets)
	}
}
func TestOpsCommandReviewExpiresAndBindsExactTargetScope(t *testing.T) {
	now := time.Now().UTC()
	review := opsCommandReview{SnapshotAt: now, TargetDigest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
	if err := opsValidateReview(review, now); err != nil {
		t.Fatal(err)
	}
	if err := opsValidateReview(review, now.Add(opsCommandFreshness+time.Second)); err == nil {
		t.Fatal("expired review accepted")
	}
	scope := opsCommandScope{Command: "pause_member", MemberKey: "pipeline", LaneKey: "autopilot", TenantID: "a"}
	target := opsPauseTarget{Member: opsFleetMember{Key: "pipeline"}, Lane: opsFleetLane{Key: "autopilot"}, TenantID: "a", Exists: true}
	original := opsTargetsDigest(scope, []opsPauseTarget{target})
	target.TenantID = "b"
	if original == opsTargetsDigest(scope, []opsPauseTarget{target}) {
		t.Fatal("review digest ignored tenant")
	}
	target.TenantID = "a"
	target.Prior = &now
	if original == opsTargetsDigest(scope, []opsPauseTarget{target}) {
		t.Fatal("review digest ignored foreign pause")
	}
}
