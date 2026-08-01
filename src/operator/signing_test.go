package operator

import "testing"

func samplePlan() CanonicalPlan {
	return CanonicalPlan{SchemaVersion: ContractVersion, PlanID: "plan-1", TenantID: "tenant-a", ActorID: "admin-a", ToolKey: "feed_integrity.refresh_snapshot", ToolVersion: "v1", TargetIDs: []string{"news:today"}, NormalizedArguments: map[string]any{"window": "today"}, EvidenceIDs: []string{"evidence-1"}, EvidenceFingerprint: "fingerprint", AccessVersion: "access-v1", RiskTier: RiskRoutine, Cancellation: "before_start_only", Rollback: "not_required_idempotent_refresh", Contingencies: []string{"snapshot_refresh_failed:stop_and_record_failure", "verification_failed:stop_and_record_failure"}}
}

func TestCanonicalPlanSignatureRejectsMutation(t *testing.T) {
	key := []byte("this-is-a-test-only-plan-signing-key-123")
	plan := samplePlan()
	digest, signature, err := SignCanonicalPlan(key, plan)
	if err != nil {
		t.Fatalf("sign plan: %v", err)
	}
	if err := VerifyCanonicalPlanSignature(key, plan, digest, signature); err != nil {
		t.Fatalf("verify original plan: %v", err)
	}
	plan.TargetIDs = []string{"news:tomorrow"}
	if err := VerifyCanonicalPlanSignature(key, plan, digest, signature); err == nil {
		t.Fatal("expected modified plan to fail signature verification")
	}
}

func TestPlanSigningKeyUsesExistingCMSIdentityAndFailsWithoutOne(t *testing.T) {
	t.Setenv("OPERATOR_PLAN_SIGNING_KEY", "")
	t.Setenv("CMS_SERVICE_TOKEN", "")
	if _, err := PlanSigningKeyFromEnv(); err == nil {
		t.Fatal("missing CMS machine identity must fail closed")
	}
	t.Setenv("CMS_SERVICE_TOKEN", "cms-machine-identity")
	derived, err := PlanSigningKeyFromEnv()
	if err != nil || len(derived) != 32 {
		t.Fatalf("expected derived signing key, got %v", err)
	}
	t.Setenv("OPERATOR_PLAN_SIGNING_KEY", "a-safe-test-plan-signing-key-with-32-bytes")
	override, err := PlanSigningKeyFromEnv()
	if err != nil || string(override) != "a-safe-test-plan-signing-key-with-32-bytes" {
		t.Fatalf("expected legacy override to be honored, got %v", err)
	}
}
