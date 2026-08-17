package supply

import "testing"

func TestAdmissionCutoverRequiresExplicitDatabaseTenantAndLane(t *testing.T) {
	if err := RequireDurableAdmission(nil, "tenant-a", "media"); err == nil {
		t.Fatal("durable admission without a database must fail closed")
	}
	if err := RequireDurableAdmission(nil, "", "media"); err == nil {
		t.Fatal("durable admission must reject implicit tenants")
	}
	if err := RequireLegacyAdmission(nil, "tenant-a", "media"); err == nil {
		t.Fatal("legacy admission without a database must fail closed")
	}
}

func TestAdmissionModeClassification(t *testing.T) {
	compatibility, needsProof, err := classifyAdmissionEpoch(admissionEpochCompatibility)
	if err != nil || compatibility != AdmissionModeCompatibility || needsProof {
		t.Fatalf("compatibility classification = %q proof=%t err=%v", compatibility, needsProof, err)
	}
	durable, needsProof, err := classifyAdmissionEpoch(admissionEpochDurable)
	if err != nil || durable != AdmissionModeDurable || !needsProof {
		t.Fatalf("durable classification = %q proof=%t err=%v", durable, needsProof, err)
	}
	if _, _, err := classifyAdmissionEpoch("unknown"); err == nil {
		t.Fatal("unknown epoch must fail closed")
	}
}
