package supply

import (
	"fmt"
	"testing"

	"content-management-system/src/models"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

func passingQualificationPayload() QualificationReportPayload {
	versions, _ := QualificationVersions(SupplyActionRepairMissedAdmission)
	payload := QualificationReportPayload{RubricVersion: MediaSupplyQualificationRubricVersion, TenantID: "tenant-a", ActionKey: SupplyActionRepairMissedAdmission, ActionVersion: versions.ActionVersion, AdapterVersion: versions.AdapterVersion, VerifierVersion: versions.VerifierVersion, SchemaVersion: versions.SchemaVersion, PolicyVersion: versions.PolicyVersion, EnvironmentIdentity: "staging", BuildIdentity: "build-1"}
	for index := 0; index < 30; index++ {
		cohort, human := "terminal", ""
		if index < 10 {
			cohort, human = "human_decision", "agreed"
		}
		payload.Cases = append(payload.Cases, qualificationReportCase{ID: fmt.Sprintf("terminal-%d", index), CaseKey: fmt.Sprintf("terminal-key-%d", index), TenantID: "tenant-a", Cohort: cohort, Origin: string(QualificationOriginOwnerVerifier), Recommendation: "would_request", VerifiedSuccess: true, IndependentVerifier: true, EffectVerdict: string(VerdictPresent), Violations: []string{}, PayloadDigest: qualificationFingerprintForTest(), HumanDecision: human})
	}
	for index, fault := range MediaSupplyQualificationFaults {
		payload.Cases = append(payload.Cases, qualificationReportCase{ID: fmt.Sprintf("fault-%d", index), CaseKey: "fault-key-" + fault, TenantID: "tenant-a", Cohort: "fault", FaultCase: fault, Origin: string(QualificationOriginIsolatedFaultHarness), Recommendation: "would_skip", VerifiedSuccess: true, IndependentVerifier: true, EffectVerdict: string(VerdictUnknown), Violations: []string{}, PayloadDigest: qualificationFingerprintForTest()})
	}
	payload.Cases = append(payload.Cases,
		qualificationReportCase{ID: "tri-absent", CaseKey: "tri-absent", TenantID: "tenant-a", Cohort: "tri_state", Origin: string(QualificationOriginOwnerVerifier), Recommendation: "would_skip", VerifiedSuccess: true, IndependentVerifier: true, EffectVerdict: string(VerdictAbsent), Violations: []string{}, PayloadDigest: qualificationFingerprintForTest()},
	)
	return payload
}

func qualificationFingerprintForTest() string {
	return "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
}

func TestQualificationRubricIsStaticAndFailClosed(t *testing.T) {
	payload := passingQualificationPayload()
	if err := validateQualificationReportPayload(payload); err != nil {
		t.Fatalf("complete qualification should pass: %v", err)
	}
	payload.Cases[0].Violations = []string{"tenant_escape"}
	if err := validateQualificationReportPayload(payload); err == nil {
		t.Fatal("any disqualifying violation must fail qualification")
	}
	payload = passingQualificationPayload()
	payload.Cases = payload.Cases[:len(payload.Cases)-1]
	if err := validateQualificationReportPayload(payload); err == nil {
		t.Fatal("missing absent tri-state proof must fail qualification")
	}
}

func TestQualificationReportRejectsCrossTenantMembership(t *testing.T) {
	payload := passingQualificationPayload()
	payload.Cases[len(payload.Cases)-1].TenantID = "tenant-b"
	if err := validateQualificationReportPayload(payload); err == nil {
		t.Fatal("a tenant-bound report admitted a cross-tenant case")
	}
}

func TestApprovalRequiredAuthorityNeedsNoPromotion(t *testing.T) {
	manual := models.MediaSupplyActionRequest{PublicID: uuid.New(), TenantID: "tenant-a", ActionKey: SupplyActionRepairMissedAdmission, ExecutionMode: SupplyExecutionApprovalRequired}
	if err := RecheckSupplyActionExecutionAuthority(nil, manual); err != nil {
		t.Fatalf("approved manual recovery must not require qualification: %v", err)
	}
	promotion := uuid.New()
	manual.PromotionID = &promotion
	if err := RecheckSupplyActionExecutionAuthority(nil, manual); err == nil {
		t.Fatal("approval-required recovery admitted autonomous authority")
	}
	auto := models.MediaSupplyActionRequest{TenantID: "tenant-a", ActionKey: SupplyActionRepairMissedAdmission, ExecutionMode: SupplyExecutionSafeAuto}
	if err := RecheckSupplyActionExecutionAuthority(nil, auto); err == nil {
		t.Fatal("Safe Auto without an exact promotion/report binding was admitted")
	}
}

func TestQualificationVersionsBindEveryRegisteredAction(t *testing.T) {
	for _, key := range SupplyActionKeys() {
		versions, ok := QualificationVersions(key)
		if !ok || versions.ActionVersion != "v1" || versions.AdapterVersion == "" || versions.VerifierVersion == "" || versions.SchemaVersion == "" || versions.PolicyVersion == "" {
			t.Fatalf("action %s has incomplete qualification versions: %#v", key, versions)
		}
	}
	if _, ok := QualificationVersions("source_run.retry_all"); ok {
		t.Fatal("unregistered aliases cannot acquire qualification versions")
	}
}

func TestQualificationCaseOriginsArePurposeScoped(t *testing.T) {
	if !isTrustedQualificationOrigin(QualificationOriginCMSObserve, qualificationPrincipalCMSObserve) {
		t.Fatal("CMS Observe must retain its explicit qualification writer identity")
	}
	if !isTrustedQualificationOrigin(QualificationOriginOwnerVerifier, qualificationPrincipalVerifier) {
		t.Fatal("owner verifier must retain its explicit qualification writer identity")
	}
	if isTrustedQualificationOrigin(QualificationOriginOwnerVerifier, "generic-service-token") || isTrustedQualificationOrigin(QualificationOriginIsolatedFaultHarness, qualificationPrincipalVerifier) {
		t.Fatal("generic or cross-purpose principals must not write qualification cases")
	}
}

func TestFaultQualificationWriterIsProductionReadOnly(t *testing.T) {
	t.Setenv("ENV", "production")
	_, err := RecordSupplyFaultQualificationCase(t.Context(), &gorm.DB{}, "tenant-a", SupplyActionRepairMissedAdmission, "fault-case", "dependency_outage", qualificationFingerprintForTest(), true, VerdictUnknown, false)
	if err == nil || err.Error() != "production cannot ingest injected fault qualification" {
		t.Fatalf("production fault fixture writer did not fail closed: %v", err)
	}
}

func TestQualificationSealIsDigestBound(t *testing.T) {
	key := []byte("01234567890123456789012345678901")
	one, err := qualificationReportSeal(key, qualificationFingerprintForTest())
	if err != nil {
		t.Fatal(err)
	}
	two, err := qualificationReportSeal(key, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	if err != nil {
		t.Fatal(err)
	}
	if one == two {
		t.Fatal("qualification seal must change with its report digest")
	}
}
