package supply

import (
	"context"
	"fmt"
	"os"
	"testing"

	"content-management-system/src/models"
	"content-management-system/src/tests/testdb"
	"github.com/google/uuid"
)

func TestQualificationWorkflowAcceptsHumanReviewOfOwnerCases(t *testing.T) {
	if os.Getenv("CMS_TEST_ADMIN_URL") == "" && os.Getenv("CMS_TEST_DATABASE_URL") == "" {
		t.Skip("guarded disposable PostgreSQL required")
	}
	t.Setenv("ENV", "test")
	t.Setenv("BUILD_ID", "supply-regression")
	t.Setenv("RELEASE_SHA", "")
	db := testdb.Open(t)
	if err := db.AutoMigrate(&models.MediaSupplyActionRequest{}, &models.MediaSupplyQualificationCase{}, &models.MediaSupplyQualificationHumanDecision{}, &models.MediaSupplyQualificationReport{}, &models.MediaSupplyQualificationReportCase{}, &models.MediaSupplyQualificationSignoff{}, &models.MediaSupplyActionPromotion{}, &models.MediaSupplyPromotionEvent{}); err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{
		`CREATE UNIQUE INDEX fixture_qualification_case ON media_supply_qualification_cases(case_key)`,
		`CREATE UNIQUE INDEX fixture_qualification_decision ON media_supply_qualification_human_decisions(case_id)`,
		`CREATE UNIQUE INDEX fixture_qualification_signoff ON media_supply_qualification_signoffs(report_id,role)`,
		`CREATE UNIQUE INDEX fixture_active_promotion ON media_supply_action_promotions(tenant_id,action_key) WHERE state='active'`,
	} {
		if err := db.Exec(sql).Error; err != nil {
			t.Fatal(err)
		}
	}
	ctx, key := context.Background(), SupplyActionRepairMissedAdmission
	for i := 0; i < 30; i++ {
		action := models.MediaSupplyActionRequest{PublicID: uuid.New(), TenantID: "tenant-a", ActionKey: key}
		row, err := RecordSupplyVerifierQualificationCase(ctx, db, action, true, VerdictPresent, false)
		if err != nil {
			t.Fatal(err)
		}
		if i < 10 {
			if _, err = RecordQualificationHumanDecision(ctx, db, "tenant-b", row.PublicID.String(), "agreed", "reviewer", "access-1"); err == nil {
				t.Fatal("foreign human decision accepted")
			}
			if _, err = RecordQualificationHumanDecision(ctx, db, "tenant-a", row.PublicID.String(), "agreed", "reviewer", "access-1"); err != nil {
				t.Fatalf("owner case cannot receive human review: %v", err)
			}
		}
	}
	for _, fault := range MediaSupplyQualificationFaults {
		if _, err := RecordSupplyFaultQualificationCase(ctx, db, "tenant-a", key, "fault:"+fault, fault, qualificationFingerprintForTest(), true, VerdictUnknown, false); err != nil {
			t.Fatal(err)
		}
	}
	for i, verdict := range []VerificationVerdict{VerdictPresent, VerdictAbsent, VerdictUnknown} {
		if _, err := RecordSupplyObserveQualificationCase(ctx, db, "tenant-a", key, fmt.Sprintf("observe:%d", i), qualificationFingerprintForTest(), false, verdict); err != nil {
			t.Fatal(err)
		}
	}
	v, _ := QualificationVersions(key)
	report, err := CreateQualificationReport(ctx, db, "tenant-a", key, v.ActionVersion, v.AdapterVersion, v.VerifierVersion, v.SchemaVersion, v.PolicyVersion)
	if err != nil {
		t.Fatal(err)
	}
	keyBytes := []byte("01234567890123456789012345678901")
	if _, err = SealQualificationReport(ctx, db, "tenant-a", report.PublicID.String(), keyBytes); err == nil {
		t.Fatal("unsigned report sealed")
	}
	for _, role := range mediaSupplyQualificationSignoffRoles {
		if _, err = AddQualificationSignoff(ctx, db, "tenant-a", report.PublicID.String(), role, "human-"+role, "fresh-access"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = SealQualificationReport(ctx, db, "tenant-a", report.PublicID.String(), keyBytes); err != nil {
		t.Fatal(err)
	}
	if _, err = PromoteQualifiedSupplyAction(ctx, db, "tenant-a", report.PublicID.String(), "promoter", "fresh-access", keyBytes); err != nil {
		t.Fatal(err)
	}
	if _, err = RequireActiveSupplyPromotion(db, "tenant-a", key, v.ActionVersion, v.AdapterVersion, v.VerifierVersion, v.SchemaVersion, v.PolicyVersion); err != nil {
		t.Fatal(err)
	}
	if _, err = PromoteQualifiedSupplyAction(ctx, db, "tenant-a", report.PublicID.String(), "promoter", "fresh-access", keyBytes); err == nil {
		t.Fatal("second active promotion admitted")
	}
}
