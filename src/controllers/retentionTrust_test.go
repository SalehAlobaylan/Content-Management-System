package controllers

import (
	"content-management-system/src/models"
	"testing"

	"gorm.io/datatypes"
)

func TestRetentionTrustPromotionOnlyAllowsDerivedSnapshotRefresh(t *testing.T) {
	if !retentionActionAutoEligible(models.RetentionActionRefreshNewsSnapshots) {
		t.Fatal("snapshot refresh should be the only Safe Auto eligible class")
	}
	for _, class := range []string{models.RetentionActionExecuteHistoricalPurge, "news_database.compact_story", models.RetentionActionRequestStorageRun, models.RetentionActionRequestMediaRun} {
		if retentionActionAutoEligible(class) {
			t.Fatalf("human-only class became Safe Auto eligible: %s", class)
		}
	}
}

func TestRetentionActionModeCannotWidenAnOverride(t *testing.T) {
	policy := models.DefaultRetentionPolicy("default")
	policy.Mode = models.RetentionModeSafeAuto
	policy.ActionModes = datatypes.JSON([]byte(`{"news_database.refresh_snapshots":"assist"}`))
	if got := retentionActionMode(policy, models.RetentionActionRefreshNewsSnapshots); got != models.RetentionModeAssist {
		t.Fatalf("effective action mode = %q, want assist override", got)
	}
}

func TestRetentionSnapshotActionFailsClosedUntilTrust(t *testing.T) {
	run := models.RetentionRun{TenantID: "default"}
	policy := models.DefaultRetentionPolicy("default")
	policy.Mode = models.RetentionModeSafeAuto
	action := buildRetentionSnapshotRefreshAction(run, policy, retentionPreview{CandidateRows: 3}, []string{models.NewsWindowToday}, false)
	if action.Mode != models.RetentionModeAssist || action.Outcome != models.RetentionActionApprovalRequired || action.Guardrail != "trust_gate" {
		t.Fatalf("untrusted promotion should fall back to Assist approval: %+v", action)
	}
	trusted := buildRetentionSnapshotRefreshAction(run, policy, retentionPreview{}, []string{models.NewsWindowWeek}, true)
	if trusted.Mode != models.RetentionModeSafeAuto || trusted.Outcome != models.RetentionActionReady {
		t.Fatalf("trusted promotion should create a ready Safe Auto action: %+v", trusted)
	}
}

func TestRetentionPolicyRejectsSafeAutoForHumanOnlyClass(t *testing.T) {
	policy := models.DefaultRetentionPolicy("default")
	policy.ActionModes = datatypes.JSON([]byte(`{"news_database.compact_story":"safe_auto"}`))
	if err := retentionPolicyValid(policy); err == nil {
		t.Fatal("canonical compaction must remain human-only")
	}
}
