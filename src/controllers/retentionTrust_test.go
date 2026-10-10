package controllers

import (
	"content-management-system/src/models"
	"gorm.io/datatypes"
	"testing"
)

func TestRetentionLegacySafeAutoIsReadOnly(t *testing.T) {
	policy := models.DefaultRetentionPolicy("default")
	policy.Mode = models.RetentionModeSafeAuto
	policy.ActionModes = datatypes.JSON([]byte(`{"news_database.refresh_snapshots":"safe_auto"}`))
	if normalizeRetentionPolicy(policy).Mode != models.RetentionModeSafeAuto {
		t.Fatal("saved policy evidence lost")
	}
	if effectiveRetentionMode(policy.Mode) != models.RetentionModeObserve {
		t.Fatal("legacy policy acquired effect authority")
	}
	if retentionPolicyValid(policy) == nil {
		t.Fatal("new Safe Auto write accepted")
	}
	policy.Mode = models.RetentionModeAssist
	if retentionPolicyValid(policy) != nil || effectiveRetentionMode(policy.Mode) != models.RetentionModeAssist {
		t.Fatal("explicit manual Assist rejected")
	}
}

func TestRetentionRetiredClassesCannotExecute(t *testing.T) {
	for _, class := range []string{models.RetentionActionRefreshNewsSnapshots, models.RetentionActionRequestStorageRun, models.RetentionActionRequestMediaRun} {
		if !retentionActionRetired(class) {
			t.Fatalf("retired class admitted: %s", class)
		}
	}
	if retentionActionRetired(retentionActionPrepareNewsCompaction) {
		t.Fatal("manual compaction was retired")
	}
}
