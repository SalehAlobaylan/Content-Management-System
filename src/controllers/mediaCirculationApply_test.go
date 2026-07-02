package controllers

import (
	"content-management-system/src/models"
	"testing"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

func TestPlannedApplyOutcome(t *testing.T) {
	tests := []struct {
		name     string
		unit     string
		verdict  string
		wantKind string
		wantExec bool
	}{
		{"pull_now executes", models.MediaCirculationUnitSource, mediaCircVerdictPullNow, mediaCircOutcomePulled, true},
		{"pull_limited executes", models.MediaCirculationUnitSource, mediaCircVerdictPullLimited, mediaCircOutcomePulled, true},
		{"deep_pull executes", models.MediaCirculationUnitSource, mediaCircVerdictDeepPull, mediaCircOutcomePulled, true},
		{"pause executes", models.MediaCirculationUnitSource, mediaCircVerdictPauseSource, mediaCircOutcomePaused, true},
		{"skip acknowledges", models.MediaCirculationUnitSource, mediaCircVerdictSkipSource, mediaCircOutcomeAcknowledged, false},
		{"needs_review acknowledges", models.MediaCirculationUnitSource, mediaCircVerdictNeedsAdminReview, mediaCircOutcomeAcknowledged, false},
		{"atomize now executes", models.MediaCirculationUnitItemFamily, mediaCircVerdictAtomizeNow, mediaCircOutcomeAtomizationEnqueued, true},
		{"blocked transcript acknowledges", models.MediaCirculationUnitItemFamily, mediaCircVerdictBlockedTranscript, mediaCircOutcomeAcknowledged, false},
		{"atomization leak acknowledges", models.MediaCirculationUnitItemFamily, mediaCircVerdictAtomizationLeak, mediaCircOutcomeAcknowledged, false},
		{"rank_down executes", models.MediaCirculationUnitItemFamily, mediaCircVerdictRankDown, mediaCircOutcomeRankedDown, true},
		{"re_encode deferred", models.MediaCirculationUnitItemFamily, mediaCircVerdictReEncode, mediaCircOutcomeDeferredToSweep, false},
		{"move_to_cold deferred", models.MediaCirculationUnitItemFamily, mediaCircVerdictMoveToCold, mediaCircOutcomeDeferredToSweep, false},
		{"recoverable_delete deferred", models.MediaCirculationUnitItemFamily, mediaCircVerdictRecoverableDelete, mediaCircOutcomeDeferredToSweep, false},
		{"protect acknowledges", models.MediaCirculationUnitItemFamily, mediaCircVerdictProtect, mediaCircOutcomeAcknowledged, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kind, exec := plannedApplyOutcome(tt.unit, tt.verdict)
			if kind != tt.wantKind || exec != tt.wantExec {
				t.Errorf("plannedApplyOutcome(%s,%s) = (%q,%v), want (%q,%v)",
					tt.unit, tt.verdict, kind, exec, tt.wantKind, tt.wantExec)
			}
		})
	}
}

func TestRollingIntakeBudgetAllows(t *testing.T) {
	if !rollingIntakeBudgetAllows(20, 5, 25) {
		t.Error("exactly exhausted budget should be allowed")
	}
	if rollingIntakeBudgetAllows(21, 5, 25) {
		t.Error("over-budget intake should be refused")
	}
	if !rollingIntakeBudgetAllows(100, 100, 0) {
		t.Error("zero max should disable the budget cap")
	}
}

func TestAtomizationBacklogIntakeFactor(t *testing.T) {
	if got := atomizationBacklogIntakeFactor(mediaCircAtomizationBacklog{BacklogDepth: 0}); got != 1 {
		t.Errorf("empty backlog factor = %.2f, want 1", got)
	}
	if got := atomizationBacklogIntakeFactor(mediaCircAtomizationBacklog{BacklogDepth: 20}); got != 0.6 {
		t.Errorf("large backlog factor = %.2f, want 0.6", got)
	}
}

// Destructive byte-actions must never execute live in v1 (safe-actions-only).
func TestApplyNeverExecutesDestructive(t *testing.T) {
	for _, v := range []string{mediaCircVerdictReEncode, mediaCircVerdictMoveToCold, mediaCircVerdictRecoverableDelete} {
		if _, exec := plannedApplyOutcome(models.MediaCirculationUnitItemFamily, v); exec {
			t.Errorf("verdict %q must not execute live in v1", v)
		}
	}
}

func TestRecommendationAllowedIntake(t *testing.T) {
	rec := models.MediaCirculationRecommendation{
		PublicID: uuid.New(),
		Metrics:  datatypes.JSON([]byte(`{"allowed_intake":3}`)),
	}
	got, err := recommendationAllowedIntake(rec)
	if err != nil {
		t.Fatalf("recommendationAllowedIntake returned error: %v", err)
	}
	if got != 3 {
		t.Errorf("allowed intake = %d, want 3", got)
	}

	rec.Metrics = datatypes.JSON([]byte(`{"allowed_intake":0}`))
	if _, err := recommendationAllowedIntake(rec); err == nil {
		t.Error("zero allowed_intake should fail closed")
	}
}

func TestLimitSourceRunSettingsPreservesStricterExistingCaps(t *testing.T) {
	settings := map[string]interface{}{
		"max_results":                float64(2),
		"initial_atomization_limit":  float64(1),
		"unrelated_source_parameter": "kept",
	}
	got := limitSourceRunSettings(settings, 5)

	if got["max_results"] != 2 || got["maxResults"] != 2 {
		t.Errorf("max results = (%v,%v), want both 2", got["max_results"], got["maxResults"])
	}
	if got["initial_atomization_limit"] != 1 || got["initialAtomizationLimit"] != 1 {
		t.Errorf("atomization limits = (%v,%v), want both 1", got["initial_atomization_limit"], got["initialAtomizationLimit"])
	}
	if got["unrelated_source_parameter"] != "kept" {
		t.Error("unrelated settings should be preserved")
	}
}

func TestLimitSourceRunSettingsAppliesAllowedIntake(t *testing.T) {
	got := limitSourceRunSettings(map[string]interface{}{"maxResults": float64(50)}, 4)
	if got["max_results"] != 4 || got["maxResults"] != 4 {
		t.Errorf("max results = (%v,%v), want both 4", got["max_results"], got["maxResults"])
	}
	if got["initial_atomization_limit"] != 4 || got["initialAtomizationLimit"] != 4 {
		t.Errorf("atomization limits = (%v,%v), want both 4", got["initial_atomization_limit"], got["initialAtomizationLimit"])
	}
}
