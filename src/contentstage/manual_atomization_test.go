package contentstage

import (
	"content-management-system/src/models"
	"github.com/google/uuid"
	"testing"
	"time"
)

func TestLegacyPlanRetirementRejectsAnyExecutionEvidence(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*models.AtomizationChapterUnit)
	}{
		{"attempt", func(u *models.AtomizationChapterUnit) { u.AttemptCount = 1 }},
		{"claim", func(u *models.AtomizationChapterUnit) { id := uuid.New(); u.ClaimToken = &id }},
		{"fence", func(u *models.AtomizationChapterUnit) { id := uuid.New(); u.FenceToken = &id }},
		{"effect", func(u *models.AtomizationChapterUnit) { now := time.Now(); u.EffectStartedAt = &now }},
		{"child", func(u *models.AtomizationChapterUnit) { id := uuid.New(); u.CandidateContentItemID = &id }},
		{"result", func(u *models.AtomizationChapterUnit) { u.Result = jsonValue(map[string]any{"media_url": "stored"}) }},
		{"artifacts", func(u *models.AtomizationChapterUnit) { u.ArtifactManifestIDs = jsonValue([]string{uuid.NewString()}) }},
		{"verified", func(u *models.AtomizationChapterUnit) { u.State = "verified" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			unit := models.AtomizationChapterUnit{State: "queued"}
			if !legacyUnitNeverStarted(unit) {
				t.Fatal("pristine queued unit rejected")
			}
			tc.change(&unit)
			if legacyUnitNeverStarted(unit) {
				t.Fatal("execution evidence was discarded")
			}
		})
	}
}

func TestLegacyPlanRetirementAllowsPersistedPlannerInput(t *testing.T) {
	plan := map[string]any{
		"title": "Chapter", "summary": nil, "start_ms": 0, "end_ms": 600000,
		"confidence": 0.65, "context_label": nil, "boundary_reason": "coverage_gap_fallback",
		"standalone_score": 0.65, "needs_review_reason": "Fallback chapter",
		"needs_review_code": "sponsor_intro", "needs_review_codes": []string{"sponsor_intro"},
		"contains_sponsor_intro": false,
	}
	unit := models.AtomizationChapterUnit{State: "queued", EndMs: 600000, Result: jsonValue(plan)}
	if !legacyUnitNeverStarted(unit) {
		t.Fatal("planner input was mistaken for execution output")
	}
	for _, field := range []string{"media_url", "playback_url", "artifact_manifest_ids", "unknown_output"} {
		plan[field] = "evidence"
		unit.Result = jsonValue(plan)
		if legacyUnitNeverStarted(unit) {
			t.Fatalf("accepted output field %s", field)
		}
		delete(plan, field)
	}
	plan["end_ms"] = 601000
	unit.Result = jsonValue(plan)
	if legacyUnitNeverStarted(unit) {
		t.Fatal("accepted changed plan boundaries")
	}
}
