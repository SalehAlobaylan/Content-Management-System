package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/podsflow"
	"content-management-system/src/utils"
	"github.com/google/uuid"
	"reflect"
	"testing"
)

func TestJourneyCapacityNeverDisclosesOtherTenantOrOverridesApproval(t *testing.T) {
	other, holder := "tenant-b", uuid.New()
	steps := []mediaJourneyStep{{Key: "planning", State: "queued"}, {Key: "download", State: "waiting", WaitingCategory: "operator"}}
	result := journeyCapacity(nil, models.ContentItem{TenantID: "tenant-a", PublicID: uuid.New()}, podsflow.Slot{TenantID: &other, RootContentItemID: &holder}, steps)
	if result["waiting"] != true || len(result) != 1 {
		t.Fatalf("cross-tenant disclosure: %+v", result)
	}
	if steps[0].WaitingCategory != "execution_capacity" || steps[1].WaitingCategory != "operator" {
		t.Fatalf("wrong wait precedence: %+v", steps)
	}
}

func TestJourneyDependenciesAllowParallelReviewReadinessAndDirectBypass(t *testing.T) {
	steps := []mediaJourneyStep{{Key: "review", Required: true}, {Key: "readiness", Required: true}, {Key: "published", Required: true}}
	journeyDependencies(steps, false)
	if !reflect.DeepEqual(steps[0].DependsOn, steps[1].DependsOn) || !reflect.DeepEqual(steps[2].DependsOn, []string{"review", "readiness"}) {
		t.Fatalf("wrong family dependencies: %+v", steps)
	}
	steps[0].Required = false
	journeyDependencies(steps, true)
	if len(steps[0].DependsOn) != 0 || !reflect.DeepEqual(steps[1].DependsOn, []string{"preparation"}) || !reflect.DeepEqual(steps[2].DependsOn, []string{"readiness"}) {
		t.Fatalf("direct media must bypass chaptering: %+v", steps)
	}
}

func TestChapterDraftActiveAndPredecessorGuardsDoNotPerformEffects(t *testing.T) {
	for _, tc := range []struct{ state, reason string }{{"claimed", "effects_active"}, {"running", "effects_active"}, {"verifying", "effects_active"}, {"uncertain", "effects_unresolved"}, {"reconciling", "effects_unresolved"}, {"blocked", "prerequisites_required"}} {
		// nil DB deliberately proves these pure guards return before querying or writing.
		reason, err := draftApplyBlock(nil, models.ContentItem{}, []models.ContentStageRequest{{State: tc.state}})
		if err != nil || reason != tc.reason {
			t.Fatalf("%s: %s %v", tc.state, reason, err)
		}
	}
}

func TestJourneyActionsRespectPermissionsAndUncertainEffects(t *testing.T) {
	item := mediaAtomizationPipelineItem{AllowedActions: []string{"download", "retry_atomization", "inspect"}, UnresolvedEffects: 1}
	for _, a := range journeyActions(item, utils.AdminPrincipal{Permissions: []string{"content:read"}}) {
		if a.Code == "download" && (a.Enabled || a.Reason != "permission_required") {
			t.Fatalf("write exposed: %+v", a)
		}
		if a.Code == "retry_atomization" && (a.Enabled || a.Reason != "effects_unresolved") {
			t.Fatalf("unsafe retry: %+v", a)
		}
		if a.Code == "inspect" && !a.Enabled {
			t.Fatal("read-only inspection unavailable")
		}
	}
}

func TestJourneyPredecessorIsNotApproval(t *testing.T) {
	s := journeyStage("transcript", &models.ContentStageRequest{State: models.ContentStageBlocked})
	if s.WaitingCategory != "predecessor" || s.Reason != "predecessor_required" {
		t.Fatalf("wrong wait: %+v", s)
	}
}

func TestJourneyCuttingUsesUnitEvidence(t *testing.T) {
	for _, tc := range []struct {
		units map[string]int64
		state string
	}{
		{map[string]int64{"queued": 3}, "queued"},
		{map[string]int64{"claimed": 1, "verified": 2}, "queued"},
		{map[string]int64{"running": 1}, "running"},
		{map[string]int64{"running": 1, "uncertain": 1}, "reconciling"},
		{map[string]int64{"failed": 1}, "failed"},
	} {
		if got := journeyCuttingState(tc.units); got.State != tc.state {
			t.Fatalf("%v: %+v", tc.units, got)
		}
	}
}

func TestChapterDraftNormalizesEditorialInput(t *testing.T) {
	plan := normalizeDraft([]map[string]any{{"title": "  Chapter  ", "start_ms": float64(0), "end_ms": float64(3000000), "artifact_manifest_id": "untrusted", "review_status": "approved"}})
	if len(plan[0]) != 4 || plan[0]["title"] != "Chapter" || plan[0]["source"] != "manual" {
		t.Fatalf("unexpected draft: %v", plan)
	}
	if len(validateDraft(plan, 3000)) == 0 {
		t.Fatal("overlong chapter accepted")
	}
	plan = normalizeDraft([]map[string]any{{"title": "One", "start_ms": float64(0), "end_ms": float64(1500000)}, {"title": "Two", "start_ms": float64(1500000), "end_ms": float64(3000000)}})
	if errors := validateDraft(plan, 3000); len(errors) != 0 {
		t.Fatalf("valid draft rejected: %v", errors)
	}
	plan[1]["start_ms"] = float64(1500001)
	if len(validateDraft(plan, 3000)) == 0 {
		t.Fatal("coverage gap accepted")
	}
}
