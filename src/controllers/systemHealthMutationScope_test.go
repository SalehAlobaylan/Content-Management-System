package controllers

import (
	"content-management-system/src/models"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"gorm.io/gorm"
)

func TestSystemMutationScopeBoundsUnionIncludingRelease(t *testing.T) {
	episodes := []models.SystemIncidentEpisode{}
	for incident := 0; incident < 2; incident++ {
		tenants := map[string]systemContainmentLedgerEntry{}
		for n := 0; n < 60; n++ {
			tenants[fmt.Sprintf("%d-%d", incident, n)] = systemContainmentLedgerEntry{WrittenUntil: time.Now().Add(time.Hour).Format(time.RFC3339Nano), Outcome: "paused"}
		}
		episodes = append(episodes, models.SystemIncidentEpisode{Status: "resolved", Containment: marshalAutopilotJSON(systemContainmentLedger{Version: 2, Siblings: map[string]map[string]systemContainmentLedgerEntry{"enrichment": tenants}})})
	}
	scope, err := prepareSystemMutationScope(nil, models.DefaultSystemAutopilotPolicy(), nil, episodes)
	if err != nil || !scope.Denied || len(scope.Targets) != 120 {
		t.Fatalf("cross-incident release scope escaped cap: %+v, %v", scope, err)
	}
	db, _ := newMockGorm(t)
	db = db.WithContext(context.WithValue(context.Background(), systemMutationScopeKey{}, &scope))
	if systemMutationTargetAllowed(db, "enrichment", "0-0") {
		t.Fatal("oversized run may still change a pause")
	}
	// Deduplicate the same native policy across episodes.
	scope, err = prepareSystemMutationScope(nil, models.DefaultSystemAutopilotPolicy(), nil, []models.SystemIncidentEpisode{episodes[0], episodes[0]})
	if err != nil || scope.Denied || len(scope.Targets) != 60 {
		t.Fatalf("duplicate targets counted twice: %+v", scope)
	}
	db = db.WithContext(context.WithValue(context.Background(), systemMutationScopeKey{}, &scope))
	if systemMutationTargetAllowed(db, "enrichment", "new-tenant") {
		t.Fatal("post-preflight target expansion was admitted")
	}
}

func TestSystemFlappingRetainsIncidentAndBlocksEffects(t *testing.T) {
	db, mock := newMockGorm(t)
	mock.ExpectQuery(`SELECT count.*system_incident_episodes`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(3))
	anomalies := applySystemFlapGuard(db, []systemAnomaly{{Service: "enrichment", Verdict: models.SystemVerdictServiceDown}}, 3, time.Now(), func(models.SystemAutopilotAction) {})
	if len(anomalies) != 1 || anomalies[0].ActionsBlocked != models.SystemAutopilotGuardFlapping {
		t.Fatal("flapping hid the incident or admitted controls")
	}
	policy := models.DefaultSystemAutopilotPolicy()
	policy.Enabled, policy.Mode = true, "safe_auto"
	ep := models.SystemIncidentEpisode{ID: 1, Status: "open"}
	blocked := false
	applied, errs := handleSystemContainment(db, policy, anomalies[0], &ep, time.Now(), func(_ *gorm.DB, a models.SystemAutopilotAction) (models.SystemAutopilotAction, error) { return a, nil }, func(a models.SystemAutopilotAction) { blocked = a.Guardrail == models.SystemAutopilotGuardFlapping }, true)
	if applied || errs != 0 || !blocked {
		t.Fatalf("flap effect result: %v %d %v", applied, errs, blocked)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
