package controllers

import (
	"content-management-system/src/models"
	"encoding/json"
	"errors"
	"github.com/DATA-DOG/go-sqlmock"
	"gorm.io/gorm"
	"testing"
	"time"
)

func TestSystemSafetyReleaseRequiresRecoveryWindow(t *testing.T) {
	for _, state := range []string{"open", "recovering", "closed_by_human", "resolved"} {
		t.Run(state, func(t *testing.T) {
			db, mock := newMockGorm(t)
			now := time.Now().UTC()
			ep := models.SystemIncidentEpisode{ID: 1, RootService: "aggregation", Status: state}
			ep.Containment = marshalAutopilotJSON(systemContainmentLedger{Version: 2, Siblings: map[string]map[string]systemContainmentLedgerEntry{"pipeline": {"tenant": {WrittenUntil: now.Add(time.Hour).Format(time.RFC3339Nano), Outcome: "paused"}}}})
			policy := models.DefaultSystemAutopilotPolicy()
			policy.Enabled = true
			policy.Mode = "safe_auto"
			healthy, _ := healthySystemSnapshotAt(now)
			// Even a resolved episode cannot release with only one healthy sample.
			actions := []models.SystemAutopilotAction{}
			errs := resumeRecoveredSystemContainment(db, policy, []models.SystemIncidentEpisode{ep}, now, systemRecoveryEvidence{Snapshot: healthy}, func(_ *gorm.DB, a models.SystemAutopilotAction) (models.SystemAutopilotAction, error) { return a, nil }, func(a models.SystemAutopilotAction) { actions = append(actions, a) }, true)
			if errs != 0 {
				t.Fatalf("unexpected errors %d", errs)
			}
			for _, a := range actions {
				if a.Action == models.SystemAutopilotActionResumeSibling {
					t.Fatal("released without qualified recovery")
				}
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSystemSafetyVerifiedResolvedIncidentReleases(t *testing.T) {
	db, mock := newMockGorm(t)
	now := time.Now().UTC()
	policy := models.DefaultSystemAutopilotPolicy()
	policy.Enabled = true
	policy.Mode = "safe_auto"
	ep := models.SystemIncidentEpisode{ID: 1, RootService: "aggregation", Status: "resolved"}
	ep.Containment = marshalAutopilotJSON(systemContainmentLedger{Version: 2, Siblings: map[string]map[string]systemContainmentLedgerEntry{"pipeline": {"tenant": {WrittenUntil: now.Add(time.Hour).Format(time.RFC3339Nano), Outcome: "paused"}}}})
	healthy, _ := healthySystemSnapshotAt(now)
	evidence := systemRecoveryEvidence{Snapshot: healthy, Previous: []systemRunSnapshot{{Services: healthy.Services}, {Services: healthy.Services}}}
	mock.ExpectQuery(`SELECT .*system_incident_episodes`).WillReturnRows(sqlmock.NewRows([]string{"id"}))
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT .*system_autopilot_policies.*FOR UPDATE`).WillReturnRows(sqlmock.NewRows([]string{"scope", "enabled", "mode"}).AddRow("platform", true, "safe_auto"))
	mock.ExpectQuery(`SELECT .*system_incident_episodes.*FOR UPDATE`).WillReturnRows(sqlmock.NewRows([]string{"id", "root_service", "status"}).AddRow(1, "aggregation", "resolved"))
	mock.ExpectExec(`UPDATE pipeline_autopilot_policies SET paused_until = NULL`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE "system_incident_episodes"`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	released := false
	errs := resumeRecoveredSystemContainment(db, policy, []models.SystemIncidentEpisode{ep}, now, evidence, func(_ *gorm.DB, a models.SystemAutopilotAction) (models.SystemAutopilotAction, error) { return a, nil }, func(a models.SystemAutopilotAction) {
		released = released || a.Action == models.SystemAutopilotActionResumeSibling
	}, true)
	if errs != 0 || !released {
		t.Fatalf("release failed: errors=%d released=%v", errs, released)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSystemSafetyHumanCloseWinsObservation(t *testing.T) {
	db, mock := newMockGorm(t)
	stale := models.SystemIncidentEpisode{ID: 1, Status: "open"}
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT .*system_incident_episodes.*FOR UPDATE`).WillReturnRows(sqlmock.NewRows([]string{"id", "status", "closed_by", "close_reason"}).AddRow(1, "closed_by_human", "operator", "investigating"))
	mock.ExpectRollback()
	if err := saveSystemEpisodeObservation(db, &stale); !errors.Is(err, errSystemIncidentClosed) {
		t.Fatalf("close was not respected: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSystemSafetyEnrichmentAdmissionChecksCurrentPolicy(t *testing.T) {
	for _, name := range []string{"allowed", "paused", "disabled", "observe", "unavailable"} {
		t.Run(name, func(t *testing.T) {
			db, mock := newMockGorm(t)
			mock.ExpectBegin()
			query := mock.ExpectQuery(`SELECT .*enrichment_autopilot_policies.*FOR SHARE`)
			if name == "unavailable" {
				query.WillReturnError(errors.New("unavailable"))
			} else {
				var until interface{}
				if name == "paused" {
					until = time.Now().Add(time.Hour)
				}
				mode := "safe_auto"
				if name == "observe" {
					mode = "observe"
				}
				query.WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "enabled", "mode", "paused_until"}).AddRow("tenant", name != "disabled", mode, until))
			}
			if name == "allowed" {
				mock.ExpectCommit()
			} else {
				mock.ExpectRollback()
			}
			dispatched := false
			err := withEnrichmentAutopilotAdmission(db, "tenant", func() { dispatched = true })
			if dispatched != (name == "allowed") || (err == nil) != (name == "allowed") {
				t.Fatalf("dispatched=%v err=%v", dispatched, err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSystemSafetyExpiredAndReplacedContainment(t *testing.T) {
	now := time.Now().UTC()
	expired := now.Add(-time.Minute)
	future := now.Add(time.Hour)
	ep := models.SystemIncidentEpisode{Status: "open"}
	for _, tc := range []struct {
		name   string
		entry  systemContainmentLedgerEntry
		actual *time.Time
		want   string
	}{
		{"expired", systemContainmentLedgerEntry{WrittenUntil: expired.Format(time.RFC3339Nano), Outcome: "paused"}, &expired, "expired"},
		{"human extension", systemContainmentLedgerEntry{WrittenUntil: expired.Format(time.RFC3339Nano), Outcome: "paused"}, &future, "human_owned"},
		{"cleared", systemContainmentLedgerEntry{WrittenUntil: future.Format(time.RFC3339Nano), Outcome: "paused"}, nil, "human_owned"},
		{"owned", systemContainmentLedgerEntry{WrittenUntil: future.Format(time.RFC3339Nano), Outcome: "paused"}, &future, "paused"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := projectSystemContainmentTarget(ep, "pipeline", "tenant", tc.entry, systemSiblingPolicyRow{PausedUntil: tc.actual}, now)
			if got.Outcome != tc.want {
				t.Fatalf("%+v", got)
			}
		})
	}
}

func TestSystemSafetyAttentionClearsRecoveredFailures(t *testing.T) {
	episodeID := uint(1)
	episode := models.SystemIncidentEpisode{ID: episodeID, Status: "resolved"}
	failure := models.SystemAutopilotAction{ID: 1, EpisodeID: &episodeID, Target: "pipeline", Status: "error"}
	if got := unresolvedSystemAttention([]models.SystemAutopilotAction{failure}, []models.SystemIncidentEpisode{episode}, systemContainmentProjection{}, nil); len(got) != 0 {
		t.Fatalf("recovered error remains: %+v", got)
	}
	episode.Status = "open"
	if got := unresolvedSystemAttention([]models.SystemAutopilotAction{failure}, []models.SystemIncidentEpisode{episode}, systemContainmentProjection{}, nil); len(got) != 1 {
		t.Fatal("unresolved error disappeared")
	}
	success := failure
	success.ID = 2
	success.Status = "success"
	if got := unresolvedSystemAttention([]models.SystemAutopilotAction{success}, []models.SystemIncidentEpisode{episode}, systemContainmentProjection{}, nil); len(got) != 0 {
		t.Fatal("successful latest decision needs no attention")
	}
	failure.EpisodeID = nil
	failure.RunID = 1
	if got := unresolvedSystemAttention([]models.SystemAutopilotAction{failure}, nil, systemContainmentProjection{}, &models.SystemAutopilotRun{ID: 2}); len(got) != 0 {
		t.Fatal("old observation warning remains")
	}
}

func TestSystemSafetyUnknownNearSampleBreaksRecovery(t *testing.T) {
	now := time.Now().UTC()
	healthy, _ := healthySystemSnapshotAt(now)
	previous := []systemRunSnapshot{{TooClose: true, Services: []systemProbeResult{{Name: "aggregation", Status: "unknown"}}}, {Services: healthy.Services}, {Services: healthy.Services}}
	if got := systemRecoverySamples(models.SystemIncidentEpisode{RootService: "aggregation"}, healthy, previous, 3); got != 1 {
		t.Fatalf("unknown observation was skipped: %d", got)
	}
}

func TestSystemSafetyActiveIncidentMustNotRelease(t *testing.T) {
	db, mock := newMockGorm(t)
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	policy := models.DefaultSystemAutopilotPolicy()
	policy.Enabled, policy.Mode = true, models.SystemAutopilotModeSafeAuto
	ep := models.SystemIncidentEpisode{ID: 1, RootService: "aggregation", Verdict: models.SystemVerdictServiceDown, Status: models.SystemIncidentStatusOpen}
	ep.Containment = marshalAutopilotJSON(systemContainmentLedger{Version: 2, Siblings: map[string]map[string]systemContainmentLedgerEntry{"pipeline": {"default": {WrittenUntil: now.Add(time.Hour).Format(time.RFC3339Nano), Outcome: "paused"}}}})
	var actions []models.SystemAutopilotAction
	store := func(_ *gorm.DB, action models.SystemAutopilotAction) (models.SystemAutopilotAction, error) {
		return action, nil
	}
	failures := resumeRecoveredSystemContainment(db, policy, []models.SystemIncidentEpisode{ep}, now, systemRecoveryEvidence{}, store, func(a models.SystemAutopilotAction) { actions = append(actions, a) }, true)
	if failures != 0 {
		t.Fatalf("fixture error count %d", failures)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	for _, a := range actions {
		if a.Action == models.SystemAutopilotActionResumeSibling {
			t.Fatal("released the pause for an OPEN service-down incident without any recovery evidence")
		}
	}
}

func systemSafetyHistoryRows(now time.Time, ages []time.Duration, anomaly systemAnomaly) *sqlmock.Rows {
	rows := sqlmock.NewRows([]string{"id", "status", "probe_results"})
	for i, age := range ages {
		payload, _ := json.Marshal(map[string]interface{}{"run_snapshot": systemRunSnapshot{Timestamp: now.Add(-age).Format(time.RFC3339Nano), Anomalies: []systemAnomaly{anomaly}}})
		rows.AddRow(i+1, "completed", string(payload))
	}
	return rows
}

func TestSystemSafetyBurstMustNotConfirm(t *testing.T) {
	db, mock := newMockGorm(t)
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	a := systemAnomaly{Key: "aggregation:service_down", Service: "aggregation", Verdict: models.SystemVerdictServiceDown}
	mock.ExpectQuery(`SELECT .*system_autopilot_runs`).WillReturnRows(systemSafetyHistoryRows(now, []time.Duration{time.Second}, a))
	previous := recentSystemRunSnapshots(db, 12, now, 10)
	if got := confirmSystemAnomalies([]systemAnomaly{a}, previous, 2); len(got) > 0 {
		t.Fatal("two probes one second apart confirmed an incident despite a ten-minute cadence")
	}
}

func TestSystemSafetyFourSpacedObservationsMustConfirm(t *testing.T) {
	db, mock := newMockGorm(t)
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	a := systemAnomaly{Key: "aggregation:service_down", Service: "aggregation", Verdict: models.SystemVerdictServiceDown}
	mock.ExpectQuery(`SELECT .*system_autopilot_runs`).WillReturnRows(systemSafetyHistoryRows(now, []time.Duration{10 * time.Minute, 20 * time.Minute, 30 * time.Minute}, a))
	previous := recentSystemRunSnapshots(db, 12, now, 10)
	if got := confirmSystemAnomalies([]systemAnomaly{a}, previous, 4); len(got) == 0 {
		t.Fatalf("four consecutive on-cadence observations never confirm: only %d prior samples survive", len(previous))
	}
}

func TestSystemSafetyAIOutageMustStillContainEnrichment(t *testing.T) {
	a := []systemAnomaly{{Key: "enrichment:service_down", Service: "enrichment", Verdict: models.SystemVerdictServiceDown}, {Key: "media:service_down", Service: "media", Verdict: models.SystemVerdictServiceDown}}
	sibling, _ := systemSiblingByKey("enrichment")
	for _, incident := range correlateSystemAnomalies(a) {
		if siblingDependsOnAnomaly(sibling, incident) {
			return
		}
	}
	t.Fatal("simultaneous Enrichment and Media failure is mapped to CMS and no longer contains Enrichment automation")
}

func TestSystemSafetyRevokedPolicyMustPreserveOwnership(t *testing.T) {
	db, mock := newMockGorm(t)
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	until := now.Add(time.Hour)
	policy := models.DefaultSystemAutopilotPolicy()
	policy.Enabled, policy.Mode = true, models.SystemAutopilotModeSafeAuto
	policy.ContainmentDisabledFor = marshalAutopilotJSON([]string{"enrichment", "embedding_lifecycle", "news_circulation", "media_circulation", "media_studio", "redundancy"})
	ep := models.SystemIncidentEpisode{ID: 1, RootService: "aggregation", Verdict: models.SystemVerdictServiceDown, Status: models.SystemIncidentStatusOpen}
	ep.Containment = marshalAutopilotJSON(systemContainmentLedger{Version: 2, Siblings: map[string]map[string]systemContainmentLedgerEntry{"pipeline": {defaultCirculationTenant: {WrittenUntil: until.Format(time.RFC3339Nano), Outcome: "paused"}}}})
	mock.ExpectQuery(`SELECT .*pipeline_autopilot_policies`).WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "paused_until"}).AddRow(defaultCirculationTenant, until))
	mock.ExpectQuery(`SELECT tenant_id, paused_until AS paused_until FROM pipeline_autopilot_policies`).WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "paused_until"}).AddRow(defaultCirculationTenant, until))
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT .*system_autopilot_policies`).WillReturnRows(sqlmock.NewRows([]string{"scope", "enabled", "mode"}).AddRow("platform", true, "observe"))
	mock.ExpectQuery(`SELECT .*system_incident_episodes.*FOR UPDATE`).WillReturnRows(sqlmock.NewRows([]string{"id", "status"}).AddRow(1, "open"))
	mock.ExpectExec(`UPDATE "system_incident_episodes"`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	store := func(_ *gorm.DB, a models.SystemAutopilotAction) (models.SystemAutopilotAction, error) { return a, nil }
	_, errs := handleSystemContainment(db, policy, systemAnomaly{Service: "aggregation", Verdict: models.SystemVerdictServiceDown}, &ep, now, store, func(models.SystemAutopilotAction) {}, true)
	if errs != 0 {
		t.Fatalf("fixture errors: %d", errs)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	ledger, _ := readSystemContainmentLedger(ep.Containment)
	entry, _ := containmentEntry(ledger, "pipeline", defaultCirculationTenant)
	if entry.WrittenUntil == "" {
		t.Fatalf("revoking Safe Auto erased the ownership proof for a still-existing pause: %+v", entry)
	}
}
