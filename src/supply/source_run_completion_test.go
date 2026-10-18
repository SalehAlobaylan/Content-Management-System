package supply

import (
	"content-management-system/src/models"
	"encoding/json"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"os"
	"strings"
	"testing"
	"time"
)

func TestActualReducerPreservesEveryTypedTerminalOutcome(t *testing.T) {
	for outcome, want := range terminalOutcomeCategories {
		if outcome == OutcomeUnknown {
			continue
		}
		t.Run(string(outcome), func(t *testing.T) {
			state := terminalUnitState(ReceiptEventProviderTerminal, outcome)
			attempt, request, terminal := aggregateUnitStates([]models.SourceRunExecutionUnit{{UnitType: "fetch_page", State: string(state), TerminalOutcome: string(outcome)}})
			if !terminal || request != want || string(attempt) != string(want) {
				t.Fatalf("producer outcome %s became %s/%s terminal=%v", outcome, attempt, request, terminal)
			}
		})
	}
	_, request, terminal := aggregateUnitStates([]models.SourceRunExecutionUnit{{State: string(UnitSucceeded)}})
	if terminal || request != RequestVerificationRequired {
		t.Fatal("legacy missing outcome was invented as success")
	}
}

func TestSourceRunDBSchedulerDoesNotLetBlockedOrDisabledSourcesConsumeFairnessSlots(t *testing.T) {
	db := openSupplyFixtureDB(t)
	source := provisionSupplyFixture(t, db, "tenant-a", "media")
	source.NextDueAt = completionTime(time.Now().UTC().Add(-time.Hour))
	if err := db.Save(&source).Error; err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		blocker := source
		blocker.ID = 0
		blocker.PublicID = uuid.New()
		blocker.NextDueAt = completionTime(time.Now().UTC().Add(-3 * time.Hour))
		if err := db.Create(&blocker).Error; err != nil {
			t.Fatal(err)
		}
		request, _, err := CreateRequest(db, CreateRequestInput{Source: blocker, Identity: supplyFixtureIdentity(blocker), RequestedBy: "schedule", Metadata: datatypes.JSON([]byte(`{"max_results":1}`))})
		if err != nil {
			t.Fatal(err)
		}
		if err = db.Model(&request).Update("state", string(RequestVerificationRequired)).Error; err != nil {
			t.Fatal(err)
		}
	}
	disabled := provisionSupplyFixture(t, db, "disabled", "media")
	if err := db.Model(&disabled).Update("next_due_at", time.Now().UTC().Add(-10*time.Hour)).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&models.MediaSupplyControl{TenantID: "disabled", ControlKey: models.MediaSupplyControlNormalIntakeScheduling, ScopeType: "tenant", ScopeID: "all", DisabledAt: time.Now(), DisabledBy: "fixture", Reason: "test"}).Error; err != nil {
		t.Fatal(err)
	}
	requests, err := AdmitDueSourceRuns(db, time.Now().UTC(), 1)
	if err != nil || len(requests) != 1 || requests[0].ContentSourceID != source.PublicID {
		t.Fatalf("eligible source starved: %+v %v", requests, err)
	}
}

func completionTime(value time.Time) *time.Time { return &value }

func TestSourceRunDBProviderAttemptTimeUsesActualStartAndCannotRegress(t *testing.T) {
	db := openSupplyFixtureDB(t)
	source := provisionSupplyFixture(t, db, "tenant-a", "media")
	request, _, err := CreateRequest(db, CreateRequestInput{Source: source, Identity: supplyFixtureIdentity(source), RequestedBy: "schedule", Metadata: datatypes.JSON([]byte(`{"max_results":1}`))})
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := CreateAttemptAndRootUnit(db, source.TenantID, request.PublicID.String())
	if err != nil {
		t.Fatal(err)
	}
	page, _, err := AuthorizeChildUnit(db, ChildUnitInput{TenantID: source.TenantID, RequestID: request.PublicID.String(), AttemptID: attempt.Attempt.PublicID.String(), ParentUnitID: attempt.RootExecutionUnit.PublicID.String(), UnitType: "fetch_page", UnitKey: "fetch:initial", PageID: "initial"})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := AcquireUnitExecution(db, source.TenantID, page.PublicID.String(), "aggregation", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now().UTC().Add(-2 * time.Minute).Truncate(time.Microsecond)
	for sequence, produced := range []time.Time{started, started.Add(-time.Minute), started.Add(30 * time.Second)} {
		if sequence == 1 {
			if err := db.Model(&page).Update("state", string(UnitSucceeded)).Error; err != nil {
				t.Fatal(err)
			}
		}
		input := ReceiptInput{TenantID: source.TenantID, ProducerEventKey: uuid.NewString(), SourceRunRequestID: request.PublicID, SourceRunAttemptID: attempt.Attempt.PublicID, ExecutionUnitID: page.PublicID, ContentSourceID: source.PublicID, UnitJobID: page.JobID, AttemptFenceToken: attempt.Attempt.FenceToken, ExecutionLeaseToken: lease.LeaseToken, SchemaVersion: ContractVersion, Producer: "aggregation", Stage: string(ReceiptStageFetch), EventType: string(ReceiptEventProviderRequestStarted), Outcome: string(OutcomeUnknown), Sequence: int64(sequence + 1), PageID: "initial", Payload: datatypes.JSON([]byte(`{"provider_calls":1}`)), PayloadDigest: "fixture", ProducedAt: produced}
		if _, _, err := RecordReceipt(db, input); err != nil {
			t.Fatal(err)
		}
		work, claimed, err := ClaimNextProjectionWork(db, "fixture", time.Minute)
		if err != nil || !claimed {
			t.Fatalf("claim %v %v", claimed, err)
		}
		if err := ApplyProjectionLease(db, work, "fixture"); err != nil {
			t.Fatal(err)
		}
		if err := db.First(&source, "public_id=?", source.PublicID).Error; err != nil {
			t.Fatal(err)
		}
		want := started
		if sequence == 2 {
			want = produced
		}
		if source.LastAttemptedAt == nil || !source.LastAttemptedAt.Equal(want) {
			t.Fatalf("attempt timestamp became receipt time, regressed, or lost late evidence: %v, want %v", source.LastAttemptedAt, want)
		}
	}
}

func TestSourceRunDBNormalizeCrashCannotVerifyAPersistedPrefix(t *testing.T) {
	db := openSupplyFixtureDB(t)
	source := provisionSupplyFixture(t, db, "tenant-a", "media")
	request, _, err := CreateRequest(db, CreateRequestInput{Source: source, Identity: supplyFixtureIdentity(source), RequestedBy: "schedule", Metadata: datatypes.JSON([]byte(`{"max_results":2}`))})
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := CreateAttemptAndRootUnit(db, source.TenantID, request.PublicID.String())
	if err != nil {
		t.Fatal(err)
	}
	page, _, err := AuthorizeChildUnit(db, ChildUnitInput{TenantID: source.TenantID, RequestID: request.PublicID.String(), AttemptID: attempt.Attempt.PublicID.String(), ParentUnitID: attempt.RootExecutionUnit.PublicID.String(), UnitType: "fetch_page", UnitKey: "fetch:initial", PageID: "initial"})
	if err != nil {
		t.Fatal(err)
	}
	inputs := []NormalizeInput{{UpstreamItemID: "first", ObservationID: uuid.NewString(), Fingerprint: strings.Repeat("a", 64)}, {UpstreamItemID: "second", ObservationID: uuid.NewString(), Fingerprint: strings.Repeat("b", 64)}}
	for _, input := range inputs {
		observationID := uuid.MustParse(input.ObservationID)
		if err := db.Create(&models.SourceUpstreamObservation{PublicID: observationID, TenantID: source.TenantID, ContentSourceID: source.PublicID, SourceRunRequestID: &request.PublicID, ProviderCapability: "replayable_listing", ProviderVersion: "fixture/v1", UpstreamItemID: input.UpstreamItemID, UpstreamFingerprint: input.Fingerprint, ReplayLocator: datatypes.JSON([]byte(`{}`)), ProviderPageID: "initial", ObservedAt: time.Now()}).Error; err != nil {
			t.Fatal(err)
		}
	}
	unit, _, err := AuthorizeChildUnit(db, ChildUnitInput{TenantID: source.TenantID, RequestID: request.PublicID.String(), AttemptID: attempt.Attempt.PublicID.String(), ParentUnitID: page.PublicID.String(), UnitType: "normalize_batch", UnitKey: "normalize:initial:0", PageID: "initial", BatchID: "0", NormalizeInputs: inputs})
	if err != nil {
		t.Fatal(err)
	}
	title := "first"
	content := models.ContentItem{PublicID: uuid.New(), TenantID: source.TenantID, Type: models.ContentTypePodcast, Status: models.ContentStatusPending, Title: &title, ContentSourceID: &source.PublicID, Metadata: datatypes.JSON([]byte(`{"source_run_execution_unit_id":"` + unit.PublicID.String() + `"}`))}
	if err := db.Create(&content).Error; err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(map[string]string{"source_run_request_id": request.PublicID.String(), "execution_unit_id": unit.PublicID.String(), "content_item_id": content.PublicID.String()})
	first := models.SourceUpstreamObservationEvent{PublicID: uuid.New(), TenantID: source.TenantID, EventKey: uuid.NewString(), ObservationID: uuid.MustParse(inputs[0].ObservationID), EventType: "materialized", CausationID: unit.PublicID.String(), Payload: payload, OccurredAt: time.Now()}
	if err := db.Create(&first).Error; err != nil {
		t.Fatal(err)
	}
	expected, observed, complete, err := observeNormalizeBatch(db, unit)
	if err != nil || complete || expected != 2 {
		t.Fatalf("partial crash was declared complete: %d/%d %v %v", observed, expected, complete, err)
	}
	if verdict, _ := observationVerdict("normalize_batch", observed, expected, complete); verdict != VerdictUnknown {
		t.Fatal("prefix proved whole batch")
	}
	second := first
	second.ID = 0
	second.PublicID = uuid.New()
	second.EventKey = uuid.NewString()
	second.ObservationID = uuid.MustParse(inputs[1].ObservationID)
	second.EventType = "filtered"
	second.Payload = datatypes.JSON([]byte(`{"source_run_request_id":"` + request.PublicID.String() + `","execution_unit_id":"` + unit.PublicID.String() + `","filter_class":"exact_duplicate"}`))
	if err := db.Create(&second).Error; err != nil {
		t.Fatal(err)
	}
	expected, observed, complete, err = observeNormalizeBatch(db, unit)
	if err != nil || !complete || observed != 1 {
		t.Fatalf("complete exact dispositions not verified: %d/%d %v %v", observed, expected, complete, err)
	}
	unit.NormalizeInputDigest = strings.Repeat("c", 64)
	if _, _, complete, err := observeNormalizeBatch(db, unit); err != nil || complete {
		t.Fatal("changed cohort digest was trusted")
	}
	unit.NormalizeInputs = nil
	unit.NormalizeInputDigest = ""
	if _, _, complete, err := observeNormalizeBatch(db, unit); err != nil || complete {
		t.Fatal("legacy missing cohort was invented as empty")
	}
}

func TestSourceRunDBTypedReceiptProjectsThroughCoordinator(t *testing.T) {
	for _, outcome := range []SourceRunOutcome{OutcomePartial, OutcomeObservationBlockedByIntake, OutcomeConfigurationBlocked} {
		t.Run(string(outcome), func(t *testing.T) {
			db := openSupplyFixtureDB(t)
			source := provisionSupplyFixture(t, db, "tenant-a", "media")
			request, _, err := CreateRequest(db, CreateRequestInput{Source: source, Identity: supplyFixtureIdentity(source), RequestedBy: "schedule", Metadata: datatypes.JSON([]byte(`{"max_results":1}`))})
			if err != nil {
				t.Fatal(err)
			}
			attempt, err := CreateAttemptAndRootUnit(db, source.TenantID, request.PublicID.String())
			if err != nil {
				t.Fatal(err)
			}
			page, _, err := AuthorizeChildUnit(db, ChildUnitInput{TenantID: source.TenantID, RequestID: request.PublicID.String(), AttemptID: attempt.Attempt.PublicID.String(), ParentUnitID: attempt.RootExecutionUnit.PublicID.String(), UnitType: "fetch_page", UnitKey: "fetch:initial", PageID: "initial"})
			if err != nil {
				t.Fatal(err)
			}
			lease, err := AcquireUnitExecution(db, source.TenantID, page.PublicID.String(), "aggregation", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			if err := FreezeFetchPageChildren(db, source.TenantID, page.PublicID.String(), 0, emptyManifestDigest(t)); err != nil {
				t.Fatal(err)
			}
			input := ReceiptInput{TenantID: source.TenantID, ProducerEventKey: "terminal-" + string(outcome), SourceRunRequestID: request.PublicID, SourceRunAttemptID: attempt.Attempt.PublicID, ExecutionUnitID: page.PublicID, ContentSourceID: source.PublicID, UnitJobID: page.JobID, AttemptFenceToken: attempt.Attempt.FenceToken, ExecutionLeaseToken: lease.LeaseToken, SchemaVersion: ContractVersion, Producer: "aggregation", Stage: string(ReceiptStageFetch), EventType: string(ReceiptEventProviderTerminal), Outcome: string(outcome), Sequence: 2, PageID: "initial", FinalPage: true, Payload: datatypes.JSON([]byte(`{"accepted":0}`)), PayloadDigest: "fixture", ProducedAt: time.Now().UTC()}
			if _, _, err := RecordReceipt(db, input); err != nil {
				t.Fatal(err)
			}
			work, claimed, err := ClaimNextProjectionWork(db, "fixture", time.Minute)
			if err != nil || !claimed {
				t.Fatalf("claim %v %v", claimed, err)
			}
			if err := ApplyProjectionLease(db, work, "fixture"); err != nil {
				t.Fatal(err)
			}
			if err := AdvanceSourceRunManifests(db, 10); err != nil {
				t.Fatal(err)
			}
			if err := db.First(&request, "public_id=?", request.PublicID).Error; err != nil {
				t.Fatal(err)
			}
			want, _ := TerminalCategoryForOutcome(outcome)
			if request.State != string(want) {
				t.Fatalf("typed receipt became %s, want %s", request.State, want)
			}
			if err := db.First(&source, "public_id=?", source.PublicID).Error; err != nil {
				t.Fatal(err)
			}
			if want == RequestBlocked && source.LastProviderSuccessAt != nil {
				t.Fatal("blocked observation manufactured provider success")
			}
		})
	}
}

func TestSourceRunDBCompletionMigrationPreservesAndFencesHistory(t *testing.T) {
	db := openSupplyFixtureDB(t)
	source := provisionSupplyFixture(t, db, "tenant-a", "media")
	request, _, err := CreateRequest(db, CreateRequestInput{Source: source, Identity: supplyFixtureIdentity(source), RequestedBy: "schedule", Metadata: datatypes.JSON([]byte(`{"max_results":1}`))})
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := CreateAttemptAndRootUnit(db, source.TenantID, request.PublicID.String())
	if err != nil {
		t.Fatal(err)
	}
	// This fixture-only schema recreates the pre-migration historical shape.
	if err := db.Exec("ALTER TABLE source_run_execution_units DROP COLUMN normalization_inputs, DROP COLUMN normalization_input_digest").Error; err != nil {
		t.Fatal(err)
	}
	migration, err := os.ReadFile("../../migrations/20261018010000_source_run_completion_evidence.sql")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Transaction(func(tx *gorm.DB) error { return tx.Exec(string(migration)).Error }); err != nil {
		t.Fatal(err)
	}
	var historical models.SourceRunExecutionUnit
	if err := db.First(&historical, "public_id=?", attempt.RootExecutionUnit.PublicID).Error; err != nil {
		t.Fatal(err)
	}
	if string(historical.NormalizeInputs) != "[]" || historical.NormalizeInputDigest != "" || historical.JobID != attempt.RootExecutionUnit.JobID {
		t.Fatal("migration changed historical authority")
	}
	if err := db.Model(&historical).Update("normalization_input_digest", strings.Repeat("a", 64)).Error; err == nil {
		t.Fatal("migration allowed cohort replacement on historical unit")
	}
	var count int64
	if err := db.Model(&models.SourceRunRequest{}).Count(&count).Error; err != nil || count != 1 {
		t.Fatal("migration lost source request history")
	}
}
