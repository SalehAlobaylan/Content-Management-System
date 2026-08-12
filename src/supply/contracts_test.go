package supply

import (
	"content-management-system/src/models"
	"encoding/json"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestSourceRunSharedFixtureMatchesCMSContract(t *testing.T) {
	bytes, err := os.ReadFile("../../../contracts/source-run-v1-fixtures.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		ContractVersion     string              `json:"contract_version"`
		RequestStates       []string            `json:"request_states"`
		AttemptStates       []string            `json:"attempt_states"`
		UnitStates          []string            `json:"unit_states"`
		ManifestStates      []string            `json:"manifest_states"`
		Verdicts            []string            `json:"verdicts"`
		ReceiptCapabilities map[string][]string `json:"receipt_capabilities"`
		TerminalOutcomes    map[string]string   `json:"terminal_outcomes"`
		Identity            struct {
			TenantID      string `json:"tenant_id"`
			RequestID     string `json:"request_id"`
			AttemptID     string `json:"attempt_id"`
			UnitID        string `json:"unit_id"`
			Fence         string `json:"fence"`
			ExpectedJobID string `json:"expected_job_id"`
		} `json:"identity_fixture"`
		Manifest struct {
			UnitKeys       []string `json:"unit_keys"`
			ExpectedDigest string   `json:"expected_digest"`
		} `json:"manifest_fixture"`
	}
	if err := json.Unmarshal(bytes, &fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.ContractVersion != ContractVersion {
		t.Fatalf("fixture contract = %s", fixture.ContractVersion)
	}
	expectedRequest := []string{string(RequestRequested), string(RequestAccepted), string(RequestRunning), string(RequestVerificationRequired), string(RequestCompleted), string(RequestSucceeded), string(RequestPartial), string(RequestBlocked), string(RequestFailed), string(RequestCancelled), string(RequestExpired)}
	expectedAttempt := []string{string(AttemptAuthorized), string(AttemptClaimed), string(AttemptRunning), string(AttemptVerificationRequired), string(AttemptSucceeded), string(AttemptPartial), string(AttemptBlocked), string(AttemptFailed), string(AttemptCancelled), string(AttemptExpired)}
	expectedUnit := []string{string(UnitAuthorized), string(UnitAccepted), string(UnitRunning), string(UnitVerificationRequired), string(UnitSucceeded), string(UnitFailed), string(UnitCancelled), string(UnitExpired)}
	if !reflect.DeepEqual(fixture.RequestStates, expectedRequest) || !reflect.DeepEqual(fixture.AttemptStates, expectedAttempt) || !reflect.DeepEqual(fixture.UnitStates, expectedUnit) || !reflect.DeepEqual(fixture.ManifestStates, []string{string(ManifestOpen), string(ManifestSealing), string(ManifestSealed)}) || !reflect.DeepEqual(fixture.Verdicts, []string{string(VerdictPresent), string(VerdictAbsent), string(VerdictUnknown)}) {
		t.Fatal("shared state fixture drifted from CMS")
	}
	for stage, events := range fixture.ReceiptCapabilities {
		sort.Strings(events)
		actual := []string{}
		for _, event := range []ReceiptEvent{ReceiptEventAccepted, ReceiptEventExecutionStarted, ReceiptEventProviderRequestStarted, ReceiptEventProviderPage, ReceiptEventProviderTerminal, ReceiptEventNormalizeScheduled, ReceiptEventNormalizeTerminal, ReceiptEventFinalization, ReceiptEventFailed, ReceiptEventCancelled, ReceiptEventDLQ} {
			if IsAllowedReceipt(ReceiptStage(stage), event) {
				actual = append(actual, string(event))
			}
		}
		sort.Strings(actual)
		if !reflect.DeepEqual(events, actual) {
			t.Fatalf("receipt capability %s drifted: %v != %v", stage, events, actual)
		}
	}
	for outcome, state := range fixture.TerminalOutcomes {
		actual, ok := TerminalCategoryForOutcome(SourceRunOutcome(outcome))
		if !ok || string(actual) != state {
			t.Fatalf("outcome %s drifted", outcome)
		}
	}
	jobID, err := DeterministicUnitJobID(fixture.Identity.TenantID, fixture.Identity.RequestID, fixture.Identity.AttemptID, fixture.Identity.UnitID, fixture.Identity.Fence)
	if err != nil || jobID != fixture.Identity.ExpectedJobID {
		t.Fatalf("shared job identity drifted: %s %v", jobID, err)
	}
	digest, err := ManifestChildDigest(fixture.Manifest.UnitKeys)
	if err != nil || digest != fixture.Manifest.ExpectedDigest {
		t.Fatalf("shared manifest digest drifted: %s %v", digest, err)
	}
}

func TestSourceRunTransitionsAreForwardOnly(t *testing.T) {
	if !CanTransitionRequest(RequestRequested, RequestAccepted) || !CanTransitionRequest(RequestRunning, RequestVerificationRequired) {
		t.Fatal("expected request transitions are not permitted")
	}
	if CanTransitionRequest(RequestSucceeded, RequestRunning) || CanTransitionAttempt(AttemptSucceeded, AttemptRunning) || CanTransitionUnit(UnitSucceeded, UnitRunning) {
		t.Fatal("terminal source-run state must never regress")
	}
	if CanTransitionUnit(UnitAuthorized, UnitSucceeded) {
		t.Fatal("a unit cannot skip acceptance and effect start")
	}
}

func TestScheduledIdentityRequiresExplicitTenantAndStableDueWindow(t *testing.T) {
	due := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	source := models.ContentSource{PublicID: uuid.New(), TenantID: "tenant-a", Category: models.SourceCategoryMedia, Type: models.SourceTypePodcast, NextDueAt: &due, SourceConfigVersion: 2, FetchIntervalMinutes: 30}
	first, err := scheduledRequestIdentity(source)
	if err != nil {
		t.Fatal(err)
	}
	second, err := scheduledRequestIdentity(source)
	if err != nil || first != second {
		t.Fatalf("scheduled identity must be stable: %v", err)
	}
	source.TenantID = ""
	if _, err := scheduledRequestIdentity(source); err == nil {
		t.Fatal("scheduler must not infer a default tenant")
	}
}

func TestDeferredObservationIdentityIsExactAndStable(t *testing.T) {
	observed := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	source := models.ContentSource{PublicID: uuid.New(), TenantID: "tenant-a", Category: models.SourceCategoryMedia, Type: models.SourceTypePodcast, SourceConfigVersion: 3}
	observation := models.SourceUpstreamObservation{PublicID: uuid.New(), TenantID: source.TenantID, ContentSourceID: source.PublicID, UpstreamItemID: "episode-42", UpstreamFingerprint: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", ProviderVersion: "podcast:v1", ObservedAt: observed}
	first, err := deferredObservationRequestIdentity(source, observation)
	if err != nil {
		t.Fatal(err)
	}
	second, err := deferredObservationRequestIdentity(source, observation)
	if err != nil || first != second || first.Purpose != "deferred_drain" || first.CadenceWindowStart != observed {
		t.Fatalf("deferred identity must bind the exact observation: %#v %v", first, err)
	}
	observation.TenantID = "tenant-b"
	if _, err := deferredObservationRequestIdentity(source, observation); err == nil {
		t.Fatal("cross-tenant observation was admitted")
	}
}

func TestSourceRunBudgetDerivationIsBounded(t *testing.T) {
	budget, err := deriveSourceRunBudget([]byte(`{"max_results":0,"max_provider_calls":4,"max_bytes":4096}`), "baseline")
	if err != nil || budget.Items != 0 || budget.ProviderCalls != 4 || budget.Bytes != 4096 || budget.Workload != 4 {
		t.Fatalf("zero-intake budget drifted: %#v %v", budget, err)
	}
	drain, err := deriveSourceRunBudget([]byte(`{"max_results":1}`), "deferred_drain")
	if err != nil || drain.Items != 1 || drain.ProviderCalls != 12 {
		t.Fatalf("drain defaults drifted: %#v %v", drain, err)
	}
	if _, err := deriveSourceRunBudget([]byte(`{"max_results":1001}`), "baseline"); err == nil {
		t.Fatal("unbounded item budget was admitted")
	}
	if _, err := deriveSourceRunBudget([]byte(`{"max_results":1,"max_provider_calls":0}`), "baseline"); err == nil {
		t.Fatal("zero provider-call budget was admitted")
	}
}

func TestManifestChildDigestIsOrderIndependentAndRejectsInvalidKeys(t *testing.T) {
	first, err := ManifestChildDigest([]string{"normalize:p1:b2", "normalize:p1:b1"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := ManifestChildDigest([]string{"normalize:p1:b1", "normalize:p1:b2"})
	if err != nil || first != second {
		t.Fatalf("manifest digest must be stable across producer order: %q, %q, %v", first, second, err)
	}
	if _, err := ManifestChildDigest([]string{""}); err == nil {
		t.Fatal("blank manifest key must be rejected")
	}
}

func TestAggregateUnitStatesDoesNotHideUnknownOrMixedOutcomes(t *testing.T) {
	attempt, request, terminal := aggregateUnitStates([]models.SourceRunExecutionUnit{{State: string(UnitVerificationRequired)}})
	if terminal || attempt != AttemptVerificationRequired || request != RequestVerificationRequired {
		t.Fatalf("unknown unit must remain verification required: %s %s %v", attempt, request, terminal)
	}
	attempt, request, terminal = aggregateUnitStates([]models.SourceRunExecutionUnit{{State: string(UnitSucceeded)}, {State: string(UnitFailed)}})
	if !terminal || attempt != AttemptPartial || request != RequestPartial {
		t.Fatalf("mixed terminal units must be partial: %s %s %v", attempt, request, terminal)
	}
}

func TestReceiptScopeRejectsCrossUnitEvidence(t *testing.T) {
	normalize := models.SourceRunExecutionUnit{UnitType: "normalize_batch", PageID: "page-1", BatchID: "batch-1"}
	if err := validateReceiptUnitScope(normalize, ReceiptInput{Stage: string(ReceiptStageNormalize), PageID: "page-1", BatchID: "batch-2"}); err == nil {
		t.Fatal("a normalize receipt must not report a sibling batch")
	}
	emptyDigest, err := ManifestChildDigest([]string{})
	if err != nil {
		t.Fatal(err)
	}
	fetch := models.SourceRunExecutionUnit{UnitType: "fetch_page", PageID: "page-1", DeclaredChildDigest: emptyDigest}
	if err := validateReceiptUnitScope(fetch, ReceiptInput{Stage: string(ReceiptStageFetch), EventType: string(ReceiptEventProviderTerminal), PageID: "page-1", FinalPage: true}); err != nil {
		t.Fatalf("matching fetch page evidence should be accepted: %v", err)
	}
}

func TestSourceRunManifestOnlyAuthorizesChildrenWhileOpen(t *testing.T) {
	if !CanAuthorizeManifestChild(ManifestOpen) || !CanTransitionManifest(ManifestOpen, ManifestSealing) || !CanTransitionManifest(ManifestSealing, ManifestSealed) {
		t.Fatal("expected manifest transitions are not permitted")
	}
	if CanAuthorizeManifestChild(ManifestSealing) || CanAuthorizeManifestChild(ManifestSealed) || CanTransitionManifest(ManifestSealed, ManifestOpen) {
		t.Fatal("sealed manifest must reject late children and never reopen")
	}
}

func TestEverySourceRunOutcomeHasOneTerminalCategory(t *testing.T) {
	outcomes := []SourceRunOutcome{
		OutcomeNewItems, OutcomeNoChange, OutcomeUpstreamChangeDeferred, OutcomeObservationBlockedByIntake,
		OutcomeConfigurationBlocked, OutcomePartial, OutcomeProviderFailed, OutcomeCancelled, OutcomeDeadLettered, OutcomeUnknown,
	}
	for _, outcome := range outcomes {
		category, ok := TerminalCategoryForOutcome(outcome)
		if !ok || category == "" {
			t.Fatalf("outcome %q has no terminal category", outcome)
		}
	}
	if _, ok := TerminalCategoryForOutcome("invented"); ok {
		t.Fatal("unregistered source-run outcome was accepted")
	}
}

func TestReceiptCapabilityMatrixRejectsUnregisteredPairings(t *testing.T) {
	if !IsAllowedReceipt(ReceiptStageFetch, ReceiptEventProviderPage) {
		t.Fatal("registered fetch receipt was rejected")
	}
	if IsAllowedReceipt(ReceiptStageDelivery, ReceiptEventProviderPage) || IsAllowedReceipt(ReceiptStage("unknown"), ReceiptEventProviderPage) {
		t.Fatal("unregistered receipt capability was accepted")
	}
}

func TestRequestIdentityIsStableAndRequiresExplicitTenant(t *testing.T) {
	identity := RequestIdentity{
		TenantID: "tenant-a", ContentSourceID: "source-a", Lane: "media", Purpose: "baseline",
		CadenceWindowStart: time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC), SourceConfigVersion: 2,
		PolicyFingerprint: "policy", ArgumentFingerprint: "args",
	}
	first, err := identity.IdempotencyKey()
	if err != nil {
		t.Fatal(err)
	}
	second, err := identity.IdempotencyKey()
	if err != nil || first != second {
		t.Fatalf("idempotency must be deterministic: %q, %q, %v", first, second, err)
	}
	identity.TenantID = ""
	if _, err := identity.IdempotencyKey(); err == nil {
		t.Fatal("implicit tenant must be rejected")
	}
}

func TestLeaseRequiresCurrentFenceTokenAndUnexpiredExecutionLease(t *testing.T) {
	now := time.Now().UTC()
	if !LeaseAllowsEffect("fence-a", "fence-a", "lease-a", "lease-a", now.Add(time.Minute), now) {
		t.Fatal("current fence and lease should authorize effect")
	}
	if LeaseAllowsEffect("fence-a", "fence-b", "lease-a", "lease-a", now.Add(time.Minute), now) || LeaseAllowsEffect("fence-a", "fence-a", "lease-a", "lease-old", now.Add(time.Minute), now) || LeaseAllowsEffect("fence-a", "fence-a", "lease-a", "lease-a", now, now) {
		t.Fatal("stale fence, lease, or expiry must fail closed")
	}
}
