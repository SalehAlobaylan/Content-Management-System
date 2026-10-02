package controllers

import (
	"encoding/json"
	"testing"
	"time"

	"content-management-system/src/contentreset"
	"content-management-system/src/feedstate"
	"content-management-system/src/models"
	"github.com/google/uuid"
)

func fullLaneRequests() []contentResetPlanRequest {
	return []contentResetPlanRequest{
		{Operation: "clear", Lane: "news", CapacityStrategy: "none", ProcessingStrategy: "none", InteractionPolicy: "protect", CoveragePolicy: "preserve_protected", IntakeAfter: "continue", Replay: contentResetReplay{Mode: "none"}},
		{Operation: "clear", Lane: "pods", CapacityStrategy: "none", ProcessingStrategy: "none", InteractionPolicy: "protect", CoveragePolicy: "preserve_protected", IntakeAfter: "continue", Replay: contentResetReplay{Mode: "none"}},
		{Operation: "clear", Lane: "both", CapacityStrategy: "none", ProcessingStrategy: "none", InteractionPolicy: "protect", CoveragePolicy: "preserve_protected", IntakeAfter: "continue", Replay: contentResetReplay{Mode: "none"}},
		{Operation: "empty", Lane: "both", CapacityStrategy: "none", ProcessingStrategy: "none", InteractionPolicy: "protect", CoveragePolicy: "require_exact", IntakeAfter: "paused", Replay: contentResetReplay{Mode: "none"}},
		{Operation: "fresh_start", Lane: "news", CapacityStrategy: "build_first", ProcessingStrategy: "copy_verified", InteractionPolicy: "protect", CoveragePolicy: "preserve_protected", IntakeAfter: "continue", Replay: contentResetReplay{Mode: "bounded_recent"}},
		{Operation: "fresh_start", Lane: "pods", CapacityStrategy: "build_first", ProcessingStrategy: "copy_verified", InteractionPolicy: "protect", CoveragePolicy: "preserve_protected", IntakeAfter: "continue", Replay: contentResetReplay{Mode: "bounded_recent"}},
		{Operation: "fresh_start", Lane: "both", CapacityStrategy: "build_first", ProcessingStrategy: "copy_verified", InteractionPolicy: "protect", CoveragePolicy: "preserve_protected", IntakeAfter: "continue", Replay: contentResetReplay{Mode: "bounded_recent"}},
		{Operation: "fresh_start", Lane: "both", CapacityStrategy: "clear_first", ProcessingStrategy: "copy_verified", InteractionPolicy: "protect", CoveragePolicy: "preserve_protected", IntakeAfter: "continue", Replay: contentResetReplay{Mode: "bounded_recent"}},
	}
}

func TestRequiredContentResetContractsAreInstalled(t *testing.T) {
	for _, req := range fullLaneRequests() {
		for _, contract := range requiredContentResetContracts(req) {
			if contract.Effect == "retire_references" || contract.Effect == "availability_exception" {
				// Optional policy owners are deliberately absent until their
				// consumer contracts exist; the preview must report them as
				// missing rather than pretending they are implemented.
				if _, installed := contentResetOwners[contract]; installed {
					t.Fatalf("%s/%s must not be installed before its consumer contract exists", contract.Owner, contract.Effect)
				}
				continue
			}
			owner, installed := contentResetOwners[contract]
			if !installed || owner == nil || owner.Contract() != contract {
				t.Fatalf("required contract %+v is not installed", contract)
			}
		}
	}
}

func TestEveryInstalledOwnerHasExpectedQualificationVersion(t *testing.T) {
	for contract := range contentResetOwners {
		if _, ok := contentreset.ExpectedQualificationVersion(contract); !ok {
			t.Fatalf("installed owner %+v has no code-owned qualification version", contract)
		}
	}
	// Every expected qualification belongs to an installed owner; a stale
	// expected version for a removed adapter must not linger.
	for _, req := range fullLaneRequests() {
		for _, contract := range requiredContentResetContracts(req) {
			if _, ok := contentResetOwners[contract]; ok {
				if _, qualified := contentreset.ExpectedQualificationVersion(contract); !qualified {
					t.Fatalf("installed required contract %+v has no expected qualification version", contract)
				}
			}
		}
	}
}

func TestContentResetRetirePlanBindsExactBatch(t *testing.T) {
	campaign := models.ContentResetCampaign{PublicID: uuid.New()}
	plan := contentResetRetirePlan(campaign, "pods", 4, "cleanup", []string{"publish"})
	if plan.Key != "retire/pods/4" || plan.Contract.Owner != "cms/pods" || plan.Contract.Effect != "retire_exact" || plan.Contract.TargetType != "manifest_batch" {
		t.Fatalf("unexpected retirement plan: %+v", plan)
	}
	var parameters contentResetRetireParameters
	if err := json.Unmarshal(plan.Parameters, &parameters); err != nil {
		t.Fatal(err)
	}
	if parameters.Lane != "pods" || parameters.Ordinal != 4 || parameters.Purpose != "cleanup" {
		t.Fatalf("unexpected parameters: %+v", parameters)
	}
	command := contentreset.Command{Parameters: plan.Parameters, Contract: plan.Contract, TargetID: campaign.PublicID.String()}
	parsed, err := parseContentResetRetireParameters(command, "retire_exact")
	if err != nil || parsed != parameters {
		t.Fatalf("parameters did not round-trip: %+v %v", parsed, err)
	}
	for _, forged := range []contentreset.Command{
		{Parameters: json.RawMessage(`{"lane":"pods","ordinal":4,"purpose":"cleanup","extra":1}`), Contract: plan.Contract},
		{Parameters: json.RawMessage(`{"lane":"pods","ordinal":4,"purpose":"unapproved"}`), Contract: plan.Contract},
		{Parameters: json.RawMessage(`{"lane":"news","ordinal":0,"purpose":"cleanup"}`), Contract: plan.Contract},
	} {
		if _, err := parseContentResetRetireParameters(forged, "retire_exact"); err == nil {
			t.Fatalf("non-canonical retirement parameters were accepted: %s", forged.Parameters)
		}
	}
}

func TestContentResetMilestoneConfirmationBindsManifest(t *testing.T) {
	hash := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	campaign := models.ContentResetCampaign{Lane: "both"}
	revision := models.ContentResetRevision{ManifestHash: &hash, TargetCount: 42}
	publish := contentResetMilestoneConfirmation("publish", campaign, revision)
	rollback := contentResetMilestoneConfirmation("rollback", campaign, revision)
	if publish != "PUBLISH BOTH 42 ITEMS 0123456789AB" {
		t.Fatalf("unexpected publication confirmation %q", publish)
	}
	if rollback == publish {
		t.Fatal("rollback and publication confirmations must differ")
	}
	changed := hash
	revision.ManifestHash = &changed
	if contentResetMilestoneConfirmation("publish", campaign, revision) != publish {
		t.Fatal("same manifest must produce the same confirmation")
	}
}

func TestContentResetPublicationReadinessHashIgnoresCommandIdentity(t *testing.T) {
	evidence := json.RawMessage(`{"lanes":["news"],"replacement_ready":true}`)
	first := contentreset.Observation{CommandID: uuid.New(), CommandHash: "a", State: "succeeded", ReasonCode: "replacement_ready", Evidence: evidence}
	second := contentreset.Observation{CommandID: uuid.New(), CommandHash: "b", State: "succeeded", ReasonCode: "replacement_ready", Evidence: evidence}
	if contentResetReadinessHash(first) != contentResetReadinessHash(second) {
		t.Fatal("readiness hash must bind only stable readiness content")
	}
	second.ReasonCode = "replacement_not_ready"
	if contentResetReadinessHash(first) == contentResetReadinessHash(second) {
		t.Fatal("readiness hash must change with readiness state")
	}
}

func TestContentResetGenerationLaneMapping(t *testing.T) {
	if got := contentResetGenerationLanes("pods"); len(got) != 1 || got[0] != "media" {
		t.Fatalf("unexpected Pods lanes: %v", got)
	}
	if got := contentResetGenerationLanes("both"); len(got) != 2 || got[0] != "news" || got[1] != "media" {
		t.Fatalf("unexpected Both lanes: %v", got)
	}
}

func TestContentResetRollbackFenceValidationMirrorsPublication(t *testing.T) {
	news := feedstate.ResetRollbackFence{Lane: "news", HeadVersion: 1, ActiveID: uuid.New(), RestoreID: uuid.New()}
	media := feedstate.ResetRollbackFence{Lane: "media", HeadVersion: 2, ActiveID: uuid.New(), RestoreID: uuid.New()}
	if err := feedstate.ValidateResetRollbackFences("both", []feedstate.ResetRollbackFence{media, news}); err != nil {
		t.Fatal(err)
	}
	if err := feedstate.ValidateResetRollbackFences("both", []feedstate.ResetRollbackFence{news}); err == nil {
		t.Fatal("incomplete rollback lane set was accepted")
	}
}

func TestContentResetRetirePageResumesFromCursor(t *testing.T) {
	pairs := []contentResetRetirementPair{}
	for index := 1; index <= 70; index++ {
		pairs = append(pairs, contentResetRetirementPair{Lane: "news", Ordinal: index})
	}
	first, next, err := contentResetRetirePage(pairs, 0, 30)
	if err != nil || len(first) != 30 || next != 30 || first[0].Ordinal != 1 || first[29].Ordinal != 30 {
		t.Fatalf("unexpected first page: len=%d next=%d err=%v", len(first), next, err)
	}
	second, next, err := contentResetRetirePage(pairs, next, 30)
	if err != nil || len(second) != 30 || next != 60 || second[0].Ordinal != 31 {
		t.Fatalf("cursor did not resume: len=%d next=%d err=%v", len(second), next, err)
	}
	// Pause commands share the page budget on the first Empty pass.
	small, next, err := contentResetRetirePage(pairs, 0, 28)
	if err != nil || len(small) != 28 || next != 28 {
		t.Fatalf("shared page budget was not respected: len=%d next=%d err=%v", len(small), next, err)
	}
	tail, next, err := contentResetRetirePage(pairs, 60, 30)
	if err != nil || len(tail) != 10 || next != 70 {
		t.Fatalf("final partial page was wrong: len=%d next=%d err=%v", len(tail), next, err)
	}
	if _, _, err := contentResetRetirePage(pairs, 71, 30); err == nil {
		t.Fatal("out-of-range cursor was accepted")
	}
}

func TestContentResetRetireKeyMatchesPlanAndWorker(t *testing.T) {
	campaign := models.ContentResetCampaign{PublicID: uuid.New()}
	plan := contentResetRetirePlan(campaign, "news", 7, "clear", []string{"prepare"})
	if plan.Key != contentResetRetireKey("news", 7) {
		t.Fatalf("planner and delegated worker disagree on the retirement step key: %q", plan.Key)
	}
	if plan.Key != "retire/news/7" {
		t.Fatalf("unexpected retirement key %q", plan.Key)
	}
}

func TestStagedDeliveryStaysFailClosedWithoutPrivateBoundary(t *testing.T) {
	if contentResetStagedDeliveryPrivate() {
		t.Skip("private staged delivery has been qualified in this build")
	}
	// A Pods replacement must not be approvable while candidate assets still
	// sit on the ordinary public delivery path.
	_, blockers := contentResetContractFor(contentResetPlanRequest{
		Operation: "fresh_start", Lane: "pods", CapacityStrategy: "build_first", ProcessingStrategy: "copy_verified",
		InteractionPolicy: "protect", CoveragePolicy: "preserve_protected", IntakeAfter: "continue",
		Replay: contentResetReplay{Mode: "bounded_recent", SourceScope: "all_active_lane_sources"},
	})
	found := false
	for _, blocker := range blockers {
		found = found || blocker.Code == "staged_delivery_unqualified"
	}
	if !found {
		t.Fatal("Pods Fresh Start did not report the staged-delivery boundary blocker")
	}
}

func TestContentResetRecoveryWindowsArePositive(t *testing.T) {
	if contentResetRecoveryWindow <= 0 || contentResetCleanupAuthorizationWindow < contentResetRecoveryWindow {
		t.Fatalf("cleanup authorization must outlast the recovery window: %s / %s", contentResetRecoveryWindow, contentResetCleanupAuthorizationWindow)
	}
	if contentResetMilestoneTTL < time.Minute {
		t.Fatal("milestone approval must remain usable long enough for the coordinator to consume it")
	}
}
