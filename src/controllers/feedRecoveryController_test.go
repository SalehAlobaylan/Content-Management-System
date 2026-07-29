package controllers

import (
	"regexp"
	"testing"

	"content-management-system/src/models"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func TestRecoveryLanesExpandsBothWithoutInvalidAvailabilityLane(t *testing.T) {
	if got := recoveryLanes("news"); len(got) != 1 || got[0] != "news" {
		t.Fatalf("news lanes = %#v", got)
	}
	got := recoveryLanes("both")
	if len(got) != 2 || got[0] != "news" || got[1] != "media" {
		t.Fatalf("both lanes = %#v, want news then media", got)
	}
}

func TestRecoveryRepairToolsAreRegisteredAndBounded(t *testing.T) {
	if len(registeredFeedRecoveryRepairTools) != 2 {
		t.Fatalf("registered repair tool count = %d, want 2", len(registeredFeedRecoveryRepairTools))
	}
	for _, tool := range registeredFeedRecoveryRepairTools {
		if tool.Name == "" || tool.Description == "" {
			t.Fatalf("repair tool is not operator-readable: %+v", tool)
		}
		if tool.Name == "content_delete" || tool.Name == "source_cleanup" {
			t.Fatalf("destructive/source tool was registered: %q", tool.Name)
		}
	}
}

func TestRecoveryPlanRejectsRollbackAcknowledgementOutsidePurge(t *testing.T) {
	_, err := buildRecoveryPlan(nil, "default", feedRecoveryPlanRequest{Lane: "news", Level: "repair", CapacityMode: "safe_cutover", NoFullRollback: true}, "admin@example.test")
	if err == nil {
		t.Fatal("repair must reject no_full_rollback acknowledgement")
	}
}

func TestRecoveryPlanRejectsLowSpaceModeOutsidePurge(t *testing.T) {
	_, err := buildRecoveryPlan(nil, "default", feedRecoveryPlanRequest{Lane: "news", Level: "repair", CapacityMode: "low_space_reset"}, "admin@example.test")
	if err == nil {
		t.Fatal("repair must not enter low-space reset mode")
	}
}

func TestRecoveryRunExpectedEmptyOnlyAppliesToTargetedLowSpaceReset(t *testing.T) {
	if !recoveryRunExpectedEmpty(models.FeedRecoveryPlan{Level: "purge_reseed", CapacityMode: "low_space_reset", TargetCount: 1}) {
		t.Fatal("targeted low-space purge must expose expected-empty")
	}
	if recoveryRunExpectedEmpty(models.FeedRecoveryPlan{Level: "purge_reseed", CapacityMode: "safe_cutover", TargetCount: 1}) {
		t.Fatal("safe cutover must not expose expected-empty")
	}
	if recoveryRunExpectedEmpty(models.FeedRecoveryPlan{Level: "purge_reseed", CapacityMode: "low_space_reset", TargetCount: 0}) {
		t.Fatal("zero-target purge must not expose expected-empty")
	}
}

func TestRecoveryPurgePhraseBindsLaneCountManifestAndRollbackProof(t *testing.T) {
	plan := models.FeedRecoveryPlan{Lane: "news", Level: "purge_reseed", TargetCount: 12, ManifestHash: "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789", NoFullRollback: true}
	want := "PURGE NEWS 12 ITEMS ABCDEF012345 NO FULL ROLLBACK"
	if got := recoveryApprovalPhrase(plan); got != want {
		t.Fatalf("approval phrase = %q, want %q", got, want)
	}
}

func TestRecoveryManifestIDsStayTypedAndLaneScoped(t *testing.T) {
	newsID, mediaID := uuid.New(), uuid.New()
	manifest := recoveryPurgeManifest{NewsContentIDs: []uuid.UUID{newsID}, MediaContentIDs: []uuid.UUID{mediaID}}
	if got := recoveryManifestIDs(manifest, "news"); len(got) != 1 || got[0] != newsID {
		t.Fatalf("news manifest IDs = %#v", got)
	}
	if got := recoveryManifestIDs(manifest, "both"); len(got) != 2 || got[1] != mediaID {
		t.Fatalf("both manifest IDs = %#v", got)
	}
}

func TestRecoveryTargetRootHashIsOrderedAndLaneTyped(t *testing.T) {
	newsID, mediaID := uuid.New(), uuid.New()
	first := recoveryPurgeManifest{NewsContentIDs: []uuid.UUID{newsID}, MediaContentIDs: []uuid.UUID{mediaID}}
	second := recoveryPurgeManifest{NewsContentIDs: []uuid.UUID{newsID}, MediaContentIDs: []uuid.UUID{mediaID}}
	if got, want := recoveryTargetRootHash(first), recoveryTargetRootHash(second); got != want || got == "" {
		t.Fatalf("target root hash = %q, want stable non-empty hash %q", got, want)
	}
	if recoveryTargetRootHash(recoveryPurgeManifest{NewsContentIDs: []uuid.UUID{mediaID}, MediaContentIDs: []uuid.UUID{newsID}}) == recoveryTargetRootHash(first) {
		t.Fatal("swapping lane target types must change the manifest root")
	}
}

func TestRecoveryProtectionFailsClosedWhenInteractionQueryErrors(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT "content_item_id" FROM "user_interactions"`)).WillReturnError(sqlmock.ErrCancelled)
	ids, err := retentionProtectedContentIDs(db, "default", []uuid.UUID{uuid.New()})
	if err == nil || ids != nil {
		t.Fatalf("protection query failure must return an error and no scope, ids=%v err=%v", ids, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRetentionOperationalVerdictPrioritizesRecoveryAndCompaction(t *testing.T) {
	policy := models.DefaultRetentionPolicy("default")
	sample := &models.RetentionDBSample{DatabaseBytes: policy.DatabaseWarningBytes + 1}
	if got := retentionOperationalVerdict(policy, sample, retentionForecast{}, retentionPreview{CandidateRows: 4}, true); got != models.RetentionVerdictRecoveryInProgress {
		t.Fatalf("active recovery verdict = %q", got)
	}
	if got := retentionOperationalVerdict(policy, sample, retentionForecast{}, retentionPreview{CandidateRows: 4}, false); got != models.RetentionVerdictCompactionDue {
		t.Fatalf("eligible compaction verdict = %q", got)
	}
}

func TestRecoverySourceRunPreservesCheckpointOnlyWithExplicitMarker(t *testing.T) {
	if !sourceRunPreservesCheckpoint(map[string]interface{}{"recovery": map[string]interface{}{"preserve_checkpoints": true}}) {
		t.Fatal("explicit recovery checkpoint marker should be honored")
	}
	if sourceRunPreservesCheckpoint(map[string]interface{}{"recovery": map[string]interface{}{"preserve_checkpoints": false}}) {
		t.Fatal("false recovery checkpoint marker must not preserve")
	}
	if sourceRunPreservesCheckpoint(nil) {
		t.Fatal("ordinary source reports must not preserve by default")
	}
}
