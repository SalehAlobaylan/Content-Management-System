package controllers

import "testing"

import (
	"content-management-system/src/models"
	"github.com/google/uuid"
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
