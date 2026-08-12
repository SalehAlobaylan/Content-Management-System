package pipeline

import (
	"encoding/json"
	"testing"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/supply"
	"github.com/google/uuid"
	"gorm.io/datatypes"
)

func TestOnlyExactAggregationStagesAreAdmitted(t *testing.T) {
	for _, stage := range []string{models.PipelineStageMediaDownload, models.PipelineStageMediaTranscode, models.PipelineStageMediaThumbnail, models.PipelineStageTextEmbedding} {
		if !isStage(stage) {
			t.Fatalf("expected %q to be a registered exact stage", stage)
		}
	}
	for _, stage := range []string{"media_artifacts", "atomization", "", "retry-any"} {
		if isStage(stage) {
			t.Fatalf("unexpected repair admission for %q", stage)
		}
	}
}

func TestRepairStageRequiresPersistedPrerequisiteArtifact(t *testing.T) {
	item := models.ContentItem{}
	if got := pipelineRepairArtifactURL(item, "pipeline_repair_original_url"); got != "" {
		t.Fatalf("missing proof returned %q", got)
	}
	raw, _ := json.Marshal(map[string]string{"pipeline_repair_original_url": "https://storage.example/original.mp4"})
	item.Metadata = datatypes.JSON(raw)
	if got := pipelineRepairArtifactURL(item, "pipeline_repair_original_url"); got != "https://storage.example/original.mp4" {
		t.Fatalf("persisted proof returned %q", got)
	}
}

func TestExactEffectVerificationRejectsUnrelatedProcessingEvent(t *testing.T) {
	now := time.Now().UTC().Round(time.Microsecond)
	repairID, attemptID, fence, eventID, itemID := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	request := models.PipelineRepairRequest{PublicID: repairID, TenantID: "tenant-a", ContentItemID: itemID, Stage: models.PipelineStageTextEmbedding, ExpectedItemUpdatedAt: now, EffectInputDigest: "digest"}
	request.EffectProducerEventID = &eventID
	attempt := models.PipelineRepairAttempt{PublicID: attemptID, FenceToken: fence}
	event := models.ContentProcessingEvent{TenantID: "tenant-a", ContentItemID: &itemID, Stage: models.PipelineStageTextEmbedding, State: "completed", ExecutionOwner: OwnerProtocol, PipelineRepairRequestID: &repairID, PipelineRepairAttemptID: &attemptID, PipelineRepairFenceToken: &fence, ProducerEventID: &eventID, ExpectedItemUpdatedAt: &now, EffectInputDigest: "digest"}
	if !matchesExactEffectEvent(request, attempt, event) {
		t.Fatal("exact fenced event should verify")
	}
	other := uuid.New()
	event.ProducerEventID = &other
	if matchesExactEffectEvent(request, attempt, event) {
		t.Fatal("an unrelated processing event must not verify a repair")
	}
}

func TestPipelineRepairIsOneStaticApprovalAction(t *testing.T) {
	d, ok := supply.SupplyAction(supply.SupplyActionPipelineResumeExactStage)
	if !ok || d.TargetType != "content_item" || d.ExecutionOwner != OwnerProtocol || d.TargetCap != 1 || d.Rollback != "forward_only" {
		t.Fatalf("pipeline repair descriptor is not a bounded static action: %#v", d)
	}
	if !models.IsKnownMediaSupplyControlKey("supply_action:pipeline.resume_exact_stage") {
		t.Fatal("pipeline repair disable-only control is not registered")
	}
}
