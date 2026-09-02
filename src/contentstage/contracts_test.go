package contentstage

import (
	"encoding/json"
	"testing"
	"time"

	"content-management-system/src/models"

	"github.com/google/uuid"
	"github.com/pgvector/pgvector-go"
)

func testItem(kind models.ContentType) models.ContentItem {
	title, excerpt, body, language, original := "  Headline  ", " Summary ", "Body\ntext", "ar", "https://example.test/item"
	return models.ContentItem{PublicID: uuid.New(), TenantID: "default", Type: kind, ProcessingGeneration: 1, Title: &title, Excerpt: &excerpt, BodyText: &body, ContentLanguage: &language, OriginalURL: &original}
}

func TestTranscriptBoundedInputCarriesOnlyBoundedCaptionArtifact(t *testing.T) {
	item := testItem(models.ContentTypePodcast)
	item.Metadata, _ = json.Marshal(map[string]any{
		"caption_artifact":         map[string]any{"full_text": "caption", "segments": []any{map[string]any{"start": 0, "end": 1, "text": "caption"}}},
		"private_provider_payload": "must-not-cross-service-boundary",
	})
	input := boundedInput(item, models.ContentStagePodsTranscript)
	if _, ok := input["caption_artifact"]; !ok {
		t.Fatal("caption artifact was not preserved for the Media-owned transcript stage")
	}
	if _, leaked := input["private_provider_payload"]; leaked {
		t.Fatal("unregistered metadata leaked into the bounded stage envelope")
	}
}

func TestStageFingerprintsOnlyChangeForAuthoritativeInputs(t *testing.T) {
	item := testItem(models.ContentTypeNews)
	descriptor := descriptors[models.ContentStageNewsTextEmbedding]
	first := stageFingerprint(item, descriptor)
	sourceName := "unrelated source label"
	item.SourceName = &sourceName
	if got := stageFingerprint(item, descriptor); got != first {
		t.Fatal("unrelated source metadata invalidated the text stage")
	}
	changed := "Different body"
	item.BodyText = &changed
	if got := stageFingerprint(item, descriptor); got == first {
		t.Fatal("authoritative text change did not invalidate the text stage")
	}
}

func TestDependentStageFingerprintsDoNotDependOnProducedArtifacts(t *testing.T) {
	news := testItem(models.ContentTypeNews)
	classification := stageFingerprint(news, descriptors[models.ContentStageNewsStoryClassification])
	space, producer := "text-space", "embedding-producer"
	news.EmbeddingSpaceID, news.EmbeddingProducerID = &space, &producer
	if got := stageFingerprint(news, descriptors[models.ContentStageNewsStoryClassification]); got != classification {
		t.Fatal("classification invalidated itself when its embedding dependency completed")
	}

	pods := testItem(models.ContentTypePodcast)
	image := stageFingerprint(pods, descriptors[models.ContentStagePodsImageEmbedding])
	thumbnail := "https://cdn.example.test/thumbnail.jpg"
	pods.ThumbnailURL = &thumbnail
	if got := stageFingerprint(pods, descriptors[models.ContentStagePodsImageEmbedding]); got != image {
		t.Fatal("image embedding invalidated itself when its media dependency produced a thumbnail")
	}
}

func TestPodsManifestSeparatesRequiredAndOptionalStages(t *testing.T) {
	descriptors := StagesForContentType(models.ContentTypePodcast)
	required, optional := map[string]bool{}, map[string]bool{}
	for _, descriptor := range descriptors {
		if descriptor.BlockingScope == models.ContentStageBlockingOptional {
			optional[descriptor.Stage] = true
		} else {
			required[descriptor.Stage] = true
		}
	}
	for _, stage := range []string{models.ContentStagePodsMediaArtifacts, models.ContentStagePodsTextEmbedding, models.ContentStagePodsTranscript, models.ContentStagePodsAtomization} {
		if !required[stage] {
			t.Fatalf("missing required Pods stage %s", stage)
		}
	}
	for _, stage := range []string{models.ContentStagePodsCaptionReembedding, models.ContentStagePodsImageEmbedding, models.ContentStagePodsLLMMetadata} {
		if !optional[stage] {
			t.Fatalf("missing optional Pods stage %s", stage)
		}
	}
}

func TestMetadataFirstAdmissionStates(t *testing.T) {
	media := descriptors[models.ContentStagePodsMediaArtifacts]
	transcript := descriptors[models.ContentStagePodsTranscript]
	if got := initialStageState(media, models.MediaAcquisitionManual); got != models.ContentStageAwaitingApproval {
		t.Fatalf("manual media state = %s", got)
	}
	if got := initialStageState(media, models.MediaAcquisitionAutomatic); got != models.ContentStageQueued {
		t.Fatalf("automatic media state = %s", got)
	}
	if got := initialStageState(transcript, models.MediaAcquisitionAutomatic); got != models.ContentStageBlocked {
		t.Fatalf("dependent transcript state = %s", got)
	}
}

func TestTranscriptAdmissionKeepsProviderCaptionsAutomatic(t *testing.T) {
	if shouldAwaitGeneratedSTT(true, false, 0) {
		t.Fatal("provider caption import must not wait for generated-STT approval")
	}
	if !shouldAwaitGeneratedSTT(false, false, 0) {
		t.Fatal("caption-less generated STT must wait when auto STT is disabled")
	}
	if shouldAwaitGeneratedSTT(false, false, 100) {
		t.Fatal("manual transcript priority must release generated STT")
	}
}

func TestMediaLeaseIsWorkloadAware(t *testing.T) {
	if got := leaseDurationForStage(models.ContentStagePodsMediaArtifacts); got != 5*time.Minute {
		t.Fatalf("media lease = %s", got)
	}
	if got := leaseDurationForStage(models.ContentStagePodsTranscript); got != 45*time.Second {
		t.Fatalf("short-stage lease = %s", got)
	}
}

func TestManifestSummaryDoesNotTreatVerifiedAsActive(t *testing.T) {
	requests := []models.ContentStageRequest{
		{Stage: models.ContentStageNewsTextEmbedding, BlockingScope: models.ContentStageBlockingContentReady, State: models.ContentStageVerified},
		{Stage: models.ContentStageNewsStoryClassification, BlockingScope: models.ContentStageBlockingContentReady, State: models.ContentStageQueued},
		{Stage: models.ContentStageNewsLLMMetadata, BlockingScope: models.ContentStageBlockingOptional, State: models.ContentStageFailed},
	}
	summary := SummarizeManifest(requests, 4, "no_change")
	if len(summary.RequiredStages) != 2 {
		t.Fatalf("required stages = %v", summary.RequiredStages)
	}
	if len(summary.ActiveStages) != 2 {
		t.Fatalf("active stages = %v", summary.ActiveStages)
	}
}

func TestCompatibilityDispositionDefersArtifactCompleteFailure(t *testing.T) {
	item := testItem(models.ContentTypeNews)
	item.Status = models.ContentStatusFailed
	model, space, producer := "qwen", "text-space", "enrichment"
	item.EmbeddingModel, item.EmbeddingSpaceID, item.EmbeddingProducerID = &model, &space, &producer
	embedding := pgvector.NewVector([]float32{1})
	item.Embedding = &embedding
	storyID := uuid.New()
	item.StoryID = &storyID

	summary := CompatibilityDisposition(item, "no_change")
	if len(summary.NextRequiredStages) != 0 {
		t.Fatalf("artifact-complete failure unexpectedly requested work: %v", summary.NextRequiredStages)
	}
	if !summary.LifecycleReconciliationRequired {
		t.Fatal("artifact-complete failure must require lifecycle reconciliation")
	}
}

func TestCompatibilityDispositionQueuesOnlyMissingMediaOwnerStages(t *testing.T) {
	item := testItem(models.ContentTypePodcast)
	item.Status = models.ContentStatusReady
	playback, duration := "https://cdn.example.test/audio.m4a", 1800
	item.PlaybackURL, item.DurationSec = &playback, &duration

	summary := CompatibilityDisposition(item, "no_change")
	if len(summary.NextRequiredStages) != 1 || summary.NextRequiredStages[0] != models.ContentStagePodsTextEmbedding {
		t.Fatalf("expected only missing embedding stage, got %v", summary.NextRequiredStages)
	}
	if !summary.LifecycleReconciliationRequired {
		t.Fatal("READY item with missing required work must request lifecycle reconciliation")
	}
}
