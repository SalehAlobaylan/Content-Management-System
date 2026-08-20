package contentstage

import (
	"encoding/json"
	"testing"

	"content-management-system/src/models"

	"github.com/google/uuid"
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
