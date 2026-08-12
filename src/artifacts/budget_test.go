package artifacts

import (
	"testing"

	"content-management-system/src/models"
)

func TestArtifactBudgetsAreClassSpecificAndBounded(t *testing.T) {
	duration := 121
	item := models.ContentItem{DurationSec: &duration}
	tests := []struct {
		artifact, unit string
		amount         float64
	}{
		{ArtifactTranscript, "media_minute", 3},
		{ArtifactImageEmbedding, "image_item", 1},
		{ArtifactTextEmbedding, "embedding_item", 1},
		{ArtifactLLMMetadata, "llm_call", 1},
	}
	for _, test := range tests {
		unit, amount, err := artifactBudget(item, test.artifact)
		if err != nil || unit != test.unit || amount != test.amount {
			t.Fatalf("%s got %s/%v/%v", test.artifact, unit, amount, err)
		}
	}
}

func TestTranscriptBudgetRejectsUnknownWorkload(t *testing.T) {
	if _, _, err := artifactBudget(models.ContentItem{}, ArtifactTranscript); err == nil {
		t.Fatal("unknown media minutes cannot reserve transcription work")
	}
}
