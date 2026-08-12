package artifacts

import (
	"testing"

	"content-management-system/src/models"
	"content-management-system/src/supply"
	"github.com/pgvector/pgvector-go"
	"gorm.io/datatypes"
)

func TestArtifactRegistryIsClosedAndOwnerBound(t *testing.T) {
	cases := []struct {
		artifact string
		action   string
		owner    string
	}{
		{ArtifactTranscript, supply.SupplyActionArtifactRequestTranscript, MediaOwner},
		{ArtifactImageEmbedding, supply.SupplyActionArtifactRequestImageEmbedding, MediaOwner},
		{ArtifactTextEmbedding, supply.SupplyActionArtifactRequestTextEmbedding, EnrichmentOwner},
		{ArtifactLLMMetadata, supply.SupplyActionArtifactRequestLLMMetadata, EnrichmentOwner},
	}
	for _, test := range cases {
		descriptor, ok := DescriptorForArtifact(test.artifact)
		if !ok || descriptor.ActionKey != test.action || descriptor.Owner != test.owner || descriptor.MaxWork != 1 {
			t.Fatalf("unexpected descriptor for %s: %#v", test.artifact, descriptor)
		}
		if byAction, ok := DescriptorForAction(test.action); !ok || byAction.Artifact != test.artifact {
			t.Fatalf("action %s did not resolve to its exact artifact", test.action)
		}
	}
	for _, forbidden := range []string{"chapter", "quality_repair", "delete", "artifact.*", "transcript,image_embedding"} {
		if _, ok := DescriptorForArtifact(forbidden); ok {
			t.Fatalf("forbidden artifact %q entered the registry", forbidden)
		}
	}
}

func TestArtifactMissingUsesPersistedEvidence(t *testing.T) {
	item := models.ContentItem{Metadata: datatypes.JSON([]byte(`{}`))}
	for _, artifact := range []string{ArtifactTranscript, ArtifactImageEmbedding, ArtifactTextEmbedding, ArtifactLLMMetadata} {
		missing, err := Missing(item, artifact)
		if err != nil || !missing {
			t.Fatalf("expected %s to be absent, got missing=%v err=%v", artifact, missing, err)
		}
	}
	text := pgvector.NewVector(make([]float32, 1024))
	image := pgvector.NewVector(make([]float32, 512))
	item.Embedding, item.ImageEmbedding = &text, &image
	item.Metadata = datatypes.JSON([]byte(`{"summary":"present"}`))
	for _, artifact := range []string{ArtifactImageEmbedding, ArtifactTextEmbedding, ArtifactLLMMetadata} {
		missing, err := Missing(item, artifact)
		if err != nil || missing {
			t.Fatalf("expected %s to be present, got missing=%v err=%v", artifact, missing, err)
		}
	}
	item.Metadata = datatypes.JSON([]byte(`{"summary":`))
	if _, err := Missing(item, ArtifactLLMMetadata); err == nil {
		t.Fatal("malformed metadata must remain unknown, not absent")
	}
}
