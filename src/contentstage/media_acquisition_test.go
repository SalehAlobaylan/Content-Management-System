package contentstage

import (
	"testing"

	"content-management-system/src/models"
	"gorm.io/datatypes"
)

func TestHasUsableCaptionArtifactRequiresCaptionContent(t *testing.T) {
	tests := []struct {
		name     string
		metadata string
		want     bool
	}{
		{name: "missing", metadata: `{}`, want: false},
		{name: "null placeholder", metadata: `{"caption_artifact":null}`, want: false},
		{name: "empty object", metadata: `{"caption_artifact":{}}`, want: false},
		{name: "empty text and segments", metadata: `{"caption_artifact":{"full_text":"","segments":[]}}`, want: false},
		{name: "partial segment", metadata: `{"caption_artifact":{"full_text":"","segments":[{"start":0,"end":1,"text":"   "}]}}`, want: false},
		{name: "full text", metadata: `{"caption_artifact":{"full_text":"hello","segments":[]}}`, want: true},
		{name: "legacy full text", metadata: `{"caption_artifact":{"full_text":"hello"}}`, want: true},
		{name: "segment only", metadata: `{"caption_artifact":{"segments":[{"start":0,"end":1,"text":"hello"}]}}`, want: false},
		{name: "invalid segment type", metadata: `{"caption_artifact":{"full_text":"hello","segments":{}}}`, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			item := models.ContentItem{Metadata: datatypes.JSON(tt.metadata)}
			if got := HasUsableCaptionArtifact(item); got != tt.want {
				t.Fatalf("HasUsableCaptionArtifact() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHasProviderCaptionDoesNotTrustStaleStateWithoutTranscript(t *testing.T) {
	state := models.CaptionStateYouTubeAuto
	item := models.ContentItem{CaptionState: &state}
	if HasProviderCaption(nil, item) {
		t.Fatal("stale provider caption state must not suppress STT without a transcript or usable artifact")
	}
}
