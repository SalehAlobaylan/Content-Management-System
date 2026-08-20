package contentstage

import (
	"testing"

	"content-management-system/src/feedcontract"
	"content-management-system/src/models"
)

func TestPodsMediaArtifactVerifierEnforcesMinimumDuration(t *testing.T) {
	playback := "https://media.example/processed.mp4"
	for name, duration := range map[string]int{"invalid": feedcontract.PodsMinDurationSec - 1, "boundary": feedcontract.PodsMinDurationSec, "long_parent": feedcontract.PodsHardMaxDuration + 1} {
		t.Run(name, func(t *testing.T) {
			item := models.ContentItem{PlaybackURL: &playback, DurationSec: &duration}
			present, _, err := artifactPresent(nil, item, models.ContentStagePodsMediaArtifacts)
			if err != nil {
				t.Fatal(err)
			}
			if want := duration >= feedcontract.PodsMinDurationSec; present != want {
				t.Fatalf("present=%v, want %v for duration %d", present, want, duration)
			}
		})
	}
}

func TestPodsAtomizationVerifierDoesNotCompleteForUndersizedParent(t *testing.T) {
	duration := feedcontract.PodsMinDurationSec - 1
	item := models.ContentItem{DurationSec: &duration}
	present, _, err := artifactPresent(nil, item, models.ContentStagePodsAtomization)
	if err != nil || present {
		t.Fatalf("undersized parent must not satisfy atomization: present=%v err=%v", present, err)
	}
}
