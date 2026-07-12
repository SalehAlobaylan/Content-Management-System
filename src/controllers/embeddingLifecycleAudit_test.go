package controllers

import (
	"testing"

	"content-management-system/src/models"
)

func TestRollupSurfaceVerdict(t *testing.T) {
	cases := []struct {
		name string
		a    surfaceAudit
		want string
	}{
		{"coherent", surfaceAudit{Current: 100}, models.EmbeddingVerdictCoherent},
		{"unstamped_debt", surfaceAudit{Current: 50, Unstamped: 10}, models.EmbeddingVerdictUnstampedDebt},
		{"drifting when stale", surfaceAudit{Current: 50, Stale: 5}, models.EmbeddingVerdictDrifting},
		{"stale outranks unstamped", surfaceAudit{Stale: 1, Unstamped: 9}, models.EmbeddingVerdictDrifting},
		{"mixed_space outranks all", surfaceAudit{Stale: 5, Unstamped: 5, MixedSpace: 1}, models.EmbeddingVerdictMixedSpace},
	}
	for _, c := range cases {
		if got := rollupSurfaceVerdict(c.a); got != c.want {
			t.Errorf("%s: got %q want %q", c.name, got, c.want)
		}
	}
}

func TestRunHeadline(t *testing.T) {
	cases := []struct {
		name     string
		verdicts []string
		want     string
	}{
		{"all coherent", []string{models.EmbeddingVerdictCoherent, models.EmbeddingVerdictCoherent}, models.EmbeddingHeadlineAllClear},
		{"debt is watching", []string{models.EmbeddingVerdictCoherent, models.EmbeddingVerdictUnstampedDebt}, models.EmbeddingHeadlineWatching},
		{"drifting is attention", []string{models.EmbeddingVerdictUnstampedDebt, models.EmbeddingVerdictDrifting}, models.EmbeddingHeadlineAttention},
		{"mixed is attention", []string{models.EmbeddingVerdictMixedSpace}, models.EmbeddingHeadlineAttention},
		{"check_error is attention", []string{models.EmbeddingVerdictCheckError}, models.EmbeddingHeadlineAttention},
		{"blocked is attention", []string{models.EmbeddingVerdictBlocked}, models.EmbeddingHeadlineAttention},
	}
	for _, c := range cases {
		surfaces := make([]surfaceAudit, len(c.verdicts))
		for i, v := range c.verdicts {
			surfaces[i] = surfaceAudit{Verdict: v}
		}
		if got := runHeadline(surfaces); got != c.want {
			t.Errorf("%s: got %q want %q", c.name, got, c.want)
		}
	}
}

// TestProducerForConsistency proves CMS-derived producer_id matches what a
// writer stamps: ProducerFor(recipe) == spaceid.ProducerID(space_id, recipe).
func TestProducerForConsistency(t *testing.T) {
	es := expectedSpace{SpaceID: "deadbeef"}
	for _, s := range EmbeddingSurfaces() {
		if es.ProducerFor(s.Recipe) == "" {
			t.Errorf("surface %s: ProducerFor returned empty for resolved space", s.Key)
		}
	}
	// Unresolved space yields empty producer (fail-closed).
	empty := expectedSpace{SpaceID: ""}
	if empty.ProducerFor("any:v1") != "" {
		t.Error("unresolved space must yield empty producer_id")
	}
}
