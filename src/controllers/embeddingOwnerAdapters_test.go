package controllers

import (
	"testing"

	"github.com/pgvector/pgvector-go"
)

func TestMeanOfVectorRows(t *testing.T) {
	rows := []struct{ Embedding pgvector.Vector }{
		{pgvector.NewVector([]float32{0, 2, 4})},
		{pgvector.NewVector([]float32{2, 4, 8})},
	}
	got := meanOfVectorRows(rows)
	want := []float32{1, 3, 6}
	if len(got) != len(want) {
		t.Fatalf("dim mismatch: got %d want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("index %d: got %v want %v", i, got[i], want[i])
		}
	}
}

func TestMeanOfVectorRowsEmpty(t *testing.T) {
	if meanOfVectorRows(nil) != nil {
		t.Error("empty input should yield nil")
	}
}

func TestToolMapping(t *testing.T) {
	if toolForCentroid("topic_centroid") != "topic_centroid_refresh" {
		t.Error("topic centroid tool mismatch")
	}
	if toolForCentroid("story_centroid") != "story_centroid_rebuild" {
		t.Error("story centroid tool mismatch")
	}
	if toolForCache("topic_proposal") != "topic_proposal_refresh" {
		t.Error("proposal cache tool mismatch")
	}
	if toolForCache("discovery_profile") != "discovery_profile_refresh" {
		t.Error("discovery cache tool mismatch")
	}
}
