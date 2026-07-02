package controllers

import (
	"content-management-system/src/models"
	"testing"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

func TestMediaCircActionLane(t *testing.T) {
	tests := map[string]string{
		mediaCircVerdictPullNow:           "pull",
		mediaCircVerdictDeepPull:          "pull",
		mediaCircVerdictPullLimited:       "limit_skip",
		mediaCircVerdictSkipSource:        "limit_skip",
		mediaCircVerdictProtect:           "protect",
		mediaCircVerdictReEncode:          "cool",
		mediaCircVerdictMoveToCold:        "cool",
		mediaCircVerdictRecoverableDelete: "cool",
		mediaCircVerdictRankDown:          "downrank",
		mediaCircVerdictNeedsAdminReview:  "review",
	}
	for verdict, want := range tests {
		if got := mediaCircActionLane(verdict); got != want {
			t.Errorf("mediaCircActionLane(%q) = %q, want %q", verdict, got, want)
		}
	}
}

func TestCockpitBucketsAddsThresholdsAndShares(t *testing.T) {
	got := cockpitBuckets([]libraryBucketHealth{
		{Bucket: "5", VisibleUnits: 25, State: "ok"},
		{Bucket: "10", VisibleUnits: 75, State: "ok"},
	})
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
	if got[0].ThinFloor != mediaCirculationBucketThinFloor || got[0].SaturatedCeil != mediaCirculationBucketSaturatedCeil {
		t.Errorf("thresholds = (%d,%d), want (%d,%d)",
			got[0].ThinFloor, got[0].SaturatedCeil,
			mediaCirculationBucketThinFloor, mediaCirculationBucketSaturatedCeil)
	}
	if got[0].SharePct < 24.9 || got[0].SharePct > 25.1 {
		t.Errorf("share = %.2f, want ~25", got[0].SharePct)
	}
}

func TestSummarizeCockpitRecommendations(t *testing.T) {
	rows := []mediaCirculationCockpitRecommendation{
		{MediaCirculationRecommendation: models.MediaCirculationRecommendation{UnitType: models.MediaCirculationUnitSource, Verdict: mediaCircVerdictPullNow, Status: models.MediaCirculationRecStatusPending}, ActionLane: "pull"},
		{MediaCirculationRecommendation: models.MediaCirculationRecommendation{UnitType: models.MediaCirculationUnitItemFamily, Verdict: mediaCircVerdictRankDown, Status: models.MediaCirculationRecStatusPending}, ActionLane: "downrank"},
		{MediaCirculationRecommendation: models.MediaCirculationRecommendation{UnitType: models.MediaCirculationUnitItemFamily, Verdict: mediaCircVerdictProtect, Status: models.MediaCirculationRecStatusApplied}, ActionLane: "protect"},
		{MediaCirculationRecommendation: models.MediaCirculationRecommendation{UnitType: models.MediaCirculationUnitSource, Verdict: mediaCircVerdictSkipSource, Status: models.MediaCirculationRecStatusDismissed}, ActionLane: "limit_skip"},
	}
	got := summarizeCockpitRecommendations(rows)
	if got.Total != 4 || got.Pending != 2 || got.Applied != 1 || got.Dismissed != 1 {
		t.Errorf("summary counts = total:%d pending:%d applied:%d dismissed:%d", got.Total, got.Pending, got.Applied, got.Dismissed)
	}
	if got.NeedsAttention != 2 {
		t.Errorf("needs_attention = %d, want 2", got.NeedsAttention)
	}
	if got.ByActionLane["pull"] != 1 || got.ByActionLane["downrank"] != 1 {
		t.Errorf("lane counts = %#v", got.ByActionLane)
	}
}

func TestCockpitDisplayAndProofFromMetrics(t *testing.T) {
	sourceID := uuid.New()
	rec := models.MediaCirculationRecommendation{
		UnitType:  models.MediaCirculationUnitSource,
		SubjectID: sourceID,
		Verdict:   mediaCircVerdictPullNow,
		Reasons:   datatypes.JSON([]byte(`["Library needs this source."]`)),
		Metrics: datatypes.JSON([]byte(`{
			"source_name":"Strong Podcast",
			"quality_prior":0.74,
			"bucket_demand_match":0.80,
			"matched_thin_buckets":["10","20"],
			"allowed_intake":4
		}`)),
	}
	metrics := mediaCircMetricsMap(rec)
	title, subtitle := cockpitDisplayText(rec, metrics, map[uuid.UUID]models.ContentSource{}, map[uuid.UUID]models.ContentItem{})
	if title != "Strong Podcast" || subtitle != "Media source" {
		t.Errorf("display = (%q,%q), want source fallback", title, subtitle)
	}
	if metric := cockpitPrimaryMetric(rec, metrics); metric != "4 allowed" {
		t.Errorf("primary metric = %q, want 4 allowed", metric)
	}
	proof := cockpitProofPoints(rec, metrics)
	if len(proof) < 4 {
		t.Fatalf("proof too short: %v", proof)
	}
}
