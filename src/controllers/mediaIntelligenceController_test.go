package controllers

import (
	"math"
	"testing"

	"content-management-system/src/intelligence"
	"content-management-system/src/models"
)

func mediaIntelFloatPtr(v float64) *float64 { return &v }
func mediaIntelIntPtr(v int) *int           { return &v }

func TestApplyMediaIntelligenceConfigPatchPreservesOmittedFields(t *testing.T) {
	base := models.DefaultMediaIntelligenceConfig("default")
	patch := mediaIntelligenceConfigPatch{
		EngagementWeight:        mediaIntelFloatPtr(0.7),
		ExploreImpressionTarget: mediaIntelIntPtr(125),
	}

	got := applyMediaIntelligenceConfigPatch(base, patch)

	if got.EngagementWeight != 0.7 {
		t.Fatalf("engagement weight = %f, want 0.7", got.EngagementWeight)
	}
	if got.ExploreImpressionTarget != 125 {
		t.Fatalf("impression target = %d, want 125", got.ExploreImpressionTarget)
	}
	if got.CompletionWeight != base.CompletionWeight ||
		got.QualityWeight != base.QualityWeight ||
		got.VelocityWeight != base.VelocityWeight ||
		got.ExplorationSliceEvery != base.ExplorationSliceEvery ||
		got.LegacyExposureViewFloor != base.LegacyExposureViewFloor ||
		got.DemotionDefaultFactor != base.DemotionDefaultFactor ||
		got.DemotionHalfLifeDays != base.DemotionHalfLifeDays {
		t.Fatalf("omitted fields changed: got %+v, base %+v", got, base)
	}
}

func TestApplyMediaIntelligenceConfigPatchHonorsExplicitZeroWeight(t *testing.T) {
	base := models.DefaultMediaIntelligenceConfig("default")
	patch := mediaIntelligenceConfigPatch{
		EngagementWeight: mediaIntelFloatPtr(0),
	}

	got := intelligence.SanitizeConfig(applyMediaIntelligenceConfigPatch(base, patch))

	if got.EngagementWeight != 0 {
		t.Fatalf("explicit zero weight must be preserved after normalization, got %f", got.EngagementWeight)
	}
	sum := got.EngagementWeight + got.CompletionWeight + got.QualityWeight + got.VelocityWeight
	if math.Abs(sum-1) > 1e-9 {
		t.Fatalf("weights must still normalize to 1.0, got %f", sum)
	}
}

func TestApplyMediaIntelligenceConfigPatchCanDetectAllZeroWeights(t *testing.T) {
	base := models.DefaultMediaIntelligenceConfig("default")
	patch := mediaIntelligenceConfigPatch{
		EngagementWeight: mediaIntelFloatPtr(0),
		CompletionWeight: mediaIntelFloatPtr(0),
		QualityWeight:    mediaIntelFloatPtr(0),
		VelocityWeight:   mediaIntelFloatPtr(0),
	}

	got := applyMediaIntelligenceConfigPatch(base, patch)
	if got.EngagementWeight+got.CompletionWeight+got.QualityWeight+got.VelocityWeight != 0 {
		t.Fatalf("all-zero weight patch should remain detectable before sanitize: %+v", got)
	}
}
