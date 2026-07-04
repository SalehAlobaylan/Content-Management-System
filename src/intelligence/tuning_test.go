package intelligence

import (
	"math"
	"testing"
	"time"

	"content-management-system/src/models"

	"github.com/google/uuid"
)

func TestDefaultTuningMatchesConstants(t *testing.T) {
	d := DefaultTuning()
	sum := d.EngagementWeight + d.CompletionWeight + d.QualityWeight + d.VelocityWeight
	if math.Abs(sum-1.0) > 1e-9 {
		t.Fatalf("default weights must sum to 1.0, got %f", sum)
	}
	if d.ExploreImpressionTarget != exploreImpressionTarget ||
		d.LegacyExposureViewFloor != legacyExposureViewFloor ||
		d.ExplorationSliceEvery != explorationSliceEvery {
		t.Fatalf("default exploration knobs drifted from constants: %+v", d)
	}
	if d.DemotionDefaultFactor != demotionDefaultFactor || d.DemotionHalfLife != DemotionHalfLife() {
		t.Fatalf("default demotion knobs drifted from constants: %+v", d)
	}
	// The model default row must match the code defaults exactly.
	m := models.DefaultMediaIntelligenceConfig("t")
	if m.EngagementWeight != d.EngagementWeight || m.CompletionWeight != d.CompletionWeight ||
		m.QualityWeight != d.QualityWeight || m.VelocityWeight != d.VelocityWeight ||
		m.DemotionHalfLifeDays != int(d.DemotionHalfLife.Hours()/24) {
		t.Fatalf("model default diverged from DefaultTuning")
	}
}

func TestSanitizeTuningNormalizesWeights(t *testing.T) {
	got := sanitizeTuning(Tuning{
		EngagementWeight: 2, CompletionWeight: 2, QualityWeight: 0, VelocityWeight: 0,
		ExplorationSliceEvery: 10, ExploreImpressionTarget: 50, LegacyExposureViewFloor: 25,
		DemotionDefaultFactor: 0.5, DemotionHalfLife: 14 * 24 * time.Hour,
	})
	sum := got.EngagementWeight + got.CompletionWeight + got.QualityWeight + got.VelocityWeight
	if math.Abs(sum-1.0) > 1e-9 {
		t.Fatalf("weights must renormalize to 1.0, got %f", sum)
	}
	if math.Abs(got.EngagementWeight-0.5) > 1e-9 || math.Abs(got.CompletionWeight-0.5) > 1e-9 {
		t.Fatalf("2:2:0:0 must become 0.5:0.5:0:0, got %+v", got)
	}
}

func TestSanitizeTuningFallsBackOnZeroWeights(t *testing.T) {
	got := sanitizeTuning(Tuning{DemotionHalfLife: 14 * 24 * time.Hour, ExplorationSliceEvery: 10, ExploreImpressionTarget: 50})
	d := DefaultTuning()
	if got.EngagementWeight != d.EngagementWeight || got.CompletionWeight != d.CompletionWeight {
		t.Fatalf("all-zero weights must fall back to defaults, got %+v", got)
	}
}

func TestSanitizeTuningClampsKnobs(t *testing.T) {
	got := sanitizeTuning(Tuning{
		EngagementWeight:        1,
		ExplorationSliceEvery:   1,                    // below min 4
		ExploreImpressionTarget: 5,                    // below min 10
		LegacyExposureViewFloor: 99999,                // above max 10000
		DemotionDefaultFactor:   2.0,                  // above max 0.95
		DemotionHalfLife:        500 * 24 * time.Hour, // above max 90d
	})
	if got.ExplorationSliceEvery != tuningSliceEveryMin {
		t.Fatalf("slice_every should clamp to %d, got %d", tuningSliceEveryMin, got.ExplorationSliceEvery)
	}
	if got.ExploreImpressionTarget != tuningImpressionTgtMin {
		t.Fatalf("impression target should clamp to %d, got %d", tuningImpressionTgtMin, got.ExploreImpressionTarget)
	}
	if got.LegacyExposureViewFloor != tuningLegacyFloorMax {
		t.Fatalf("legacy floor should clamp to %d, got %d", tuningLegacyFloorMax, got.LegacyExposureViewFloor)
	}
	if got.DemotionDefaultFactor != tuningDemotionFactorMax {
		t.Fatalf("demotion factor should clamp to %f, got %f", tuningDemotionFactorMax, got.DemotionDefaultFactor)
	}
	if int(got.DemotionHalfLife.Hours()/24) != tuningHalfLifeDaysMax {
		t.Fatalf("half-life should clamp to %d days, got %v", tuningHalfLifeDaysMax, got.DemotionHalfLife)
	}
}

// The blend must actually respond to the weights: the same items scored under
// engagement-heavy vs completion-heavy tuning order differently.
func TestScoreRespondsToWeights(t *testing.T) {
	engager := mediaItem(func(i *models.ContentItem) { i.PublicID = uuid.New(); i.LikeCount = 50; i.ImpressionCount = 200 })
	finisher := mediaItem(func(i *models.ContentItem) { i.PublicID = uuid.New(); i.ImpressionCount = 200 })

	base := batchContext{
		completes:       map[string]int64{finisher.PublicID.String(): 180, engager.PublicID.String(): 0},
		views:           map[string]int64{finisher.PublicID.String(): 200, engager.PublicID.String(): 200},
		recent:          map[string]int64{},
		engagementPrior: 0.02,
		completionPrior: 0.25,
	}

	engHeavy := base
	engHeavy.tuning = sanitizeTuning(Tuning{EngagementWeight: 0.9, CompletionWeight: 0.1, DemotionHalfLife: 14 * 24 * time.Hour, ExplorationSliceEvery: 10, ExploreImpressionTarget: 50})
	compHeavy := base
	compHeavy.tuning = sanitizeTuning(Tuning{EngagementWeight: 0.1, CompletionWeight: 0.9, DemotionHalfLife: 14 * 24 * time.Hour, ExplorationSliceEvery: 10, ExploreImpressionTarget: 50})

	underEng := scoreWithContext(engager, engHeavy).Value - scoreWithContext(finisher, engHeavy).Value
	underComp := scoreWithContext(engager, compHeavy).Value - scoreWithContext(finisher, compHeavy).Value
	if !(underEng > 0 && underComp < 0) {
		t.Fatalf("weights must flip ordering: engager−finisher under engagement-heavy=%f, under completion-heavy=%f", underEng, underComp)
	}
}

func TestExplorationTargetTunable(t *testing.T) {
	item := mediaItem(func(i *models.ContentItem) { i.ImpressionCount = 30 })
	ctx := batchContext{completes: map[string]int64{}, views: map[string]int64{}, recent: map[string]int64{}}

	ctx.tuning = sanitizeTuning(Tuning{EngagementWeight: 1, DemotionHalfLife: 14 * 24 * time.Hour, ExplorationSliceEvery: 10, ExploreImpressionTarget: 50, LegacyExposureViewFloor: 25})
	if s := scoreWithContext(item, ctx).ExplorationState; s != ExplorationExploring {
		t.Fatalf("30 impressions with target 50 must be exploring, got %s", s)
	}
	ctx.tuning = sanitizeTuning(Tuning{EngagementWeight: 1, DemotionHalfLife: 14 * 24 * time.Hour, ExplorationSliceEvery: 10, ExploreImpressionTarget: 20, LegacyExposureViewFloor: 25})
	if s := scoreWithContext(item, ctx).ExplorationState; s != ExplorationEstablished {
		t.Fatalf("30 impressions with target 20 must be established, got %s", s)
	}
}

func TestEffectiveDemotionAtCustomHalfLife(t *testing.T) {
	now := time.Now()
	fast := 7 * 24 * time.Hour
	slow := 14 * 24 * time.Hour
	// After 7 days: fast (7d half-life) has decayed one half-life; slow half.
	at7 := now.Add(-7 * 24 * time.Hour)
	dFast := EffectiveDemotionAt(0.5, at7, now, fast)
	dSlow := EffectiveDemotionAt(0.5, at7, now, slow)
	if math.Abs(dFast-0.75) > 1e-9 {
		t.Fatalf("7d half-life after 7d must be 0.75, got %f", dFast)
	}
	if dSlow >= dFast {
		t.Fatalf("slower half-life must decay less (stay more demoted): slow=%f fast=%f", dSlow, dFast)
	}
}

func TestTuningFallsBackWithoutDB(t *testing.T) {
	got := Engine{}.Tuning("any-tenant")
	if got != DefaultTuning() {
		t.Fatalf("Engine with no DB must return DefaultTuning, got %+v", got)
	}
}
