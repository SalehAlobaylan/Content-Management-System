package intelligence

import (
	"math"
	"testing"
	"time"

	"content-management-system/src/models"

	"github.com/google/uuid"
)

func mediaItem(mut func(*models.ContentItem)) models.ContentItem {
	dur := 600
	thumb := "https://cdn/thumb.jpg"
	item := models.ContentItem{
		Type:        models.ContentTypeVideo,
		Source:      models.SourceTypeYouTube,
		Status:      models.ContentStatusReady,
		DurationSec: &dur,
		ThumbnailURL: &thumb,
	}
	if mut != nil {
		mut(&item)
	}
	return item
}

// FallbackValue must preserve the original thin-seam ordering guarantees — the
// same invariants the circulation evict tests pin on circulationMediaValue.
func TestFallbackValueOrdering(t *testing.T) {
	high := mediaItem(func(i *models.ContentItem) { i.LikeCount = 500; i.ViewCount = 10000; i.ShareCount = 100 })
	low := mediaItem(nil)
	if FallbackValue(high) <= FallbackValue(low) {
		t.Fatalf("high engagement (%f) should outrank low (%f)", FallbackValue(high), FallbackValue(low))
	}

	unsuitable := mediaItem(func(i *models.ContentItem) { i.MediaSuitability = models.MediaSuitabilityUnsuitable })
	if FallbackValue(unsuitable) >= FallbackValue(low) {
		t.Fatalf("unsuitable (%f) should score below neutral (%f)", FallbackValue(unsuitable), FallbackValue(low))
	}

	audio := mediaItem(func(i *models.ContentItem) { i.MediaSuitability = models.MediaSuitabilityAudioFirstShow })
	if FallbackValue(audio) <= FallbackValue(low) {
		t.Fatalf("audio-first (%f) should score above neutral (%f)", FallbackValue(audio), FallbackValue(low))
	}

	a, b := mediaItem(nil), mediaItem(nil)
	if d := FallbackValue(a) - FallbackValue(b); math.Abs(d) > 1e-12 {
		t.Fatalf("identical items must score identically (diff %g)", d)
	}
}

func TestFallbackValueBounds(t *testing.T) {
	maxed := mediaItem(func(i *models.ContentItem) {
		i.LikeCount = 1 << 30
		i.ViewCount = 1 << 30
		i.ShareCount = 1 << 30
		i.CommentCount = 1 << 30
		i.MediaSuitability = models.MediaSuitabilityAudioFirstShow
	})
	if v := FallbackValue(maxed); v < 0 || v > 1 {
		t.Fatalf("value out of [0,1]: %f", v)
	}
	zero := models.ContentItem{}
	if v := FallbackValue(zero); v < 0 || v > 1 {
		t.Fatalf("zero item out of [0,1]: %f", v)
	}
}

// The exposure-normalized regime must separate "unpopular" from "unseen":
// with measured impressions, an item engaging above the corpus rate outranks
// one engaging below it, even when the raw counts say the opposite.
func TestExposureNormalizedEngagement(t *testing.T) {
	ctx := batchContext{
		completes:       map[string]int64{},
		views:           map[string]int64{},
		recent:          map[string]int64{},
		engagementPrior: 0.02,
	}

	// 10 likes over 100 impressions = 10% rate — a hit.
	efficient := mediaItem(func(i *models.ContentItem) { i.LikeCount = 10; i.ImpressionCount = 100 })
	// 40 likes over 10000 impressions = 0.4% rate — well below prior.
	wasteful := mediaItem(func(i *models.ContentItem) { i.LikeCount = 40; i.ImpressionCount = 10000 })

	ve := scoreWithContext(efficient, ctx)
	vw := scoreWithContext(wasteful, ctx)
	if ve.Breakdown.Engagement <= vw.Breakdown.Engagement {
		t.Fatalf("engagement-per-impression must beat raw counts: efficient %f <= wasteful %f",
			ve.Breakdown.Engagement, vw.Breakdown.Engagement)
	}
	if !ve.Breakdown.ExposureMeasured || !vw.Breakdown.ExposureMeasured {
		t.Fatalf("both items have measured exposure")
	}
}

// Below the impression floor the model bridges to the legacy raw-count squash
// instead of pretending to normalize by unmeasured exposure.
func TestLegacyBridgeBelowImpressionFloor(t *testing.T) {
	ctx := batchContext{completes: map[string]int64{}, views: map[string]int64{}, recent: map[string]int64{}}
	item := mediaItem(func(i *models.ContentItem) { i.LikeCount = 20; i.ImpressionCount = 3 })
	v := scoreWithContext(item, ctx)
	if v.Breakdown.ExposureMeasured {
		t.Fatalf("3 impressions must not count as measured exposure")
	}
	if want := legacyEngagementSquash(item); math.Abs(v.Breakdown.Engagement-want) > 1e-12 {
		t.Fatalf("legacy bridge expected %f, got %f", want, v.Breakdown.Engagement)
	}
}

// Shrinkage: tiny samples must not produce extreme rates. 1 like / 12
// impressions is NOT a 8.3% engagement verdict — it's mostly prior.
func TestShrinkageDampsTinySamples(t *testing.T) {
	ctx := batchContext{completes: map[string]int64{}, views: map[string]int64{}, recent: map[string]int64{}, engagementPrior: 0.02}
	lucky := mediaItem(func(i *models.ContentItem) { i.LikeCount = 1; i.ImpressionCount = 12 })
	proven := mediaItem(func(i *models.ContentItem) { i.LikeCount = 100; i.ImpressionCount = 1200 })
	vl := scoreWithContext(lucky, ctx)
	vp := scoreWithContext(proven, ctx)
	if vl.Breakdown.Engagement >= vp.Breakdown.Engagement {
		t.Fatalf("a proven 8%% rate (%f) must outrank a lucky 8%% rate (%f)",
			vp.Breakdown.Engagement, vl.Breakdown.Engagement)
	}
}

func TestCompletionSignal(t *testing.T) {
	finisher := mediaItem(func(i *models.ContentItem) { i.PublicID = uuid.New(); i.ImpressionCount = 100 })
	quitter := mediaItem(func(i *models.ContentItem) { i.PublicID = uuid.New(); i.ImpressionCount = 100 })
	ctx := batchContext{
		completes: map[string]int64{
			finisher.PublicID.String(): 80,
			quitter.PublicID.String():  2,
		},
		views: map[string]int64{
			finisher.PublicID.String(): 100,
			quitter.PublicID.String():  100,
		},
		recent:          map[string]int64{},
		completionPrior: 0.25,
	}
	vf := scoreWithContext(finisher, ctx)
	vq := scoreWithContext(quitter, ctx)
	if vf.Breakdown.Completion <= vq.Breakdown.Completion {
		t.Fatalf("80%% completion (%f) must beat 2%% (%f)", vf.Breakdown.Completion, vq.Breakdown.Completion)
	}
	if vf.Value <= vq.Value {
		t.Fatalf("completion must move total value: %f <= %f", vf.Value, vq.Value)
	}
}

func TestConfidenceAndExploration(t *testing.T) {
	ctx := batchContext{completes: map[string]int64{}, views: map[string]int64{}, recent: map[string]int64{}}

	fresh := mediaItem(func(i *models.ContentItem) { i.ImpressionCount = 0 })
	vf := scoreWithContext(fresh, ctx)
	if vf.ExplorationState != ExplorationExploring {
		t.Fatalf("0 impressions must be exploring, got %s", vf.ExplorationState)
	}
	if vf.Confidence != 0 {
		t.Fatalf("0 impressions must be 0 confidence, got %f", vf.Confidence)
	}

	seasoned := mediaItem(func(i *models.ContentItem) { i.ImpressionCount = 500 })
	vs := scoreWithContext(seasoned, ctx)
	if vs.ExplorationState != ExplorationEstablished {
		t.Fatalf("500 impressions must be established, got %s", vs.ExplorationState)
	}
	if vs.Confidence <= vf.Confidence || vs.Confidence >= 1 {
		t.Fatalf("confidence must grow with impressions and stay < 1, got %f", vs.Confidence)
	}

	// Cold-start bridge: an item with substantial LEGACY views had exposure
	// before impression telemetry existed — it is not "never given a chance".
	legacy := mediaItem(func(i *models.ContentItem) { i.ImpressionCount = 0; i.ViewCount = 200 })
	vl := scoreWithContext(legacy, ctx)
	if vl.ExplorationState != ExplorationEstablished {
		t.Fatalf("legacy-viewed item must be established, got %s", vl.ExplorationState)
	}
}

func TestCostEfficiencyPenalty(t *testing.T) {
	median := 10_000_000.0 // 10MB per useful minute

	normal := mediaItem(func(i *models.ContentItem) { i.FileSizeBytes = 100_000_000 }) // 600s → 10MB/min
	if p := costEfficiencyPenalty(normal, median); p != 0 {
		t.Fatalf("at-median item must not be penalized, got %f", p)
	}

	bloated := mediaItem(func(i *models.ContentItem) { i.FileSizeBytes = 400_000_000 }) // 40MB/min = 4× median
	p := costEfficiencyPenalty(bloated, median)
	if p <= 0 || p > costPenaltyMax {
		t.Fatalf("4× median must be penalized within (0, %f], got %f", costPenaltyMax, p)
	}

	if p := costEfficiencyPenalty(normal, 0); p != 0 {
		t.Fatalf("unknown corpus median must not penalize, got %f", p)
	}
	noSize := mediaItem(nil)
	if p := costEfficiencyPenalty(noSize, median); p != 0 {
		t.Fatalf("no size data must not penalize, got %f", p)
	}
}

func TestEffectiveDemotionDecay(t *testing.T) {
	now := time.Now()

	if d := EffectiveDemotion(0.5, now, now); math.Abs(d-0.5) > 1e-9 {
		t.Fatalf("fresh demotion must equal its factor, got %f", d)
	}
	oneHalfLife := now.Add(-DemotionHalfLife())
	if d := EffectiveDemotion(0.5, oneHalfLife, now); math.Abs(d-0.75) > 1e-9 {
		t.Fatalf("after one half-life 0.5 must decay to 0.75, got %f", d)
	}
	ancient := now.Add(-10 * DemotionHalfLife())
	if d := EffectiveDemotion(0.5, ancient, now); d < 0.999 {
		t.Fatalf("ancient demotion must have decayed away, got %f", d)
	}
	if d := EffectiveDemotion(1.0, now, now); d != 1 {
		t.Fatalf("factor 1 means no demotion, got %f", d)
	}
	if d := EffectiveDemotion(-0.2, now, now); d != 1 {
		t.Fatalf("invalid factor must be ignored, got %f", d)
	}
}

func TestValueStaysBounded(t *testing.T) {
	ctx := batchContext{
		completes:       map[string]int64{},
		views:           map[string]int64{},
		recent:          map[string]int64{},
		engagementPrior: 0.02,
		completionPrior: 0.25,
		costMedian:      10_000_000,
	}
	extremes := []models.ContentItem{
		mediaItem(func(i *models.ContentItem) {
			i.LikeCount = 1 << 30
			i.ImpressionCount = 1
		}),
		mediaItem(func(i *models.ContentItem) {
			i.ImpressionCount = 1 << 40
			i.FileSizeBytes = 1 << 50
		}),
		{},
	}
	for n, item := range extremes {
		v := scoreWithContext(item, ctx)
		if v.Value < 0 || v.Value > 1 {
			t.Fatalf("extreme case %d out of [0,1]: %f", n, v.Value)
		}
		if v.Confidence < 0 || v.Confidence >= 1 {
			t.Fatalf("extreme case %d confidence out of [0,1): %f", n, v.Confidence)
		}
		if len(v.Reasons) == 0 {
			t.Fatalf("extreme case %d must still explain itself", n)
		}
	}
}
