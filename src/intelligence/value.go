package intelligence

import (
	"fmt"
	"math"

	"content-management-system/src/models"
)

// batchContext carries the DB-loaded per-item signals and corpus statistics one
// ScoreBatch call needs. Loaded in bulk by loadBatchContext (context.go); a
// zero-value context degrades every rate signal to its prior — never panics.
type batchContext struct {
	// Per-item interaction tallies keyed by content_items.public_id.
	completes map[string]int64 // 'complete' interactions
	views     map[string]int64 // 'view' interactions
	recent    map[string]int64 // interactions inside the velocity window

	// Corpus statistics.
	engagementPrior float64 // engagement events per impression across the corpus
	completionPrior float64 // completes per view across the corpus
	costMedian      float64 // median storage bytes per useful minute (0 = unknown)

	// Per-tenant tuning (weights + exploration thresholds). A zero-value tuning
	// (old tests, zero-value ctx) is substituted with DefaultTuning below.
	tuning Tuning
}

// scoreWithContext computes the full six-signal value model for one item.
func scoreWithContext(item models.ContentItem, ctx batchContext) ValuedItem {
	// A zero-value tuning (weights sum to 0) means the caller didn't populate
	// it — fall back to defaults so scoring never collapses to zero.
	if ctx.tuning.EngagementWeight+ctx.tuning.CompletionWeight+ctx.tuning.QualityWeight+ctx.tuning.VelocityWeight <= 0 {
		ctx.tuning = DefaultTuning()
	}
	id := item.PublicID.String()
	impressions := float64(item.ImpressionCount)

	// ── Signal 1: exposure-normalized engagement ────────────────────────────
	// Weighted engagement events per impression, shrunk toward the corpus
	// prior. Below minImpressionsForRate the rate is noise, so we bridge to
	// the legacy raw-count squash (the pre-telemetry regime) instead of
	// pretending we can normalize by exposure we never measured.
	engagementEvents := engagementEventCount(item)
	exposureMeasured := impressions >= minImpressionsForRate
	var engagement float64
	if exposureMeasured {
		prior := ctx.engagementPrior
		if prior <= 0 {
			prior = defaultEngagementPrior
		}
		rate := (engagementEvents + prior*engagementShrinkK) / (impressions + engagementShrinkK)
		// Normalize: an average item (rate == prior) lands at 0.25; 3× the
		// corpus rate lands at 0.5. Keeps spread without a hard ceiling.
		engagement = rate / (rate + 3*prior)
	} else {
		engagement = legacyEngagementSquash(item)
	}

	// ── Signal 2: completion rate ───────────────────────────────────────────
	completes := float64(ctx.completes[id])
	views := float64(ctx.views[id])
	completionPrior := ctx.completionPrior
	if completionPrior <= 0 {
		completionPrior = defaultCompletionPrior
	}
	completion := (completes + completionPrior*completionShrinkK) / (views + completionShrinkK)
	completion = clamp01(completion)

	// ── Signal 3: velocity ──────────────────────────────────────────────────
	recentPerHour := float64(ctx.recent[id]) / float64(velocityWindowHours)
	velocity := recentPerHour / (recentPerHour + velocityHalf)

	// ── Signal 4: durable quality ───────────────────────────────────────────
	quality := durableQuality(item)

	// ── Blend + adjustments ─────────────────────────────────────────────────
	// Weights come from the per-tenant tuning (control room); they are already
	// normalized to sum 1.0 by sanitizeTuning.
	value := ctx.tuning.EngagementWeight*engagement +
		ctx.tuning.CompletionWeight*completion +
		ctx.tuning.QualityWeight*quality +
		ctx.tuning.VelocityWeight*velocity

	// Signal 5: suitability (audio-first platform).
	suitAdj := suitabilityAdjustment(item)
	value += suitAdj

	// Signal 6: cost efficiency.
	costPenalty := costEfficiencyPenalty(item, ctx.costMedian)
	value -= costPenalty

	value = clamp01(value)

	confidence := impressions / (impressions + confidenceHalfImpressions)
	state := ExplorationEstablished
	// Exploring = never given a chance. Items with substantial LEGACY views had
	// their chance before impression telemetry existed (stage-4 cold start) —
	// treating the whole pre-existing library as exploring would freeze every
	// negative decision (incl. storage relief) for weeks. Both thresholds are
	// per-tenant tunable.
	if int(item.ImpressionCount) < ctx.tuning.ExploreImpressionTarget && item.ViewCount < ctx.tuning.LegacyExposureViewFloor {
		state = ExplorationExploring
	}

	return ValuedItem{
		ContentItemID:    item.PublicID,
		Value:            value,
		Confidence:       confidence,
		ExplorationState: state,
		Breakdown: Breakdown{
			Engagement:       engagement,
			Completion:       completion,
			Quality:          quality,
			Velocity:         velocity,
			SuitabilityAdj:   suitAdj,
			CostPenalty:      costPenalty,
			ExposureMeasured: exposureMeasured,
		},
		Reasons: valueReasons(item, engagement, completion, quality, velocity, suitAdj, costPenalty, exposureMeasured, state, ctx.tuning.ExploreImpressionTarget),
	}
}

// FallbackValue is the pre-intelligence thin-seam math, byte-for-byte: it is
// used for items that have no persisted score yet (cold start, brand-new
// ingest) so consumers always get a sane [0,1] value. Composition matches the
// original circulationMediaValue: 0.6·legacy-engagement + 0.4·quality with the
// suitability adjustment, freshness deliberately absent.
func FallbackValue(item models.ContentItem) float64 {
	value := 0.6*legacyEngagementSquash(item) + 0.4*durableQuality(item)
	value += suitabilityAdjustment(item)
	return clamp01(value)
}

// legacyEngagementSquash is the raw-count engagement composite from the ranking
// engine, soft-saturated into [0,1]. Kept as the pre-telemetry bridge: it
// conflates "unpopular" with "unseen", which is exactly what the exposure-
// normalized signal replaces once impressions exist.
func legacyEngagementSquash(item models.ContentItem) float64 {
	raw := 1.0*math.Log1p(float64(item.LikeCount)) +
		0.3*math.Log1p(float64(item.ViewCount)) +
		0.5*math.Log1p(float64(item.ShareCount)) +
		0.4*math.Log1p(float64(item.CommentCount))
	return raw / (raw + legacyEngagementHalf)
}

// engagementEventCount is the weighted engagement event total used by the
// exposure-normalized rate. Views are excluded — a view is exposure, not
// engagement, once impressions are measured.
func engagementEventCount(item models.ContentItem) float64 {
	return 1.0*float64(item.LikeCount) +
		0.5*float64(item.ShareCount) +
		0.4*float64(item.CommentCount)
}

// durableQuality mirrors the ranking engine's completeness + source tier
// signal. Owned here now — the intelligence package is the one place that
// defines what quality means for durable value.
func durableQuality(item models.ContentItem) float64 {
	var completeness float64
	if item.ThumbnailURL != nil && *item.ThumbnailURL != "" {
		completeness += 0.2
	}
	if item.Excerpt != nil && *item.Excerpt != "" {
		completeness += 0.15
	}
	if item.TranscriptID != nil {
		completeness += 0.2
	}
	if len(item.TopicTags) > 0 {
		completeness += 0.15
	}
	if item.Embedding != nil {
		completeness += 0.15
	}
	if item.DurationSec != nil && *item.DurationSec > 0 {
		completeness += 0.15
	}

	tier, ok := sourceTier[item.Source]
	if !ok {
		tier = 0.5
	}
	return 0.6*completeness + 0.4*tier
}

var sourceTier = map[models.SourceType]float64{
	models.SourceTypeManual:   1.0,
	models.SourceTypeUpload:   0.9,
	models.SourceTypeRSS:      0.8,
	models.SourceTypeYouTube:  0.8,
	models.SourceTypePodcast:  0.8,
	models.SourceTypeTelegram: 0.7,
	models.SourceTypeWebsite:  0.6,
}

func suitabilityAdjustment(item models.ContentItem) float64 {
	switch item.MediaSuitability {
	case models.MediaSuitabilityAudioFirstTalkingHead, models.MediaSuitabilityAudioFirstShow:
		return suitabilityAudioBonus
	case models.MediaSuitabilityUnsuitable, models.MediaSuitabilityVisualDependent:
		return -suitabilityPenalty
	default:
		return 0
	}
}

// costEfficiencyPenalty charges items whose bytes-per-useful-minute is a corpus
// outlier. No penalty when the corpus median is unknown or the item has no
// size/duration data — absence of evidence is not an outlier.
func costEfficiencyPenalty(item models.ContentItem, costMedian float64) float64 {
	if costMedian <= 0 || item.FileSizeBytes <= 0 || item.DurationSec == nil || *item.DurationSec < usefulMinuteFloorSec {
		return 0
	}
	perMin := float64(item.FileSizeBytes) / (float64(*item.DurationSec) / 60.0)
	ratio := perMin / costMedian
	if ratio <= costOutlierRatio {
		return 0
	}
	penalty := costPenaltySlope * (ratio - costOutlierRatio)
	if penalty > costPenaltyMax {
		penalty = costPenaltyMax
	}
	return penalty
}

// costPerUsefulMinute returns bytes/minute for items with usable size+duration,
// used to build the corpus median. Second return reports usability.
func costPerUsefulMinute(item models.ContentItem) (float64, bool) {
	if item.FileSizeBytes <= 0 || item.DurationSec == nil || *item.DurationSec < usefulMinuteFloorSec {
		return 0, false
	}
	return float64(item.FileSizeBytes) / (float64(*item.DurationSec) / 60.0), true
}

// valueReasons renders the human-readable proof for a score — every consumer
// (cockpit, storage candidates, stage-5 ledger) shows these verbatim.
func valueReasons(item models.ContentItem, engagement, completion, quality, velocity, suitAdj, costPenalty float64, exposureMeasured bool, state string, exploreTarget int) []string {
	reasons := make([]string, 0, 4)
	if state == ExplorationExploring {
		reasons = append(reasons, fmt.Sprintf("Still exploring: %d/%d impressions — value leans on priors until exposure accumulates.", item.ImpressionCount, exploreTarget))
	}
	if exposureMeasured {
		reasons = append(reasons, fmt.Sprintf("Engagement per impression scores %.2f over %d measured impressions.", engagement, item.ImpressionCount))
	} else {
		reasons = append(reasons, fmt.Sprintf("Exposure not yet measured — engagement %.2f from raw interaction counts (legacy regime).", engagement))
	}
	if completion >= 0.5 {
		reasons = append(reasons, fmt.Sprintf("Strong completion signal (%.2f) — listeners finish this.", completion))
	} else if completion < 0.15 {
		reasons = append(reasons, fmt.Sprintf("Weak completion signal (%.2f) — listeners drop off.", completion))
	}
	if velocity > 0.3 {
		reasons = append(reasons, fmt.Sprintf("Actively circulating: velocity %.2f in the last %dh.", velocity, velocityWindowHours))
	}
	if quality < 0.4 {
		reasons = append(reasons, fmt.Sprintf("Low durable quality (%.2f): missing metadata/enrichment drags value.", quality))
	}
	if suitAdj < 0 {
		reasons = append(reasons, "Marked visually-dependent/unsuitable for an audio-first feed.")
	}
	if costPenalty > 0 {
		reasons = append(reasons, fmt.Sprintf("Storage-cost outlier: −%.2f for bytes-per-minute far above the corpus median.", costPenalty))
	}
	return reasons
}

func clamp01(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}
