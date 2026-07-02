package controllers

import "content-management-system/src/models"

// circulationMediaValue is the THIN backing of the D3 value seam: a deterministic,
// durable media-value score in [0,1]. It is NOT the real intelligence engine — that
// is built at stage 4 (Ranking/Intelligence refinement) and slots into this same
// function's callers without a reshape. See docs/media-circulation-engine.md (D3).
//
// Composition — freshness is deliberately DEMOTED (absent), because media value is
// durable, not recency-driven (D2a). It reuses the ranking engine's signal helpers
// rather than inventing new scoring:
//   - engagement  ← computeEngagementRaw, squashed into [0,1]
//   - quality     ← computeQuality (completeness + source tier), already [0,1]
//   - suitability ← Wahb is audio-first: audio-first bonus, unsuitable/visual penalty
//
// Used by Slice 2 to pick rank_down candidates (visible units below the value floor)
// and reused by Slice 3's intake marginal-comparison gate.
func circulationMediaValue(item models.ContentItem) float64 {
	// Engagement: reuse the ranking composite, soft-saturate the unbounded log-sum
	// into [0,1]. engagementHalf is the engagement level that maps to 0.5.
	const engagementHalf = 4.0
	engRaw := computeEngagementRaw(item)
	engagement := engRaw / (engRaw + engagementHalf)

	// Completeness + source tier — already normalized to [0,1].
	quality := computeQuality(item)

	value := 0.6*engagement + 0.4*quality

	switch item.MediaSuitability {
	case models.MediaSuitabilityAudioFirstTalkingHead, models.MediaSuitabilityAudioFirstShow:
		value += 0.05
	case models.MediaSuitabilityUnsuitable, models.MediaSuitabilityVisualDependent:
		value -= 0.15
	}

	return clampFloat(value, 0, 1)
}
