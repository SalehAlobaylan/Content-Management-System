// Package intelligence is the Ranking/Intelligence System — the shared media-value
// pillar that Circulation, Storage, the For You feed hooks, and (at stage 5) the
// Autopilot consume for their decisions. It is stage 4 of the media roadmap.
//
// Design contract (locked via docs/ranking-intelligence-grilling.md, Q1–Q14):
//
//   - Value    — durable media value in [0,1] with breakdown + confidence + reasons.
//     Six signals: exposure-normalized engagement, completion rate, velocity,
//     durable quality, suitability, cost efficiency. Freshness is deliberately
//     ABSENT (media value is durable, not recency-driven — design doc D2a).
//   - Demand   — per-bucket (later per-topic) demand/coverage/gap measured from
//     serve-side telemetry (built across slices 2–3).
//   - Exploration — per-item exploration state so consumers can distinguish
//     "low value" from "never given a chance" (slice 4 wires the feed slice).
//
// Every rate signal is shrunk toward a corpus prior weighted by sample size
// (empirical-Bayes), which is also what produces the confidence output — one
// mechanism serving the value model, the storage fallback, and exploration.
//
// Package discipline (grilling Q2): no gin/controller imports, no controller
// globals, contract types owned here, DB injected — so a later extraction to a
// dedicated service is a transport change, not a redesign.
package intelligence

import (
	"math"
	"time"

	"github.com/google/uuid"
)

// Exploration states (grilling Q7). "retrial" is a bounded re-entry into
// exploration when a source's track record materially improves (slice 4).
const (
	ExplorationExploring   = "exploring"
	ExplorationEstablished = "established"
	ExplorationRetrial     = "retrial"
)

// Code-default tuning constants. The OPERATIONAL subset (the four signal
// weights, exploreImpressionTarget, legacyExposureViewFloor, demotionDefault-
// Factor, demotionHalfLifeDays, explorationSliceEvery) is now the fallback for
// DefaultTuning — admins can override those per-tenant through the control room
// (media_intelligence_configs). The MODEL-SHAPE constants (shrinkage, priors,
// confidence half, refresh cadence, velocity window, cost/suitability) stay
// code-only per Config Discipline — they define the model, not its operation.
const (
	// Signal weights for the full model. Sum of the four rate/state signals is
	// 1.0; suitability and cost apply as bounded adjustments after the blend.
	weightEngagement = 0.35
	weightCompletion = 0.25
	weightQuality    = 0.20
	weightVelocity   = 0.20

	// Suitability adjustment — Wahb is audio-first. Mirrors the thin-seam
	// values so the fallback and the full model agree on suitability.
	suitabilityAudioBonus = 0.05
	suitabilityPenalty    = 0.15

	// Cost-efficiency penalty: items whose storage cost per useful minute is a
	// corpus outlier bleed up to this much value (a cache pays per byte).
	costPenaltyMax        = 0.10
	costOutlierRatio      = 2.0  // penalty starts above 2× the corpus median
	costPenaltySlope      = 0.05 // penalty per 1× beyond the outlier ratio
	minCostSampleSize     = 5    // need at least this many sized items for a median
	usefulMinuteFloorSec  = 60   // guard against divide-by-tiny durations
	legacyEngagementHalf  = 4.0  // thin-seam squash constant (kept for fallback)
	minImpressionsForRate = 10   // below this, exposure-normalized rates are noise

	// Shrinkage pseudo-counts (empirical-Bayes k).
	engagementShrinkK = 25.0
	completionShrinkK = 10.0

	// Corpus-prior floors when the corpus itself has no signal yet.
	defaultEngagementPrior = 0.02 // engagement events per impression
	defaultCompletionPrior = 0.25 // completes per view

	// Confidence + exploration thresholds. Confidence is impressions-driven:
	// conf = I / (I + confidenceHalfImpressions). An item is `exploring` until
	// it has seen exploreImpressionTarget impressions — unless its legacy view
	// count proves it already had (unmeasured) exposure before stage 4.
	confidenceHalfImpressions = 50.0
	exploreImpressionTarget   = 50
	legacyExposureViewFloor   = 25

	// explorationSliceEvery: one slot in this many For You positions is reserved
	// for a still-exploring item (~10%). Default here; the feed hook reads the
	// per-tenant tuning override. Consumed in controllers/feedIntelligenceHooks.go.
	explorationSliceEvery = 10

	// Velocity: recent interactions per hour, squashed at velocityHalf.
	velocityWindowHours = 24
	velocityHalf        = 0.5

	// Refresh discipline (grilling Q9 — three triggers, bounded batches).
	refreshTTL                = 24 * time.Hour
	refreshBatchSize          = 200
	refreshImpressionDelta    = 20 // event-nudge: impressions moved this much since compute
	refreshEngagementDelta    = 5  // event-nudge: like+share+comment moved this much
	refreshLoopInterval       = 10 * time.Minute
	demotionHalfLifeDays      = 14 // rank_down decay half-life (slice 4 consumes this)
)

// Storage-eligibility thresholds (grilling Q3): storage may treat an item as
// evictable on VALUE grounds only when the model is confident; low-confidence
// items fall back to the legacy age+views rule and the exploration guard.
// Code defaults per Config Discipline.
const (
	StorageEligibilityMinConfidence = 0.5
	StorageEligibilityValueFloor    = 0.25
)

// Breakdown is the per-signal decomposition of a value score. All fields are in
// [0,1] except the adjustments, which are signed.
type Breakdown struct {
	Engagement       float64 `json:"engagement"`
	Completion       float64 `json:"completion"`
	Quality          float64 `json:"quality"`
	Velocity         float64 `json:"velocity"`
	SuitabilityAdj   float64 `json:"suitability_adj"`
	CostPenalty      float64 `json:"cost_penalty"`
	ExposureMeasured bool    `json:"exposure_measured"` // false = legacy raw-count regime (pre-telemetry)
}

// ValuedItem is the Value-surface output for one content item.
type ValuedItem struct {
	ContentItemID    uuid.UUID
	Value            float64
	Confidence       float64
	ExplorationState string
	Breakdown        Breakdown
	Reasons          []string
}

// DemotionHalfLife exposes the rank_down decay half-life to consumers (the
// feed hook applies it; the cockpit displays it).
func DemotionHalfLife() time.Duration {
	return time.Duration(demotionHalfLifeDays) * 24 * time.Hour
}

// EffectiveDemotion returns the decayed demotion multiplier at the default
// half-life. Thin wrapper over EffectiveDemotionAt kept for back-compat and
// display callers.
func EffectiveDemotion(factor float64, demotedAt time.Time, now time.Time) float64 {
	return EffectiveDemotionAt(factor, demotedAt, now, DemotionHalfLife())
}

// EffectiveDemotionAt returns the decayed demotion multiplier for a rank_down
// applied at demotedAt with the stored factor and a given half-life (grilling
// Q10):
//
//	effective = 1 − (1 − factor) · 2^(−age/half_life)
//
// factor is the immediate post-apply multiplier (e.g. 0.5); the result drifts
// back toward 1.0 (no demotion) as the demotion ages. Pure function — the
// half-life is passed in so per-tenant tuning threads through without globals.
func EffectiveDemotionAt(factor float64, demotedAt time.Time, now time.Time, halfLife time.Duration) float64 {
	if factor >= 1 || factor < 0 {
		return 1
	}
	if halfLife <= 0 {
		halfLife = DemotionHalfLife()
	}
	age := now.Sub(demotedAt)
	if age < 0 {
		age = 0
	}
	halfLives := age.Hours() / halfLife.Hours()
	decayed := 1 - (1-factor)*math.Exp2(-halfLives)
	if decayed > 1 {
		return 1
	}
	if decayed < 0 {
		return 0
	}
	return decayed
}
