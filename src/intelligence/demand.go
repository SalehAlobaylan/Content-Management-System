package intelligence

import (
	"fmt"
	"time"

	"content-management-system/src/models"
)

// The Demand surface (grilling Q6): measured demand per duration bucket,
// value-weighted coverage, and the gap between them. `gap` is what the D13
// intake opportunity score consumes as bucket demand once telemetry is
// measured; until then consumers fall back to the supply-count guess and the
// snapshot says Measured=false so every surface can show which regime it's in.

const (
	// demandLookback: how much serve history feeds a demand snapshot.
	demandLookback = 7 * 24 * time.Hour
	// measuredMinServes: below this many serves in the lookback the demand
	// signal is noise — consumers stay on the supply-based fallback.
	measuredMinServes = 50
	// unscoredCoverageValue: neutral value assumed for visible units that have
	// no persisted score yet, so coverage is defined from day one.
	unscoredCoverageValue = 0.4
	// Demand blend weights: serve share carries most signal, exhaustion is the
	// true miss, repeat pressure the small-library symptom.
	demandWeightServeShare = 0.50
	demandWeightExhaustion = 0.35
	demandWeightRepeat     = 0.15
	// gapThinThreshold: a gap beyond ±this classifies a bucket thin/saturated
	// in the measured regime.
	gapThinThreshold = 0.25
)

// DemandSnapshot is the Demand-surface output for one duration bucket.
type DemandSnapshot struct {
	Bucket        string  `json:"bucket"`
	DemandScore   float64 `json:"demand_score"`   // [0,1]
	CoverageScore float64 `json:"coverage_score"` // [0,1], value-weighted supply
	Gap           float64 `json:"gap"`            // demand − coverage, [-1,1]
	Measured      bool    `json:"measured"`

	// Raw inputs, for explainability.
	Serves       int64 `json:"serves"`
	Exhaustions  int64 `json:"exhaustions"`
	RepeatServes int64 `json:"repeat_serves"`
}

// GapState maps a measured gap onto the cockpit's display vocabulary.
func GapState(gap float64) string {
	switch {
	case gap > gapThinThreshold:
		return "thin"
	case gap < -gapThinThreshold:
		return "saturated"
	default:
		return "ok"
	}
}

// GapDemandWeight converts a measured gap into the D13 intake demand weight,
// spanning the same range the state-based weights did (saturated ≈ 0.05,
// neutral = 0.3, starving = 1.0).
func GapDemandWeight(gap float64) float64 {
	w := 0.3 + 0.7*gap
	if w < 0.05 {
		return 0.05
	}
	if w > 1 {
		return 1
	}
	return w
}

// DemandSnapshots aggregates the lookback's serve telemetry and the corpus's
// value-weighted supply into one snapshot per requested bucket. The second
// return reports whether the demand side is measured (enough serves) — when
// false, DemandScore/Gap are still computed from what little exists but
// consumers should prefer their supply-based fallback.
func (e Engine) DemandSnapshots(tenantID string, buckets []string) (map[string]DemandSnapshot, bool) {
	if e.DB == nil {
		return map[string]DemandSnapshot{}, false
	}
	cutoff := time.Now().Add(-demandLookback)

	// Serve telemetry per bucket (bucket-level rows only; topic axis keeps its
	// own rows with topic != '').
	type demandRow struct {
		Bucket       string `gorm:"column:bucket"`
		Serves       int64  `gorm:"column:serves"`
		Exhaustions  int64  `gorm:"column:exhaustions"`
		RepeatServes int64  `gorm:"column:repeat_serves"`
	}
	var rows []demandRow
	e.DB.Model(&models.MediaDemandStat{}).
		Select("bucket, COALESCE(SUM(serves),0) AS serves, COALESCE(SUM(exhaustions),0) AS exhaustions, COALESCE(SUM(repeat_serves),0) AS repeat_serves").
		Where("tenant_id = ? AND topic = '' AND window_start > ?", tenantID, cutoff).
		Group("bucket").
		Scan(&rows)

	byBucket := make(map[string]demandRow, len(rows))
	var totalServes, globalExhaustions int64
	for _, r := range rows {
		if r.Bucket == demandBucketAll {
			globalExhaustions += r.Exhaustions
			continue
		}
		byBucket[r.Bucket] = r
		totalServes += r.Serves
	}
	measured := totalServes >= measuredMinServes

	// Value-weighted coverage per bucket: the sum of durable values sitting
	// visible in each shelf (unscored units count at the neutral prior).
	type coverageRow struct {
		Bucket string  `gorm:"column:bucket"`
		Weight float64 `gorm:"column:weight"`
	}
	var covRows []coverageRow
	e.DB.Model(&models.ContentItem{}).
		Select(fmt.Sprintf("content_items.duration_bucket AS bucket, COALESCE(SUM(COALESCE(mis.value, %f)),0) AS weight", unscoredCoverageValue)).
		Joins("LEFT JOIN media_intelligence_scores mis ON mis.content_item_id = content_items.public_id").
		Where("content_items.tenant_id = ?", tenantID).
		Where("content_items.is_feed_unit = TRUE AND content_items.feed_visibility = 'visible'").
		Where("content_items.status = ?", models.ContentStatusReady).
		Where("content_items.type IN ?", []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}).
		Where("content_items.duration_bucket IS NOT NULL").
		Group("content_items.duration_bucket").
		Scan(&covRows)
	coverageWeight := make(map[string]float64, len(covRows))
	var maxCoverage float64
	for _, r := range covRows {
		coverageWeight[r.Bucket] = r.Weight
		if r.Weight > maxCoverage {
			maxCoverage = r.Weight
		}
	}

	// Normalization maxima across the requested buckets.
	var maxServes, maxExhaustions int64
	for _, b := range buckets {
		r := byBucket[b]
		if r.Serves > maxServes {
			maxServes = r.Serves
		}
		// Global (unfiltered-feed) exhaustions are attributed to buckets by
		// serve share below; include them in the max estimate conservatively.
		if r.Exhaustions > maxExhaustions {
			maxExhaustions = r.Exhaustions
		}
	}

	out := make(map[string]DemandSnapshot, len(buckets))
	for _, b := range buckets {
		r := byBucket[b]

		// Attribute global exhaustions proportionally to serve share — an
		// unfiltered page running short is pressure on the whole library.
		exhaustions := r.Exhaustions
		if globalExhaustions > 0 && totalServes > 0 {
			exhaustions += int64(float64(globalExhaustions) * float64(r.Serves) / float64(totalServes))
		}

		var serveNorm, exhaustNorm, repeatNorm float64
		if maxServes > 0 {
			serveNorm = float64(r.Serves) / float64(maxServes)
		}
		if denom := maxExhaustions + globalExhaustions; denom > 0 {
			exhaustNorm = clamp01(float64(exhaustions) / float64(denom))
		}
		if r.Serves > 0 {
			repeatNorm = clamp01(float64(r.RepeatServes) / float64(r.Serves))
		}
		demand := demandWeightServeShare*serveNorm + demandWeightExhaustion*exhaustNorm + demandWeightRepeat*repeatNorm

		var coverage float64
		if maxCoverage > 0 {
			coverage = coverageWeight[b] / maxCoverage
		}

		gap := demand - coverage
		if gap > 1 {
			gap = 1
		}
		if gap < -1 {
			gap = -1
		}

		out[b] = DemandSnapshot{
			Bucket:        b,
			DemandScore:   demand,
			CoverageScore: coverage,
			Gap:           gap,
			Measured:      measured,
			Serves:        r.Serves,
			Exhaustions:   exhaustions,
			RepeatServes:  r.RepeatServes,
		}
	}
	return out, measured
}
