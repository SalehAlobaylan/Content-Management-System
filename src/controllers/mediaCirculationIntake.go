package controllers

import (
	"content-management-system/src/models"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// Media Circulation — intake (source × bucket) opportunity scorer (Slice 3, D13).
//
// The one genuinely new algorithm: decide, per approved media source, whether to
// pull now / how much / skip — driven by whether the library needs what the source
// produces (bucket demand) and whether predicted incoming quality clears the bar
// (the D8 gate). It is deterministic and reuses existing signals: circulationMediaValue
// (D3 seam), computeLibraryBucketInventory (Slice 1), storage-health headroom, and
// SourceRunTelemetry. The real intelligence arrives at stage 4 through the same seam.

const (
	mediaCircVerdictPullNow          = "pull_now"
	mediaCircVerdictPullLimited      = "pull_limited"
	mediaCircVerdictDeepPull         = "deep_pull"
	mediaCircVerdictSkipSource       = "skip_source"
	mediaCircVerdictPauseSource      = "pause_source"
	mediaCircVerdictNeedsAdminReview = "needs_admin_review"

	mediaCircSourceRecentItemsLimit = 100
	mediaCircIntakeFailureRateHigh  = 0.35
	mediaCircDeepPullHeadroom       = 0.5
	mediaCircDeepPullPrior          = 0.6
	mediaCircOrderingFloor          = 0.1
	mediaCircSaturatedDemandWeight  = 0.05
)

type sourceOpportunity struct {
	QualityPrior     float64
	BucketMatch      float64
	Freshness        float64
	CostHeadroom     float64
	OrderingHeadroom float64
	FailureRate      float64
	BacklogFactor    float64
	PremiumBoost     float64
	Score            float64
	FillsThin        bool
	MatchedBuckets   []string
}

// computeIntakeRecommendations scores every active media source and returns bounded
// source recommendations (verdict + allowed_intake).
func computeIntakeRecommendations(db *gorm.DB, tenantID string, circPolicy models.MediaCirculationPolicy, storagePolicy models.StoragePolicy, storage storageHealthResponse) []circulationRecInput {
	var sources []models.ContentSource
	db.Where("tenant_id = ? AND category = ? AND is_active = TRUE", tenantID, models.SourceCategoryMedia).Find(&sources)
	if len(sources) == 0 {
		return nil
	}

	// Bucket demand (D5) + cost headroom (D7).
	buckets := computeLibraryBucketInventory(db, tenantID)
	bucketState := make(map[string]string, len(buckets))
	for _, b := range buckets {
		bucketState[b.Bucket] = b.State
	}
	costHeadroom := storageCostHeadroom(storagePolicy, storage.Proof)
	opBudget := getStorageOpBudgetStatus(db, tenantID)
	backlog := computeMediaCircAtomizationBacklog(db, tenantID)
	backlogFactor := atomizationBacklogIntakeFactor(backlog)

	// Gate baseline (D8): value floor below target; at/above target, incoming must
	// beat what we'd evict (P25 visible value + margin).
	baseline := circPolicy.ValueFloor
	if costHeadroom <= 0 {
		baseline = clampFloat(visibleValuePercentile(db, tenantID, 0.25)+circPolicy.MarginalMargin, 0, 1)
	}

	ids := make([]uuid.UUID, len(sources))
	for i, s := range sources {
		ids[i] = s.PublicID
	}
	stats := sourceTelemetryStats(db, tenantID, ids)
	overrides := loadActiveMediaCircOverrides(db, tenantID)

	type evaluated struct {
		src     models.ContentSource
		opp     sourceOpportunity
		hard    string // non-empty = terminal verdict, not eligible for budget
		reasons []string
	}
	evals := make([]evaluated, 0, len(sources))
	now := time.Now()
	for _, s := range sources {
		meanVal, count, dist := sourceProducedProfile(db, tenantID, s.Name)
		st := stats[s.PublicID]
		prior, hasHistory := sourceQualityPrior(st, meanVal, count)
		premiumOverride, premium := mediaCircHasOverride(overrides, "source", s.PublicID, models.MediaCirculationOverridePremiumSource)
		_, sourceHold := mediaCircHasOverride(overrides, "source", s.PublicID, models.MediaCirculationOverrideEditorialHold)
		match, matchedThin, fillsThin := bucketDemandMatch(dist, bucketState)
		fresh := freshnessComponentAt(s, circPolicy.FreshnessDemandWeight, circPolicy.SourceMinIntervalMinutes, circPolicy.SourceMaxIntervalMinutes, now)
		failRate := sourceFailureRate(st)
		cadenceOK, cadenceReason := sourceCadenceEligibleAt(s, circPolicy, now)
		premiumBoost := 1.0
		if premium {
			premiumBoost = 1.15
		}

		opp := sourceOpportunity{
			QualityPrior:     prior,
			BucketMatch:      match,
			Freshness:        fresh,
			CostHeadroom:     costHeadroom,
			OrderingHeadroom: maxFloat(costHeadroom, mediaCircOrderingFloor),
			FailureRate:      failRate,
			BacklogFactor:    backlogFactor,
			PremiumBoost:     premiumBoost,
			Score:            prior * match * fresh * maxFloat(costHeadroom, mediaCircOrderingFloor) * backlogFactor * premiumBoost,
			FillsThin:        fillsThin,
			MatchedBuckets:   matchedThin,
		}

		reasons := []string{}
		if premium {
			reasons = append(reasons, mediaCircOverrideReason(premiumOverride))
		}
		hard := ""
		switch {
		case sourceHold:
			hard = mediaCircVerdictSkipSource
			reasons = append(reasons, "Editorial hold blocks automated intake for this source.")
		case !cadenceOK:
			hard = mediaCircVerdictSkipSource
			reasons = append(reasons, cadenceReason)
		case opBudget.ClassAStatus == "cap":
			hard = mediaCircVerdictSkipSource
			reasons = append(reasons, "Op budget cap reached; pulling would risk object-store Class A overage.")
		case !hasHistory:
			hard = mediaCircVerdictNeedsAdminReview
			reasons = append(reasons, "No fetch history or produced items yet; an admin should decide before intake.")
		case failRate > mediaCircIntakeFailureRateHigh:
			if premium {
				reasons = append(reasons, fmt.Sprintf("High failure rate (%.0f%%); premium source is not auto-paused.", failRate*100))
			} else {
				hard = mediaCircVerdictPauseSource
				reasons = append(reasons, fmt.Sprintf("High failure rate (%.0f%%); pause until the source recovers.", failRate*100))
			}
		case prior < baseline:
			hard = mediaCircVerdictSkipSource
			reasons = append(reasons, fmt.Sprintf("Predicted value %.2f is below the bar %.2f; pulling would replace better content with worse.", prior, baseline))
		case match <= 0:
			hard = mediaCircVerdictSkipSource
			reasons = append(reasons, "Source only fills already-saturated duration buckets; no library need right now.")
		}
		evals = append(evals, evaluated{src: s, opp: opp, hard: hard, reasons: reasons})
	}

	recs := make([]circulationRecInput, 0, len(evals))
	eligible := make([]evaluated, 0, len(evals))
	for _, e := range evals {
		if e.hard != "" {
			recs = append(recs, sourceRec(e.src, e.hard, e.opp, e.reasons, 0))
			continue
		}
		eligible = append(eligible, e)
	}

	// Budget allocation (D12): highest score first.
	sort.Slice(eligible, func(i, j int) bool { return eligible[i].opp.Score > eligible[j].opp.Score })
	allocs := allocateIntakeBudget(len(eligible), circPolicy.MaxIntakePerCycle, circPolicy.MaxIntakePerSourcePerCycle)
	for i, e := range eligible {
		allowed := allocs[i]
		if allowed <= 0 {
			recs = append(recs, sourceRec(e.src, mediaCircVerdictSkipSource, e.opp,
				append(e.reasons, "This cycle's intake budget is already spent."), 0))
			continue
		}
		verdict := mediaCircVerdictPullNow
		reason := "Library needs this source's content and predicted quality clears the bar."
		if e.opp.BacklogFactor < 1 {
			reason += " Atomization backlog is damping intake priority."
		}
		switch {
		case e.opp.QualityPrior >= mediaCircDeepPullPrior && e.opp.FillsThin && e.opp.CostHeadroom >= mediaCircDeepPullHeadroom:
			verdict = mediaCircVerdictDeepPull
			reason = "Strong source filling thin buckets with ample headroom; pull deeply."
		case allowed < circPolicy.MaxIntakePerSourcePerCycle:
			verdict = mediaCircVerdictPullLimited
			reason = "Pull a limited batch — remaining cycle budget is small."
		}
		recs = append(recs, sourceRec(e.src, verdict, e.opp, append(e.reasons, reason), allowed))
	}
	return recs
}

func sourceRec(s models.ContentSource, verdict string, opp sourceOpportunity, reasons []string, allowed int) circulationRecInput {
	action := "pull"
	switch verdict {
	case mediaCircVerdictSkipSource:
		action = "skip"
	case mediaCircVerdictPauseSource:
		action = "pause"
	case mediaCircVerdictNeedsAdminReview:
		action = "review"
	}
	return circulationRecInput{
		SubjectID:   s.PublicID,
		SubjectKind: "content_source",
		Verdict:     verdict,
		Action:      action,
		Score:       opp.Score,
		Reasons:     reasons,
		Metrics: map[string]interface{}{
			"source_name":                s.Name,
			"quality_prior":              opp.QualityPrior,
			"bucket_demand_match":        opp.BucketMatch,
			"freshness":                  opp.Freshness,
			"cost_headroom":              opp.CostHeadroom,
			"ordering_headroom":          opp.OrderingHeadroom,
			"failure_rate":               opp.FailureRate,
			"atomization_backlog_factor": opp.BacklogFactor,
			"premium_boost":              opp.PremiumBoost,
			"allowed_intake":             allowed,
			"matched_thin_buckets":       opp.MatchedBuckets,
		},
	}
}

// allocateIntakeBudget spends a per-cycle intake budget across score-ranked sources
// (index 0 = highest score), each capped per source. Pure and total-bounded:
// sum(result) <= maxPerCycle, each element <= maxPerSource, sources past the budget
// get 0.
func allocateIntakeBudget(n, maxPerCycle, maxPerSource int) []int {
	out := make([]int, n)
	budget := maxPerCycle
	for i := 0; i < n; i++ {
		if budget <= 0 || maxPerSource <= 0 {
			out[i] = 0
			continue
		}
		allowed := maxPerSource
		if allowed > budget {
			allowed = budget
		}
		out[i] = allowed
		budget -= allowed
	}
	return out
}

// ---- signal helpers (pure where possible) ----

// sourceQualityPrior blends acceptance yield (telemetry) with produced-item quality.
// Returns hasHistory=false when there is neither telemetry nor produced items.
func sourceQualityPrior(stats sourceRecommendationStats, producedValue float64, producedCount int) (float64, bool) {
	hasTelemetry := stats.RunCount >= 2 && stats.Fetched > 0
	hasProduced := producedCount > 0
	if !hasTelemetry && !hasProduced {
		return 0, false
	}
	yield := 0.0
	if stats.Fetched > 0 {
		yield = float64(stats.Accepted) / float64(stats.Fetched)
	}
	switch {
	case hasTelemetry && hasProduced:
		return clampFloat(0.7*producedValue+0.3*yield, 0, 1), true
	case hasProduced:
		return clampFloat(producedValue, 0, 1), true
	default:
		return clampFloat(yield, 0, 1), true
	}
}

func sourceFailureRate(stats sourceRecommendationStats) float64 {
	denom := stats.Fetched + stats.Failed
	if denom <= 0 {
		return 0
	}
	return float64(stats.Failed) / float64(denom)
}

func demandWeight(state string) float64 {
	switch state {
	case "thin":
		return 1.0
	case "ok":
		return 0.3
	default: // saturated / unknown
		return mediaCircSaturatedDemandWeight
	}
}

// bucketDemandMatch weights a source's produced-bucket distribution by current demand.
func bucketDemandMatch(dist map[string]float64, bucketState map[string]string) (float64, []string, bool) {
	match := 0.0
	matchedThin := []string{}
	fillsThin := false
	for bucket, share := range dist {
		match += share * demandWeight(bucketState[bucket])
		if bucketState[bucket] == "thin" && share > 0 {
			matchedThin = append(matchedThin, bucket)
			fillsThin = true
		}
	}
	sort.Strings(matchedThin)
	return match, matchedThin, fillsThin
}

func freshnessComponent(source models.ContentSource, w float64) float64 {
	return freshnessComponentAt(source, w, 1, 0, time.Now())
}

func freshnessComponentAt(source models.ContentSource, w float64, minIntervalMinutes, maxIntervalMinutes int, now time.Time) float64 {
	overdue := 1.0 // never fetched = maximally overdue
	interval := boundedSourceIntervalMinutes(source.FetchIntervalMinutes, minIntervalMinutes, maxIntervalMinutes)
	if source.LastFetchedAt != nil && interval > 0 {
		elapsed := now.Sub(*source.LastFetchedAt).Minutes()
		overdue = clampFloat(elapsed/float64(interval), 0, 1)
	}
	return (1 - w) + w*overdue
}

func sourceCadenceEligibleAt(source models.ContentSource, policy models.MediaCirculationPolicy, now time.Time) (bool, string) {
	if source.LastFetchedAt == nil {
		return true, ""
	}
	minInterval := policy.SourceMinIntervalMinutes
	if minInterval < 1 {
		minInterval = 1
	}
	elapsed := now.Sub(*source.LastFetchedAt).Minutes()
	if elapsed < float64(minInterval) {
		return false, fmt.Sprintf("Fetched %.0f minutes ago; source minimum interval is %d minutes.", elapsed, minInterval)
	}
	return true, ""
}

func boundedSourceIntervalMinutes(sourceInterval, minInterval, maxInterval int) int {
	interval := sourceInterval
	if interval <= 0 {
		interval = minInterval
	}
	if minInterval > 0 && interval < minInterval {
		interval = minInterval
	}
	if maxInterval > 0 && interval > maxInterval {
		interval = maxInterval
	}
	if interval <= 0 {
		interval = 1
	}
	return interval
}

func storageCostHeadroom(policy models.StoragePolicy, proof storageProofMetrics) float64 {
	target := float64(policy.TargetUtilizationPct)
	if target <= 0 {
		target = 80
	}
	if proof.QuotaBytes <= 0 {
		return 1 // no cost cap configured; the value-floor gate still applies
	}
	return clampFloat((target-proof.UtilizationPct)/target, 0, 1)
}

// ---- data-gathering helpers ----

// sourceProducedProfile returns the mean circulation value, item count, and
// duration-bucket distribution of a source's recent produced feed units.
func sourceProducedProfile(db *gorm.DB, tenantID, sourceName string) (float64, int, map[string]float64) {
	var items []models.ContentItem
	db.Where("tenant_id = ? AND source_name = ?", tenantID, sourceName).
		Where("is_feed_unit = TRUE").
		Where("status = ?", models.ContentStatusReady).
		Order("COALESCE(published_at, created_at) DESC").
		Limit(mediaCircSourceRecentItemsLimit).
		Find(&items)
	if len(items) == 0 {
		return 0, 0, map[string]float64{}
	}
	sum := 0.0
	bucketCounts := map[string]float64{}
	bucketed := 0.0
	for _, it := range items {
		sum += circulationMediaValue(it)
		if it.DurationBucket != nil && *it.DurationBucket != "" {
			bucketCounts[*it.DurationBucket]++
			bucketed++
		}
	}
	dist := map[string]float64{}
	if bucketed > 0 {
		for b, c := range bucketCounts {
			dist[b] = c / bucketed
		}
	}
	return sum / float64(len(items)), len(items), dist
}

func sourceTelemetryStats(db *gorm.DB, tenantID string, sourceIDs []uuid.UUID) map[uuid.UUID]sourceRecommendationStats {
	cutoff := time.Now().AddDate(0, 0, -7)
	var rows []sourceRecommendationStats
	db.Model(&models.SourceRunTelemetry{}).
		Select("source_id, COUNT(*) AS run_count, COALESCE(SUM(fetched),0) AS fetched, COALESCE(SUM(accepted),0) AS accepted, COALESCE(SUM(failed),0) AS failed, COALESCE(SUM(duplicates),0) AS duplicates, COALESCE(SUM(filtered),0) AS filtered").
		Where("tenant_id = ? AND source_id IN ? AND finished_at > ?", tenantID, sourceIDs, cutoff).
		Group("source_id").
		Scan(&rows)
	m := make(map[uuid.UUID]sourceRecommendationStats, len(rows))
	for _, r := range rows {
		m[r.SourceID] = r
	}
	return m
}

func visibleValuePercentile(db *gorm.DB, tenantID string, percentile float64) float64 {
	var items []models.ContentItem
	db.Where("tenant_id = ? AND is_feed_unit = TRUE AND feed_visibility = ? AND status = ?",
		tenantID, "visible", models.ContentStatusReady).
		Where("type IN ?", []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}).
		Order("view_count ASC, created_at ASC").
		Limit(mediaCircRankDownScanLimit).
		Find(&items)
	if len(items) == 0 {
		return 0
	}
	values := make([]float64, 0, len(items))
	for _, it := range items {
		values = append(values, circulationMediaValue(it))
	}
	sort.Float64s(values)
	return percentileValue(values, percentile)
}

func percentileValue(sortedValues []float64, percentile float64) float64 {
	if len(sortedValues) == 0 {
		return 0
	}
	if percentile <= 0 {
		return sortedValues[0]
	}
	if percentile >= 1 {
		return sortedValues[len(sortedValues)-1]
	}
	idx := int(float64(len(sortedValues)-1) * percentile)
	return sortedValues[idx]
}

func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
