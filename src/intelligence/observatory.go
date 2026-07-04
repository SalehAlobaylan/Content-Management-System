package intelligence

import (
	"sort"

	"content-management-system/src/models"
)

// Observatory read model — the visualization surface for the automated
// media-value engine. It composes the diagnostics counts with three richer
// aggregates that make the engine's automated processes legible:
//   - the value HISTOGRAM (the shape of the library's worth, split by state)
//   - the corpus SIGNAL AVERAGES (how value is currently composed)
//   - per-bucket DEMAND (measured supply-vs-demand economics)
//
// All read-only bounded aggregates; no new tables.

// ValueBinCount is the number of fine bins the [0,1] value axis is split into
// for the value spectrum.
const ValueBinCount = 40

// ValueBin is one column of the value spectrum: how many items fall in this
// value range, split by exploration state.
type ValueBin struct {
	// Min/Max are the value range this bin covers (Min inclusive).
	Min float64 `json:"min"`
	Max float64 `json:"max"`
	// Counts by state.
	Exploring   int64 `json:"exploring"`
	Established int64 `json:"established"`
	Total       int64 `json:"total"`
}

// SignalAverages is the mean contribution of each value signal across the
// scored corpus — the live composition of value.
type SignalAverages struct {
	Engagement     float64 `json:"engagement"`
	Completion     float64 `json:"completion"`
	Quality        float64 `json:"quality"`
	Velocity       float64 `json:"velocity"`
	SuitabilityAdj float64 `json:"suitability_adj"`
	CostPenalty    float64 `json:"cost_penalty"`
}

// Observatory is the full visualization snapshot for the Intelligence page.
type Observatory struct {
	Diagnostics // embedded: exploration counts, freshness, topic demand

	// Value spectrum — the signature visualization.
	ValueHistogram []ValueBin `json:"value_histogram"`
	ValueMedian    float64    `json:"value_median"`
	ValueP25       float64    `json:"value_p25"` // the intake gate reference
	ValueMean      float64    `json:"value_mean"`

	// Model composition — mean signal contributions across the corpus.
	SignalAverages SignalAverages `json:"signal_averages"`

	// Demand economics per duration bucket.
	BucketDemand []DemandSnapshot `json:"bucket_demand"`

	// Tuning echoed so the UI can draw the live confidence/decay curves and the
	// weight-vs-actual composition without a second call.
	Tuning ObservatoryTuning `json:"tuning"`
}

// ObservatoryTuning is the tuning subset the visualizations need (weights +
// the exploration/decay knobs that drive the live curves).
type ObservatoryTuning struct {
	EngagementWeight        float64 `json:"engagement_weight"`
	CompletionWeight        float64 `json:"completion_weight"`
	QualityWeight           float64 `json:"quality_weight"`
	VelocityWeight          float64 `json:"velocity_weight"`
	ExploreImpressionTarget int     `json:"explore_impression_target"`
	DemotionHalfLifeDays    int     `json:"demotion_half_life_days"`
	DemotionDefaultFactor   float64 `json:"demotion_default_factor"`
}

// ObservatorySnapshot assembles the full visualization read model. `buckets` is
// the duration-bucket list (passed from the controller to avoid a
// controller→intelligence import).
func (e Engine) ObservatorySnapshot(tenantID string, buckets []string) Observatory {
	o := Observatory{
		Diagnostics:    e.DiagnosticsSnapshot(tenantID),
		ValueHistogram: e.valueHistogram(tenantID),
		SignalAverages: e.signalAverages(tenantID),
	}
	o.ValueMedian, o.ValueP25, o.ValueMean = e.valuePercentiles(tenantID)

	demand, _ := e.DemandSnapshots(tenantID, buckets)
	o.BucketDemand = make([]DemandSnapshot, 0, len(buckets))
	for _, b := range buckets {
		if snap, ok := demand[b]; ok {
			o.BucketDemand = append(o.BucketDemand, snap)
		}
	}

	t := e.Tuning(tenantID)
	o.Tuning = ObservatoryTuning{
		EngagementWeight:        t.EngagementWeight,
		CompletionWeight:        t.CompletionWeight,
		QualityWeight:           t.QualityWeight,
		VelocityWeight:          t.VelocityWeight,
		ExploreImpressionTarget: t.ExploreImpressionTarget,
		DemotionHalfLifeDays:    int(t.DemotionHalfLife.Hours() / 24),
		DemotionDefaultFactor:   t.DemotionDefaultFactor,
	}
	return o
}

// valueHistogram bins persisted values into ValueBinCount columns, split by
// exploration state. One grouped query; folded into fixed bins in Go so the
// axis is always the full [0,1] regardless of which bins are populated.
func (e Engine) valueHistogram(tenantID string) []ValueBin {
	bins := make([]ValueBin, ValueBinCount)
	width := 1.0 / float64(ValueBinCount)
	for i := range bins {
		bins[i].Min = float64(i) * width
		bins[i].Max = float64(i+1) * width
	}
	if e.DB == nil {
		return bins
	}

	type row struct {
		Bin   int    `gorm:"column:bin"`
		State string `gorm:"column:exploration_state"`
		Count int64  `gorm:"column:count"`
	}
	var rows []row
	// LEAST guards value == 1.0 landing in bin 40 (out of range).
	e.DB.Model(&models.MediaIntelligenceScore{}).
		Select("LEAST(FLOOR(value * ?), ?) AS bin, exploration_state, COUNT(*) AS count", ValueBinCount, ValueBinCount-1).
		Where("tenant_id = ?", tenantID).
		Group("bin, exploration_state").
		Scan(&rows)

	for _, r := range rows {
		if r.Bin < 0 || r.Bin >= ValueBinCount {
			continue
		}
		b := &bins[r.Bin]
		b.Total += r.Count
		switch r.State {
		case ExplorationExploring, ExplorationRetrial:
			b.Exploring += r.Count
		case ExplorationEstablished:
			b.Established += r.Count
		}
	}
	return bins
}

// signalAverages averages each breakdown signal across the scored corpus via
// jsonb extraction.
func (e Engine) signalAverages(tenantID string) SignalAverages {
	var out SignalAverages
	if e.DB == nil {
		return out
	}
	e.DB.Model(&models.MediaIntelligenceScore{}).
		Select(`
			COALESCE(AVG((breakdown->>'engagement')::float), 0) AS engagement,
			COALESCE(AVG((breakdown->>'completion')::float), 0) AS completion,
			COALESCE(AVG((breakdown->>'quality')::float), 0) AS quality,
			COALESCE(AVG((breakdown->>'velocity')::float), 0) AS velocity,
			COALESCE(AVG((breakdown->>'suitability_adj')::float), 0) AS suitability_adj,
			COALESCE(AVG((breakdown->>'cost_penalty')::float), 0) AS cost_penalty`).
		Where("tenant_id = ? AND breakdown IS NOT NULL", tenantID).
		Scan(&out)
	return out
}

// valuePercentiles returns the median, P25, and mean of persisted values —
// P25 is the reference the intake gate uses, so it anchors the spectrum.
func (e Engine) valuePercentiles(tenantID string) (median, p25, mean float64) {
	if e.DB == nil {
		return 0, 0, 0
	}
	var values []float64
	e.DB.Model(&models.MediaIntelligenceScore{}).
		Where("tenant_id = ?", tenantID).
		Order("value ASC").
		Pluck("value", &values)
	if len(values) == 0 {
		return 0, 0, 0
	}
	if !sort.Float64sAreSorted(values) {
		sort.Float64s(values)
	}
	var sum float64
	for _, v := range values {
		sum += v
	}
	mean = sum / float64(len(values))
	median = values[len(values)/2]
	p25 = values[len(values)/4]
	return median, p25, mean
}
