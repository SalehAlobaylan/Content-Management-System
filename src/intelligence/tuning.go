package intelligence

import (
	"sync/atomic"
	"time"

	"content-management-system/src/models"
)

// Tuning is the resolved, sanitized set of OPERATIONAL knobs for the media-value
// engine — the runtime-overridable subset of the code constants (grilling Q12
// superseded by the control-room decision). Model-shape constants stay in code.
//
// It is loaded per-tenant from media_intelligence_configs with a short cache and
// falls back to DefaultTuning when there is no row (or no DB). Every value the
// scoring/feed/demotion paths read comes from here, so a hand-edited DB row can
// never break the blend — Tuning always passes through sanitizeTuning.
type Tuning struct {
	EngagementWeight float64
	CompletionWeight float64
	QualityWeight    float64
	VelocityWeight   float64

	ExplorationSliceEvery   int
	ExploreImpressionTarget int
	LegacyExposureViewFloor int

	DemotionDefaultFactor float64
	DemotionHalfLife      time.Duration
}

// DefaultTuning returns the code defaults — the fallback when a tenant has no
// override row. Values MUST match models.DefaultMediaIntelligenceConfig and the
// constants in intelligence.go.
func DefaultTuning() Tuning {
	return Tuning{
		EngagementWeight:        weightEngagement,
		CompletionWeight:        weightCompletion,
		QualityWeight:           weightQuality,
		VelocityWeight:          weightVelocity,
		ExplorationSliceEvery:   explorationSliceEvery,
		ExploreImpressionTarget: exploreImpressionTarget,
		LegacyExposureViewFloor: legacyExposureViewFloor,
		DemotionDefaultFactor:   demotionDefaultFactor,
		DemotionHalfLife:        DemotionHalfLife(),
	}
}

// Sanitize/clamp bounds (control-room validation, applied on both read and
// write so the engine is safe regardless of how a row got into the DB).
const (
	tuningSliceEveryMin     = 4
	tuningSliceEveryMax     = 50
	tuningImpressionTgtMin  = 10
	tuningImpressionTgtMax  = 1000
	tuningLegacyFloorMin    = 0
	tuningLegacyFloorMax    = 10000
	tuningDemotionFactorMin = 0.05
	tuningDemotionFactorMax = 0.95
	tuningHalfLifeDaysMin   = 1
	tuningHalfLifeDaysMax   = 90
)

// tuningFromModel maps a persisted config row into a Tuning (unsanitized).
func tuningFromModel(m models.MediaIntelligenceConfig) Tuning {
	return Tuning{
		EngagementWeight:        m.EngagementWeight,
		CompletionWeight:        m.CompletionWeight,
		QualityWeight:           m.QualityWeight,
		VelocityWeight:          m.VelocityWeight,
		ExplorationSliceEvery:   m.ExplorationSliceEvery,
		ExploreImpressionTarget: m.ExploreImpressionTarget,
		LegacyExposureViewFloor: m.LegacyExposureViewFloor,
		DemotionDefaultFactor:   m.DemotionDefaultFactor,
		DemotionHalfLife:        time.Duration(m.DemotionHalfLifeDays) * 24 * time.Hour,
	}
}

// sanitizeTuning normalizes the weights to sum 1.0 and clamps every knob to its
// safe range. A zero/negative weight sum falls back to the default weights.
func sanitizeTuning(t Tuning) Tuning {
	w := []float64{t.EngagementWeight, t.CompletionWeight, t.QualityWeight, t.VelocityWeight}
	sum := 0.0
	for i, v := range w {
		if v < 0 {
			v = 0
			w[i] = 0
		}
		sum += v
	}
	if sum <= 0 {
		d := DefaultTuning()
		t.EngagementWeight, t.CompletionWeight, t.QualityWeight, t.VelocityWeight =
			d.EngagementWeight, d.CompletionWeight, d.QualityWeight, d.VelocityWeight
	} else {
		t.EngagementWeight = w[0] / sum
		t.CompletionWeight = w[1] / sum
		t.QualityWeight = w[2] / sum
		t.VelocityWeight = w[3] / sum
	}

	t.ExplorationSliceEvery = clampInt(t.ExplorationSliceEvery, tuningSliceEveryMin, tuningSliceEveryMax)
	t.ExploreImpressionTarget = clampInt(t.ExploreImpressionTarget, tuningImpressionTgtMin, tuningImpressionTgtMax)
	t.LegacyExposureViewFloor = clampInt(t.LegacyExposureViewFloor, tuningLegacyFloorMin, tuningLegacyFloorMax)

	if t.DemotionDefaultFactor < tuningDemotionFactorMin {
		t.DemotionDefaultFactor = tuningDemotionFactorMin
	}
	if t.DemotionDefaultFactor > tuningDemotionFactorMax {
		t.DemotionDefaultFactor = tuningDemotionFactorMax
	}

	halfDays := int(t.DemotionHalfLife.Hours() / 24)
	halfDays = clampInt(halfDays, tuningHalfLifeDaysMin, tuningHalfLifeDaysMax)
	t.DemotionHalfLife = time.Duration(halfDays) * 24 * time.Hour

	return t
}

// SanitizeConfig clamps/normalizes a config model for the write path (the
// control-room PUT), returning the values that will actually be persisted.
func SanitizeConfig(m models.MediaIntelligenceConfig) models.MediaIntelligenceConfig {
	t := sanitizeTuning(tuningFromModel(m))
	m.EngagementWeight = t.EngagementWeight
	m.CompletionWeight = t.CompletionWeight
	m.QualityWeight = t.QualityWeight
	m.VelocityWeight = t.VelocityWeight
	m.ExplorationSliceEvery = t.ExplorationSliceEvery
	m.ExploreImpressionTarget = t.ExploreImpressionTarget
	m.LegacyExposureViewFloor = t.LegacyExposureViewFloor
	m.DemotionDefaultFactor = t.DemotionDefaultFactor
	m.DemotionHalfLifeDays = int(t.DemotionHalfLife.Hours() / 24)
	return m
}

func clampInt(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// ── per-tenant tuning cache (mirrors loadTenantConfig) ──────────────────────

type cachedTuning struct {
	tenantID  string
	tuning    Tuning
	fetchedAt time.Time
}

const tuningCacheTTL = 15 * time.Second

var tuningMem atomic.Pointer[cachedTuning]

// InvalidateTuningCache drops the cached tuning so the next read reflects a
// just-saved config (called from the control-room PUT handler).
func InvalidateTuningCache() {
	tuningMem.Store(nil)
}

// Tuning returns the sanitized per-tenant tuning, cached for tuningCacheTTL.
// With no DB or no override row it returns DefaultTuning — so pure unit tests
// (Engine{}) and cold tenants both get sane values without panicking.
func (e Engine) Tuning(tenantID string) Tuning {
	if c := tuningMem.Load(); c != nil && c.tenantID == tenantID && time.Since(c.fetchedAt) < tuningCacheTTL {
		return c.tuning
	}
	if e.DB == nil {
		return DefaultTuning()
	}
	var cfg models.MediaIntelligenceConfig
	if err := e.DB.Where("tenant_id = ?", tenantID).First(&cfg).Error; err != nil {
		return DefaultTuning()
	}
	t := sanitizeTuning(tuningFromModel(cfg))
	tuningMem.Store(&cachedTuning{tenantID: tenantID, tuning: t, fetchedAt: time.Now()})
	return t
}
