package controllers

import (
	"content-management-system/src/models"
	"testing"
	"time"
)

func TestSourceQualityPrior(t *testing.T) {
	// No telemetry and no produced items -> no history.
	if _, ok := sourceQualityPrior(sourceRecommendationStats{}, 0, 0); ok {
		t.Error("empty source should report no history")
	}
	// High yield + high produced value -> high prior.
	strong := sourceRecommendationStats{RunCount: 5, Fetched: 100, Accepted: 80}
	pHigh, ok := sourceQualityPrior(strong, 0.9, 40)
	if !ok || pHigh < 0.6 {
		t.Errorf("strong source prior = %.3f (ok=%v), want high", pHigh, ok)
	}
	// Low yield + low produced value -> low prior.
	weak := sourceRecommendationStats{RunCount: 5, Fetched: 100, Accepted: 2}
	pLow, _ := sourceQualityPrior(weak, 0.1, 40)
	if pLow >= pHigh {
		t.Errorf("weak prior %.3f should be below strong prior %.3f", pLow, pHigh)
	}
}

func TestBucketDemandMatch(t *testing.T) {
	state := map[string]string{"5": "thin", "10": "saturated", "20": "ok"}
	thinFiller := map[string]float64{"5": 1.0} // fills a thin bucket
	satFiller := map[string]float64{"10": 1.0} // fills a saturated bucket
	mThin, matched, fills := bucketDemandMatch(thinFiller, state)
	mSat, _, _ := bucketDemandMatch(satFiller, state)
	if mThin <= mSat {
		t.Errorf("thin-filling match %.2f should exceed saturated-filling match %.2f", mThin, mSat)
	}
	if !fills || len(matched) != 1 || matched[0] != "5" {
		t.Errorf("expected fillsThin with matched=[5], got fills=%v matched=%v", fills, matched)
	}
	if mSat != 0 {
		t.Errorf("saturated-only source should have zero demand match, got %.2f", mSat)
	}
}

func TestSourceFailureRate(t *testing.T) {
	if r := sourceFailureRate(sourceRecommendationStats{}); r != 0 {
		t.Errorf("empty failure rate = %.2f, want 0", r)
	}
	r := sourceFailureRate(sourceRecommendationStats{Fetched: 6, Failed: 4})
	if r < 0.39 || r > 0.41 {
		t.Errorf("failure rate = %.3f, want ~0.4", r)
	}
}

func TestStorageCostHeadroom(t *testing.T) {
	// Below target -> positive headroom.
	below := storageCostHeadroom(models.StoragePolicy{TargetUtilizationPct: 80},
		storageProofMetrics{QuotaBytes: 1000, UtilizationPct: 40})
	if below <= 0 {
		t.Errorf("below-target headroom = %.2f, want > 0", below)
	}
	// Over target -> zero headroom.
	over := storageCostHeadroom(models.StoragePolicy{TargetUtilizationPct: 80},
		storageProofMetrics{QuotaBytes: 1000, UtilizationPct: 95})
	if over != 0 {
		t.Errorf("over-target headroom = %.2f, want 0", over)
	}
	// No quota configured -> full headroom.
	if h := storageCostHeadroom(models.StoragePolicy{}, storageProofMetrics{QuotaBytes: 0}); h != 1 {
		t.Errorf("no-quota headroom = %.2f, want 1", h)
	}
}

func TestAllocateIntakeBudget(t *testing.T) {
	// 3 sources, cycle budget 5, per-source cap 2 -> [2,2,1], 4th+ get 0.
	got := allocateIntakeBudget(4, 5, 2)
	want := []int{2, 2, 1, 0}
	total := 0
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("alloc[%d] = %d, want %d", i, got[i], want[i])
		}
		if got[i] > 2 {
			t.Errorf("alloc[%d] = %d exceeds per-source cap", i, got[i])
		}
		total += got[i]
	}
	if total > 5 {
		t.Errorf("total allocated %d exceeds cycle budget 5", total)
	}
}

func TestFreshnessComponentSoft(t *testing.T) {
	// Freshness is a soft demand multiplier in [1-w, 1] — never zeroes the score.
	old := time.Now().Add(-1000 * time.Hour)
	fresh := time.Now()
	w := 0.2
	overdueSrc := models.ContentSource{LastFetchedAt: &old, FetchIntervalMinutes: 60}
	freshSrc := models.ContentSource{LastFetchedAt: &fresh, FetchIntervalMinutes: 60}
	fo := freshnessComponent(overdueSrc, w)
	ff := freshnessComponent(freshSrc, w)
	if fo <= ff {
		t.Errorf("overdue freshness %.2f should exceed fresh %.2f", fo, ff)
	}
	if ff < (1-w)-1e-9 {
		t.Errorf("fresh component %.3f dropped below floor %.3f — freshness must not gate", ff, 1-w)
	}
}

func TestSourceCadenceEligibleAt(t *testing.T) {
	now := time.Now()
	recent := now.Add(-30 * time.Minute)
	old := now.Add(-2 * time.Hour)
	policy := models.MediaCirculationPolicy{SourceMinIntervalMinutes: 60}

	ok, reason := sourceCadenceEligibleAt(models.ContentSource{LastFetchedAt: &recent}, policy, now)
	if ok {
		t.Error("source fetched inside the min interval should not be eligible")
	}
	if reason == "" {
		t.Error("cadence skip should include a reason")
	}

	ok, _ = sourceCadenceEligibleAt(models.ContentSource{LastFetchedAt: &old}, policy, now)
	if !ok {
		t.Error("source outside the min interval should be eligible")
	}

	ok, _ = sourceCadenceEligibleAt(models.ContentSource{}, policy, now)
	if !ok {
		t.Error("never-fetched source should be eligible")
	}
}

func TestFreshnessComponentClampsSourceIntervalToPolicyBounds(t *testing.T) {
	now := time.Now()
	lastFetched := now.Add(-2 * time.Hour)
	source := models.ContentSource{LastFetchedAt: &lastFetched, FetchIntervalMinutes: 10080}

	got := freshnessComponentAt(source, 0.2, 60, 120, now)
	if got != 1 {
		t.Errorf("freshness with max interval clamp = %.3f, want 1", got)
	}

	shortIntervalSource := models.ContentSource{LastFetchedAt: &lastFetched, FetchIntervalMinutes: 5}
	got = freshnessComponentAt(shortIntervalSource, 0.2, 240, 10080, now)
	if got >= 1 {
		t.Errorf("freshness with min interval clamp = %.3f, want below 1", got)
	}
}
