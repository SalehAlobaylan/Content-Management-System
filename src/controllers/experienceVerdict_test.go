package controllers

import (
	"testing"

	"content-management-system/src/models"
)

func spec(metric, dir string, target float64, critical bool, latP75 int) sliSpec {
	return sliSpec{metric: metric, direction: dir, target: target, critical: critical, latencyP75: latP75, label: metric}
}

func TestEvaluateSLI_InsufficientBelowFloor(t *testing.T) {
	st := evaluateSLI(spec(mFeedRenderSuccess, "min", 0.995, true, 0), 40, 40, 0, 0, [7]int64{}, 50)
	if st.Status != "insufficient" {
		t.Fatalf("expected insufficient below sample floor, got %s", st.Status)
	}
}

func TestEvaluateSLI_HealthyAboveTarget(t *testing.T) {
	st := evaluateSLI(spec(mFeedRenderSuccess, "min", 0.995, true, 0), 999, 1000, 999, 1000, [7]int64{}, 50)
	if st.Status != "ok" {
		t.Fatalf("expected ok, got %s (value=%v)", st.Status, st.Value)
	}
}

func TestEvaluateSLI_BreachBelowTarget(t *testing.T) {
	st := evaluateSLI(spec(mPlaybackStartSuccess, "min", 0.985, true, 0), 900, 1000, 900, 0, [7]int64{}, 50)
	if st.Status != "breach" {
		t.Fatalf("expected breach, got %s", st.Status)
	}
}

func TestEvaluateSLI_MaxDirectionBreach(t *testing.T) {
	// fatal-rate style: value must stay <= target.
	st := evaluateSLI(spec(mPlaybackFatalRate, "max", 0.01, false, 0), 50, 1000, 0, 0, [7]int64{}, 50)
	if st.Status != "breach" {
		t.Fatalf("expected breach on max-direction overrun, got %s (value=%v)", st.Status, st.Value)
	}
}

func TestEvaluateSLI_LatencyBreach(t *testing.T) {
	// Value passes, but p75 exceeds budget → watching-grade latency breach.
	buckets := [7]int64{0, 0, 0, 0, 0, 100, 0} // all samples in the 4000-8000 bucket
	st := evaluateSLI(spec(mFeedRenderSuccess, "min", 0.995, true, 2500), 1000, 1000, 100, 500000, buckets, 50)
	if st.Status != "latency_breach" {
		t.Fatalf("expected latency_breach, got %s (p75=%d)", st.Status, st.P75)
	}
}

func TestSurfaceVerdict_CriticalBeatsDegraded(t *testing.T) {
	statuses := []sliStatus{
		{Status: "breach", Critical: false},
		{Status: "breach", Critical: true},
		{Status: "ok"},
	}
	if v := surfaceVerdict(statuses, true); v != models.RuxVerdictCritical {
		t.Fatalf("expected critical, got %s", v)
	}
}

func TestSurfaceVerdict_TelemetryDegradedShortCircuits(t *testing.T) {
	statuses := []sliStatus{{Status: "ok"}}
	if v := surfaceVerdict(statuses, false); v != models.RuxVerdictTelemetryDegrade {
		t.Fatalf("expected telemetry_degraded, got %s", v)
	}
}

func TestSurfaceVerdict_AllInsufficient(t *testing.T) {
	statuses := []sliStatus{{Status: "insufficient"}, {Status: "insufficient"}}
	if v := surfaceVerdict(statuses, true); v != models.RuxVerdictInsufficient {
		t.Fatalf("expected insufficient_data, got %s", v)
	}
}

func TestSurfaceVerdict_LatencyOnlyIsWatching(t *testing.T) {
	statuses := []sliStatus{{Status: "latency_breach"}, {Status: "ok"}}
	if v := surfaceVerdict(statuses, true); v != models.RuxVerdictWatching {
		t.Fatalf("expected watching, got %s", v)
	}
}

// Terminal-outcome counting: autoplay_blocked must NOT enter the start-success
// denominator or the fatal-rate; it is its own rate (plan §8).
func TestContributions_AutoplayExcludedFromStartAndFatal(t *testing.T) {
	fc := "autoplay_blocked"
	ev := &models.ExperienceEvent{EventType: "playback_failed", FailureClass: &fc}
	cons := contributionsFor(ev)
	for _, con := range cons {
		if con.metric == mPlaybackStartSuccess {
			t.Error("autoplay_blocked must not contribute to playback_start_success")
		}
		if con.metric == mPlaybackFatalRate && con.num > 0 {
			t.Error("autoplay_blocked must not count as a fatal failure")
		}
	}
	// It should contribute to the autoplay rate numerator.
	found := false
	for _, con := range cons {
		if con.metric == mAutoplayBlockedRate && con.num == 1 {
			found = true
		}
	}
	if !found {
		t.Error("autoplay_blocked should increment autoplay_blocked_rate numerator")
	}
}

func TestContributions_FatalPlaybackFailure(t *testing.T) {
	fc := "media_error"
	ev := &models.ExperienceEvent{EventType: "playback_failed", FailureClass: &fc}
	cons := contributionsFor(ev)
	var startDenom, fatalNum int64
	for _, con := range cons {
		if con.metric == mPlaybackStartSuccess {
			startDenom += con.denom
		}
		if con.metric == mPlaybackFatalRate {
			fatalNum += con.num
		}
	}
	if startDenom != 1 || fatalNum != 1 {
		t.Fatalf("fatal failure should hit start denom(=1) and fatal num(=1); got %d/%d", startDenom, fatalNum)
	}
}

func TestContributions_BackgroundedIsExcluded(t *testing.T) {
	ev := &models.ExperienceEvent{EventType: "playback_backgrounded"}
	if cons := contributionsFor(ev); len(cons) != 0 {
		t.Fatalf("backgrounded must contribute nothing, got %d", len(cons))
	}
}

func TestP75FromBuckets(t *testing.T) {
	// 90 samples in <250, 10 in <500 → p75 sits in the first bucket.
	b := [7]int64{90, 10, 0, 0, 0, 0, 0}
	if p := p75FromBuckets(b); p != 250 {
		t.Fatalf("expected p75=250, got %d", p)
	}
}
