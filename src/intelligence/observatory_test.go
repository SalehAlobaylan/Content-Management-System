package intelligence

import "testing"

// The histogram helper must always return a full [0,1] axis of fixed bins even
// with no DB, so the spectrum axis is stable.
func TestValueHistogramFullAxisWithoutDB(t *testing.T) {
	bins := Engine{}.valueHistogram("t")
	if len(bins) != ValueBinCount {
		t.Fatalf("expected %d bins, got %d", ValueBinCount, len(bins))
	}
	if bins[0].Min != 0 {
		t.Fatalf("first bin must start at 0, got %f", bins[0].Min)
	}
	last := bins[ValueBinCount-1]
	if last.Max < 0.999 || last.Max > 1.001 {
		t.Fatalf("last bin must end at 1.0, got %f", last.Max)
	}
	// Contiguous, non-overlapping.
	for i := 1; i < len(bins); i++ {
		if bins[i].Min != bins[i-1].Max {
			t.Fatalf("bins not contiguous at %d: %f != %f", i, bins[i].Min, bins[i-1].Max)
		}
	}
}

func TestSignalAveragesZeroWithoutDB(t *testing.T) {
	s := Engine{}.signalAverages("t")
	if s != (SignalAverages{}) {
		t.Fatalf("no DB must yield zero-value averages, got %+v", s)
	}
}

func TestValuePercentilesEmptyWithoutDB(t *testing.T) {
	m, p, mean := Engine{}.valuePercentiles("t")
	if m != 0 || p != 0 || mean != 0 {
		t.Fatalf("no DB must yield zero percentiles, got median=%f p25=%f mean=%f", m, p, mean)
	}
}

// ObservatorySnapshot must compose without panicking on a bare Engine and echo
// the default tuning for the curves.
func TestObservatorySnapshotComposesWithoutDB(t *testing.T) {
	o := Engine{}.ObservatorySnapshot("t", []string{"5m", "10m", "40m"})
	if len(o.ValueHistogram) != ValueBinCount {
		t.Fatalf("histogram must be present, got %d bins", len(o.ValueHistogram))
	}
	d := DefaultTuning()
	if o.Tuning.EngagementWeight != d.EngagementWeight ||
		o.Tuning.DemotionHalfLifeDays != int(d.DemotionHalfLife.Hours()/24) ||
		o.Tuning.ExploreImpressionTarget != d.ExploreImpressionTarget {
		t.Fatalf("observatory tuning must echo DefaultTuning, got %+v", o.Tuning)
	}
}
