package operator

import (
	"math"
	"testing"
	"time"
)

func TestSummarizeTemporalOrdersAndBoundsEvidence(t *testing.T) {
	now := time.Date(2026, time.July, 31, 0, 0, 0, 0, time.UTC)
	summary, err := SummarizeTemporal([]TemporalSample{{EvidenceID: "later", ObservedAt: now.Add(time.Minute), Value: 0.9}, {EvidenceID: "first", ObservedAt: now, Value: 0.2}}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Direction != "increased" || math.Abs(summary.Delta-0.7) > 0.000001 || len(summary.EvidenceIDs) != 2 || !summary.WindowStart.Equal(now) {
		t.Fatalf("unexpected temporal summary: %#v", summary)
	}
}

func TestSummarizeTemporalRejectsUnboundedOrUnevidencedSamples(t *testing.T) {
	now := time.Now().UTC()
	if _, err := SummarizeTemporal([]TemporalSample{{EvidenceID: "", ObservedAt: now, Value: 1}}, 1); err == nil {
		t.Fatal("temporal observations require evidence")
	}
	if _, err := SummarizeTemporal([]TemporalSample{{EvidenceID: "ev", ObservedAt: now, Value: 1}}, 201); err == nil {
		t.Fatal("temporal analysis must respect the hard row budget")
	}
}
