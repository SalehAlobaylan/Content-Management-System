package operator

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// TemporalSample is an evidence-bound numeric observation. The fabric never
// extrapolates from it; conclusions are limited to the stated window and
// samples supplied by a registered adapter.
type TemporalSample struct {
	EvidenceID string
	ObservedAt time.Time
	Value      float64
}

type TemporalSummary struct {
	WindowStart time.Time
	WindowEnd   time.Time
	FirstValue  float64
	LatestValue float64
	Delta       float64
	Direction   string
	EvidenceIDs []string
}

func SummarizeTemporal(samples []TemporalSample, maxPoints int) (TemporalSummary, error) {
	if len(samples) == 0 || maxPoints < 1 || maxPoints > normalQuestionBudget.MaxRowsPerDomain || len(samples) > maxPoints {
		return TemporalSummary{}, fmt.Errorf("%w: invalid temporal sample set", ErrInvalidContract)
	}
	ordered := append([]TemporalSample(nil), samples...)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].ObservedAt.Before(ordered[j].ObservedAt) })
	evidenceIDs := make([]string, 0, len(ordered))
	seenEvidence := make(map[string]struct{}, len(ordered))
	for _, sample := range ordered {
		if strings.TrimSpace(sample.EvidenceID) == "" || sample.ObservedAt.IsZero() {
			return TemporalSummary{}, fmt.Errorf("%w: temporal samples require evidence and an observation time", ErrInvalidContract)
		}
		if _, seen := seenEvidence[sample.EvidenceID]; !seen {
			seenEvidence[sample.EvidenceID] = struct{}{}
			evidenceIDs = append(evidenceIDs, sample.EvidenceID)
		}
	}
	first, latest := ordered[0], ordered[len(ordered)-1]
	direction := "unchanged"
	if latest.Value > first.Value {
		direction = "increased"
	} else if latest.Value < first.Value {
		direction = "decreased"
	}
	return TemporalSummary{WindowStart: first.ObservedAt, WindowEnd: latest.ObservedAt, FirstValue: first.Value, LatestValue: latest.Value, Delta: latest.Value - first.Value, Direction: direction, EvidenceIDs: evidenceIDs}, nil
}
