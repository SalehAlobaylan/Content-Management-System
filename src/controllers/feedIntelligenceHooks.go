package controllers

import (
	"sort"

	"content-management-system/src/intelligence"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// The two Ranking/Intelligence feed hooks (stage 4, grilling Q7 + Q10) —
// the only places the engine touches For You assembly:
//
//  1. Soft eviction: an applied rank_down decays the item's ordering score by
//     the engine-owned demotion multiplier (half-life decay; revert clears it).
//  2. Exploration slice: ~1 slot in explorationSliceEvery goes to a still-
//     exploring item so low-exposure content can accumulate the impressions
//     its value verdict needs (breaks the W1 circularity).
//
// Both hooks run on the RANKED path only — the chronological path is the
// legacy fallback when ranking is disabled, and it stays untouched.
// The feed's hard constraints (270–2400s, READY-only, eligibility) are all
// enforced upstream in forYouEligibleMediaQuery; these hooks only reorder and
// interleave already-eligible items, which is what keeps the characterization
// tests green.

// applyIntelligenceFeedHooks applies demotion decay and the exploration slice
// to a scored, sorted candidate list. Returns a list with the same items (each
// exactly once). The exploration cadence + demotion half-life come from the
// tenant's tuning (control room).
func applyIntelligenceFeedHooks(db *gorm.DB, tenantID string, scored []ScoredItem) []ScoredItem {
	if len(scored) < 2 {
		return scored
	}
	engine := intelligence.Engine{DB: db}
	sliceEvery := engine.Tuning(tenantID).ExplorationSliceEvery
	ids := make([]uuid.UUID, len(scored))
	for i, s := range scored {
		ids[i] = s.Item.PublicID
	}
	signals := engine.FeedSignals(tenantID, ids)

	// Hook 1 — demotion: multiply ordering scores by the decayed factor, then
	// restore ordering + diversity (the multipliers can reorder the list).
	demoted := false
	for i := range scored {
		if sig, ok := signals[scored[i].Item.PublicID]; ok && sig.Demotion < 1 {
			scored[i].FinalScore *= sig.Demotion
			demoted = true
		}
	}
	if demoted {
		sort.SliceStable(scored, func(i, j int) bool { return scored[i].FinalScore > scored[j].FinalScore })
		scored = applyDiversityPenalty(scored)
	}

	// Hook 2 — exploration slice: walk the page order; every Nth slot pops the
	// highest-ranked still-unplaced exploring item instead of the next item in
	// line. Items surface exactly once; when no exploring item remains the
	// slot falls through to normal order.
	return injectExplorationSlice(scored, signals, sliceEvery)
}

func injectExplorationSlice(scored []ScoredItem, signals map[uuid.UUID]intelligence.FeedSignal, sliceEvery int) []ScoredItem {
	if sliceEvery < 1 {
		sliceEvery = 10
	}
	anyExploring := false
	for _, s := range scored {
		if sig, ok := signals[s.Item.PublicID]; ok && sig.Exploring {
			anyExploring = true
			break
		}
	}
	if !anyExploring {
		return scored
	}

	placed := make([]bool, len(scored))
	out := make([]ScoredItem, 0, len(scored))
	nextUnplaced := func(exploringOnly bool) int {
		for i := range scored {
			if placed[i] {
				continue
			}
			if !exploringOnly {
				return i
			}
			if sig, ok := signals[scored[i].Item.PublicID]; ok && sig.Exploring {
				return i
			}
		}
		return -1
	}
	for len(out) < len(scored) {
		idx := -1
		if (len(out)+1)%sliceEvery == 0 {
			idx = nextUnplaced(true)
		}
		if idx == -1 {
			idx = nextUnplaced(false)
		}
		placed[idx] = true
		out = append(out, scored[idx])
	}
	return out
}
