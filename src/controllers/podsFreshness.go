package controllers

import (
	"sort"
	"strings"
	"time"

	"content-management-system/src/models"
)

// Pods ranking remains personalized and quality-aware, but lifetime engagement
// must never make newly eligible inventory unreachable. These code defaults are
// product invariants rather than operator tuning: a fresh session reserves up
// to 20% of its first 20 positions for the newest eligible items, with at most two
// reservations from one source. Existing frozen sessions remain immutable.
const (
	podsFreshnessReservationWindow       = 7 * 24 * time.Hour
	podsFreshnessReservationMaxPerSource = 2
)

var podsFreshnessReservationPositions = [...]int{1, 6, 11, 16}

func podsPublicationTime(item models.ContentItem) time.Time {
	if item.PublishedAt != nil {
		return item.PublishedAt.UTC()
	}
	return item.CreatedAt.UTC()
}

func podsFreshnessSourceKey(item models.ContentItem) string {
	if item.ContentSourceID != nil {
		return "source:" + item.ContentSourceID.String()
	}
	if item.SourceName != nil && strings.TrimSpace(*item.SourceName) != "" {
		return "name:" + strings.ToLower(strings.TrimSpace(*item.SourceName))
	}
	return "unknown"
}

func isPodsFreshnessReservationCandidate(item ScoredItem, now time.Time) bool {
	// Pinned content already has an absolute ordering contract. Suppressed
	// content must not be promoted merely because it is recent.
	if item.ScoreBreakdown.Flags == "pinned" || item.ScoreBreakdown.Flags == "suppressed" {
		return false
	}
	published := podsPublicationTime(item.Item)
	if published.IsZero() || published.After(now.Add(15*time.Minute)) {
		return false
	}
	age := now.Sub(published)
	return age >= 0 && age <= podsFreshnessReservationWindow
}

// reserveFreshPodsFirstPage is a final soft-ordering contract. It runs after
// hard eligibility, user mutes, personalization, intelligence
// demotion/exploration, and chapter spacing. Cursor requests rebuild the same
// transformation before locating the cursor, preventing promoted items from
// reappearing at their former rank. Selected fresh items are deterministic;
// all non-selected items retain their exact relative order.
func reserveFreshPodsFirstPage(items []ScoredItem, now time.Time) []ScoredItem {
	return enforcePodsFirstPageConstraints(reserveFreshPodsFirstPageUnconstrained(items, now))
}

// reserveFreshPodsFirstPageUnconstrained applies only the freshness slots. It
// is kept separate so diagnostics can distinguish freshness movement from the
// later source-spacing/capacity movement.
func reserveFreshPodsFirstPageUnconstrained(items []ScoredItem, now time.Time) []ScoredItem {
	if len(items) < 2 {
		return items
	}

	candidates := make([]ScoredItem, 0, len(items))
	for _, item := range items {
		if isPodsFreshnessReservationCandidate(item, now) {
			candidates = append(candidates, item)
		}
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		a, b := podsPublicationTime(candidates[i].Item), podsPublicationTime(candidates[j].Item)
		if a.Equal(b) {
			return candidates[i].Item.PublicID.String() < candidates[j].Item.PublicID.String()
		}
		return a.After(b)
	})

	pinnedPrefix := 0
	for pinnedPrefix < len(items) && items[pinnedPrefix].ScoreBreakdown.Flags == "pinned" {
		pinnedPrefix++
	}
	reservationPositions := make([]int, 0, len(podsFreshnessReservationPositions))
	for _, position := range podsFreshnessReservationPositions {
		if position >= pinnedPrefix && position < len(items) {
			reservationPositions = append(reservationPositions, position)
		}
	}
	selected := make([]ScoredItem, 0, len(reservationPositions))
	selectedIDs := make(map[string]struct{}, len(reservationPositions))
	sourceCounts := make(map[string]int)
	// First pass maximizes source breadth. The second pass permits one more item
	// per source so a sparse inventory still receives useful freshness coverage.
	for sourceLimit := 1; sourceLimit <= podsFreshnessReservationMaxPerSource; sourceLimit++ {
		for _, item := range candidates {
			if len(selected) == len(reservationPositions) {
				break
			}
			id := item.Item.PublicID.String()
			if _, exists := selectedIDs[id]; exists {
				continue
			}
			source := podsFreshnessSourceKey(item.Item)
			if sourceCounts[source] >= sourceLimit {
				continue
			}
			item.ScoreBreakdown.FreshnessReserved = true
			selected = append(selected, item)
			selectedIDs[id] = struct{}{}
			sourceCounts[source]++
		}
	}
	if len(selected) == 0 {
		return items
	}

	remaining := make([]ScoredItem, 0, len(items)-len(selected))
	for _, item := range items {
		if _, reserved := selectedIDs[item.Item.PublicID.String()]; !reserved {
			remaining = append(remaining, item)
		}
	}

	out := make([]ScoredItem, 0, len(items))
	reservedIndex, normalIndex := 0, 0
	for len(out) < len(items) {
		position := len(out)
		isReservedPosition := reservedIndex < len(selected) &&
			position == reservationPositions[reservedIndex]
		if isReservedPosition || normalIndex >= len(remaining) {
			out = append(out, selected[reservedIndex])
			reservedIndex++
			continue
		}
		out = append(out, remaining[normalIndex])
		normalIndex++
	}
	return out
}

// enforcePodsFirstPageConstraints is the single final assembler used by the
// public feed and every preview/probe path. Reserved and ordinary candidates
// are each consumed in stable score order unless a source constraint requires
// the earliest legal candidate. Reservation positions therefore never drift.
func enforcePodsFirstPageConstraints(items []ScoredItem) []ScoredItem {
	if len(items) < 2 {
		return items
	}
	limit := len(items)
	if limit > 20 {
		limit = 20
	}
	pinnedPrefix := 0
	for pinnedPrefix < len(items) && items[pinnedPrefix].ScoreBreakdown.Flags == "pinned" {
		pinnedPrefix++
	}
	// The cap is feasible only when the full candidate inventory can fill the
	// requested first page under it. Otherwise the page stays full and the
	// diagnostic reports the relaxed constraint.
	distinct := map[string]struct{}{}
	countsBySource := map[string]int{}
	for _, item := range items {
		source := podsFreshnessSourceKey(item.Item)
		distinct[source] = struct{}{}
		countsBySource[source]++
	}
	maxPerSource := limit
	capacity := 0
	for _, count := range countsBySource {
		if count > 4 {
			count = 4
		}
		capacity += count
	}
	if len(distinct) >= 5 && capacity >= limit {
		maxPerSource = 4
	}

	// Preserve the pinned editorial prefix exactly. The bounded DFS below only
	// considers first-page slots and has a node budget, so a pathological sparse
	// inventory cannot turn a feed request into unbounded combinatorial work.
	result := append([]ScoredItem(nil), items[:pinnedPrefix]...)
	used := make([]bool, len(items))
	for index := 0; index < pinnedPrefix; index++ {
		used[index] = true
	}
	sourceCounts := map[string]int{}
	for index := 0; index < pinnedPrefix; index++ {
		sourceCounts[podsFreshnessSourceKey(items[index].Item)]++
	}
	selected := make([]int, 0, limit-pinnedPrefix)
	nodes := 0
	var place func(int, string) bool
	place = func(position int, previousSource string) bool {
		if position >= limit {
			return true
		}
		nodes++
		if nodes > 100000 {
			return false
		}
		wantReserved := items[position].ScoreBreakdown.FreshnessReserved
		for candidateIndex := pinnedPrefix; candidateIndex < len(items); candidateIndex++ {
			if used[candidateIndex] || items[candidateIndex].ScoreBreakdown.FreshnessReserved != wantReserved {
				continue
			}
			source := podsFreshnessSourceKey(items[candidateIndex].Item)
			if source == previousSource || sourceCounts[source] >= maxPerSource {
				continue
			}
			used[candidateIndex], sourceCounts[source] = true, sourceCounts[source]+1
			selected = append(selected, candidateIndex)
			if place(position+1, source) {
				return true
			}
			selected = selected[:len(selected)-1]
			used[candidateIndex], sourceCounts[source] = false, sourceCounts[source]-1
		}
		return false
	}
	if place(pinnedPrefix, "") {
		for _, index := range selected {
			result = append(result, items[index])
		}
		for index := pinnedPrefix; index < len(items); index++ {
			if !used[index] {
				result = append(result, items[index])
			}
		}
		return result
	}

	// Sparse/infeasible fallback: keep every item, retain reservation markers,
	// and relax adjacency/capacity only when no legal full-page arrangement was
	// found. The page still remains deterministic and complete.
	result = append([]ScoredItem(nil), items...)
	return repairPodsFirstPageAdjacency(result, limit, maxPerSource, pinnedPrefix)
}

func repairPodsFirstPageAdjacency(items []ScoredItem, limit, maxPerSource, pinnedPrefix int) []ScoredItem {
	if limit > len(items) {
		limit = len(items)
	}
	for index := pinnedPrefix + 1; index < limit; index++ {
		if podsFreshnessSourceKey(items[index-1].Item) != podsFreshnessSourceKey(items[index].Item) {
			continue
		}
		for candidateIndex := index + 1; candidateIndex < len(items); candidateIndex++ {
			if items[candidateIndex].ScoreBreakdown.FreshnessReserved != items[index].ScoreBreakdown.FreshnessReserved {
				continue
			}
			candidateSource := podsFreshnessSourceKey(items[candidateIndex].Item)
			if candidateSource == podsFreshnessSourceKey(items[index-1].Item) {
				continue
			}
			items[index], items[candidateIndex] = items[candidateIndex], items[index]
			counts := map[string]int{}
			for position := pinnedPrefix; position < limit; position++ {
				counts[podsFreshnessSourceKey(items[position].Item)]++
			}
			valid := true
			for _, count := range counts {
				if count > maxPerSource {
					valid = false
					break
				}
			}
			if valid {
				break
			}
			items[index], items[candidateIndex] = items[candidateIndex], items[index]
		}
	}
	return items
}

func podsFirstPageConstraintStatus(items []ScoredItem) (bool, string) {
	limit := len(items)
	if limit > 20 {
		limit = 20
	}
	if limit < 1 {
		return false, ""
	}
	counts := map[string]int{}
	distinct := map[string]struct{}{}
	for _, item := range items {
		source := podsFreshnessSourceKey(item.Item)
		counts[source]++
		distinct[source] = struct{}{}
	}
	if len(distinct) < 5 {
		return false, "fewer_than_five_eligible_sources"
	}
	capacity := 0
	for _, count := range counts {
		if count > 4 {
			count = 4
		}
		capacity += count
	}
	if capacity < limit {
		return false, "source_cap_cannot_fill_requested_page"
	}

	// Report what the caller actually received. The serving algorithm preserves
	// pinned editorial content as an immovable prefix, so pinned items may
	// intentionally exceed the soft cap; that is a relaxation, not a silent
	// false positive in diagnostics.
	pinnedPrefix := 0
	for pinnedPrefix < limit && items[pinnedPrefix].ScoreBreakdown.Flags == "pinned" {
		pinnedPrefix++
	}
	pageCounts := map[string]int{}
	for index := pinnedPrefix; index < limit; index++ {
		pageCounts[podsFreshnessSourceKey(items[index].Item)]++
	}
	for _, count := range pageCounts {
		if count > 4 {
			return false, "source_cap_relaxed_by_order_constraints"
		}
	}
	pinnedCounts := map[string]int{}
	for index := 0; index < pinnedPrefix; index++ {
		pinnedCounts[podsFreshnessSourceKey(items[index].Item)]++
	}
	for _, count := range pinnedCounts {
		if count > 4 {
			return true, "pinned_editorial_override"
		}
	}
	return true, ""
}
