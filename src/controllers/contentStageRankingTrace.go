package controllers

import "github.com/google/uuid"

// rankingTraceForItem reports the exact positions from the canonical Pods
// assembly stages. Keeping the pre-source-constraint slice explicit prevents
// freshness-slot movement from being mislabeled as source-spacing movement.
func rankingTraceForItem(raw, beforeSourceConstraints, final []ScoredItem, itemID uuid.UUID) map[string]any {
	result := map[string]any{
		"raw_rank":                nil,
		"freshness_reserved":      false,
		"source_spacing_movement": 0,
		"final_rank":              nil,
	}
	rawRank := 0
	for i, candidate := range raw {
		if candidate.Item.PublicID == itemID {
			rawRank = i + 1
			break
		}
	}
	finalRank := 0
	reserved := false
	for i, candidate := range final {
		if candidate.Item.PublicID == itemID {
			finalRank = i + 1
			reserved = candidate.ScoreBreakdown.FreshnessReserved
			break
		}
	}
	if rawRank > 0 {
		result["raw_rank"] = rawRank
	}
	if finalRank > 0 {
		result["final_rank"] = finalRank
		result["freshness_reserved"] = reserved
		beforeSourceRank := 0
		for i, candidate := range beforeSourceConstraints {
			if candidate.Item.PublicID == itemID {
				beforeSourceRank = i + 1
				break
			}
		}
		if beforeSourceRank > 0 {
			result["source_spacing_movement"] = finalRank - beforeSourceRank
		}
	}
	return result
}
