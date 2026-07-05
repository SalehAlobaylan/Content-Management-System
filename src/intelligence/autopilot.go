package intelligence

import (
	"time"

	"github.com/google/uuid"

	"content-management-system/src/models"
)

// ScoreSnapshot is the Autopilot-facing view of one item's durable value:
// what the engine currently believes, plus whether that belief is fresh and
// settled enough to act on autonomously (plan G7/G13). An id absent from the
// FreshScores result, or present with Fresh=false, must never be auto-acted on.
type ScoreSnapshot struct {
	Value            float64
	Confidence       float64
	ExplorationState string
	Fresh            bool
}

// FreshScores is the targeted pre-flight refresh (G13): given the exact evict
// candidates a run is about to judge, it refreshes stale or missing score rows
// through the normal ScoreBatch path (bounded by the standard batch size) and
// returns a snapshot per id. It refreshes only what the run needs — never the
// whole corpus — and a candidate the refresh could not reach comes back stale
// so the runner surfaces it for approval instead of acting.
func (e Engine) FreshScores(tenantID string, ids []uuid.UUID) map[uuid.UUID]ScoreSnapshot {
	result := make(map[uuid.UUID]ScoreSnapshot, len(ids))
	if e.DB == nil || len(ids) == 0 {
		return result
	}
	cutoff := time.Now().Add(-refreshTTL)

	rows := e.loadScoreRows(tenantID, ids)
	stale := make([]uuid.UUID, 0, len(ids))
	for _, id := range ids {
		row, ok := rows[id]
		if !ok || row.ComputedAt.Before(cutoff) {
			stale = append(stale, id)
		}
	}

	if len(stale) > 0 {
		if len(stale) > refreshBatchSize {
			stale = stale[:refreshBatchSize]
		}
		var items []models.ContentItem
		if err := e.DB.
			Where("tenant_id = ? AND public_id IN ?", tenantID, stale).
			Find(&items).Error; err == nil && len(items) > 0 {
			e.ScoreBatch(tenantID, items)         // persists refreshed rows
			rows = e.loadScoreRows(tenantID, ids) // reload only when scores actually changed
		}
	}

	for _, id := range ids {
		row, ok := rows[id]
		if !ok {
			continue // never scored and refresh could not reach it — caller treats as stale
		}
		result[id] = ScoreSnapshot{
			Value:            row.Value,
			Confidence:       row.Confidence,
			ExplorationState: row.ExplorationState,
			Fresh:            !row.ComputedAt.Before(cutoff),
		}
	}
	return result
}

func (e Engine) loadScoreRows(tenantID string, ids []uuid.UUID) map[uuid.UUID]models.MediaIntelligenceScore {
	out := make(map[uuid.UUID]models.MediaIntelligenceScore, len(ids))
	var rows []models.MediaIntelligenceScore
	if err := e.DB.
		Where("tenant_id = ? AND content_item_id IN ?", tenantID, ids).
		Find(&rows).Error; err != nil {
		return out
	}
	for _, r := range rows {
		out[r.ContentItemID] = r
	}
	return out
}
