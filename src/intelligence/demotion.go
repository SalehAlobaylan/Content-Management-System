package intelligence

import (
	"time"

	"content-management-system/src/models"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// Soft eviction (grilling Q10): an applied rank_down sets an engine-owned
// demotion on the score row; the For You feed multiplies ordering scores by
// the half-life-decayed effective demotion; revert clears it. Deliberately
// separate from the human-owned editorial ContentFlag — the stage-5 ledger
// and the human-exceptions layer need engine and editorial decisions to stay
// distinguishable.

// demotionDefaultFactor is the immediate post-apply score multiplier. It
// decays back toward 1.0 with DemotionHalfLife (an item can re-earn exposure —
// grilling Q7.3).
const demotionDefaultFactor = 0.5

// ApplyDemotion sets the demotion on an item's score row, scoring the item
// first if it was never scored (the row carries the demotion, so it must
// exist).
func (e Engine) ApplyDemotion(tenantID string, itemID uuid.UUID) error {
	var exists int64
	e.DB.Model(&models.MediaIntelligenceScore{}).
		Where("content_item_id = ?", itemID).
		Count(&exists)
	if exists == 0 {
		var item models.ContentItem
		if err := e.DB.Where("tenant_id = ? AND public_id = ?", tenantID, itemID).First(&item).Error; err != nil {
			return err
		}
		e.ScoreBatch(tenantID, []models.ContentItem{item})
	}
	now := time.Now()
	factor := e.Tuning(tenantID).DemotionDefaultFactor
	return e.DB.Model(&models.MediaIntelligenceScore{}).
		Where("content_item_id = ?", itemID).
		Updates(map[string]interface{}{
			"demotion_factor": factor,
			"demoted_at":      now,
		}).Error
}

// ClearDemotion reverts a rank_down: the item returns to full exposure.
func (e Engine) ClearDemotion(itemID uuid.UUID) error {
	return e.DB.Model(&models.MediaIntelligenceScore{}).
		Where("content_item_id = ?", itemID).
		Updates(map[string]interface{}{
			"demotion_factor": gorm.Expr("NULL"),
			"demoted_at":      gorm.Expr("NULL"),
		}).Error
}

// FeedSignal is what the For You assembly needs per candidate: whether the
// item is still exploring (feeds the injection slice + negative-decision
// immunity) and the decayed demotion multiplier (soft eviction).
type FeedSignal struct {
	Exploring bool
	// Demotion is the effective (decayed) score multiplier in (0,1]; 1 = none.
	Demotion float64
}

// FeedSignals batch-loads the feed hooks' inputs for a candidate set. Items
// with no score row count as exploring (never scored = never given a chance)
// with no demotion. The demotion half-life comes from the tenant's tuning.
func (e Engine) FeedSignals(tenantID string, ids []uuid.UUID) map[uuid.UUID]FeedSignal {
	signals := make(map[uuid.UUID]FeedSignal, len(ids))
	for _, id := range ids {
		signals[id] = FeedSignal{Exploring: true, Demotion: 1}
	}
	if len(ids) == 0 {
		return signals
	}
	halfLife := e.Tuning(tenantID).DemotionHalfLife
	var rows []models.MediaIntelligenceScore
	e.DB.Select("content_item_id", "exploration_state", "demotion_factor", "demoted_at").
		Where("content_item_id IN ?", ids).
		Find(&rows)
	now := time.Now()
	for _, r := range rows {
		demotion := 1.0
		if r.DemotionFactor != nil && r.DemotedAt != nil {
			demotion = EffectiveDemotionAt(*r.DemotionFactor, *r.DemotedAt, now, halfLife)
		}
		signals[r.ContentItemID] = FeedSignal{
			Exploring: r.ExplorationState == ExplorationExploring || r.ExplorationState == ExplorationRetrial,
			Demotion:  demotion,
		}
	}
	return signals
}
