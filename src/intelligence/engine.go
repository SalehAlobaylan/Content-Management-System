package intelligence

import (
	"encoding/json"
	"log"
	"sort"
	"time"

	"content-management-system/src/models"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Engine is the Value surface with its dependencies injected. All methods are
// safe to call from request handlers and background loops alike; every query
// is bounded.
type Engine struct {
	DB *gorm.DB
}

// ScoreBatch computes the full value model for a bounded set of items, loading
// the interaction/corpus context in bulk, and persists the results into
// media_intelligence_scores (upsert). This is the on-demand refresh trigger —
// circulation's generate pass calls it for the sets it already evaluates.
func (e Engine) ScoreBatch(tenantID string, items []models.ContentItem) []ValuedItem {
	if len(items) == 0 {
		return nil
	}
	ctx := e.loadBatchContext(tenantID, items)
	out := make([]ValuedItem, 0, len(items))
	rows := make([]models.MediaIntelligenceScore, 0, len(items))
	now := time.Now()
	for _, item := range items {
		v := scoreWithContext(item, ctx)
		out = append(out, v)
		rows = append(rows, toScoreRow(tenantID, item, v, now))
	}
	e.persistScores(rows)
	return out
}

// Values returns the persisted durable value for each item, falling back to
// FallbackValue for items that have never been scored. This is the primary
// consumer API for circulation/storage paths that already hold loaded items.
func (e Engine) Values(tenantID string, items []models.ContentItem) map[uuid.UUID]float64 {
	values := make(map[uuid.UUID]float64, len(items))
	if len(items) == 0 {
		return values
	}
	ids := make([]uuid.UUID, 0, len(items))
	for _, it := range items {
		ids = append(ids, it.PublicID)
	}
	var rows []models.MediaIntelligenceScore
	e.DB.Where("tenant_id = ? AND content_item_id IN ?", tenantID, ids).Find(&rows)
	persisted := make(map[uuid.UUID]float64, len(rows))
	for _, r := range rows {
		persisted[r.ContentItemID] = r.Value
	}
	for _, it := range items {
		if v, ok := persisted[it.PublicID]; ok {
			values[it.PublicID] = v
		} else {
			values[it.PublicID] = FallbackValue(it)
		}
	}
	return values
}

// RefreshStale is the scheduled + event-nudged refresh trigger in one bounded
// pass (grilling Q9): it recomputes items that (a) have never been scored,
// (b) have a score older than the TTL, or (c) moved past the impression or
// engagement delta thresholds since their last compute — stalest first, at
// most `budget` items.
func (e Engine) RefreshStale(tenantID string, budget int) int {
	if budget <= 0 {
		budget = refreshBatchSize
	}
	cutoff := time.Now().Add(-refreshTTL)

	var items []models.ContentItem
	err := e.DB.Model(&models.ContentItem{}).
		Joins("LEFT JOIN media_intelligence_scores mis ON mis.content_item_id = content_items.public_id").
		Where("content_items.tenant_id = ?", tenantID).
		Where("content_items.type IN ?", []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}).
		Where(`mis.content_item_id IS NULL
			OR mis.computed_at < ?
			OR content_items.impression_count - mis.impressions_at_compute >= ?
			OR (content_items.like_count + content_items.share_count + content_items.comment_count) - mis.engagement_at_compute >= ?`,
			cutoff, refreshImpressionDelta, refreshEngagementDelta).
		Order("mis.computed_at ASC NULLS FIRST").
		Limit(budget).
		Find(&items).Error
	if err != nil {
		log.Printf("intelligence: refresh query failed for tenant %s: %v", tenantID, err)
		return 0
	}
	if len(items) == 0 {
		return 0
	}
	e.ScoreBatch(tenantID, items)
	return len(items)
}

// StartRefreshLoop launches the scheduled refresh heartbeat: every tick it
// walks the tenants that have media items and refreshes each one's stale
// scores under the batch budget. Mirrors the news-circulation automation
// pattern (single lightweight ticker, work skipped when nothing is stale).
func StartRefreshLoop(db *gorm.DB) {
	engine := Engine{DB: db}
	go func() {
		ticker := time.NewTicker(refreshLoopInterval)
		defer ticker.Stop()
		for range ticker.C {
			var tenants []string
			db.Model(&models.ContentItem{}).
				Where("type IN ?", []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}).
				Distinct().
				Pluck("tenant_id", &tenants)
			for _, tenant := range tenants {
				if n := engine.RefreshStale(tenant, refreshBatchSize); n > 0 {
					log.Printf("intelligence: refreshed %d scores for tenant %s", n, tenant)
				}
			}
		}
	}()
}

// loadBatchContext bulk-loads the per-item interaction tallies and corpus
// statistics one scoring pass needs — three grouped queries total, regardless
// of batch size.
func (e Engine) loadBatchContext(tenantID string, items []models.ContentItem) batchContext {
	ctx := batchContext{
		completes:       map[string]int64{},
		views:           map[string]int64{},
		recent:          map[string]int64{},
		engagementPrior: defaultEngagementPrior,
		completionPrior: defaultCompletionPrior,
		tuning:          e.Tuning(tenantID),
	}
	ids := make([]uuid.UUID, 0, len(items))
	for _, it := range items {
		ids = append(ids, it.PublicID)
	}

	// Completion + view tallies per item.
	type interactionTally struct {
		ContentItemID uuid.UUID `gorm:"column:content_item_id"`
		Type          string    `gorm:"column:type"`
		Count         int64     `gorm:"column:count"`
	}
	var tallies []interactionTally
	e.DB.Model(&models.UserInteraction{}).
		Select("content_item_id, type, COUNT(*) AS count").
		Where("content_item_id IN ? AND type IN ?", ids,
			[]models.InteractionType{models.InteractionTypeView, models.InteractionTypeComplete}).
		Group("content_item_id, type").
		Scan(&tallies)
	for _, t := range tallies {
		switch models.InteractionType(t.Type) {
		case models.InteractionTypeComplete:
			ctx.completes[t.ContentItemID.String()] = t.Count
		case models.InteractionTypeView:
			ctx.views[t.ContentItemID.String()] = t.Count
		}
	}

	// Velocity: interactions inside the window, per item.
	type velocityTally struct {
		ContentItemID uuid.UUID `gorm:"column:content_item_id"`
		Count         int64     `gorm:"column:count"`
	}
	var recent []velocityTally
	windowStart := time.Now().Add(-time.Duration(velocityWindowHours) * time.Hour)
	e.DB.Model(&models.UserInteraction{}).
		Select("content_item_id, COUNT(*) AS count").
		Where("content_item_id IN ? AND created_at > ?", ids, windowStart).
		Group("content_item_id").
		Scan(&recent)
	for _, r := range recent {
		ctx.recent[r.ContentItemID.String()] = r.Count
	}

	// Corpus priors: engagement events per impression and completes per view
	// across the tenant's measured media corpus. Falls back to defaults when
	// telemetry hasn't accumulated (division guarded).
	type corpusRow struct {
		Impressions int64 `gorm:"column:impressions"`
		Engagement  int64 `gorm:"column:engagement"`
	}
	var corpus corpusRow
	e.DB.Model(&models.ContentItem{}).
		Select("COALESCE(SUM(impression_count),0) AS impressions, COALESCE(SUM(like_count + share_count + comment_count),0) AS engagement").
		Where("tenant_id = ? AND type IN ?", tenantID,
			[]models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}).
		Scan(&corpus)
	if corpus.Impressions >= minImpressionsForRate {
		ctx.engagementPrior = float64(corpus.Engagement) / float64(corpus.Impressions)
		if ctx.engagementPrior <= 0 {
			ctx.engagementPrior = defaultEngagementPrior
		}
	}
	var totalCompletes, totalViews int64
	for _, c := range ctx.completes {
		totalCompletes += c
	}
	for _, v := range ctx.views {
		totalViews += v
	}
	if totalViews >= 20 {
		ctx.completionPrior = float64(totalCompletes) / float64(totalViews)
		if ctx.completionPrior <= 0 {
			ctx.completionPrior = 0.01
		}
	}

	// Cost median over the batch's sized items.
	costs := make([]float64, 0, len(items))
	for _, it := range items {
		if c, ok := costPerUsefulMinute(it); ok {
			costs = append(costs, c)
		}
	}
	if len(costs) >= minCostSampleSize {
		sort.Float64s(costs)
		ctx.costMedian = costs[len(costs)/2]
	}
	return ctx
}

func toScoreRow(tenantID string, item models.ContentItem, v ValuedItem, now time.Time) models.MediaIntelligenceScore {
	breakdown, _ := json.Marshal(v.Breakdown)
	reasons, _ := json.Marshal(v.Reasons)
	return models.MediaIntelligenceScore{
		ContentItemID:        item.PublicID,
		TenantID:             tenantID,
		Value:                v.Value,
		Confidence:           v.Confidence,
		ExplorationState:     v.ExplorationState,
		ImpressionsAtCompute: item.ImpressionCount,
		EngagementAtCompute:  int64(item.LikeCount + item.ShareCount + item.CommentCount),
		Breakdown:            breakdown,
		Reasons:              reasons,
		ComputedAt:           now,
	}
}

// persistScores upserts score rows, preserving the engine-owned demotion
// columns (a recompute must not clear an active rank_down).
func (e Engine) persistScores(rows []models.MediaIntelligenceScore) {
	if len(rows) == 0 {
		return
	}
	err := e.DB.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "content_item_id"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"tenant_id", "value", "confidence", "exploration_state",
			"impressions_at_compute", "engagement_at_compute",
			"breakdown", "reasons", "computed_at", "updated_at",
		}),
	}).Create(&rows).Error
	if err != nil {
		log.Printf("intelligence: persisting %d scores failed: %v", len(rows), err)
	}
}
