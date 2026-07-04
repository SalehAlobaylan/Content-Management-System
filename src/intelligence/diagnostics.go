package intelligence

import (
	"time"

	"content-management-system/src/models"
)

// Diagnostics read model (grilling Q12): the observability surface for the
// intelligence system — exploration pipeline counts, score-refresh health, and
// the per-topic demand table. Read-only; no tuning knobs (weights stay code
// defaults until the model has operating history).

// TopicDemand is one row of the per-topic demand table (slice 6 / D5-C).
type TopicDemand struct {
	Topic        string  `json:"topic"`
	Serves       int64   `json:"serves"`
	RepeatServes int64   `json:"repeat_serves"`
	DemandScore  float64 `json:"demand_score"`   // [0,1], serve-share + repeat blend
	CoverageScore float64 `json:"coverage_score"` // [0,1], value-weighted supply
	Gap          float64 `json:"gap"`            // demand − coverage
	VisibleUnits int64   `json:"visible_units"`
}

// Diagnostics is the full intelligence-observability snapshot.
type Diagnostics struct {
	// Exploration pipeline.
	ExploringCount   int64 `json:"exploring_count"`
	EstablishedCount int64 `json:"established_count"`
	RetrialCount     int64 `json:"retrial_count"`
	DemotedCount     int64 `json:"demoted_count"`

	// Score-refresh health.
	ScoredCount     int64      `json:"scored_count"`
	UnscoredCount   int64      `json:"unscored_count"`
	StaleCount      int64      `json:"stale_count"`
	OldestComputeAt *time.Time `json:"oldest_computed_at,omitempty"`

	// Demand regime.
	DemandMeasured bool          `json:"demand_measured"`
	TopicDemand    []TopicDemand `json:"topic_demand"`
}

// TopicDemandLimit bounds the diagnostics topic table.
const TopicDemandLimit = 20

// DiagnosticsSnapshot assembles the observability read model — a handful of
// bounded aggregate queries.
func (e Engine) DiagnosticsSnapshot(tenantID string) Diagnostics {
	d := Diagnostics{TopicDemand: []TopicDemand{}}
	if e.DB == nil {
		return d
	}

	// Exploration pipeline counts.
	type stateRow struct {
		State string `gorm:"column:exploration_state"`
		Count int64  `gorm:"column:count"`
	}
	var states []stateRow
	e.DB.Model(&models.MediaIntelligenceScore{}).
		Select("exploration_state, COUNT(*) AS count").
		Where("tenant_id = ?", tenantID).
		Group("exploration_state").
		Scan(&states)
	for _, s := range states {
		switch s.State {
		case ExplorationExploring:
			d.ExploringCount = s.Count
		case ExplorationEstablished:
			d.EstablishedCount = s.Count
		case ExplorationRetrial:
			d.RetrialCount = s.Count
		}
		d.ScoredCount += s.Count
	}
	e.DB.Model(&models.MediaIntelligenceScore{}).
		Where("tenant_id = ? AND demotion_factor IS NOT NULL", tenantID).
		Count(&d.DemotedCount)

	// Refresh health.
	e.DB.Model(&models.ContentItem{}).
		Where("tenant_id = ? AND type IN ?", tenantID,
			[]models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}).
		Where("NOT EXISTS (SELECT 1 FROM media_intelligence_scores s WHERE s.content_item_id = content_items.public_id)").
		Count(&d.UnscoredCount)
	e.DB.Model(&models.MediaIntelligenceScore{}).
		Where("tenant_id = ? AND computed_at < ?", tenantID, time.Now().Add(-refreshTTL)).
		Count(&d.StaleCount)
	var oldest models.MediaIntelligenceScore
	if err := e.DB.Where("tenant_id = ?", tenantID).Order("computed_at ASC").First(&oldest).Error; err == nil {
		t := oldest.ComputedAt
		d.OldestComputeAt = &t
	}

	// Topic demand table.
	d.TopicDemand, d.DemandMeasured = e.topicDemand(tenantID)
	return d
}

// topicDemand aggregates the lookback's per-topic serve telemetry against the
// corpus's value-weighted per-topic supply.
func (e Engine) topicDemand(tenantID string) ([]TopicDemand, bool) {
	cutoff := time.Now().Add(-demandLookback)

	type topicRow struct {
		Topic        string `gorm:"column:topic"`
		Serves       int64  `gorm:"column:serves"`
		RepeatServes int64  `gorm:"column:repeat_serves"`
	}
	var rows []topicRow
	e.DB.Model(&models.MediaDemandStat{}).
		Select("topic, COALESCE(SUM(serves),0) AS serves, COALESCE(SUM(repeat_serves),0) AS repeat_serves").
		Where("tenant_id = ? AND topic <> '' AND window_start > ?", tenantID, cutoff).
		Group("topic").
		Order("SUM(serves) DESC").
		Limit(TopicDemandLimit).
		Scan(&rows)
	if len(rows) == 0 {
		return []TopicDemand{}, false
	}

	// Value-weighted supply per topic over the visible media corpus.
	type coverageRow struct {
		Topic  string  `gorm:"column:topic"`
		Weight float64 `gorm:"column:weight"`
		Units  int64   `gorm:"column:units"`
	}
	var covRows []coverageRow
	e.DB.Raw(`
		SELECT topic, SUM(item_value) AS weight, COUNT(*) AS units FROM (
			SELECT unnest(ci.topic_tags) AS topic, COALESCE(mis.value, ?) AS item_value
			FROM content_items ci
			LEFT JOIN media_intelligence_scores mis ON mis.content_item_id = ci.public_id
			WHERE ci.tenant_id = ?
				AND ci.type IN ('VIDEO', 'PODCAST')
				AND ci.status = 'READY'
				AND ci.is_feed_unit = TRUE
				AND ci.feed_visibility = 'visible'
		) topics
		GROUP BY topic`, unscoredCoverageValue, tenantID).Scan(&covRows)
	coverage := make(map[string]coverageRow, len(covRows))
	var maxCoverage float64
	for _, r := range covRows {
		coverage[r.Topic] = r
		if r.Weight > maxCoverage {
			maxCoverage = r.Weight
		}
	}

	var maxServes, totalServes int64
	for _, r := range rows {
		totalServes += r.Serves
		if r.Serves > maxServes {
			maxServes = r.Serves
		}
	}
	measured := totalServes >= measuredMinServes

	out := make([]TopicDemand, 0, len(rows))
	for _, r := range rows {
		var demand, repeatNorm float64
		if maxServes > 0 {
			demand = float64(r.Serves) / float64(maxServes)
		}
		if r.Serves > 0 {
			repeatNorm = clamp01(float64(r.RepeatServes) / float64(r.Serves))
		}
		demand = clamp01(0.8*demand + 0.2*repeatNorm)

		cov := coverage[r.Topic]
		var coverageScore float64
		if maxCoverage > 0 {
			coverageScore = cov.Weight / maxCoverage
		}
		out = append(out, TopicDemand{
			Topic:         r.Topic,
			Serves:        r.Serves,
			RepeatServes:  r.RepeatServes,
			DemandScore:   demand,
			CoverageScore: coverageScore,
			Gap:           demand - coverageScore,
			VisibleUnits:  cov.Units,
		})
	}
	return out, measured
}
