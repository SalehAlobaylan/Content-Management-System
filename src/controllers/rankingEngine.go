package controllers

import (
	"content-management-system/src/models"
	"math"
	"sort"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// ScoreBreakdown shows the contribution of each signal to the final score.
type ScoreBreakdown struct {
	Freshness  float64 `json:"freshness"`
	Engagement float64 `json:"engagement"`
	Velocity   float64 `json:"velocity"`
	Similarity float64 `json:"similarity"`
	Quality    float64 `json:"quality"`
	Diversity  float64 `json:"diversity"`
	Trending   float64 `json:"trending"`
	Flags      string  `json:"flags"` // "boosted", "suppressed", "pinned", ""
}

// ScoredItem holds a content item with its computed ranking score.
type ScoredItem struct {
	Item           models.ContentItem `json:"item"`
	FinalScore     float64            `json:"final_score"`
	ScoreBreakdown ScoreBreakdown     `json:"score_breakdown"`
}

// VelocityData maps content_item public_id → recent interaction count.
type VelocityData map[uuid.UUID]int

// ----------------------------------------------------------------
// Signal 1 — Freshness (exponential half-life decay)
// ----------------------------------------------------------------

func computeFreshness(item models.ContentItem, decayHours int, now time.Time) float64 {
	if decayHours <= 0 {
		decayHours = 72
	}
	var pubTime time.Time
	if item.PublishedAt != nil {
		pubTime = *item.PublishedAt
	} else {
		pubTime = item.CreatedAt
	}
	hoursAge := now.Sub(pubTime).Hours()
	if hoursAge < 0 {
		hoursAge = 0
	}
	lambda := math.Ln2 / float64(decayHours)
	return math.Exp(-lambda * hoursAge)
}

// ----------------------------------------------------------------
// Signal 2 — Engagement (weighted log composite)
// ----------------------------------------------------------------

func computeEngagementRaw(item models.ContentItem) float64 {
	return 1.0*math.Log1p(float64(item.LikeCount)) +
		0.3*math.Log1p(float64(item.ViewCount)) +
		0.5*math.Log1p(float64(item.ShareCount)) +
		0.4*math.Log1p(float64(item.CommentCount))
}

// ----------------------------------------------------------------
// Signal 3 — Velocity (engagement rate in window)
// ----------------------------------------------------------------

func computeVelocityRaw(itemID uuid.UUID, velocityData VelocityData, windowHours int) float64 {
	if windowHours <= 0 {
		windowHours = 6
	}
	count, ok := velocityData[itemID]
	if !ok || count == 0 {
		return 0
	}
	return float64(count) / float64(windowHours)
}

// ----------------------------------------------------------------
// Signal 4 — Similarity (pgvector cosine — placeholder for user context)
// Without user context this returns 0; the controller can inject a reference embedding.
// ----------------------------------------------------------------

func computeSimilarity(_ models.ContentItem) float64 {
	// Requires reference embedding from user context.
	// Actual pgvector <=> query is done at DB level in the controller preview endpoint.
	return 0
}

// ----------------------------------------------------------------
// Signal 5 — Quality (content completeness + source tier)
// ----------------------------------------------------------------

var sourceTierMap = map[models.SourceType]float64{
	models.SourceTypeManual:   1.0,
	models.SourceTypeUpload:   0.9,
	models.SourceTypeRSS:      0.8,
	models.SourceTypeYouTube:  0.8,
	models.SourceTypePodcast:  0.8,
	models.SourceTypeTelegram: 0.7,
	models.SourceTypeWebsite:  0.6,
}

func computeQuality(item models.ContentItem) float64 {
	var completeness float64
	if item.ThumbnailURL != nil && *item.ThumbnailURL != "" {
		completeness += 0.2
	}
	if item.Excerpt != nil && *item.Excerpt != "" {
		completeness += 0.15
	}
	if item.TranscriptID != nil {
		completeness += 0.2
	}
	if len(item.TopicTags) > 0 {
		completeness += 0.15
	}
	if item.Embedding != nil {
		completeness += 0.15
	}
	if item.DurationSec != nil && *item.DurationSec > 0 {
		completeness += 0.15
	}

	tier, ok := sourceTierMap[item.Source]
	if !ok {
		tier = 0.5
	}

	return 0.6*completeness + 0.4*tier
}

// ----------------------------------------------------------------
// Signal 7 — Trending (spike detection)
// ----------------------------------------------------------------

func computeTrending(item models.ContentItem, velocityData VelocityData, windowHours int, thresholdMul float64, now time.Time) float64 {
	if thresholdMul <= 0 {
		thresholdMul = 2.0
	}
	if windowHours <= 0 {
		windowHours = 6
	}

	var pubTime time.Time
	if item.PublishedAt != nil {
		pubTime = *item.PublishedAt
	} else {
		pubTime = item.CreatedAt
	}
	hoursAge := now.Sub(pubTime).Hours()
	if hoursAge < 1 {
		hoursAge = 1
	}

	totalInteractions := float64(item.LikeCount + item.ViewCount + item.ShareCount + item.CommentCount)
	avgHourlyRate := totalInteractions / hoursAge

	recentCount := float64(velocityData[item.PublicID])
	recentHourlyRate := recentCount / float64(windowHours)

	denominator := avgHourlyRate * thresholdMul
	if denominator <= 0 {
		return 0
	}
	score := recentHourlyRate / denominator
	if score > 1.0 {
		score = 1.0
	}
	return score
}

// ----------------------------------------------------------------
// Batch velocity loader
// ----------------------------------------------------------------

// LoadVelocityData queries the user_interactions table to get recent interaction
// counts grouped by content_item_id within the velocity window.
func LoadVelocityData(db *gorm.DB, contentIDs []uuid.UUID, windowHours int, now time.Time) VelocityData {
	data := make(VelocityData)
	if len(contentIDs) == 0 {
		return data
	}
	cutoff := now.Add(-time.Duration(windowHours) * time.Hour)

	type result struct {
		ContentItemID uuid.UUID `gorm:"column:content_item_id"`
		Count         int       `gorm:"column:count"`
	}
	var results []result
	db.Model(&models.UserInteraction{}).
		Select("content_item_id, COUNT(*) as count").
		Where("content_item_id IN ? AND created_at > ?", contentIDs, cutoff).
		Group("content_item_id").
		Scan(&results)

	for _, r := range results {
		data[r.ContentItemID] = r.Count
	}
	return data
}

// ----------------------------------------------------------------
// Batch flag loader
// ----------------------------------------------------------------

// LoadContentFlags loads editorial flags for a set of content items.
func LoadContentFlags(db *gorm.DB, tenantID string, contentIDs []uuid.UUID) map[uuid.UUID]models.ContentFlag {
	flags := make(map[uuid.UUID]models.ContentFlag)
	if len(contentIDs) == 0 {
		return flags
	}

	var flagList []models.ContentFlag
	db.Where("tenant_id = ? AND content_item_id IN ?", tenantID, contentIDs).Find(&flagList)
	for _, f := range flagList {
		flags[f.ContentItemID] = f
	}
	return flags
}

// ----------------------------------------------------------------
// Core scoring pipeline
// ----------------------------------------------------------------

// ScoreItems scores a batch of content items using the 7-signal ranking engine.
func ScoreItems(items []models.ContentItem, config models.RankingConfig, flagMap map[uuid.UUID]models.ContentFlag, velocityData VelocityData, now time.Time) []ScoredItem {
	if len(items) == 0 {
		return nil
	}

	// Pre-compute raw engagement + velocity for batch normalization
	engagementRaws := make([]float64, len(items))
	velocityRaws := make([]float64, len(items))
	var maxEng, maxVel float64

	for i, item := range items {
		engagementRaws[i] = computeEngagementRaw(item)
		velocityRaws[i] = computeVelocityRaw(item.PublicID, velocityData, config.VelocityWindowHours)
		if engagementRaws[i] > maxEng {
			maxEng = engagementRaws[i]
		}
		if velocityRaws[i] > maxVel {
			maxVel = velocityRaws[i]
		}
	}

	scored := make([]ScoredItem, 0, len(items))

	for i, item := range items {
		flag, hasFlag := flagMap[item.PublicID]

		// Skip excluded items
		if hasFlag && flag.ExcludeFromFeed {
			continue
		}

		// Compute 7 signals
		freshness := computeFreshness(item, config.FreshnessDecayHours, now)

		var engagement float64
		if maxEng > 0 {
			engagement = engagementRaws[i] / maxEng
		}

		var velocity float64
		if maxVel > 0 {
			velocity = velocityRaws[i] / maxVel
		}

		similarity := computeSimilarity(item) // 0 without user context

		quality := computeQuality(item)

		trending := computeTrending(item, velocityData, config.VelocityWindowHours, config.TrendingThresholdMultiplier, now)

		// Diversity is applied as a post-processing penalty (Signal 6)
		diversity := 1.0

		// Weighted sum
		rawScore := config.FreshnessWeight*freshness +
			config.EngagementWeight*engagement +
			config.VelocityWeight*velocity +
			config.SimilarityWeight*similarity +
			config.QualityWeight*quality +
			config.DiversityWeight*diversity +
			config.TrendingWeight*trending

		// Apply editorial flags
		finalScore := rawScore
		flagLabel := ""

		if hasFlag {
			if flag.PinToTop {
				finalScore = math.MaxFloat64
				flagLabel = "pinned"
			} else if flag.Boost {
				mul := flag.BoostMultiplier
				if mul <= 0 {
					mul = 1.5
				}
				finalScore = rawScore * mul
				flagLabel = "boosted"
			} else if flag.Suppress {
				finalScore = rawScore * 0.1
				flagLabel = "suppressed"
			}
		}

		scored = append(scored, ScoredItem{
			Item:       item,
			FinalScore: finalScore,
			ScoreBreakdown: ScoreBreakdown{
				Freshness:  freshness,
				Engagement: engagement,
				Velocity:   velocity,
				Similarity: similarity,
				Quality:    quality,
				Diversity:  diversity,
				Trending:   trending,
				Flags:      flagLabel,
			},
		})
	}

	// Sort by final score descending
	sort.Slice(scored, func(i, j int) bool {
		return scored[i].FinalScore > scored[j].FinalScore
	})

	// Signal 6 — Diversity post-processing pass
	scored = applyDiversityPenalty(scored)

	return scored
}

// ----------------------------------------------------------------
// Signal 6 — Diversity (anti-repetition penalty, applied at assembly level)
// ----------------------------------------------------------------

func applyDiversityPenalty(scored []ScoredItem) []ScoredItem {
	if len(scored) <= 1 {
		return scored
	}

	for i := 1; i < len(scored); i++ {
		// Same-source penalty
		if scored[i].Item.SourceName != nil && scored[i-1].Item.SourceName != nil {
			if *scored[i].Item.SourceName == *scored[i-1].Item.SourceName {
				scored[i].FinalScore *= 0.7
				scored[i].ScoreBreakdown.Diversity = 0.7
			}
		}

		// Topic overlap penalty with last 3 items
		if len(scored[i].Item.TopicTags) > 0 {
			windowStart := i - 3
			if windowStart < 0 {
				windowStart = 0
			}
			for k := windowStart; k < i; k++ {
				overlap := topicOverlap(scored[i].Item.TopicTags, scored[k].Item.TopicTags)
				if overlap > 0.5 {
					scored[i].FinalScore *= 0.8
					scored[i].ScoreBreakdown.Diversity = math.Min(scored[i].ScoreBreakdown.Diversity, 0.8)
					break
				}
			}
		}
	}

	// Re-sort after diversity penalties
	sort.Slice(scored, func(i, j int) bool {
		return scored[i].FinalScore > scored[j].FinalScore
	})

	return scored
}

func topicOverlap(a, b []string) float64 {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}
	set := make(map[string]bool, len(b))
	for _, t := range b {
		set[t] = true
	}
	var overlap int
	for _, t := range a {
		if set[t] {
			overlap++
		}
	}
	total := len(a)
	if len(b) < total {
		total = len(b)
	}
	return float64(overlap) / float64(total)
}
