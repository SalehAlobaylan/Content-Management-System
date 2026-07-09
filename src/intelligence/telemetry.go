package intelligence

import (
	"log"
	"time"

	"content-management-system/src/models"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// Serve-side telemetry (grilling Q5 + Q6) — the feed's instrumentation point.
//
// Impressions: every item in a For You response counts as one impression.
// This is the interim serve-side proxy; Wahb-Platform viewport events are the
// planned upgrade (the counter doesn't care where the increment comes from).
//
// Demand: the cache's miss signal, recorded per tenant × bucket × hour window:
//   - serves       — units of each duration bucket actually served
//   - exhaustions  — a page came back shorter than asked (the true cache miss)
//   - repeat serves — an item re-served within the repeat window (the
//     small-library symptom: circulating the same units for lack of others)

const (
	// repeatServeWindow: a re-serve of the same item inside this window counts
	// as repeat pressure rather than normal circulation.
	repeatServeWindow = 6 * time.Hour
	// demandExhaustionBucketAll aggregates exhaustion when no duration filter
	// scoped the request to a specific bucket.
	demandBucketAll = "all"
)

// ServeRecord captures one For You response for telemetry purposes.
type ServeRecord struct {
	TenantID       string
	Items          []models.ContentItem
	RequestedLimit int
	// DurationBucket is the explicit ?duration= filter expressed as a bucket
	// label ("30m"), or "" when the request was unfiltered.
	DurationBucket string
}

// RecordServe writes impression increments and demand-stat upserts for one
// feed response. Designed to run AFTER the response is sent (call it from a
// goroutine) — it must never add latency to the serve path, and any failure is
// logged, never surfaced.
func RecordServe(db *gorm.DB, rec ServeRecord) {
	if len(rec.Items) == 0 && rec.RequestedLimit <= 0 {
		return
	}
	now := time.Now()
	window := now.Truncate(time.Hour)

	// 1. Impressions + last_served_at, one batched UPDATE.
	if len(rec.Items) > 0 {
		ids := make([]uuid.UUID, 0, len(rec.Items))
		for _, it := range rec.Items {
			ids = append(ids, it.PublicID)
		}
		if err := db.Model(&models.ContentItem{}).
			Where("public_id IN ?", ids).
			Updates(map[string]interface{}{
				"impression_count": gorm.Expr("impression_count + 1"),
				"last_served_at":   now,
			}).Error; err != nil {
			log.Printf("intelligence: impression update failed: %v", err)
		}
	}

	// 2. Demand stats per bucket (topic = '') and per topic × bucket (the
	// topic axis, slice 6): serves + repeat serves.
	type tally struct{ serves, repeats int64 }
	byBucket := map[string]*tally{}
	type topicKey struct{ bucket, topic string }
	byTopic := map[topicKey]*tally{}
	topicSlugsByItem := catalogTopicSlugsByItem(db, rec.Items)
	for _, it := range rec.Items {
		bucket := itemBucketLabel(it)
		if bucket == "" {
			continue
		}
		repeat := it.LastServedAt != nil && now.Sub(*it.LastServedAt) < repeatServeWindow
		t, ok := byBucket[bucket]
		if !ok {
			t = &tally{}
			byBucket[bucket] = t
		}
		t.serves++
		if repeat {
			t.repeats++
		}
		for _, topic := range topicSlugsByItem[it.PublicID] {
			key := topicKey{bucket: bucket, topic: topic}
			tt, ok := byTopic[key]
			if !ok {
				tt = &tally{}
				byTopic[key] = tt
			}
			tt.serves++
			if repeat {
				tt.repeats++
			}
		}
	}
	for bucket, t := range byBucket {
		upsertDemandStat(db, rec.TenantID, bucket, "", window, t.serves, 0, t.repeats)
	}
	for key, t := range byTopic {
		upsertDemandStat(db, rec.TenantID, key.bucket, key.topic, window, t.serves, 0, t.repeats)
	}

	// 3. Exhaustion: the page came back shorter than asked. Attributed to the
	// explicitly requested bucket when a duration filter scoped the request,
	// otherwise to the whole-feed pseudo-bucket.
	if rec.RequestedLimit > 0 && len(rec.Items) < rec.RequestedLimit {
		bucket := rec.DurationBucket
		if bucket == "" {
			bucket = demandBucketAll
		}
		upsertDemandStat(db, rec.TenantID, bucket, "", window, 0, 1, 0)
	}
}

func catalogTopicSlugsByItem(db *gorm.DB, items []models.ContentItem) map[uuid.UUID][]string {
	const maxTopicsPerItem = 3
	out := map[uuid.UUID][]string{}
	if len(items) == 0 {
		return out
	}
	ids := make([]uuid.UUID, 0, len(items))
	for _, item := range items {
		ids = append(ids, item.PublicID)
	}
	type row struct {
		ContentItemID uuid.UUID
		Slug          string
		Score         float64
	}
	var rows []row
	db.Table("content_item_topics cit").
		Select("cit.content_item_id, topics.slug, cit.score").
		Joins("JOIN topics ON topics.public_id = cit.topic_id").
		Where("cit.content_item_id IN ? AND topics.active = ?", ids, true).
		Order("cit.content_item_id ASC, cit.score DESC").
		Scan(&rows)
	for _, r := range rows {
		if r.Slug == "" || len(out[r.ContentItemID]) >= maxTopicsPerItem {
			continue
		}
		out[r.ContentItemID] = append(out[r.ContentItemID], r.Slug)
	}
	return out
}

func upsertDemandStat(db *gorm.DB, tenantID, bucket, topic string, window time.Time, serves, exhaustions, repeats int64) {
	err := db.Exec(`
		INSERT INTO media_demand_stats (tenant_id, bucket, topic, window_start, serves, exhaustions, repeat_serves, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, now(), now())
		ON CONFLICT (tenant_id, bucket, topic, window_start)
		DO UPDATE SET
			serves = media_demand_stats.serves + EXCLUDED.serves,
			exhaustions = media_demand_stats.exhaustions + EXCLUDED.exhaustions,
			repeat_serves = media_demand_stats.repeat_serves + EXCLUDED.repeat_serves,
			updated_at = now()`,
		tenantID, bucket, topic, window, serves, exhaustions, repeats).Error
	if err != nil {
		log.Printf("intelligence: demand upsert failed (bucket %s, topic %q): %v", bucket, topic, err)
	}
}

// itemBucketLabel returns the item's canonical duration bucket, deriving it
// from duration_sec when the column hasn't been backfilled (nearest of
// 5/10/15/20/30/40 minutes — the same rule the atomization layer uses).
func itemBucketLabel(item models.ContentItem) string {
	if item.DurationBucket != nil && *item.DurationBucket != "" {
		return *item.DurationBucket
	}
	if item.DurationSec == nil || *item.DurationSec <= 0 {
		return ""
	}
	return BucketLabelForDuration(*item.DurationSec)
}

// BucketLabelForDuration maps a duration to the nearest canonical For You
// bucket label.
func BucketLabelForDuration(durationSec int) string {
	buckets := []int{5, 10, 15, 20, 30, 40}
	minutes := float64(durationSec) / 60.0
	best := buckets[0]
	bestDist := absFloat(minutes - float64(buckets[0]))
	for _, b := range buckets[1:] {
		if d := absFloat(minutes - float64(b)); d < bestDist {
			best = b
			bestDist = d
		}
	}
	return intToLabel(best)
}

func intToLabel(minutes int) string {
	switch minutes {
	case 5:
		return "5m"
	case 10:
		return "10m"
	case 15:
		return "15m"
	case 20:
		return "20m"
	case 30:
		return "30m"
	default:
		return "40m"
	}
}

func absFloat(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}
