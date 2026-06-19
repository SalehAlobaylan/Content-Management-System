package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"errors"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/pgvector/pgvector-go"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// storyMatchThresholdDefault is the fallback minimum cosine similarity for a
// content item to join an existing story (topic) when the tenant's
// RankingConfig.StoryMatchThreshold is unset/zero. Tuned for the Qwen3
// embedding space, where related articles sit around ~0.65–0.75 cosine (vs
// BGE-M3's higher distribution) — 0.70 clusters genuinely-related coverage into
// event stories without over-merging. Admin-tunable via the ranking config.
const storyMatchThresholdDefault = 0.70

// storyActivityWindowDays bounds a story in time: an item only joins a story
// whose most recent member published within this many days of the item's own
// publish time (either direction — backfilled old items must not join current
// stories either). Without it, stories absorb semantically-similar items
// forever and decay into evergreen topics instead of events.
const storyActivityWindowDays = 7

// classifyContentTopic assigns a first-class topic to one content item from its
// dense embedding: nearest existing topic by cosine, else a new LLM-labeled
// topic. Fire-and-forget safe (gorm.DB is goroutine-safe). Best-effort — leaves
// the item unclassified on any failure so a later backfill can retry.
func classifyContentTopic(db *gorm.DB, contentID uuid.UUID) {
	var item models.ContentItem
	if err := db.Where("public_id = ?", contentID).First(&item).Error; err != nil {
		return
	}
	if item.Embedding == nil {
		return
	}
	emb := item.Embedding.Slice()
	if len(emb) == 0 {
		return
	}
	lit := utils.PgvectorToLiteral(emb)

	// Nearest ACTIVE topic in this tenant by cosine distance. The activity
	// window keeps stories event-bounded: only stories whose latest member
	// published within ±storyActivityWindowDays of this item are candidates.
	// NULL last_member_at (legacy rows) stays eligible.
	itemT := itemTime(item)
	windowStart := itemT.AddDate(0, 0, -storyActivityWindowDays)
	windowEnd := itemT.AddDate(0, 0, storyActivityWindowDays)
	type nearestRow struct {
		PublicID uuid.UUID
		Distance float64
	}
	var nearest nearestRow
	_ = db.Model(&models.Topic{}).
		Select("public_id, (embedding <=> '"+lit+"') AS distance").
		Where("tenant_id = ? AND embedding IS NOT NULL", item.TenantID).
		Where("last_member_at IS NULL OR last_member_at BETWEEN ? AND ?", windowStart, windowEnd).
		Order("embedding <=> '" + lit + "'").
		Limit(1).
		Scan(&nearest).Error

	// Event-level threshold, admin-tunable per tenant via RankingConfig.
	threshold := loadTenantConfig(db, item.TenantID).StoryMatchThreshold
	if threshold <= 0 {
		threshold = storyMatchThresholdDefault
	}

	if nearest.PublicID != uuid.Nil && (1.0-nearest.Distance) >= threshold {
		assignTopicToItem(db, &item, nearest.PublicID, emb)
		return
	}

	// No close topic — ask the LLM for a meaningful label and create one.
	// Classification must NOT depend on LLM availability: on any labeling
	// failure fall back to a placeholder label derived from the item itself
	// (Labeled=false, renamed later by /admin/topics/label-batch). Returning
	// without a topic would silently exclude the item from the News feed.
	labeled := true
	label, err := generateTopicLabelViaEnrichment(topicSeedTexts(&item))
	label = strings.TrimSpace(label)
	if err != nil || label == "" {
		labeled = false
		label = placeholderStoryLabel(&item)
	}

	var topic models.Topic
	findErr := db.Where("tenant_id = ? AND label = ?", item.TenantID, label).First(&topic).Error
	if errors.Is(findErr, gorm.ErrRecordNotFound) {
		vec := pgvector.NewVector(emb)
		topic = models.Topic{
			TenantID:     item.TenantID,
			Label:        label,
			Embedding:    &vec,
			ArticleCount: 0,
			Labeled:      labeled, // false = placeholder awaiting label-batch
			LastMemberAt: &itemT,
		}
		if createErr := db.Create(&topic).Error; createErr != nil {
			// Lost a create race on the unique (tenant,label) — fetch the winner.
			if refetch := db.Where("tenant_id = ? AND label = ?", item.TenantID, label).
				First(&topic).Error; refetch != nil {
				return
			}
		}
	} else if findErr != nil {
		return
	}

	assignTopicToItem(db, &item, topic.PublicID, emb)
}

// assignTopicToItem points an article at a topic and folds its embedding into
// the topic's centroid (running mean), maintaining article_count. Runs in a
// transaction with a row lock so concurrent classifications don't clobber the
// centroid. Decrements the previous topic's count when an item is moved.
func assignTopicToItem(db *gorm.DB, item *models.ContentItem, topicID uuid.UUID, emb []float32) {
	alreadyMember := item.TopicID != nil && *item.TopicID == topicID

	_ = db.Transaction(func(tx *gorm.DB) error {
		if item.TopicID != nil && *item.TopicID != topicID {
			tx.Model(&models.Topic{}).
				Where("public_id = ?", *item.TopicID).
				UpdateColumn("article_count", gorm.Expr("GREATEST(article_count - 1, 0)"))
		}

		if !alreadyMember {
			var topic models.Topic
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
				Where("public_id = ?", topicID).First(&topic).Error; err != nil {
				return err
			}
			var centroid []float32
			if topic.Embedding != nil {
				centroid = topic.Embedding.Slice()
			}
			newCentroid := runningMean(centroid, topic.ArticleCount, emb)
			vec := pgvector.NewVector(newCentroid)
			updates := map[string]interface{}{
				"embedding":     &vec,
				"article_count": topic.ArticleCount + 1,
			}
			// Advance the story's activity time when this member is newer —
			// keeps the activity window tracking real event time.
			if t := itemTime(*item); topic.LastMemberAt == nil || t.After(*topic.LastMemberAt) {
				updates["last_member_at"] = t
			}
			if err := tx.Model(&models.Topic{}).
				Where("public_id = ?", topicID).
				Updates(updates).Error; err != nil {
				return err
			}
		}

		if err := tx.Model(&models.ContentItem{}).
			Where("public_id = ?", item.PublicID).
			UpdateColumn("topic_id", topicID).Error; err != nil {
			return err
		}
		return nil
	})

	item.TopicID = &topicID

	if !alreadyMember {
		// A story just gained a member — a news event the feed must reflect.
		// Invalidate the read-through cache (next read assembles live) and
		// recompute this story's related-story order at write time (debounced;
		// cross-encoder reranked when NewsRerankEnabled).
		markNewsSnapshotDirty(db, item.TenantID)
		go refreshStoryRelated(db, item.TenantID, topicID)
		// Source-grounded story digest (debounced/gated/best-effort; Slice 8).
		go refreshStorySummary(db, item.TenantID, topicID)
	}
}

// runningMean folds embedding e into a centroid of n existing members.
// For the first member (or a dimension mismatch) the centroid IS the embedding.
func runningMean(centroid []float32, n int, e []float32) []float32 {
	if n <= 0 || len(centroid) != len(e) {
		out := make([]float32, len(e))
		copy(out, e)
		return out
	}
	out := make([]float32, len(e))
	fn := float32(n)
	for i := range e {
		out[i] = (centroid[i]*fn + e[i]) / (fn + 1)
	}
	return out
}

// placeholderStoryLabel builds an LLM-free fallback label for a brand-new
// story: the item's title (or body snippet), suffixed with a short id so the
// (tenant, label) unique index can't accidentally merge two distinct events
// that share a generic headline ("عاجل:"). label-batch renames these later.
func placeholderStoryLabel(item *models.ContentItem) string {
	base := ""
	if item.Title != nil {
		base = strings.TrimSpace(*item.Title)
	}
	if base == "" && item.BodyText != nil {
		runes := []rune(strings.TrimSpace(*item.BodyText))
		if len(runes) > 80 {
			runes = runes[:80]
		}
		base = string(runes)
	}
	if base == "" {
		base = "Story"
	} else if runes := []rune(base); len(runes) > 120 {
		base = string(runes[:120])
	}
	return base + " · " + item.PublicID.String()[:8]
}

// classificationBackfillRunning guards against concurrent backfill passes —
// two passes classifying the same unclassified rows would race topic creation.
var classificationBackfillRunning atomic.Bool

// StartClassificationBackfill classifies every embedded-but-unclassified READY
// NEWS item in the background, then rebuilds the precompute News snapshot so
// the feed reflects the healed taxonomy. Called on CMS startup and from the
// admin snapshot-refresh endpoint; no-ops if a pass is already running.
//
// This is the self-healing half of the classification contract: the embedding
// write-back fires classification per item, and this sweep catches everything
// that slipped through (LLM outages before the placeholder fallback existed,
// crashed goroutines, bulk re-embeds, taxonomy wipes).
func StartClassificationBackfill(db *gorm.DB) {
	if !classificationBackfillRunning.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer classificationBackfillRunning.Store(false)

		const batchSize = 50
		total := 0
		prevRemaining := int64(-1)
		for {
			var remaining int64
			db.Model(&models.ContentItem{}).
				Where("type = ? AND status = ? AND embedding IS NOT NULL AND topic_id IS NULL",
					models.ContentTypeNews, models.ContentStatusReady).
				Count(&remaining)
			if remaining == 0 {
				break
			}
			// Stall guard: if a full pass made no progress (persistent DB
			// errors), stop rather than spin forever; the next startup or
			// snapshot refresh retries.
			if remaining == prevRemaining {
				log.Printf("[classification-backfill] stalled with %d unclassified items — giving up this pass", remaining)
				break
			}
			prevRemaining = remaining

			var ids []uuid.UUID
			// Chronological by PUBLISH time — the story activity window assumes
			// stories form in event order (an item only joins stories active
			// near its own publish time), so backfill must replay history.
			db.Model(&models.ContentItem{}).
				Where("type = ? AND status = ? AND embedding IS NOT NULL AND topic_id IS NULL",
					models.ContentTypeNews, models.ContentStatusReady).
				Order("COALESCE(published_at, created_at) ASC").
				Limit(batchSize).
				Pluck("public_id", &ids)
			for _, id := range ids {
				classifyContentTopic(db, id)
			}
			total += len(ids)
			time.Sleep(500 * time.Millisecond)
		}

		if total > 0 {
			log.Printf("[classification-backfill] classified %d items", total)
			// Refresh the precompute snapshot so the healed stories are served.
			config := loadTenantConfig(db, "default")
			if config.NewsFeedMode != "on_demand" {
				if n, err := buildNewsSnapshot(db, "default"); err != nil {
					log.Printf("[classification-backfill] snapshot rebuild failed: %v", err)
				} else {
					log.Printf("[classification-backfill] snapshot rebuilt (%d slides)", n)
				}
			}
		}
	}()
}

// topicSeedTexts builds the snippet list used to name a brand-new topic.
func topicSeedTexts(item *models.ContentItem) []string {
	parts := make([]string, 0, 2)
	if item.Title != nil && strings.TrimSpace(*item.Title) != "" {
		parts = append(parts, *item.Title)
	}
	if item.Excerpt != nil && strings.TrimSpace(*item.Excerpt) != "" {
		parts = append(parts, *item.Excerpt)
	} else if item.BodyText != nil && strings.TrimSpace(*item.BodyText) != "" {
		body := *item.BodyText
		runes := []rune(body)
		if len(runes) > 500 {
			body = string(runes[:500])
		}
		parts = append(parts, body)
	}
	return parts
}

// relatedBackfillRunning guards the one-time related_ids sweep per boot.
var relatedBackfillRunning atomic.Bool

// StartRelatedBackfill precomputes topics.related_ids for stories that predate
// the write-time related feature (or whose refresh never landed). Without it,
// every feed read falls back to a per-slide centroid fetch + kNN — the
// centroid alone is ~12-17KB of wire text per slide against a WAN DB.
// Sequential and bounded (refreshStoryRelated self-limits via
// storyRelatedWorkers); a no-op when nothing is missing.
func StartRelatedBackfill(db *gorm.DB) {
	if !relatedBackfillRunning.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer relatedBackfillRunning.Store(false)
		type row struct {
			PublicID uuid.UUID
			TenantID string
		}
		var rows []row
		db.Model(&models.Topic{}).
			Select("public_id, tenant_id").
			Where("related_ids IS NULL AND embedding IS NOT NULL AND article_count > 0").
			Scan(&rows)
		if len(rows) == 0 {
			return
		}
		log.Printf("[related-backfill] computing related stories for %d topics", len(rows))
		for _, r := range rows {
			refreshStoryRelated(db, r.TenantID, r.PublicID)
		}
		log.Printf("[related-backfill] done (%d topics)", len(rows))
	}()
}
