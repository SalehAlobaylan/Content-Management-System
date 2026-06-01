package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"errors"
	"strings"

	"github.com/google/uuid"
	"github.com/pgvector/pgvector-go"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// topicMatchThreshold is the minimum cosine similarity for an article to join
// an existing topic. Below it, a new topic is created (LLM-labeled). Code
// default per Config Discipline — surface in admin tuning later if needed.
const topicMatchThreshold = 0.60

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

	// Nearest topic in this tenant by cosine distance.
	type nearestRow struct {
		PublicID uuid.UUID
		Distance float64
	}
	var nearest nearestRow
	_ = db.Model(&models.Topic{}).
		Select("public_id, (embedding <=> '"+lit+"') AS distance").
		Where("tenant_id = ? AND embedding IS NOT NULL", item.TenantID).
		Order("embedding <=> '" + lit + "'").
		Limit(1).
		Scan(&nearest).Error

	if nearest.PublicID != uuid.Nil && (1.0-nearest.Distance) >= topicMatchThreshold {
		assignTopicToItem(db, &item, nearest.PublicID, emb)
		return
	}

	// No close topic — ask the LLM for a meaningful label and create one.
	label, err := generateTopicLabelViaEnrichment(topicSeedTexts(&item))
	label = strings.TrimSpace(label)
	if err != nil || label == "" {
		return
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
			Labeled:      true, // LLM-named at creation
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
			if err := tx.Model(&models.Topic{}).
				Where("public_id = ?", topicID).
				Updates(map[string]interface{}{
					"embedding":     &vec,
					"article_count": topic.ArticleCount + 1,
				}).Error; err != nil {
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
