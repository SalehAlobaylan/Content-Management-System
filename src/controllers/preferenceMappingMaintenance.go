package controllers

import (
	"content-management-system/src/models"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/pgvector/pgvector-go"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Preferences Autopilot — cap-aware, checkpointed mapping maintenance (plan §0.1,
// §8). These are the ONLY mapping primitives Safe Auto is allowed to call. They
// never run the destructive full remap (remapCatalogTopics(full=true)): each
// subject is upsert-and-pruned individually, so a mid-run failure can only leave
// the ONE in-flight subject briefly changed, never erase all boosts.

// upsertItemTopics writes the item's full top-K mapping set, then prunes any stale
// topic rows for that same subject — never leaving it with zero rows mid-op. An
// empty match set removes the item's rows entirely (dirty-topic re-eval case).
func upsertItemTopics(db *gorm.DB, itemID uuid.UUID, matches []models.ContentItemTopic) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if len(matches) == 0 {
			return tx.Where("content_item_id = ?", itemID).Delete(&models.ContentItemTopic{}).Error
		}
		rows := make([]models.ContentItemTopic, 0, len(matches))
		keep := make([]uuid.UUID, 0, len(matches))
		for _, m := range matches {
			rows = append(rows, models.ContentItemTopic{ContentItemID: itemID, TopicID: m.TopicID, Score: m.Score})
			keep = append(keep, m.TopicID)
		}
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "content_item_id"}, {Name: "topic_id"}},
			DoUpdates: clause.AssignmentColumns([]string{"score"}),
		}).Create(&rows).Error; err != nil {
			return err
		}
		return tx.Where("content_item_id = ? AND topic_id NOT IN ?", itemID, keep).Delete(&models.ContentItemTopic{}).Error
	})
}

// upsertStoryTopics is the story analogue of upsertItemTopics.
func upsertStoryTopics(db *gorm.DB, storyID uuid.UUID, matches []models.ContentItemTopic) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if len(matches) == 0 {
			return tx.Where("story_id = ?", storyID).Delete(&models.StoryTopic{}).Error
		}
		rows := make([]models.StoryTopic, 0, len(matches))
		keep := make([]uuid.UUID, 0, len(matches))
		for _, m := range matches {
			rows = append(rows, models.StoryTopic{StoryID: storyID, TopicID: m.TopicID, Score: m.Score})
			keep = append(keep, m.TopicID)
		}
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "story_id"}, {Name: "topic_id"}},
			DoUpdates: clause.AssignmentColumns([]string{"score"}),
		}).Create(&rows).Error; err != nil {
			return err
		}
		return tx.Where("story_id = ? AND topic_id NOT IN ?", storyID, keep).Delete(&models.StoryTopic{}).Error
	})
}

type mapSweepResult struct {
	ItemsExamined   int
	ItemsMapped     int
	StoriesExamined int
	StoriesMapped   int
	NextItemCursor  uint
	NextStoryCursor uint
	EmptyCatalog    bool
}

// itemTopicMapping and storyTopicMapping keep one subject's complete desired
// top-K set together so a bounded page can be replaced in a handful of database
// round trips. The caller wraps replacement in a transaction, so observers never
// see the deliberate delete-before-insert window.
type itemTopicMapping struct {
	ItemID  uuid.UUID
	Matches []models.ContentItemTopic
}

type storyTopicMapping struct {
	StoryID uuid.UUID
	Matches []models.ContentItemTopic
}

func replaceItemTopicMappings(db *gorm.DB, mappings []itemTopicMapping) error {
	if len(mappings) == 0 {
		return nil
	}
	ids := make([]uuid.UUID, 0, len(mappings))
	rows := make([]models.ContentItemTopic, 0, len(mappings)*topicMappingTopK)
	for _, mapping := range mappings {
		ids = append(ids, mapping.ItemID)
		for _, match := range mapping.Matches {
			rows = append(rows, models.ContentItemTopic{
				ContentItemID: mapping.ItemID,
				TopicID:       match.TopicID,
				Score:         match.Score,
			})
		}
	}
	if err := db.Where("content_item_id IN ?", ids).Delete(&models.ContentItemTopic{}).Error; err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	return db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "content_item_id"}, {Name: "topic_id"}},
		DoUpdates: clause.AssignmentColumns([]string{"score"}),
	}).Create(&rows).Error
}

func replaceStoryTopicMappings(db *gorm.DB, mappings []storyTopicMapping) error {
	if len(mappings) == 0 {
		return nil
	}
	ids := make([]uuid.UUID, 0, len(mappings))
	rows := make([]models.StoryTopic, 0, len(mappings)*topicMappingTopK)
	for _, mapping := range mappings {
		ids = append(ids, mapping.StoryID)
		for _, match := range mapping.Matches {
			rows = append(rows, models.StoryTopic{
				StoryID: mapping.StoryID,
				TopicID: match.TopicID,
				Score:   match.Score,
			})
		}
	}
	if err := db.Where("story_id IN ?", ids).Delete(&models.StoryTopic{}).Error; err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	return db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "story_id"}, {Name: "topic_id"}},
		DoUpdates: clause.AssignmentColumns([]string{"score"}),
	}).Create(&rows).Error
}

// limitedMapSweep is the routine cursor-paged sweep (§8.1). It examines up to
// itemCap unmapped items with id > itemCursor and storyCap unmapped stories with
// id > storyCursor, evaluates each against ALL active topic centroids, and
// upsert-and-prunes its full top-K. Its caps bound WORK per run (unlike
// remapCatalogTopics, whose batch size only bounds memory). When a side reaches
// the tail (fewer than its cap returned) its cursor wraps to 0 so holes left by
// deletes/merges get re-swept on a later run.
func limitedMapSweep(db *gorm.DB, tenantID string, itemCap, storyCap int, itemCursor, storyCursor uint) (mapSweepResult, error) {
	res := mapSweepResult{NextItemCursor: itemCursor, NextStoryCursor: storyCursor}
	topics, err := activeTopicVectors(db, tenantID)
	if err != nil {
		return res, err
	}
	if len(topics) == 0 {
		res.EmptyCatalog = true
		return res, nil
	}
	err = db.Transaction(func(tx *gorm.DB) error {
		if itemCap > 0 {
			var items []models.ContentItem
			if err := tx.Select("id, public_id, embedding").
				Where("tenant_id = ? AND status = ? AND embedding IS NOT NULL AND id > ?", tenantID, models.ContentStatusReady, itemCursor).
				Where("NOT EXISTS (SELECT 1 FROM content_item_topics cit WHERE cit.content_item_id = content_items.public_id)").
				Order("id ASC").Limit(itemCap).Find(&items).Error; err != nil {
				return err
			}
			mappings := make([]itemTopicMapping, 0, len(items))
			for _, item := range items {
				res.ItemsExamined++
				res.NextItemCursor = item.ID
				if item.Embedding == nil {
					continue
				}
				matches := topTopicMatches(item.Embedding.Slice(), topics)
				mappings = append(mappings, itemTopicMapping{ItemID: item.PublicID, Matches: matches})
				if len(matches) > 0 {
					res.ItemsMapped++
				}
			}
			if err := replaceItemTopicMappings(tx, mappings); err != nil {
				return err
			}
			if len(items) < itemCap { // reached the tail — wrap so holes get re-swept
				res.NextItemCursor = 0
			}
		}

		if storyCap > 0 {
			var stories []models.Story
			if err := tx.Select("id, public_id, embedding").
				Where("tenant_id = ? AND embedding IS NOT NULL AND id > ?", tenantID, storyCursor).
				Where("NOT EXISTS (SELECT 1 FROM story_topics st WHERE st.story_id = stories.public_id)").
				Order("id ASC").Limit(storyCap).Find(&stories).Error; err != nil {
				return err
			}
			mappings := make([]storyTopicMapping, 0, len(stories))
			for _, story := range stories {
				res.StoriesExamined++
				res.NextStoryCursor = story.ID
				if story.Embedding == nil {
					continue
				}
				matches := topTopicMatches(story.Embedding.Slice(), topics)
				mappings = append(mappings, storyTopicMapping{StoryID: story.PublicID, Matches: matches})
				if len(matches) > 0 {
					res.StoriesMapped++
				}
			}
			if err := replaceStoryTopicMappings(tx, mappings); err != nil {
				return err
			}
			if len(stories) < storyCap {
				res.NextStoryCursor = 0
			}
		}
		return nil
	})
	return res, err
}

type dirtySweepResult struct {
	TopicsProcessed int
	ItemsExamined   int
	StoriesExamined int
	ClearedSlugs    []string
	NextItemCursor  uint
	NextStoryCursor uint
	CycleComplete   bool
}

// sweepDirtyTopics consumes topics.needs_remap in bounded order (§8.2). A topic
// goes dirty only on a human label/category/activation/approval change or a
// centroid recovery — rare — so at most maxDirty topics are handled per run.
//
// Inactive / NULL-centroid dirty topics have their mapping rows purged. For active
// dirty topics we re-evaluate the corpus against ALL active centroids (a single
// paged pass, memory-bounded at remapCatalogBatchSize) and upsert-and-prune each
// subject's FULL top-K — this both applies the dirty topic's new state AND keeps
// previously-unmapped rows' other edges correct, avoiding remapSingleTopic's
// single-edge trap. Flags clear only after the pass succeeds for that topic.
func sweepDirtyTopics(db *gorm.DB, tenantID string, maxDirty, itemCap, storyCap int, itemCursor, storyCursor uint) (dirtySweepResult, error) {
	res := dirtySweepResult{NextItemCursor: itemCursor, NextStoryCursor: storyCursor}
	if maxDirty <= 0 {
		return res, nil
	}
	var dirty []models.Topic
	if err := db.Where("tenant_id = ? AND needs_remap = ?", tenantID, true).
		Order("updated_at ASC").Limit(maxDirty).Find(&dirty).Error; err != nil || len(dirty) == 0 {
		return res, err
	}
	topics, err := activeTopicVectors(db, tenantID)
	if err != nil {
		return res, err
	}
	err = db.Transaction(func(tx *gorm.DB) error {
		activeDirty := make([]models.Topic, 0, len(dirty))
		for _, t := range dirty {
			if t.Active && t.Centroid != nil {
				activeDirty = append(activeDirty, t)
				continue
			}
			if err := tx.Where("topic_id = ?", t.PublicID).Delete(&models.ContentItemTopic{}).Error; err != nil {
				return err
			}
			if err := tx.Where("topic_id = ?", t.PublicID).Delete(&models.StoryTopic{}).Error; err != nil {
				return err
			}
			if err := tx.Model(&models.Topic{}).Where("tenant_id = ? AND public_id = ?", tenantID, t.PublicID).UpdateColumn("needs_remap", false).Error; err != nil {
				return err
			}
			res.TopicsProcessed++
			res.ClearedSlugs = append(res.ClearedSlugs, t.Slug)
		}
		if len(activeDirty) == 0 {
			res.CycleComplete = true
			return nil
		}
		itemsDone, storiesDone, err := reevaluateCorpusPage(tx, tenantID, topics, itemCap, storyCap, itemCursor, storyCursor, &res)
		if err != nil {
			return err
		}
		res.CycleComplete = itemsDone && storiesDone
		if res.CycleComplete {
			for _, t := range activeDirty {
				if err := tx.Model(&models.Topic{}).Where("tenant_id = ? AND public_id = ?", tenantID, t.PublicID).UpdateColumn("needs_remap", false).Error; err != nil {
					return err
				}
				res.TopicsProcessed++
				res.ClearedSlugs = append(res.ClearedSlugs, t.Slug)
			}
			res.NextItemCursor, res.NextStoryCursor = 0, 0
		}
		return nil
	})
	if err != nil {
		return res, err
	}
	if res.TopicsProcessed > 0 {
		if err := refreshTopicMemberCounts(db, tenantID); err != nil {
			return res, err
		}
	}
	return res, nil
}

func reevaluateCorpusPage(db *gorm.DB, tenantID string, topics []topicVector, itemCap, storyCap int, itemCursor, storyCursor uint, res *dirtySweepResult) (bool, bool, error) {
	itemsDone, storiesDone := itemCap <= 0, storyCap <= 0
	if itemCap > 0 {
		var items []models.ContentItem
		if err := db.Select("id, public_id, embedding").
			Where("tenant_id = ? AND status = ? AND embedding IS NOT NULL AND id > ?", tenantID, models.ContentStatusReady, itemCursor).
			Order("id ASC").Limit(itemCap).Find(&items).Error; err != nil {
			return false, false, err
		}
		mappings := make([]itemTopicMapping, 0, len(items))
		for _, item := range items {
			res.NextItemCursor = item.ID
			res.ItemsExamined++
			if item.Embedding != nil {
				mappings = append(mappings, itemTopicMapping{ItemID: item.PublicID, Matches: topTopicMatches(item.Embedding.Slice(), topics)})
			}
		}
		if err := replaceItemTopicMappings(db, mappings); err != nil {
			return false, false, err
		}
		itemsDone = len(items) < itemCap
	}
	if storyCap > 0 {
		var stories []models.Story
		if err := db.Select("id, public_id, embedding").
			Where("tenant_id = ? AND embedding IS NOT NULL AND id > ?", tenantID, storyCursor).
			Order("id ASC").Limit(storyCap).Find(&stories).Error; err != nil {
			return false, false, err
		}
		mappings := make([]storyTopicMapping, 0, len(stories))
		for _, story := range stories {
			res.NextStoryCursor = story.ID
			res.StoriesExamined++
			if story.Embedding != nil {
				mappings = append(mappings, storyTopicMapping{StoryID: story.PublicID, Matches: topTopicMatches(story.Embedding.Slice(), topics)})
			}
		}
		if err := replaceStoryTopicMappings(db, mappings); err != nil {
			return false, false, err
		}
		storiesDone = len(stories) < storyCap
	}
	return itemsDone, storiesDone, nil
}

// refreshNullCentroidsBounded recovers up to `cap` active topics whose centroid is
// NULL (they silently map nothing — activeTopicVectors skips them) by embedding
// their approved labels (§8.3). A recovered centroid marks the topic dirty so the
// next dirty sweep maps it against the corpus. V1 does NOT do running-mean
// centroid drift: a topic keeps its label seed and surfaces as an attention item
// instead of drifting silently. Uses UpdateColumn for the derived member/centroid
// write path is elsewhere; here we DO set needs_remap deliberately.
func refreshNullCentroidsBounded(db *gorm.DB, tenantID string, limitN int) (int, error) {
	if limitN <= 0 {
		return 0, nil
	}
	var topics []models.Topic
	if err := db.Where("tenant_id = ? AND active = ? AND centroid IS NULL", tenantID, true).
		Order("updated_at ASC").Limit(limitN).Find(&topics).Error; err != nil {
		return 0, err
	}
	recovered, failed := 0, 0
	for _, topic := range topics {
		emb, err := embedQueryViaEnrichment(topic.LabelEN + " " + topic.LabelAR)
		if err != nil || len(emb) != 1024 {
			failed++
			continue
		}
		vec := pgvector.NewVector(emb)
		if err := db.Model(&models.Topic{}).Where("tenant_id = ? AND public_id = ?", tenantID, topic.PublicID).
			Updates(map[string]interface{}{"centroid": vec, "needs_remap": true}).Error; err == nil {
			recovered++
		} else {
			failed++
		}
	}
	if failed > 0 {
		return recovered, fmt.Errorf("failed to recover %d of %d null centroids", failed, len(topics))
	}
	return recovered, nil
}

// mineTopicProposals keeps its original signature for the baseline heartbeat and
// the admin Mine button; it delegates to the capped variant with the historical
// 100-candidate ceiling.
func mineTopicProposals(db *gorm.DB, tenantID string) (int, error) {
	return mineTopicProposalsCapped(db, tenantID, 100)
}

// mineTopicProposalsCapped mines new topic proposals, capping how many NEW rows a
// single run creates (§6, §16-file-3) and persisting bounded demand + sample
// evidence (sample_content_ids, served_member_count, impression_count) alongside
// member count / titles so the autopilot scorer can compute review priority.
func mineTopicProposalsCapped(db *gorm.DB, tenantID string, limitN int) (int, error) {
	if limitN <= 0 {
		limitN = 25
	}
	ensureDefaultTopicCategories(db, tenantID)
	type row struct {
		Slug            string
		MemberCount     int64
		ImpressionCount int64
		ServedMembers   int64
		Samples         datatypes.JSON
		SampleIDs       datatypes.JSON
	}
	var rows []row
	if err := db.Raw(`
		SELECT lower(trim(tag)) AS slug,
		       COUNT(*) AS member_count,
		       COALESCE(SUM(impression_count), 0) AS impression_count,
		       COUNT(*) FILTER (WHERE last_served_at IS NOT NULL) AS served_members,
		       COALESCE(jsonb_agg(title) FILTER (WHERE title IS NOT NULL), '[]'::jsonb) AS samples,
		       COALESCE(jsonb_agg(public_id), '[]'::jsonb) AS sample_ids
		FROM content_items, unnest(topic_tags) AS tag
		WHERE tenant_id = ? AND status = ? AND trim(tag) <> ''
		GROUP BY 1 HAVING COUNT(*) >= ?
		ORDER BY COUNT(*) DESC LIMIT 100
	`, tenantID, models.ContentStatusReady, topicMineMinMembers).Scan(&rows).Error; err != nil {
		return 0, err
	}
	normalized := make([]string, 0, len(rows))
	candidateSlugs := make([]string, 0, len(rows))
	seen := map[string]bool{}
	for _, r := range rows {
		slug := normalizedTopicSlug(r.Slug)
		if len([]rune(slug)) < 2 {
			normalized = append(normalized, "")
			continue
		}
		normalized = append(normalized, slug)
		if !seen[slug] {
			seen[slug] = true
			candidateSlugs = append(candidateSlugs, slug)
		}
	}
	taken := map[string]bool{}
	if len(candidateSlugs) > 0 {
		var existingTopics []string
		db.Model(&models.Topic{}).Where("tenant_id = ? AND slug IN ?", tenantID, candidateSlugs).Pluck("slug", &existingTopics)
		for _, s := range existingTopics {
			taken[s] = true
		}
		var existingProposals []string
		db.Model(&models.TopicProposal{}).Where("tenant_id = ? AND suggested_slug IN ? AND status IN ?", tenantID, candidateSlugs, []string{"pending", "rejected"}).Pluck("suggested_slug", &existingProposals)
		for _, s := range existingProposals {
			taken[s] = true
		}
	}

	created := 0
	for i, r := range rows {
		if created >= limitN {
			break
		}
		slug := normalized[i]
		if slug == "" || taken[slug] {
			continue
		}
		taken[slug] = true
		// Trim sample titles to a bounded set to keep the evidence JSON small.
		samples := boundedSampleTitles(r.Samples, 5)
		sampleIDs := boundedSampleIDs(r.SampleIDs, 5)
		evidence, _ := json.Marshal(map[string]interface{}{
			"member_count":        r.MemberCount,
			"impression_count":    r.ImpressionCount,
			"served_member_count": r.ServedMembers,
			"sample_titles":       samples,
			"sample_content_ids":  sampleIDs,
		})
		p := models.TopicProposal{
			TenantID: tenantID, SuggestedSlug: slug,
			SuggestedLabelEN:  strings.ReplaceAll(slug, "-", " "),
			SuggestedLabelAR:  strings.ReplaceAll(slug, "-", " "),
			SuggestedCategory: "general",
			Evidence:          datatypes.JSON(evidence),
		}
		if err := db.Create(&p).Error; err == nil {
			created++
		}
	}
	return created, nil
}

func boundedSampleTitles(raw datatypes.JSON, n int) []string {
	var titles []string
	_ = json.Unmarshal(raw, &titles)
	out := make([]string, 0, n)
	for _, t := range titles {
		t = strings.TrimSpace(t)
		if t == "" {
			continue
		}
		out = append(out, t)
		if len(out) >= n {
			break
		}
	}
	return out
}

func boundedSampleIDs(raw datatypes.JSON, n int) []string {
	var ids []string
	_ = json.Unmarshal(raw, &ids)
	if len(ids) > n {
		ids = ids[:n]
	}
	return ids
}

// enqueueAffinityRecompute adds a tenant/user to the durable recompute queue,
// deduplicated on (tenant, user). Used by catalog merges and failed synchronous
// recomputes (§10) so the bounded runner can drain them authoritatively later.
func enqueueAffinityRecompute(db *gorm.DB, tenantID string, userID uuid.UUID, reason string) error {
	row := models.PreferenceAffinityRecomputeQueue{TenantID: tenantID, UserID: userID, Reason: reason}
	return db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "tenant_id"}, {Name: "user_id"}},
		DoUpdates: clause.Assignments(map[string]interface{}{"reason": reason, "updated_at": time.Now()}),
	}).Create(&row).Error
}
