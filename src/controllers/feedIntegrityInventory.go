package controllers

import (
	"content-management-system/src/feedstate"
	"content-management-system/src/models"
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"strings"
	"time"
)

func (c *integrityCollector) laneError(lane, feed string, err error) {
	for _, o := range c.observations {
		if feedIntegrityCheckLane(o.CheckKey) == lane && (feed == "" || o.Feed == feed) {
			c.add(o.CheckKey, lane, o.Feed, o.Variant, o.Axis, o.Severity, "check_error", "checker", lane, 0, map[string]interface{}{"error": safeIntegrityError(err)})
		}
	}
}
func (c *integrityCollector) predicate(key, variant, targetType, target string, bad bool, evidence interface{}) {
	for _, check := range feedIntegrityChecks {
		if check.Key == key {
			status := "ok"
			if bad {
				status = "violation"
			}
			c.add(key, check.Lane, check.Feed, variant, check.Axis, check.Severity, status, targetType, target, 1, evidence)
			return
		}
	}
}
func collectFeedIntegrityInventory(ctx context.Context, db *gorm.DB, tenant string, c *integrityCollector) {
	db = db.WithContext(ctx)
	// Declared-visible debt is a separate question from otherwise-servable
	// metadata debt. Excluded ARCHIVED rows are never playback violations.
	var items []models.ContentItem
	query := db.Select("id, public_id, status, duration_sec, parent_content_item_id, playback_url, media_url, thumbnail_url, duration_bucket, media_renditions").
		Where("tenant_id=? AND type IN ? AND is_feed_unit=TRUE AND feed_visibility=? AND status=?", tenant, []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}, feedVisibilityVisible, models.ContentStatusReady)
	if err := query.FindInBatches(&items, 500, func(tx *gorm.DB, _ int) error {
		for _, item := range items {
			id := item.PublicID.String()
			valid := item.DurationSec != nil && *item.DurationSec >= podsMinDurationSec && *item.DurationSec <= podsHardMaxDurationSec
			playback := (item.PlaybackURL != nil && strings.TrimSpace(*item.PlaybackURL) != "") || (item.MediaURL != nil && strings.TrimSpace(*item.MediaURL) != "")
			c.predicate("inv_pods_bounds", "default", "content_item", id, !valid, map[string]interface{}{"duration_sec": item.DurationSec})
			c.predicate("inv_pods_parent_leak", "default", "content_item", id, item.ParentContentItemID == nil && item.DurationSec != nil && *item.DurationSec > podsHardMaxDurationSec, nil)
			c.predicate("inv_pods_playback_missing", "default", "content_item", id, valid && !playback, nil)
			mismatch := item.DurationSec != nil && item.DurationBucket != nil && *item.DurationBucket != durationBucketLabel(*item.DurationSec*1000)
			c.predicate("inv_pods_bucket_mismatch", "default", "content_item", id, mismatch, map[string]interface{}{"actual": item.DurationBucket})
			var renditions []map[string]interface{}
			c.predicate("inv_pods_renditions_malformed", "default", "content_item", id, len(item.MediaRenditions) > 0 && json.Unmarshal(item.MediaRenditions, &renditions) != nil, nil)
		}
		return ctx.Err()
	}).Error; err != nil {
		c.laneError("inventory", "pods", err)
	} else {
		var archived int64
		q := podsEligibleMediaQuery(db, tenant, supportsAtomizedPodsSchema(db)).Where("status <> ?", models.ContentStatusReady)
		if err := q.Count(&archived).Error; err != nil {
			c.laneError("inventory", "pods", err)
		} else {
			c.predicate("inv_pods_status_contract", "default", "predicate", "READY-only", archived > 0, nil)
		}
		var art []models.ContentItem
		q = podsEligibleMediaQuery(db, tenant, supportsAtomizedPodsSchema(db)).Select("id, public_id, thumbnail_url")
		if err := q.FindInBatches(&art, 500, func(tx *gorm.DB, _ int) error {
			for _, item := range art {
				c.predicate("inv_pods_thumb_missing", "default", "content_item", item.PublicID.String(), item.ThumbnailURL == nil || strings.TrimSpace(*item.ThumbnailURL) == "", nil)
			}
			return ctx.Err()
		}).Error; err != nil {
			c.laneError("inventory", "pods", err)
		}
	}
	var stories []models.Story
	q := feedstate.ActiveNewsStoryQuery(db, tenant).Select("id, public_id, last_member_at, labeled, related_ids")
	if err := q.Find(&stories).Error; err != nil {
		c.laneError("inventory", "news", err)
		return
	}
	var allIDs []uuid.UUID
	if err := db.Model(&models.Story{}).Where("tenant_id=?", tenant).Pluck("public_id", &allIDs).Error; err != nil {
		c.laneError("inventory", "news", err)
		return
	}
	storySet := map[uuid.UUID]bool{}
	for _, id := range allIDs {
		storySet[id] = true
	}
	type countRow struct {
		StoryID uuid.UUID
		N       int64
	}
	var counts []countRow
	memberQuery := db.Model(&models.ContentItem{}).Select("story_id, COUNT(*) AS n").Where("tenant_id=? AND type='NEWS' AND status='READY' AND story_id IS NOT NULL", tenant).Where(newsRetentionFeedPredicate)
	memberQuery = applyActiveGenerationMembership(db, memberQuery, tenant, "news", "news_item", "content_items.public_id")
	if err := memberQuery.Group("story_id").Scan(&counts).Error; err != nil {
		c.laneError("inventory", "news", err)
		return
	}
	members := map[uuid.UUID]int64{}
	for _, r := range counts {
		members[r.StoryID] = r.N
	}
	for _, story := range stories {
		id := story.PublicID.String()
		active := story.LastMemberAt != nil && story.LastMemberAt.After(time.Now().Add(-31*24*time.Hour))
		if active {
			c.predicate("inv_news_empty_story", "today", "story", id, members[story.PublicID] == 0, nil)
			c.predicate("inv_news_unlabeled_stale", "today", "story", id, !story.Labeled && time.Since(*story.LastMemberAt) > time.Hour, nil)
		}
		var refs []string
		bad := len(story.RelatedIDs) > 0 && json.Unmarshal(story.RelatedIDs, &refs) != nil
		for _, ref := range refs {
			rid, err := uuid.Parse(ref)
			if err != nil || !storySet[rid] {
				bad = true
			}
		}
		c.predicate("inv_news_related_dangling", "today", "story", id, bad, nil)
	}
	var head models.FeedGenerationHead
	if err := db.Where("tenant_id=? AND lane='news'", tenant).First(&head).Error; err != nil {
		c.laneError("inventory", "news", err)
		return
	}
	generation := head.ActiveGenerationID
	var missing int64
	if err := db.Raw(`SELECT COUNT(DISTINCT ci.story_id) FROM content_items ci
 WHERE ci.tenant_id=? AND ci.type='NEWS' AND ci.status='READY' AND ci.story_id IS NOT NULL
 AND COALESCE(ci.published_at,ci.created_at)>? AND (COALESCE(ci.news_retention_state,'full')='full' OR ci.news_feed_role IN ('lead','representative'))
 AND NOT EXISTS(SELECT 1 FROM feed_generation_memberships m WHERE m.generation_id=? AND m.member_type='story' AND m.member_id=ci.story_id)`, tenant, time.Now().Add(-31*24*time.Hour), generation).Scan(&missing).Error; err != nil {
		c.laneError("inventory", "news", err)
	} else if generation != nil {
		c.predicate("inv_news_generation_membership_drift", "today", "generation", fmt.Sprint(generation), missing > 0, map[string]interface{}{"missing_story_count": missing})
	}
	var snapshots []models.NewsSnapshot
	if err := db.Select("id, tenant_id, \"window\", built_at, dirty, generation, slide_count").Where("tenant_id=?", tenant).Find(&snapshots).Error; err != nil {
		c.laneError("inventory", "news", err)
		return
	}
	seen := map[string]bool{}
	for _, snap := range snapshots {
		seen[snap.Window] = true
		c.predicate("inv_news_cache_rebuild_debt", snap.Window, "snapshot", snap.Window, snap.Dirty || time.Since(snap.BuiltAt) > newsSnapshotTTL, map[string]interface{}{"dirty": snap.Dirty, "built_at": snap.BuiltAt, "generation": snap.Generation, "age_seconds": time.Since(snap.BuiltAt).Seconds()})
	}
	for _, window := range []string{"today", "week", "month"} {
		if !seen[window] {
			c.add("inv_news_cache_rebuild_debt", "inventory", "news", window, "readiness", "minor", "skipped:not_applicable", "snapshot", window, 0, map[string]interface{}{"reason": "no_snapshot"})
		}
	}
	// Empty predicates have evaluated applicability; they are not failed reads.
	for _, o := range c.observations {
		if feedIntegrityCheckLane(o.CheckKey) == "inventory" && o.Outcome == "skipped:not_evaluated" {
			o.Outcome = "skipped:not_applicable"
			o.ObservedAt = time.Now().UTC()
		}
	}
}

func integrityNewsCorpus(db *gorm.DB, tenant, window string) (int64, error) {
	var policy models.NewsCirculationPolicy
	if err := db.Where("tenant_id=?", tenant).First(&policy).Error; err != nil {
		return 0, err
	}
	circ := circulationContextFromPolicy(policy, window, time.Now())
	q := db.Model(&models.ContentItem{}).Where("tenant_id=? AND type='NEWS' AND status='READY' AND story_id IS NOT NULL", tenant).Where(newsRetentionFeedPredicate).Where("COALESCE(published_at,created_at)>?", circ.Window.QueryStart)
	q = q.Where("NOT EXISTS (SELECT 1 FROM news_story_overrides ov WHERE ov.tenant_id=content_items.tenant_id AND ov.story_id=content_items.story_id AND ov.exclude_from_feed=TRUE AND (ov.expires_at IS NULL OR ov.expires_at>?))", time.Now())
	q = applyActiveGenerationMembership(db, q, tenant, "news", "story", "content_items.story_id")
	q = applyActiveGenerationMembership(db, q, tenant, "news", "news_item", "content_items.public_id")
	var n int64
	err := q.Select("COUNT(DISTINCT story_id)").Scan(&n).Error
	return n, err
}
