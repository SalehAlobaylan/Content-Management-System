package controllers

import (
	"fmt"
	"strings"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/spaceid"

	"github.com/google/uuid"
	"github.com/pgvector/pgvector-go"
	"gorm.io/gorm"
)

// Embedding & Model Lifecycle System (stage 10) — owner rebuild handshake
// (Slice 4). Lifecycle NEVER computes or writes a vector payload directly for
// another owner's surface; it invokes typed, bounded owner adapters and verifies
// by database readback. Ordering doctrine (§6): current-space item rows first,
// then ACTIVE story/topic centroids (only once their members are target-space),
// then caches, then completion.
//
// A campaign only leaves `verifying` for `completed` when every in-scope surface
// is coherent (stale=unstamped=0 for items, all centroids rebuilt in one space).

// advanceCampaignVerification drives one campaign through the centroid/cache
// handshake and, when all invariants hold, completes it. Called from the
// campaign tick for campaigns in `verifying`.
func advanceCampaignVerification(db *gorm.DB, c *models.EmbeddingCampaign) {
	if c.State != models.EmbeddingCampaignVerifying {
		return
	}
	es := refreshExpectedSpace(c.Space)
	if es.SpaceID == "" || es.SpaceID != c.TargetSpaceID {
		c.State = models.EmbeddingCampaignBlocked
		c.BlockedReason = "expected space unavailable during verification"
		db.Save(c)
		return
	}

	// Items must be fully migrated before centroids are touched (ordering §6).
	if remainingItemTargets(db, c, es) > 0 {
		c.State = models.EmbeddingCampaignRunning // fell behind — resume item lane
		db.Save(c)
		return
	}

	batchID := "verify-" + time.Now().UTC().Format("20060102T150405")
	for _, s := range EmbeddingSurfaces() {
		if s.Space != c.Space {
			continue
		}
		switch s.Kind {
		case SurfaceKindCentroid:
			rebuildCentroidSurface(db, c, s, es, batchID)
		case SurfaceKindCache:
			refreshCacheSurface(db, c, s, es, batchID)
		}
	}
	if c.Space == EmbeddingSpaceText {
		// Replay work deliberately held by the comparability firewall. Completion
		// waits for both the News backlog and Preferences dirty-remap handshake.
		StartClassificationBackfill(db)
		var heldNews, dirtyTopics int64
		db.Model(&models.ContentItem{}).
			Where("type = ? AND status = ? AND embedding_space_id = ? AND story_id IS NULL",
				models.ContentTypeNews, models.ContentStatusReady, es.SpaceID).
			Count(&heldNews)
		db.Model(&models.Topic{}).Where("active = ? AND needs_remap = ?", true, true).Count(&dirtyTopics)
		if heldNews > 0 || dirtyTopics > 0 {
			return
		}
	}

	// Completion invariants: no stale/unstamped anywhere in scope, no mixed-space.
	if campaignFullyCoherent(db, c, es) {
		now := time.Now()
		// A prior waiver narrows the truthful terminal state (§6).
		var openWaivers int64
		db.Model(&models.EmbeddingCampaignException{}).
			Where("campaign_id = ? AND status = ?", c.ID, models.EmbeddingExceptionWaived).Count(&openWaivers)
		if openWaivers > 0 {
			c.State = models.EmbeddingCampaignCompletedWithWaiver
		} else {
			c.State = models.EmbeddingCampaignCompleted
		}
		c.CompletedAt = &now
		db.Save(c)
	}
}

// rebuildCentroidSurface rebuilds every stale/unstamped centroid whose members
// are all target-space, via the owning surface's typed adapter, then verifies.
func rebuildCentroidSurface(db *gorm.DB, c *models.EmbeddingCampaign, s EmbeddingSurface, es expectedSpace, batchID string) {
	expectedProducer := campaignExpectedProducer(c, s)
	var ids []string
	db.Raw(fmt.Sprintf(
		`SELECT %[1]s::text FROM %[2]s WHERE %[3]s IS NULL OR %[3]s IS DISTINCT FROM ? LIMIT ?`,
		s.IDCol, s.Table, s.ProducerIDCol), expectedProducer, c.ItemsPerBatch).Scan(&ids)

	for _, id := range ids {
		start := time.Now()
		act, claimed := claimOwnerAction(db, c, s, id, batchID, toolForCentroid(s.Key), expectedProducer)
		if !claimed {
			continue
		}
		var skip string
		var err error
		switch s.Key {
		case "story_centroid":
			skip, err = rebuildStoryCentroid(db, id, es)
		case "topic_centroid":
			skip, err = refreshTopicCentroid(db, id, es)
		default:
			continue
		}
		act.LatencyMS = time.Since(start).Milliseconds()
		switch {
		case err != nil:
			act.Status, act.Reason = models.EmbeddingActionFailed, err.Error()
			recordAdapterFailure(db, c, s, id, err.Error())
		case skip != "":
			act.Status, act.Guardrail = models.EmbeddingActionSkipped, skip
			recordAdapterFailure(db, c, s, id, skip)
			db.Model(c).UpdateColumn("skipped_count", gorm.Expr("skipped_count + 1"))
		default:
			observed := readbackProducer(db, s, id)
			act.ObservedProducerID = observed
			if observed == expectedProducer {
				act.Status = models.EmbeddingActionCompleted
				resolveCampaignException(db, c.ID, s.Key, id)
				db.Model(c).UpdateColumn("completed_count", gorm.Expr("completed_count + 1"))
			} else {
				act.Status, act.Reason = models.EmbeddingActionFailed, "readback mismatch"
				recordAdapterFailure(db, c, s, id, "readback mismatch")
			}
		}
		db.Save(act)
	}
}

func toolForCentroid(key string) string {
	if key == "topic_centroid" {
		return models.EmbeddingToolTopicCentroid
	}
	return models.EmbeddingToolStoryCentroid
}

// rebuildStoryCentroid recomputes a story's centroid as the mean of its member
// text vectors — but ONLY when EVERY member with an embedding is already
// target-space (§6). Otherwise returns a `members_not_ready` skip so the mean is
// never poisoned by a mixed-space average. Atomically stamps identity on write.
func rebuildStoryCentroid(db *gorm.DB, storyPublicID string, es expectedSpace) (skip string, err error) {
	var counts struct {
		Total   int64
		Current int64
	}
	if e := db.Raw(
		`SELECT COUNT(*) FILTER (WHERE embedding IS NOT NULL) AS total,
		        COUNT(*) FILTER (WHERE embedding IS NOT NULL AND embedding_space_id = ?) AS current
		 FROM content_items WHERE story_id = ?`, es.SpaceID, storyPublicID).Scan(&counts).Error; e != nil {
		return "", e
	}
	if counts.Total == 0 {
		return "no_members_with_vector", nil
	}
	if counts.Current < counts.Total {
		return "members_not_ready", nil
	}

	// Fetch current-space member vectors (bounded) and average in Go — avoids a
	// hard dependency on the pgvector avg() aggregate being registered.
	var rows []struct {
		Embedding pgvector.Vector
	}
	if e := db.Raw(
		`SELECT embedding FROM content_items
		 WHERE story_id = ? AND embedding IS NOT NULL AND embedding_space_id = ?`,
		storyPublicID, es.SpaceID).Scan(&rows).Error; e != nil {
		return "", e
	}
	if len(rows) == 0 {
		return "members_not_ready", nil
	}
	mean := meanOfVectorRows(rows)
	vec := pgvector.NewVector(mean)
	producer := es.ProducerFor(spaceid.RecipeStoryCentroid)
	if e := db.Model(&models.Story{}).Where("public_id = ?", storyPublicID).
		Updates(map[string]interface{}{
			"embedding":             vec,
			"embedding_model":       es.Model,
			"embedding_space_id":    es.SpaceID,
			"embedding_producer_id": producer,
		}).Error; e != nil {
		return "", e
	}
	if storyID, parseErr := uuid.Parse(storyPublicID); parseErr == nil {
		var tenantID string
		db.Model(&models.Story{}).Where("public_id = ?", storyID).Pluck("tenant_id", &tenantID)
		if tenantID != "" {
			go refreshStoryRelated(db, tenantID, storyID)
		}
	}
	return "", nil
}

func meanOfVectorRows(rows []struct{ Embedding pgvector.Vector }) []float32 {
	if len(rows) == 0 {
		return nil
	}
	dim := len(rows[0].Embedding.Slice())
	acc := make([]float64, dim)
	for _, r := range rows {
		for i, v := range r.Embedding.Slice() {
			if i < dim {
				acc[i] += float64(v)
			}
		}
	}
	out := make([]float32, dim)
	n := float64(len(rows))
	for i := range acc {
		out[i] = float32(acc[i] / n)
	}
	return out
}

// refreshTopicCentroid re-embeds the topic's approved bilingual label seed (a
// single embedding, not a running mean), stamps both identities, and marks the
// topic needs_remap so the Preferences dirty sweep re-maps edges afterward (§6).
func refreshTopicCentroid(db *gorm.DB, topicPublicID string, es expectedSpace) (skip string, err error) {
	var topic models.Topic
	if e := db.Where("public_id = ?", topicPublicID).First(&topic).Error; e != nil {
		return "", e
	}
	emb, observedSpace, e := embedQueryViaEnrichmentWithSpace(topic.LabelEN + " " + topic.LabelAR)
	if e != nil || len(emb) != 1024 {
		return "", fmt.Errorf("label embed failed: %v", e)
	}
	if observedSpace != es.SpaceID {
		return "", fmt.Errorf("label embed space changed during campaign")
	}
	vec := pgvector.NewVector(emb)
	producer := es.ProducerFor(spaceid.RecipeTopicCentroid)
	if e := db.Model(&models.Topic{}).Where("public_id = ?", topicPublicID).
		Updates(map[string]interface{}{
			"centroid":             vec,
			"centroid_model":       es.Model,
			"centroid_space_id":    es.SpaceID,
			"centroid_producer_id": producer,
			"needs_remap":          true,
		}).Error; e != nil {
		return "", e
	}
	return "", nil
}

// refreshCacheSurface force-refreshes proposal/discovery cache vectors.
func refreshCacheSurface(db *gorm.DB, c *models.EmbeddingCampaign, s EmbeddingSurface, es expectedSpace, batchID string) {
	expectedProducer := campaignExpectedProducer(c, s)
	var ids []string
	db.Raw(fmt.Sprintf(
		`SELECT %[1]s::text FROM %[2]s WHERE %[3]s IS NOT NULL AND (%[4]s IS DISTINCT FROM ?) LIMIT ?`,
		s.IDCol, s.Table, s.VecCol, s.ProducerIDCol), expectedProducer, c.ItemsPerBatch).Scan(&ids)

	for _, id := range ids {
		start := time.Now()
		act, claimed := claimOwnerAction(db, c, s, id, batchID, toolForCache(s.Key), expectedProducer)
		if !claimed {
			continue
		}
		var err error
		switch s.Key {
		case "topic_proposal":
			err = refreshTopicProposalVector(db, id, es)
		case "discovery_profile":
			err = refreshDiscoveryProfileVector(db, id, es)
		default:
			continue
		}
		act.LatencyMS = time.Since(start).Milliseconds()
		if err != nil {
			act.Status, act.Reason = models.EmbeddingActionFailed, err.Error()
			recordAdapterFailure(db, c, s, id, err.Error())
		} else {
			observed := readbackProducer(db, s, id)
			act.ObservedProducerID = observed
			if observed == expectedProducer {
				act.Status = models.EmbeddingActionCompleted
				resolveCampaignException(db, c.ID, s.Key, id)
				db.Model(c).UpdateColumn("completed_count", gorm.Expr("completed_count + 1"))
			} else {
				act.Status, act.Reason = models.EmbeddingActionFailed, "readback mismatch"
				recordAdapterFailure(db, c, s, id, "readback mismatch")
			}
		}
		db.Save(act)
	}
}

func resolveCampaignException(db *gorm.DB, campaignID uint, surfaceKey, targetID string) {
	db.Model(&models.EmbeddingCampaignException{}).
		Where("campaign_id = ? AND surface_key = ? AND target_id = ?", campaignID, surfaceKey, targetID).
		Updates(map[string]interface{}{"status": models.EmbeddingExceptionResolved})
}

func claimOwnerAction(db *gorm.DB, c *models.EmbeddingCampaign, s EmbeddingSurface, targetID, batchID, tool, expectedProducer string) (*models.EmbeddingCampaignAction, bool) {
	retryNumber := 0
	var ex models.EmbeddingCampaignException
	if err := db.Where("campaign_id = ? AND surface_key = ? AND target_id = ?", c.ID, s.Key, targetID).First(&ex).Error; err == nil {
		if ex.Status == models.EmbeddingExceptionWaived {
			if ex.WaiverExpires != nil && ex.WaiverExpires.After(time.Now()) {
				return nil, false
			}
			ex.Status = models.EmbeddingExceptionOpen
			_ = db.Save(&ex).Error
		}
		retryNumber = ex.Attempts
		if ex.Attempts >= c.RetryCeiling && ex.Status != models.EmbeddingExceptionRetrying {
			c.State = models.EmbeddingCampaignBlocked
			c.BlockedReason = fmt.Sprintf("retry ceiling reached for %s/%s", s.Key, targetID)
			_ = db.Save(c).Error
			return nil, false
		}
	}
	action := &models.EmbeddingCampaignAction{
		CampaignID: c.ID, TenantID: c.TenantID, BatchID: batchID,
		SurfaceKey: s.Key, Tool: tool, TargetID: targetID,
		ExpectedProducerID: expectedProducer, RetryNumber: retryNumber,
		Status: models.EmbeddingActionAttempted,
	}
	if err := db.Create(action).Error; err != nil {
		return nil, false
	}
	return action, true
}

func toolForCache(key string) string {
	if key == "topic_proposal" {
		return models.EmbeddingToolTopicProposal
	}
	return models.EmbeddingToolDiscovery
}

func refreshTopicProposalVector(db *gorm.DB, proposalID string, es expectedSpace) error {
	var p models.TopicProposal
	if e := db.Where("id = ?", proposalID).First(&p).Error; e != nil {
		return e
	}
	text := p.SuggestedLabelEN + " " + p.SuggestedLabelAR + " " + p.SuggestedSlug
	emb, observedSpace, e := embedQueryViaEnrichmentWithSpace(text)
	if e != nil || len(emb) != 1024 {
		return fmt.Errorf("proposal embed failed: %v", e)
	}
	if observedSpace != es.SpaceID {
		return fmt.Errorf("proposal embed space changed during campaign")
	}
	vec := pgvector.NewVector(emb)
	producer := es.ProducerFor(spaceid.RecipeTopicProposal)
	return db.Model(&models.TopicProposal{}).Where("id = ?", proposalID).
		Updates(map[string]interface{}{
			"embedding":             vec,
			"embedding_model":       es.Model,
			"embedding_space_id":    es.SpaceID,
			"embedding_producer_id": producer,
		}).Error
}

func refreshDiscoveryProfileVector(db *gorm.DB, profilePublicID string, es expectedSpace) error {
	var profile models.DiscoveryProfile
	if e := db.Where("public_id = ?", profilePublicID).First(&profile).Error; e != nil {
		return e
	}
	text := strings.TrimSpace(profile.Name + ". " + profile.Description + ". " + strings.Join([]string(profile.Keywords), ", "))
	if text == "" {
		return fmt.Errorf("profile has no embedding input")
	}
	emb, observedSpace, err := embedQueryViaEnrichmentWithSpace(text)
	if err != nil || len(emb) != 1024 {
		return fmt.Errorf("profile embed failed: %v", err)
	}
	if observedSpace != es.SpaceID {
		return fmt.Errorf("profile embed space changed during campaign")
	}
	vec := pgvector.NewVector(emb)
	return db.Model(&models.DiscoveryProfile{}).Where("public_id = ?", profilePublicID).
		Updates(map[string]interface{}{
			"embedding": vec, "embedding_model": es.Model,
			"embedding_space_id":    es.SpaceID,
			"embedding_producer_id": es.ProducerFor(spaceid.RecipeDiscoveryProfile),
		}).Error
}

// campaignFullyCoherent reports whether every in-scope surface has zero stale +
// zero unstamped rows (the completion invariant, §6/§16).
func campaignFullyCoherent(db *gorm.DB, c *models.EmbeddingCampaign, es expectedSpace) bool {
	for _, s := range EmbeddingSurfaces() {
		if s.Space != c.Space {
			continue
		}
		expectedProducer := campaignExpectedProducer(c, s)
		var bad int64
		db.Raw(fmt.Sprintf(
			`SELECT COUNT(*) FROM %[1]s x
			 WHERE %[2]s IS NOT NULL AND (%[3]s IS NULL OR %[3]s IS DISTINCT FROM ?)
			   AND NOT EXISTS (
			     SELECT 1 FROM embedding_campaign_exceptions e
			      WHERE e.campaign_id = ? AND e.surface_key = ?
			        AND e.target_id = x.%[4]s::text AND e.status = 'waived'
			        AND e.waiver_expires > now()
			   )`,
			s.Table, s.VecCol, s.ProducerIDCol, s.IDCol), expectedProducer, c.ID, s.Key).Scan(&bad)
		if bad > 0 {
			return false
		}
		if s.Kind == SurfaceKindCentroid {
			mixed, err := countMixedSpaceCentroids(db, s)
			if err != nil || mixed > 0 {
				return false
			}
		}
	}
	return true
}

// sweepEmbeddingLifecycleRetention deletes old audit findings + completed runs
// beyond the family retention windows. Campaigns/actions/exceptions are kept.
func sweepEmbeddingLifecycleRetention(db *gorm.DB) {
	findingCutoff := time.Now().Add(-90 * 24 * time.Hour)
	db.Where("created_at < ?", findingCutoff).Delete(&models.EmbeddingLifecycleFinding{})
	runCutoff := time.Now().Add(-365 * 24 * time.Hour)
	db.Where("started_at < ? AND status IN ?", runCutoff,
		[]string{models.EmbeddingRunCompleted, models.EmbeddingRunPartial}).
		Delete(&models.EmbeddingLifecycleRun{})
}
