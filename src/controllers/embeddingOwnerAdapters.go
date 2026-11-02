package controllers

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"content-management-system/src/feedcontract"
	"content-management-system/src/feedstate"
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
	if c.State != models.EmbeddingCampaignVerifying || !campaignCanContinue(db, c) {
		return
	}
	db = feedcontract.WithReadSchema(db)
	if db.Error != nil {
		campaignSetState(db, c, models.EmbeddingCampaignBlocked, "serving schema unavailable: "+db.Error.Error())
		return
	}
	es := refreshExpectedSpace(c.Space)
	if es.SpaceID == "" || es.SpaceID != c.TargetSpaceID {
		campaignSetState(db, c, models.EmbeddingCampaignBlocked, "expected space unavailable during verification")
		return
	}

	// Items must be fully migrated before centroids are touched (ordering §6).
	remaining := remainingItemTargets(db, c, es)
	if remaining < 0 {
		campaignSetState(db, c, models.EmbeddingCampaignBlocked, "item inventory unavailable during verification")
		return
	}
	if remaining > 0 {
		campaignSetState(db, c, models.EmbeddingCampaignRunning, "")
		return
	}

	budget, err := campaignBudgetRemaining(db, c)
	if err != nil {
		campaignSetState(db, c, models.EmbeddingCampaignBlocked, "campaign budget unavailable: "+err.Error())
		return
	}
	batchID := uuid.NewString()
	for _, s := range EmbeddingSurfaces() {
		if s.Space != c.Space {
			continue
		}
		switch s.Kind {
		case SurfaceKindCentroid:
			rebuildCentroidSurface(db, c, s, es, batchID, &budget)
		case SurfaceKindCache:
			refreshCacheSurface(db, c, s, es, batchID, &budget)
		}
		if !campaignCanContinue(db, c) {
			return
		}
		// Do not refresh a dependent surface against a partially migrated catalog.
		if s.Kind != SurfaceKindItem {
			pending, err := remainingOwnerTargets(db, c, s)
			if err != nil {
				campaignSetState(db, c, models.EmbeddingCampaignBlocked, "owner inventory unavailable: "+err.Error())
				return
			}
			if pending > 0 {
				return
			}
		}
	}
	if c.Space == EmbeddingSpaceText {
		if !replayCampaignClassification(db, c, es, batchID, &budget) {
			return
		}
		if !refreshCampaignTopicMappings(db, c, es, batchID, &budget) {
			return
		}
		if !refreshCampaignStoryRelations(db, c, es, batchID, &budget) {
			return
		}
	}

	// Completion invariants: no stale/unstamped anywhere in scope, no mixed-space.
	if !campaignCanContinue(db, c) || c.State != models.EmbeddingCampaignVerifying {
		return
	}
	if campaignFullyCoherent(db, c, es) {
		// A prior waiver narrows the truthful terminal state (§6).
		var openWaivers int64
		if err := db.Model(&models.EmbeddingCampaignException{}).
			Where("campaign_id = ? AND status = ?", c.ID, models.EmbeddingExceptionWaived).Count(&openWaivers).Error; err != nil {
			campaignSetState(db, c, models.EmbeddingCampaignBlocked, "exception inventory unavailable: "+err.Error())
			return
		}
		state := models.EmbeddingCampaignCompleted
		if openWaivers > 0 {
			state = models.EmbeddingCampaignCompletedWithWaiver
		}
		campaignSetState(db, c, state, "")
	}
}

// rebuildCentroidSurface rebuilds every stale/unstamped centroid whose members
// are all target-space, via the owning surface's typed adapter, then verifies.
func rebuildCentroidSurface(db *gorm.DB, c *models.EmbeddingCampaign, s EmbeddingSurface, es expectedSpace, batchID string, budget *int) {
	runOwnerSurface(db, c, s, es, batchID, toolForCentroid(s.Key), budget)
}

// All derived surfaces share the campaign's approved batch and daily budget.
func runOwnerSurface(db *gorm.DB, c *models.EmbeddingCampaign, s EmbeddingSurface, es expectedSpace, batchID, tool string, budget *int) {
	if *budget <= 0 || !campaignCanContinue(db, c) {
		return
	}
	capacityDone, capacityErr := beginWorkloadSweep(db, c.TenantID, "embedding_campaign", "campaign")
	if capacityErr != nil {
		return
	}
	defer capacityDone()
	predicate, args := ownerDebtPredicate(db, c, s)
	args = append(args, *budget)
	var targets []struct {
		ID       string
		TenantID string
	}
	order := "x." + s.IDCol
	if s.Key == "story_centroid" {
		order = "x.last_member_at DESC NULLS FIRST, x." + s.IDCol
	} else if s.Key == "topic_centroid" {
		order = "x.active DESC, x." + s.IDCol
	}
	if err := db.Raw(fmt.Sprintf("SELECT x.%s::text AS id, x.%s AS tenant_id FROM %s x WHERE %s ORDER BY %s LIMIT ?", s.IDCol, s.TenantCol, s.Table, predicate, order), args...).Scan(&targets).Error; err != nil {
		campaignSetState(db, c, models.EmbeddingCampaignBlocked, "owner inventory unavailable: "+err.Error())
		return
	}
	for _, target := range targets {
		id := target.ID
		if *budget <= 0 || !campaignCanContinue(db, c) {
			return
		}
		available, err := campaignBudgetRemaining(db, c)
		if err != nil || available <= 0 {
			return
		}
		start := time.Now()
		act, claimed := claimOwnerAction(db, c, s, id, batchID, tool, campaignExpectedProducer(c, s), target.TenantID)
		if !claimed {
			continue
		}
		*budget--
		callContext := withWorkloadContext(db, spendContext(target.TenantID, "embedding_campaign", fmt.Sprint(c.ID)), "campaign")
		var lease *controllerWorkloadLease
		if s.Key != "story_centroid" {
			lease, err = acquireControllerWorkload(db, target.TenantID, "embedding_campaign", "text_embedding", batchID, "campaign")
			if err != nil {
				act.Status, act.Guardrail, act.Reason = models.EmbeddingActionSkipped, "workload_governor", err.Error()
				if saveErr := db.Save(act).Error; saveErr != nil {
					campaignSetState(db, c, models.EmbeddingCampaignBlocked, "owner action persistence failed: "+saveErr.Error())
					return
				}
				continue
			}
			callContext.workloadLease = lease
		}
		var skip string
		switch s.Key {
		case "story_centroid":
			skip, err = rebuildStoryCentroid(db, id, es)
		case "topic_centroid":
			skip, err = refreshTopicCentroid(db, id, es, callContext)
		case "topic_proposal":
			err = refreshTopicProposalVector(db, id, es, callContext)
		case "discovery_profile":
			err = refreshDiscoveryProfileVector(db, id, es, callContext)
		}
		lease.cancelBeforeEffect()
		act.LatencyMS = time.Since(start).Milliseconds()
		switch {
		case isWorkloadDeferred(err):
			act.Status, act.Guardrail, act.Reason = models.EmbeddingActionSkipped, "workload_governor", err.Error()
		case err != nil:
			act.Status, act.Reason = models.EmbeddingActionFailed, err.Error()
			recordAdapterFailure(db, c, s, id, err.Error())
		case skip != "":
			act.Status, act.Guardrail = models.EmbeddingActionSkipped, skip
			recordAdapterFailure(db, c, s, id, skip)
		default:
			if err := ownerReadback(db, s, id, es.SpaceID, act.ExpectedProducerID); err != nil {
				act.Status, act.Reason = models.EmbeddingActionFailed, err.Error()
				recordAdapterFailure(db, c, s, id, err.Error())
			} else {
				act.Status, act.ObservedProducerID = models.EmbeddingActionCompleted, act.ExpectedProducerID
				resolveCampaignException(db, c.ID, s.Key, id)
				_ = db.Model(c).UpdateColumn("completed_count", gorm.Expr("completed_count + 1")).Error
			}
		}
		if err := db.Save(act).Error; err != nil {
			campaignSetState(db, c, models.EmbeddingCampaignBlocked, "owner action persistence failed: "+err.Error())
			return
		}
	}
}

func ownerDebtPredicate(db *gorm.DB, c *models.EmbeddingCampaign, s EmbeddingSurface) (string, []any) {
	producer := campaignExpectedProducer(c, s)
	debt := fmt.Sprintf("x.%s IS DISTINCT FROM ? OR x.%s IS DISTINCT FROM ?", s.ProducerIDCol, s.SpaceIDCol)
	args := []any{producer, c.TargetSpaceID}
	if s.Key == "story_centroid" && feedcontract.HasNewsStoryProjection(db) {
		debt += ` OR EXISTS (SELECT 1 FROM news_feed_story_projections p
		 JOIN feed_generation_heads h ON h.tenant_id=p.tenant_id AND h.lane='news' AND h.active_generation_id=p.generation_id
		 WHERE p.tenant_id=x.tenant_id AND p.story_id=x.public_id
		 AND (p.projection->>'embedding_space_id' IS DISTINCT FROM ? OR p.projection->>'embedding_producer_id' IS DISTINCT FROM ?))`
		args = append(args, c.TargetSpaceID, producer)
	}
	predicate := fmt.Sprintf(`x.%s IS NOT NULL AND (%s) AND NOT EXISTS (
	 SELECT 1 FROM embedding_campaign_exceptions e WHERE e.campaign_id=? AND e.surface_key=?
	 AND e.target_id=x.%s::text AND e.status='waived' AND e.waiver_expires>now())`, s.VecCol, debt, s.IDCol)
	return predicate, append(args, c.ID, s.Key)
}

func remainingOwnerTargets(db *gorm.DB, c *models.EmbeddingCampaign, s EmbeddingSurface) (int64, error) {
	predicate, args := ownerDebtPredicate(db, c, s)
	var count int64
	err := db.Raw(fmt.Sprintf("SELECT COUNT(*) FROM %s x WHERE %s", s.Table, predicate), args...).Scan(&count).Error
	return count, err
}

func ownerReadback(db *gorm.DB, s EmbeddingSurface, id, spaceID, producerID string) error {
	var valid int64
	err := db.Raw(fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s::text=? AND %s IS NOT NULL AND %s=? AND %s=?", s.Table, s.IDCol, s.VecCol, s.SpaceIDCol, s.ProducerIDCol), id, spaceID, producerID).Scan(&valid).Error
	if err != nil {
		return err
	}
	if valid != 1 {
		return fmt.Errorf("owner identity readback mismatch")
	}
	if s.Key == "story_centroid" && feedcontract.HasNewsStoryProjection(db) {
		var invalid int64
		err := db.Raw(`SELECT COUNT(*) FROM news_feed_story_projections p
		 JOIN feed_generation_heads h ON h.tenant_id=p.tenant_id AND h.lane='news' AND h.active_generation_id=p.generation_id
		 WHERE p.story_id::text=? AND (p.projection->>'embedding_space_id' IS DISTINCT FROM ? OR p.projection->>'embedding_producer_id' IS DISTINCT FROM ?)`, id, spaceID, producerID).Scan(&invalid).Error
		if err != nil {
			return err
		}
		if invalid > 0 {
			return fmt.Errorf("active story projection readback mismatch")
		}
	}
	return nil
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
	storyID, err := uuid.Parse(storyPublicID)
	if err != nil {
		return "", err
	}
	err = db.Transaction(func(tx *gorm.DB) error {
		var story models.Story
		if err := tx.Select("public_id,tenant_id").Where("public_id=?", storyID).First(&story).Error; err != nil {
			return err
		}
		producer := es.ProducerFor(spaceid.RecipeStoryCentroid)
		// Lock the serving head before updating canonical story state, matching the
		// News owner's lock order and preserving generation/member fencing.
		if err := feedstate.RefreshActiveNewsStoryProjection(tx, story.TenantID, storyID, es.SpaceID, producer); err != nil {
			return err
		}
		var members []struct {
			Embedding pgvector.Vector
			SpaceID   string
		}
		if err := tx.Raw(`SELECT embedding, embedding_space_id AS space_id FROM content_items
		 WHERE story_id=? AND embedding IS NOT NULL AND COALESCE(news_retention_state,'full')='full' FOR SHARE`, storyID).Scan(&members).Error; err != nil {
			return err
		}
		if len(members) == 0 {
			skip = "no_members_with_vector"
			return nil
		}
		rows := make([]struct{ Embedding pgvector.Vector }, 0, len(members))
		for _, member := range members {
			if member.SpaceID != es.SpaceID {
				skip = "members_not_ready"
				return fmt.Errorf("story members are not target-space")
			}
			rows = append(rows, struct{ Embedding pgvector.Vector }{member.Embedding})
		}
		result := tx.Model(&models.Story{}).Where("public_id=? AND tenant_id=?", storyID, story.TenantID).
			Updates(map[string]interface{}{
				"embedding":       pgvector.NewVector(meanOfVectorRows(rows)),
				"embedding_model": es.Model, "embedding_space_id": es.SpaceID,
				"embedding_producer_id": producer,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return fmt.Errorf("story disappeared during centroid rebuild")
		}
		return nil
	})
	return skip, err
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
func refreshTopicCentroid(db *gorm.DB, topicPublicID string, es expectedSpace, contexts ...AISpendContext) (skip string, err error) {
	var topic models.Topic
	if e := db.Where("public_id = ?", topicPublicID).First(&topic).Error; e != nil {
		return "", e
	}
	emb, observedSpace, e := embedQueryViaEnrichmentWithSpace(topic.LabelEN+" "+topic.LabelAR, contexts...)
	if e != nil {
		return "", fmt.Errorf("label embed failed: %w", e)
	}
	if len(emb) != 1024 {
		return "", fmt.Errorf("label embed returned an invalid vector")
	}
	if observedSpace != es.SpaceID {
		return "", fmt.Errorf("label embed space changed during campaign")
	}
	vec := pgvector.NewVector(emb)
	producer := es.ProducerFor(spaceid.RecipeTopicCentroid)
	result := db.Model(&models.Topic{}).Where("public_id = ? AND label_en=? AND label_ar=?", topicPublicID, topic.LabelEN, topic.LabelAR).
		Updates(map[string]interface{}{
			"centroid":             vec,
			"centroid_model":       es.Model,
			"centroid_space_id":    es.SpaceID,
			"centroid_producer_id": producer,
			"needs_remap":          true,
		})
	if result.Error != nil {
		return "", result.Error
	}
	if result.RowsAffected != 1 {
		return "", fmt.Errorf("topic input changed during refresh")
	}
	return "", nil
}

// refreshCacheSurface force-refreshes proposal/discovery cache vectors.
func refreshCacheSurface(db *gorm.DB, c *models.EmbeddingCampaign, s EmbeddingSurface, es expectedSpace, batchID string, budget *int) {
	runOwnerSurface(db, c, s, es, batchID, toolForCache(s.Key), budget)
}

func resolveCampaignException(db *gorm.DB, campaignID uint, surfaceKey, targetID string) {
	db.Model(&models.EmbeddingCampaignException{}).
		Where("campaign_id = ? AND surface_key = ? AND target_id = ?", campaignID, surfaceKey, targetID).
		Updates(map[string]interface{}{"status": models.EmbeddingExceptionResolved})
}

func claimOwnerAction(db *gorm.DB, c *models.EmbeddingCampaign, s EmbeddingSurface, targetID, batchID, tool, expectedProducer, tenantID string) (*models.EmbeddingCampaignAction, bool) {
	retryNumber := 0
	var ex models.EmbeddingCampaignException
	err := db.Where("campaign_id = ? AND surface_key = ? AND target_id = ?", c.ID, s.Key, targetID).First(&ex).Error
	if err != nil && err != gorm.ErrRecordNotFound {
		campaignSetState(db, c, models.EmbeddingCampaignBlocked, "exception read failed: "+err.Error())
		return nil, false
	}
	if err == nil {
		if ex.Status == models.EmbeddingExceptionWaived && ex.WaiverExpires != nil && ex.WaiverExpires.After(time.Now()) {
			return nil, false
		}
		if ex.Attempts >= c.RetryCeiling && ex.Status != models.EmbeddingExceptionRetrying {
			campaignSetState(db, c, models.EmbeddingCampaignBlocked, fmt.Sprintf("retry ceiling reached for %s/%s", s.Key, targetID))
			return nil, false
		}
	}
	var previous models.EmbeddingCampaignAction
	err = db.Where("campaign_id=? AND surface_key=? AND target_id=? AND expected_producer_id=?", c.ID, s.Key, targetID, expectedProducer).
		Order("retry_number DESC").First(&previous).Error
	if err == nil {
		if previous.Status == models.EmbeddingActionAttempted {
			return nil, false // expired claims are recovered by the campaign runner
		}
		retryNumber = previous.RetryNumber + 1
	} else if err != gorm.ErrRecordNotFound {
		campaignSetState(db, c, models.EmbeddingCampaignBlocked, "action read failed: "+err.Error())
		return nil, false
	}
	action := &models.EmbeddingCampaignAction{
		CampaignID: c.ID, TenantID: tenantID, BatchID: batchID,
		SurfaceKey: s.Key, Tool: tool, TargetID: targetID,
		ExpectedProducerID: expectedProducer, RetryNumber: retryNumber,
		Status: models.EmbeddingActionAttempted,
	}
	if err := claimCampaignAction(db, c, action); err != nil {
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

func refreshTopicProposalVector(db *gorm.DB, proposalID string, es expectedSpace, contexts ...AISpendContext) error {
	var p models.TopicProposal
	if e := db.Where("id = ?", proposalID).First(&p).Error; e != nil {
		return e
	}
	text := strings.TrimSpace(p.SuggestedLabelEN + " " + p.SuggestedLabelAR + " " + strings.ReplaceAll(p.SuggestedSlug, "-", " "))
	emb, observedSpace, e := embedQueryViaEnrichmentWithSpace(text, contexts...)
	if e != nil {
		return fmt.Errorf("proposal embed failed: %w", e)
	}
	if len(emb) != 1024 {
		return fmt.Errorf("proposal embed returned an invalid vector")
	}
	if observedSpace != es.SpaceID {
		return fmt.Errorf("proposal embed space changed during campaign")
	}
	vec := pgvector.NewVector(emb)
	producer := es.ProducerFor(spaceid.RecipeTopicProposal)
	result := db.Model(&models.TopicProposal{}).Where("id = ? AND suggested_slug=? AND COALESCE(suggested_label_en,'')=? AND COALESCE(suggested_label_ar,'')=? AND COALESCE(suggested_category,'')=?",
		proposalID, p.SuggestedSlug, p.SuggestedLabelEN, p.SuggestedLabelAR, p.SuggestedCategory).
		Updates(map[string]interface{}{
			"embedding":             vec,
			"embedding_input_hash":  hashProposalEmbeddingInput(p.SuggestedSlug, p.SuggestedLabelAR, p.SuggestedLabelEN, p.SuggestedCategory),
			"embedded_at":           time.Now(),
			"embedding_model":       es.Model,
			"embedding_space_id":    es.SpaceID,
			"embedding_producer_id": producer,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return fmt.Errorf("proposal input changed during refresh")
	}
	return nil
}

func refreshDiscoveryProfileVector(db *gorm.DB, profilePublicID string, es expectedSpace, contexts ...AISpendContext) error {
	var profile models.DiscoveryProfile
	if e := db.Where("public_id = ?", profilePublicID).First(&profile).Error; e != nil {
		return e
	}
	text := strings.TrimSpace(profile.Name + ". " + profile.Description + ". " + strings.Join([]string(profile.Keywords), ", "))
	if text == "" {
		return fmt.Errorf("profile has no embedding input")
	}
	emb, observedSpace, err := embedQueryViaEnrichmentWithSpace(text, contexts...)
	if err != nil {
		return fmt.Errorf("profile embed failed: %w", err)
	}
	if len(emb) != 1024 {
		return fmt.Errorf("profile embed returned an invalid vector")
	}
	if observedSpace != es.SpaceID {
		return fmt.Errorf("profile embed space changed during campaign")
	}
	vec := pgvector.NewVector(emb)
	result := db.Model(&models.DiscoveryProfile{}).Where("public_id = ? AND updated_at=?", profilePublicID, profile.UpdatedAt).
		Updates(map[string]interface{}{
			"embedding": vec, "embedding_model": es.Model,
			"embedding_space_id":    es.SpaceID,
			"embedding_producer_id": es.ProducerFor(spaceid.RecipeDiscoveryProfile),
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return fmt.Errorf("profile input changed during refresh")
	}
	return nil
}

// campaignFullyCoherent reports whether every in-scope surface has zero stale +
// zero unstamped rows (the completion invariant, §6/§16).
func campaignFullyCoherent(db *gorm.DB, c *models.EmbeddingCampaign, es expectedSpace) bool {
	for _, s := range EmbeddingSurfaces() {
		if s.Space != c.Space {
			continue
		}
		bad, err := remainingOwnerTargets(db, c, s)
		if err != nil || bad > 0 {
			return false
		}
		if s.Kind == SurfaceKindCentroid {
			mixed, err := countCampaignMixedSpaceCentroids(db, c, s)
			if err != nil || mixed > 0 {
				return false
			}
		}
	}
	var unresolved int64
	if err := db.Model(&models.EmbeddingCampaignException{}).Where("campaign_id=? AND (status IN ? OR (status=? AND (waiver_expires IS NULL OR waiver_expires<=?)))", c.ID,
		[]string{models.EmbeddingExceptionOpen, models.EmbeddingExceptionRetrying}, models.EmbeddingExceptionWaived, time.Now()).Count(&unresolved).Error; err != nil || unresolved > 0 {
		return false
	}
	if err := db.Model(&models.EmbeddingCampaignAction{}).Where("campaign_id=? AND status=?", c.ID, models.EmbeddingActionAttempted).Count(&unresolved).Error; err != nil || unresolved > 0 {
		return false
	}
	return true
}

func campaignActiveWaiver(db *gorm.DB, c *models.EmbeddingCampaign, surface, target string) (bool, error) {
	var count int64
	err := db.Model(&models.EmbeddingCampaignException{}).Where("campaign_id=? AND surface_key=? AND target_id=? AND status=? AND waiver_expires>?", c.ID, surface, target, models.EmbeddingExceptionWaived, time.Now()).Count(&count).Error
	return count > 0, err
}

func campaignHeldTenants(db *gorm.DB, c *models.EmbeddingCampaign, surface string) ([]string, error) {
	var tenants []string
	query := db.Model(&models.Topic{}).Where("active AND needs_remap")
	if surface == "story_classification" {
		query = db.Model(&models.ContentItem{}).Where("type='NEWS' AND status='READY' AND story_id IS NULL AND embedding IS NOT NULL AND embedding_space_id=? AND COALESCE(news_retention_state,'full')='full'", c.TargetSpaceID)
	}
	if err := query.Distinct("tenant_id").Order("tenant_id").Pluck("tenant_id", &tenants).Error; err != nil {
		return nil, err
	}
	// An interrupted page may already have drained its rows. Revisit its durable
	// exception too, so successful readback can resolve it without new dirty work.
	var exceptions []string
	if err := db.Model(&models.EmbeddingCampaignException{}).Where("campaign_id=? AND surface_key=? AND status IN ?", c.ID, surface, []string{models.EmbeddingExceptionOpen, models.EmbeddingExceptionRetrying}).Pluck("target_id", &exceptions).Error; err != nil {
		return nil, err
	}
	seen := make(map[string]bool, len(tenants))
	for _, tenant := range tenants {
		seen[tenant] = true
	}
	for _, tenant := range exceptions {
		if !seen[tenant] {
			tenants = append(tenants, tenant)
			seen[tenant] = true
		}
	}
	sort.Strings(tenants)
	return tenants, nil
}

// Only the mapping owner runs this page, under its existing tenant lock. Its
// normal scheduler may remain paused; an approved campaign invokes the same
// bounded primitive directly and checkpoints in its own durable action ledger.
func refreshCampaignTopicMappings(db *gorm.DB, c *models.EmbeddingCampaign, es expectedSpace, batchID string, budget *int) bool {
	tenants, err := campaignHeldTenants(db, c, "topic_remap")
	if err != nil {
		campaignSetState(db, c, models.EmbeddingCampaignBlocked, "topic backlog unavailable: "+err.Error())
		return false
	}
	allDone := true
	for _, tenant := range tenants {
		waived, err := campaignActiveWaiver(db, c, "topic_remap", tenant)
		if err != nil {
			return false
		}
		if waived {
			continue
		}
		allDone = false
		if *budget <= 0 || !campaignCanContinue(db, c) {
			return false
		}
		act, claimed := claimOwnerAction(db, c, EmbeddingSurface{Key: "topic_remap"}, tenant, batchID, "topic_mapping_remap", es.ProducerFor(spaceid.RecipeTopicCentroid), tenant)
		if !claimed {
			continue
		}
		*budget--
		err = runCampaignTopicRemapPage(db, c, act, es)
		if err != nil {
			act.Status, act.Reason = models.EmbeddingActionFailed, err.Error()
			recordAdapterFailure(db, c, EmbeddingSurface{Key: "topic_remap"}, tenant, err.Error())
		} else {
			act.Status, act.ObservedProducerID = models.EmbeddingActionCompleted, act.ExpectedProducerID
			resolveCampaignException(db, c.ID, "topic_remap", tenant)
		}
		if err := db.Save(act).Error; err != nil {
			campaignSetState(db, c, models.EmbeddingCampaignBlocked, "topic remap action persistence failed: "+err.Error())
			return false
		}
	}
	if allDone {
		return true
	}
	// Re-read actual dirty flags; a completed page is not proof of a drained pass.
	tenants, err = campaignHeldTenants(db, c, "topic_remap")
	if err != nil {
		return false
	}
	for _, tenant := range tenants {
		waived, err := campaignActiveWaiver(db, c, "topic_remap", tenant)
		if err != nil || !waived {
			return false
		}
	}
	return true
}

func runCampaignTopicRemapPage(db *gorm.DB, c *models.EmbeddingCampaign, action *models.EmbeddingCampaignAction, es expectedSpace) error {
	release, acquired := tryAcquirePreferenceAutopilotLock(db, action.TenantID)
	if !acquired {
		return fmt.Errorf("preferences owner is busy")
	}
	defer release()
	var dirty int64
	if err := db.Model(&models.Topic{}).Where("tenant_id=? AND active AND needs_remap", action.TenantID).Count(&dirty).Error; err != nil {
		return err
	}
	if dirty == 0 {
		action.Evidence = jsonEvidence(map[string]any{"CycleComplete": true})
		return nil
	}
	ready, err := topicMappingReady(db, action.TenantID, es.SpaceID)
	if err != nil {
		return err
	}
	if !ready {
		return fmt.Errorf("active topic catalog is not target-space; repair it or explicitly waive held topic remap")
	}
	policy := models.DefaultPreferenceAutopilotPolicy(action.TenantID)
	if err := db.Where("tenant_id=?", action.TenantID).First(&policy).Error; err != nil && err != gorm.ErrRecordNotFound {
		return err
	}
	policy = sanitizePreferenceAutopilotPolicy(policy)
	if policy.MaxDirtyTopics <= 0 || policy.MaxItemCandidates <= 0 || policy.MaxStoryCandidates <= 0 {
		return fmt.Errorf("tenant topic-remap capacity is disabled")
	}
	var previous models.EmbeddingCampaignAction
	var cursor dirtySweepResult
	err = db.Where("campaign_id=? AND surface_key=? AND target_id=? AND status=? AND id<>?", c.ID, "topic_remap", action.TargetID, models.EmbeddingActionCompleted, action.ID).Order("id DESC").First(&previous).Error
	if err == nil && len(previous.Evidence) > 0 {
		if err := json.Unmarshal(previous.Evidence, &cursor); err != nil {
			return err
		}
	} else if err != nil && err != gorm.ErrRecordNotFound {
		return err
	}
	result, err := sweepDirtyTopics(db, action.TenantID, min(policy.MaxDirtyTopics, c.ItemsPerBatch), min(policy.MaxItemCandidates, c.ItemsPerBatch), min(policy.MaxStoryCandidates, c.ItemsPerBatch), cursor.NextItemCursor, cursor.NextStoryCursor)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(result)
	if err != nil {
		return err
	}
	action.Evidence = raw
	return nil
}

// Classification is also an explicit owner replay. Missing active centroids
// produce a tenant-scoped exception rather than an invisible permanent wait.
func replayCampaignClassification(db *gorm.DB, c *models.EmbeddingCampaign, es expectedSpace, batchID string, budget *int) bool {
	tenants, err := campaignHeldTenants(db, c, "story_classification")
	if err != nil {
		campaignSetState(db, c, models.EmbeddingCampaignBlocked, "classification backlog unavailable: "+err.Error())
		return false
	}
	for _, tenant := range tenants {
		waived, err := campaignActiveWaiver(db, c, "story_classification", tenant)
		if err != nil {
			return false
		}
		if waived {
			continue
		}
		if *budget <= 0 || !campaignCanContinue(db, c) {
			return false
		}
		act, claimed := claimOwnerAction(db, c, EmbeddingSurface{Key: "story_classification"}, tenant, batchID, "story_classification_replay", es.ProducerFor(spaceid.RecipeStoryCentroid), tenant)
		if !claimed {
			continue
		}
		*budget--
		err = runCampaignClassificationPage(db, c, act, es)
		if err != nil {
			act.Status, act.Reason = models.EmbeddingActionFailed, err.Error()
			recordAdapterFailure(db, c, EmbeddingSurface{Key: "story_classification"}, tenant, err.Error())
		} else {
			act.Status, act.ObservedProducerID = models.EmbeddingActionCompleted, act.ExpectedProducerID
			resolveCampaignException(db, c.ID, "story_classification", tenant)
		}
		if err := db.Save(act).Error; err != nil {
			campaignSetState(db, c, models.EmbeddingCampaignBlocked, "classification action persistence failed: "+err.Error())
			return false
		}
	}
	tenants, err = campaignHeldTenants(db, c, "story_classification")
	if err != nil {
		return false
	}
	for _, tenant := range tenants {
		waived, err := campaignActiveWaiver(db, c, "story_classification", tenant)
		if err != nil || !waived {
			return false
		}
	}
	return true
}

func runCampaignClassificationPage(db *gorm.DB, c *models.EmbeddingCampaign, action *models.EmbeddingCampaignAction, es expectedSpace) error {
	if !classificationBackfillRunning.CompareAndSwap(false, true) {
		return fmt.Errorf("classification owner is busy")
	}
	defer classificationBackfillRunning.Store(false)
	var ids []uuid.UUID
	if err := db.Model(&models.ContentItem{}).Where("tenant_id=? AND type='NEWS' AND status='READY' AND story_id IS NULL AND embedding IS NOT NULL AND embedding_space_id=? AND COALESCE(news_retention_state,'full')='full'", action.TenantID, es.SpaceID).
		Order("COALESCE(published_at,created_at),public_id").Limit(c.ItemsPerBatch).Pluck("public_id", &ids).Error; err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	var pending int64
	if err := db.Model(&models.Story{}).Where("tenant_id=? AND embedding IS NOT NULL", action.TenantID).
		Where("last_member_at IS NULL OR last_member_at>?", time.Now().AddDate(0, 0, -storyActivityWindowDays)).
		Where("embedding_space_id IS DISTINCT FROM ?", es.SpaceID).Count(&pending).Error; err != nil {
		return err
	}
	if pending > 0 {
		return fmt.Errorf("active story catalog is not target-space; repair it or explicitly waive held classification")
	}
	action.Evidence = jsonEvidence(map[string]any{"target_ids": ids})
	if err := db.Model(action).UpdateColumn("evidence", action.Evidence).Error; err != nil {
		return err
	}
	for _, id := range ids {
		if !campaignCanContinue(db, c) {
			break
		}
		ClassifyContentStage(db, id)
	}
	var remaining int64
	if len(ids) > 0 {
		if err := db.Model(&models.ContentItem{}).Where("public_id IN ? AND story_id IS NULL", ids).Count(&remaining).Error; err != nil {
			return err
		}
		if remaining == int64(len(ids)) {
			return fmt.Errorf("classification owner made no progress")
		}
	}
	return nil
}

// Waivers narrow campaign scope, never the audit's account of remaining debt.
func countCampaignMixedSpaceCentroids(db *gorm.DB, c *models.EmbeddingCampaign, s EmbeddingSurface) (int64, error) {
	var query string
	switch s.Key {
	case "story_centroid":
		query = `SELECT COUNT(DISTINCT x.public_id) FROM stories x JOIN content_items ci ON ci.story_id=x.public_id
		 WHERE x.embedding_space_id IS NOT NULL AND ci.embedding_space_id IS NOT NULL
		 AND ci.embedding_space_id<>x.embedding_space_id`
	case "topic_centroid":
		query = `SELECT COUNT(DISTINCT x.public_id) FROM topics x JOIN content_item_topics cit ON cit.topic_id=x.public_id
		 JOIN content_items ci ON ci.public_id=cit.content_item_id
		 WHERE x.centroid_space_id IS NOT NULL AND ci.embedding_space_id IS NOT NULL
		 AND ci.embedding_space_id<>x.centroid_space_id`
	default:
		return 0, nil
	}
	query += ` AND NOT EXISTS (SELECT 1 FROM embedding_campaign_exceptions e WHERE e.campaign_id=? AND e.surface_key=?
	 AND e.target_id=x.public_id::text AND e.status='waived' AND e.waiver_expires>now())`
	var count int64
	err := db.Raw(query, c.ID, s.Key).Scan(&count).Error
	return count, err
}

func campaignStoryRelationsQuery(db *gorm.DB, c *models.EmbeddingCampaign) *gorm.DB {
	surface, _ := EmbeddingSurfaceByKey("story_centroid")
	producer := campaignExpectedProducer(c, surface)
	query := db.Model(&models.Story{}).Where("embedding IS NOT NULL AND embedding_space_id=?", c.TargetSpaceID).
		Where(`NOT EXISTS (SELECT 1 FROM embedding_campaign_actions a WHERE a.campaign_id=? AND a.surface_key='story_related'
		 AND a.target_id=stories.public_id::text AND a.expected_producer_id=? AND a.status='completed')`, c.ID, producer).
		Where(`NOT EXISTS (SELECT 1 FROM embedding_campaign_exceptions e WHERE e.campaign_id=? AND e.surface_key='story_related'
		 AND e.target_id=stories.public_id::text AND e.status='waived' AND e.waiver_expires>now())`, c.ID)
	if feedcontract.HasNewsStoryProjection(db) {
		query = query.Where(`EXISTS (SELECT 1 FROM feed_generation_heads h JOIN feed_generation_memberships m
		 ON m.generation_id=h.active_generation_id AND m.member_type='story' AND m.member_id=stories.public_id
		 WHERE h.tenant_id=stories.tenant_id AND h.lane='news')`)
	}
	return query
}

// Includes not-yet-attempted targets; in-flight action count is not backlog size.
func countCampaignPendingStoryRelations(db *gorm.DB, c *models.EmbeddingCampaign) (int64, error) {
	if c.Space != EmbeddingSpaceText {
		return 0, nil
	}
	var count int64
	err := campaignStoryRelationsQuery(db, c).Count(&count).Error
	return count, err
}

// Relations are rebuilt after the complete target-space catalog and held News
// classification have settled. The existing action ledger is the bounded cursor.
func refreshCampaignStoryRelations(db *gorm.DB, c *models.EmbeddingCampaign, es expectedSpace, batchID string, budget *int) bool {
	producer := es.ProducerFor(spaceid.RecipeStoryCentroid)

	var stories []models.Story
	if *budget > 0 {
		if err := campaignStoryRelationsQuery(db, c).Select("public_id,tenant_id").Order("id ASC").Limit(*budget).Find(&stories).Error; err != nil {
			campaignSetState(db, c, models.EmbeddingCampaignBlocked, "related-story inventory unavailable: "+err.Error())
			return false
		}
	}
	for _, story := range stories {
		if *budget <= 0 || !campaignCanContinue(db, c) {
			return false
		}
		available, err := campaignBudgetRemaining(db, c)
		if err != nil || available <= 0 {
			return false
		}
		s := EmbeddingSurface{Key: "story_related"}
		act, claimed := claimOwnerAction(db, c, s, story.PublicID.String(), batchID, "story_related_refresh", producer, story.TenantID)
		if !claimed {
			continue
		}
		*budget--
		started := time.Now()
		err = refreshStoryRelatedChecked(db, story.TenantID, story.PublicID)
		if err == nil {
			err = readbackStoryRelations(db, story.TenantID, story.PublicID, es.SpaceID)
		}
		act.LatencyMS = time.Since(started).Milliseconds()
		if err != nil {
			act.Status, act.Reason = models.EmbeddingActionFailed, err.Error()
			recordAdapterFailure(db, c, s, act.TargetID, err.Error())
		} else {
			act.Status, act.ObservedProducerID = models.EmbeddingActionCompleted, producer
			resolveCampaignException(db, c.ID, s.Key, act.TargetID)
		}
		if err := db.Save(act).Error; err != nil {
			campaignSetState(db, c, models.EmbeddingCampaignBlocked, "related-story action persistence failed: "+err.Error())
			return false
		}
	}
	count, err := countCampaignPendingStoryRelations(db, c)
	if err != nil {
		campaignSetState(db, c, models.EmbeddingCampaignBlocked, "related-story inventory unavailable: "+err.Error())
		return false
	}
	return count == 0
}

func readbackStoryRelations(db *gorm.DB, tenantID string, storyID uuid.UUID, targetSpaceID string) error {
	var observed struct {
		RelatedIDs json.RawMessage
		SpaceID    string
	}
	if err := feedstate.ActiveNewsStoryQuery(db, tenantID).Select("related_ids, embedding_space_id AS space_id").Where("public_id=?", storyID).Scan(&observed).Error; err != nil {
		return err
	}
	if observed.SpaceID != targetSpaceID || !json.Valid(observed.RelatedIDs) || !strings.HasPrefix(strings.TrimSpace(string(observed.RelatedIDs)), "[") {
		return fmt.Errorf("related-story readback mismatch")
	}
	return nil
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
