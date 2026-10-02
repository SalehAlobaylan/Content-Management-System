package feedstate

import (
	"errors"
	"fmt"
	"time"

	"content-management-system/src/contentreset"
	"content-management-system/src/lifecycle"
	"content-management-system/src/models"
	"github.com/google/uuid"
	"github.com/pgvector/pgvector-go"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func BeginNewsFreshStartCandidate(tx *gorm.DB, tenant string, campaignID uint) (models.FeedGeneration, error) {
	if tx == nil || tenant == "" || campaignID == 0 || !tx.Migrator().HasTable(&models.NewsFeedStoryProjection{}) {
		return models.FeedGeneration{}, errors.New("isolated News projection schema is unavailable")
	}
	var campaign models.ContentResetCampaign
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND id=?", tenant, campaignID).First(&campaign).Error; err != nil {
		return models.FeedGeneration{}, err
	}
	if campaign.State != "executing" || campaign.Operation != "fresh_start" || (campaign.Lane != "news" && campaign.Lane != "both") {
		return models.FeedGeneration{}, errors.New("News candidate requires an executing News Fresh Start campaign")
	}
	var revision models.ContentResetRevision
	if err := tx.Where("tenant_id=? AND campaign_id=? AND revision=?", tenant, campaignID, campaign.CurrentRevision).First(&revision).Error; err != nil {
		return models.FeedGeneration{}, err
	}
	if revision.State != "previewed" || revision.ManifestHash == nil {
		return models.FeedGeneration{}, errors.New("News candidate requires a frozen revision")
	}
	var claims int64
	if err := tx.Model(&models.LifecycleOperationClaim{}).Where("tenant_id=? AND campaign_id=? AND owner='cms/content-reset' AND resource_type=? AND resource_key='news' AND state=?", tenant, campaignID, lifecycle.ResourceLane, lifecycle.ClaimActive).Count(&claims).Error; err != nil {
		return models.FeedGeneration{}, err
	}
	if claims != 1 {
		return models.FeedGeneration{}, errors.New("News candidate requires its active lane claim")
	}
	var head models.FeedGenerationHead
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND lane='news'", tenant).First(&head).Error; err != nil {
		return models.FeedGeneration{}, err
	}
	if head.ActiveGenerationID == nil {
		return models.FeedGeneration{}, errors.New("News has no active generation")
	}
	if head.CandidateGenerationID != nil {
		var existing models.FeedGeneration
		if err := tx.Where("tenant_id=? AND public_id=? AND lane='news'", tenant, *head.CandidateGenerationID).First(&existing).Error; err != nil {
			return existing, err
		}
		if existing.State == "candidate" && existing.Purpose == "content_reset" && existing.ContentResetCampaignID != nil && *existing.ContentResetCampaignID == campaignID && existing.PreviousGenerationID != nil && *existing.PreviousGenerationID == *head.ActiveGenerationID {
			return existing, nil
		}
		return existing, errors.New("another candidate owns the News head")
	}
	generation := models.FeedGeneration{PublicID: uuid.New(), TenantID: tenant, Lane: newsLane, State: "candidate", Purpose: "content_reset", ContentResetCampaignID: &campaignID, PreviousGenerationID: head.ActiveGenerationID, BuildWatermark: time.Now().UTC(), Verification: datatypes.JSON(`{"complete":false}`)}
	if err := tx.Create(&generation).Error; err != nil {
		return generation, err
	}
	if err := tx.Exec(`INSERT INTO feed_generation_memberships(generation_id,member_type,member_id,attached_at)
      SELECT ?, 'news_item', m.member_id, NOW() FROM feed_generation_memberships m
      JOIN content_items c ON c.public_id=m.member_id AND c.tenant_id=? AND c.type='NEWS' AND c.status='READY'
      WHERE m.generation_id=? AND m.member_type='news_item' AND NOT EXISTS (
        SELECT 1 FROM content_reset_targets t WHERE t.tenant_id=? AND t.revision_id=? AND t.content_item_id=m.member_id AND t.disposition='selected' AND NOT t.protected
      ) ON CONFLICT DO NOTHING`, generation.PublicID, tenant, *head.ActiveGenerationID, tenant, revision.ID).Error; err != nil {
		return generation, err
	}
	if err := tx.Exec(`INSERT INTO feed_generation_memberships(generation_id,member_type,member_id,attached_at)
      SELECT DISTINCT ?, 'story', c.story_id, NOW() FROM content_items c
      JOIN feed_generation_memberships m ON m.member_id=c.public_id AND m.generation_id=? AND m.member_type='news_item'
      WHERE c.tenant_id=? AND c.story_id IS NOT NULL ON CONFLICT DO NOTHING`, generation.PublicID, generation.PublicID, tenant).Error; err != nil {
		return generation, err
	}
	if err := RebuildNewsGenerationProjections(tx, tenant, generation.PublicID); err != nil {
		return generation, err
	}
	result := tx.Model(&head).Where("generation=? AND candidate_generation_id IS NULL", head.Generation).Update("candidate_generation_id", generation.PublicID)
	if result.Error != nil {
		return generation, result.Error
	}
	if result.RowsAffected != 1 {
		return generation, errors.New("News candidate head fence changed")
	}
	return generation, nil
}

// ClassifyStagedNews owns an isolated item's story assignment. The campaign
// lock serializes publication with these admitted writebacks. No active Story
// row or projection is updated, even when the selected story UUID is a survivor.
func ClassifyStagedNews(tx *gorm.DB, item models.ContentItem, threshold float64) error {
	instance, err := CampaignInstanceForItem(tx, item.TenantID, item.PublicID)
	if err != nil {
		return err
	}
	if instance == nil || instance.CampaignID == nil || instance.State != "staged" {
		return errors.New("isolated classification requires a staged campaign instance")
	}
	var campaign models.ContentResetCampaign
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND id=?", item.TenantID, *instance.CampaignID).First(&campaign).Error; err != nil {
		return err
	}
	if campaign.State != "executing" && campaign.State != "partial" {
		return errors.New("staged campaign is no longer building")
	}
	var run models.ContentResetExecution
	if err := tx.Where("tenant_id=? AND campaign_id=? AND published_at IS NULL AND started_at IS NOT NULL", item.TenantID, campaign.ID).First(&run).Error; err != nil {
		return err
	}
	if err := contentreset.ValidateExecutionEnvironment(tx, run); err != nil {
		return err
	}
	var head models.FeedGenerationHead
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND lane='news'", item.TenantID).First(&head).Error; err != nil {
		return err
	}
	if head.CandidateGenerationID == nil {
		return errors.New("isolated News candidate has not been created")
	}
	var generation models.FeedGeneration
	if err := tx.Where("tenant_id=? AND public_id=? AND purpose='content_reset' AND state='candidate' AND content_reset_campaign_id=?", item.TenantID, *head.CandidateGenerationID, campaign.ID).First(&generation).Error; err != nil {
		return err
	}
	var current models.ContentItem
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=?", item.TenantID, item.PublicID).First(&current).Error; err != nil {
		return err
	}
	if current.Type != models.ContentTypeNews || current.Embedding == nil || current.EmbeddingSpaceID == nil || *current.EmbeddingSpaceID == "" {
		return errors.New("isolated News item lacks its embedding-space proof")
	}
	if len(current.Embedding.Slice()) != 1024 {
		return errors.New("isolated News embedding dimension is invalid")
	}
	if current.Status != models.ContentStatusReady {
		return errors.New("isolated News classification waits for READY")
	}
	storyID := uuid.Nil
	if current.StoryID != nil {
		storyID = *current.StoryID
		var existingMembership int64
		if err := tx.Model(&models.FeedGenerationMembership{}).Where("generation_id=? AND member_type='story' AND member_id=?", generation.PublicID, storyID).Count(&existingMembership).Error; err != nil {
			return err
		}
		if existingMembership != 1 {
			return errors.New("staged News story assignment has no candidate membership")
		}
	}
	if storyID == uuid.Nil {
		stamp := current.CreatedAt
		if current.PublishedAt != nil {
			stamp = *current.PublishedAt
		}
		var nearest struct {
			PublicID uuid.UUID
			Distance float64
		}
		if err := tx.Raw(`SELECT story_id AS public_id, ((projection->>'embedding')::vector <=> ?::vector) AS distance
          FROM news_feed_story_projections WHERE tenant_id=? AND generation_id=? AND projection->>'embedding_space_id'=?
          AND (projection->>'last_member_at')::timestamptz BETWEEN ? AND ?
          ORDER BY distance, story_id LIMIT 1`, current.Embedding.String(), item.TenantID, generation.PublicID, *current.EmbeddingSpaceID, stamp.AddDate(0, 0, -7), stamp.AddDate(0, 0, 7)).Scan(&nearest).Error; err != nil {
			return err
		}
		if threshold <= 0 || threshold > 1 {
			return errors.New("isolated News matching threshold is invalid")
		}
		if nearest.PublicID != uuid.Nil && 1-nearest.Distance >= threshold {
			storyID = nearest.PublicID
		} else {
			storyID = uuid.New()
			vec := pgvector.NewVector(current.Embedding.Slice())
			story := models.Story{PublicID: storyID, TenantID: item.TenantID, Label: "reset/" + generation.PublicID.String() + "/" + storyID.String(), Embedding: &vec, EmbeddingModel: current.EmbeddingModel, EmbeddingSpaceID: current.EmbeddingSpaceID, ArticleCount: 0, Labeled: false, LastMemberAt: &stamp}
			if err := tx.Create(&story).Error; err != nil {
				return err
			}
		}
		if err := tx.Model(&current).UpdateColumn("story_id", storyID).Error; err != nil {
			return err
		}
	}
	for _, membership := range []models.FeedGenerationMembership{
		{GenerationID: generation.PublicID, MemberType: "story", MemberID: storyID, AttachedAt: time.Now().UTC()},
		{GenerationID: generation.PublicID, MemberType: "news_item", MemberID: current.PublicID, AttachedAt: time.Now().UTC()},
	} {
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&membership).Error; err != nil {
			return err
		}
	}
	return RebuildNewsStoryProjection(tx, item.TenantID, generation.PublicID, storyID)
}

type NewsFreshStartCandidateProof struct {
	GenerationID         uuid.UUID `json:"generation_id"`
	TotalItems           int64     `json:"total_items"`
	TotalStories         int64     `json:"total_stories"`
	SelectedOldItems     int64     `json:"selected_old_items"`
	MissingProjections   int64     `json:"missing_projections"`
	StaleProjections     int64     `json:"stale_projections"`
	UndeliveredInstances int64     `json:"undelivered_instances"`
}

func VerifyNewsFreshStartCandidate(db *gorm.DB, tenant string, campaignID uint, generationID uuid.UUID) (NewsFreshStartCandidateProof, error) {
	proof := NewsFreshStartCandidateProof{GenerationID: generationID}
	var campaign models.ContentResetCampaign
	if err := db.Where("tenant_id=? AND id=?", tenant, campaignID).First(&campaign).Error; err != nil {
		return proof, err
	}
	var generation models.FeedGeneration
	if err := db.Where("tenant_id=? AND public_id=? AND lane='news' AND purpose='content_reset' AND state='candidate' AND content_reset_campaign_id=?", tenant, generationID, campaignID).First(&generation).Error; err != nil {
		return proof, err
	}
	var revision models.ContentResetRevision
	if err := db.Where("tenant_id=? AND campaign_id=? AND revision=?", tenant, campaignID, campaign.CurrentRevision).First(&revision).Error; err != nil {
		return proof, err
	}
	for _, count := range []struct {
		kind string
		out  *int64
	}{{"news_item", &proof.TotalItems}, {"story", &proof.TotalStories}} {
		if err := db.Model(&models.FeedGenerationMembership{}).Where("generation_id=? AND member_type=?", generationID, count.kind).Count(count.out).Error; err != nil {
			return proof, err
		}
	}
	if err := db.Raw(`SELECT COUNT(*) FROM feed_generation_memberships m JOIN content_reset_targets t ON t.content_item_id=m.member_id
      WHERE m.generation_id=? AND m.member_type='news_item' AND t.tenant_id=? AND t.revision_id=? AND t.disposition='selected' AND NOT t.protected`, generationID, tenant, revision.ID).Scan(&proof.SelectedOldItems).Error; err != nil {
		return proof, err
	}
	if err := db.Raw(`SELECT COUNT(*) FROM feed_generation_memberships m WHERE m.generation_id=? AND m.member_type='story' AND NOT EXISTS (
      SELECT 1 FROM news_feed_story_projections p WHERE p.tenant_id=? AND p.generation_id=m.generation_id AND p.story_id=m.member_id
    )`, generationID, tenant).Scan(&proof.MissingProjections).Error; err != nil {
		return proof, err
	}
	if err := db.Raw(`WITH current_members AS (
      SELECT c.story_id, COUNT(*) AS member_count,
        encode(sha256(convert_to(jsonb_agg(jsonb_build_array(c.public_id,c.processing_generation,c.embedding_space_id,encode(sha256(convert_to(c.embedding::text,'UTF8')),'hex'),c.title,c.published_at,c.status) ORDER BY c.public_id)::text,'UTF8')),'hex') AS member_hash
      FROM content_items c JOIN feed_generation_memberships m ON m.member_id=c.public_id AND m.generation_id=? AND m.member_type='news_item'
      WHERE c.tenant_id=? AND c.type='NEWS' AND c.status='READY' GROUP BY c.story_id
    ) SELECT COUNT(*) FROM news_feed_story_projections p LEFT JOIN current_members c ON c.story_id=p.story_id
      WHERE p.tenant_id=? AND p.generation_id=? AND (c.story_id IS NULL OR p.member_hash<>c.member_hash
        OR (p.projection->>'article_count')::bigint<>c.member_count)`, generationID, tenant, tenant, generationID).Scan(&proof.StaleProjections).Error; err != nil {
		return proof, err
	}
	if err := db.Raw(`SELECT COUNT(*) FROM source_item_instances i LEFT JOIN content_items c ON c.tenant_id=i.tenant_id AND c.public_id=i.content_item_id
      WHERE i.tenant_id=? AND i.campaign_id=? AND i.state='staged' AND (c.public_id IS NULL OR (c.type='NEWS' AND NOT EXISTS (
        SELECT 1 FROM feed_generation_memberships m WHERE m.generation_id=? AND m.member_type='news_item' AND m.member_id=c.public_id
      ) AND NOT (c.status='ARCHIVED' AND EXISTS (SELECT 1 FROM source_upstream_observation_events e WHERE e.tenant_id=i.tenant_id AND e.observation_id=i.source_observation_id AND e.event_type='filtered' AND e.payload->>'filter_class'='moderation_rejected'))))`, tenant, campaignID, generationID).Scan(&proof.UndeliveredInstances).Error; err != nil {
		return proof, err
	}
	if proof.TotalItems == 0 || proof.TotalStories == 0 || proof.SelectedOldItems != 0 || proof.MissingProjections != 0 || proof.StaleProjections != 0 || proof.UndeliveredInstances != 0 {
		return proof, fmt.Errorf("News candidate is not ready: %+v", proof)
	}
	var invalid int64
	if err := db.Raw(`SELECT COUNT(*) FROM feed_generation_memberships m LEFT JOIN content_items c ON c.tenant_id=? AND c.public_id=m.member_id
      WHERE m.generation_id=? AND m.member_type='news_item' AND (c.public_id IS NULL OR c.type<>'NEWS' OR c.status<>'READY' OR c.story_id IS NULL
        OR NOT EXISTS (SELECT 1 FROM feed_generation_memberships s WHERE s.generation_id=m.generation_id AND s.member_type='story' AND s.member_id=c.story_id))`, tenant, generationID).Scan(&invalid).Error; err != nil {
		return proof, err
	}
	if invalid != 0 {
		return proof, errors.New("News candidate contains invalid delivery membership")
	}
	return proof, nil
}
