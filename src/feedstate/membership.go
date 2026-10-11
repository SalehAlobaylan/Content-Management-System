// Package feedstate owns the transactional serving-membership side of a
// content lifecycle mutation. Controllers may decide an allowed typed change,
// but none may save a media item and later best-effort attach it to a feed
// generation outside the same transaction.
package feedstate

import (
	"errors"
	"fmt"
	"time"

	"content-management-system/src/feedcontract"
	"content-management-system/src/lifecycle"
	"content-management-system/src/models"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const mediaLane = "media"
const newsLane = "news"

// AttachReadyNewsStory closes the two valid ordering races in the News
// pipeline: classification may finish before READY, or READY may finish before
// classification. Both write boundaries call this function, so whichever
// transition happens second attaches the story to the active (and candidate)
// serving generation in the same transaction as that transition.
func AttachReadyNewsStory(tx *gorm.DB, item models.ContentItem) error {
	if tx == nil || item.TenantID == "" || item.PublicID == uuid.Nil {
		return fmt.Errorf("news feed membership requires a tenant-scoped item")
	}
	if item.Type != models.ContentTypeNews || item.Status != models.ContentStatusReady || item.StoryID == nil || *item.StoryID == uuid.Nil {
		return nil
	}
	if err := lifecycle.Check(tx, lifecycle.Scope{
		TenantID: item.TenantID,
		Lane:     newsLane,
		SourceID: lifecycleSourceID(item.ContentSourceID),
		ItemID:   item.PublicID.String(),
	}, lifecycle.PhaseFeedMembership); err != nil {
		return err
	}
	var head models.FeedGenerationHead
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND lane=?", item.TenantID, newsLane).First(&head).Error
	if err == gorm.ErrRecordNotFound {
		return nil
	}
	if err != nil {
		// Preserve the explicit pre-migration compatibility window without adding
		// two information_schema round trips to every READY transition.
		if !tx.Migrator().HasTable(&models.FeedGenerationHead{}) {
			return nil
		}
		return err
	}
	generations, err := loadHeadGenerations(tx, head)
	if err != nil {
		return err
	}
	for _, generation := range generations {
		allowed, err := generationAcceptsNewsItem(tx, generation, item.PublicID)
		if err != nil {
			return err
		}
		if !allowed {
			if err := tx.Where("generation_id=? AND member_type=? AND member_id=?", generation.PublicID, "news_item", item.PublicID).Delete(&models.FeedGenerationMembership{}).Error; err != nil {
				return err
			}
			continue
		}
		memberships := []models.FeedGenerationMembership{{GenerationID: generation.PublicID, MemberType: "story", MemberID: *item.StoryID, AttachedAt: time.Now().UTC()}}
		if item.NewsRetentionState == nil || *item.NewsRetentionState == "" || *item.NewsRetentionState == "full" || (item.NewsFeedRole != nil && (*item.NewsFeedRole == "lead" || *item.NewsFeedRole == "representative")) {
			memberships = append(memberships, models.FeedGenerationMembership{GenerationID: generation.PublicID, MemberType: "news_item", MemberID: item.PublicID, AttachedAt: time.Now().UTC()})
		}
		for _, membership := range memberships {
			if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&membership).Error; err != nil {
				return err
			}
		}

	}
	return nil
}

// ReconcileNewsMembership repairs READY/classified handshakes with the same
// campaign routing constraints used by individual membership writes.
func ReconcileNewsMembership(db *gorm.DB, tenant string) (int64, error) {
	if db == nil || tenant == "" {
		return 0, fmt.Errorf("news membership reconciliation requires a tenant")
	}
	if !db.Migrator().HasTable(&models.FeedGenerationHead{}) || !db.Migrator().HasTable(&models.FeedGenerationMembership{}) {
		return 0, nil
	}
	var attached int64
	err := db.Transaction(func(tx *gorm.DB) error {
		var head models.FeedGenerationHead
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND lane=?", tenant, newsLane).First(&head).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		generations, err := loadHeadGenerations(tx, head)
		if err != nil {
			return err
		}
		registered := tx.Migrator().HasTable(&models.SourceItemInstance{})
		for _, generation := range generations {
			if generation.State == "candidate" && generation.Purpose == "content_reset" {
				continue
			}
			query := tx.Model(&models.ContentItem{}).Select("public_id,story_id").Where("tenant_id=? AND type='NEWS' AND status='READY' AND story_id IS NOT NULL", tenant).
				Where("COALESCE(news_retention_state,'full')='full' OR news_feed_role IN ('lead','representative')")
			if registered {
				query = query.Where(`NOT EXISTS (SELECT 1 FROM source_item_instances instance
     WHERE instance.tenant_id=content_items.tenant_id AND instance.content_item_id=content_items.public_id
       AND (instance.state IN ('staged','retired') OR instance.campaign_id IS NOT NULL))`)
			}
			result := tx.Exec(`INSERT INTO feed_generation_memberships (generation_id,member_type,member_id,attached_at)
     SELECT DISTINCT ?::uuid, 'story', story_id, NOW() FROM (?) AS candidates ON CONFLICT DO NOTHING`, generation.PublicID, query)
			if result.Error != nil {
				return result.Error
			}
			attached += result.RowsAffected
			result = tx.Exec(`INSERT INTO feed_generation_memberships (generation_id,member_type,member_id,attached_at)
     SELECT ?::uuid, 'news_item', public_id, NOW() FROM (?) AS candidates ON CONFLICT DO NOTHING`, generation.PublicID, query)
			if result.Error != nil {
				return result.Error
			}
			attached += result.RowsAffected
		}
		return nil
	})
	return attached, err
}

// SyncMediaMembership applies the canonical base eligibility result to the
// active and candidate generations while their head is locked. It is safe to
// call after every typed item mutation and is intentionally a no-op before the
// feed-generation schema has been migrated.
func SyncMediaMembership(tx *gorm.DB, item models.ContentItem) error {
	if tx == nil || item.TenantID == "" || item.PublicID == uuid.Nil {
		return fmt.Errorf("feed membership requires a tenant-scoped item")
	}
	if item.Type == models.ContentTypeVideo || item.Type == models.ContentTypePodcast {
		if err := lifecycle.Check(tx, lifecycle.Scope{
			TenantID: item.TenantID,
			Lane:     "pods",
			SourceID: lifecycleSourceID(item.ContentSourceID),
			ItemID:   item.PublicID.String(),
		}, lifecycle.PhaseFeedMembership); err != nil {
			return err
		}
	}
	if !tx.Migrator().HasTable(&models.FeedGenerationHead{}) || !tx.Migrator().HasTable(&models.FeedGenerationMembership{}) {
		return nil
	}
	var head models.FeedGenerationHead
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND lane=?", item.TenantID, mediaLane).First(&head).Error
	if err == gorm.ErrRecordNotFound {
		return nil
	}
	if err != nil {
		return err
	}
	eligible := false
	if item.Type == models.ContentTypeVideo || item.Type == models.ContentTypePodcast {
		var count int64
		query := feedcontract.PodsEligibleMediaQuery(tx, item.TenantID, feedcontract.SupportsAtomizedPodsSchema(tx)).Where("content_items.public_id=?", item.PublicID)
		if err := query.Count(&count).Error; err != nil {
			return err
		}
		eligible = count == 1
		if eligible {
			var holds int64
			if err := tx.Model(&models.MediaCirculationOverride{}).Where("tenant_id=? AND subject_id=? AND override_type=? AND (expires_at IS NULL OR expires_at>?)", item.TenantID, item.PublicID, models.MediaCirculationOverrideEditorialHold, time.Now().UTC()).Count(&holds).Error; err != nil {
				return err
			}
			eligible = holds == 0
		}
	}
	generations, err := loadHeadGenerations(tx, head)
	if err != nil {
		return err
	}
	for _, generation := range generations {
		allowed, err := generationAcceptsMediaItem(tx, generation, item.PublicID)
		if err != nil {
			return err
		}
		if !allowed {
			if err := tx.Where("generation_id=? AND member_type=? AND member_id=?", generation.PublicID, "feed_unit", item.PublicID).Delete(&models.FeedGenerationMembership{}).Error; err != nil {
				return err
			}
			continue
		}
		if eligible {
			membership := models.FeedGenerationMembership{GenerationID: generation.PublicID, MemberType: "feed_unit", MemberID: item.PublicID, AttachedAt: time.Now().UTC()}
			if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&membership).Error; err != nil {
				return err
			}
		} else if err := tx.Where("generation_id=? AND member_type=? AND member_id=?", generation.PublicID, "feed_unit", item.PublicID).Delete(&models.FeedGenerationMembership{}).Error; err != nil {
			return err
		}
	}
	return nil
}

func loadHeadGenerations(tx *gorm.DB, head models.FeedGenerationHead) ([]models.FeedGeneration, error) {
	ids := make([]uuid.UUID, 0, 2)
	for _, id := range []*uuid.UUID{head.ActiveGenerationID, head.CandidateGenerationID} {
		if id != nil && *id != uuid.Nil {
			ids = append(ids, *id)
		}
	}
	if len(ids) == 0 {
		return nil, nil
	}
	var generations []models.FeedGeneration
	if err := tx.Where("tenant_id = ? AND public_id IN ?", head.TenantID, ids).Find(&generations).Error; err != nil {
		return nil, err
	}
	if len(generations) != len(ids) {
		return nil, fmt.Errorf("feed generation head references a missing generation")
	}
	return generations, nil
}

func generationAcceptsNewsItem(tx *gorm.DB, generation models.FeedGeneration, itemID uuid.UUID) (bool, error) {
	if generation.Purpose == "content_reset" && (tx == nil || !tx.Migrator().HasTable(&models.NewsFeedStoryProjection{})) {
		return false, nil
	}
	return generationAcceptsCampaignInstance(tx, generation, itemID)
}

func generationAcceptsMediaItem(tx *gorm.DB, generation models.FeedGeneration, itemID uuid.UUID) (bool, error) {
	return generationAcceptsCampaignInstance(tx, generation, itemID)
}

func generationAcceptsCampaignInstance(tx *gorm.DB, generation models.FeedGeneration, itemID uuid.UUID) (bool, error) {
	if generation.Purpose == "content_reset" || generation.ContentResetCampaignID != nil {
		return false, nil
	}
	instance, err := CampaignInstanceForItem(tx, generation.TenantID, itemID)
	if err != nil {
		return false, err
	}
	if instance == nil {
		return true, nil
	}
	return instance.CampaignID == nil && instance.State != "staged" && !(instance.ContentItemID == itemID && instance.State == "retired"), nil
}

// CampaignInstanceForItem includes canonical media children in their parent's
// campaign boundary. Direct identity takes precedence; arbitrary source-name
// or URL similarity never grants campaign membership.
func CampaignInstanceForItem(db *gorm.DB, tenant string, itemID uuid.UUID) (*models.SourceItemInstance, error) {
	if !db.Migrator().HasTable(&models.SourceItemInstance{}) {
		return nil, nil
	}
	var instance models.SourceItemInstance
	err := db.Where("tenant_id=? AND content_item_id=?", tenant, itemID).First(&instance).Error
	if err == nil {
		return &instance, nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}
	err = db.Table("source_item_instances AS instance").Select("instance.*").
		Joins("JOIN content_items child ON child.tenant_id=instance.tenant_id AND child.parent_content_item_id=instance.content_item_id").
		Where("child.tenant_id=? AND child.public_id=?", tenant, itemID).First(&instance).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &instance, nil
}

func lifecycleSourceID(id *uuid.UUID) string {
	if id == nil {
		return ""
	}
	return id.String()
}
