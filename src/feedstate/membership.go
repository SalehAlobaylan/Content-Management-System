// Package feedstate owns the transactional serving-membership side of a
// content lifecycle mutation. Controllers may decide an allowed typed change,
// but none may save a media item and later best-effort attach it to a feed
// generation outside the same transaction.
package feedstate

import (
	"fmt"
	"time"

	"content-management-system/src/feedcontract"
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
	for _, generationID := range []*uuid.UUID{head.ActiveGenerationID, head.CandidateGenerationID} {
		if generationID == nil || *generationID == uuid.Nil {
			continue
		}
		membership := models.FeedGenerationMembership{GenerationID: *generationID, MemberType: "story", MemberID: *item.StoryID, AttachedAt: time.Now().UTC()}
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&membership).Error; err != nil {
			return err
		}
	}
	return nil
}

// ReconcileNewsMembership repairs historical rows created while one side of
// the READY/classified handshake was missing. It is additive and idempotent:
// canonical content remains untouched, and ON CONFLICT makes repeat startup
// passes safe.
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
		if err == gorm.ErrRecordNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		for _, generationID := range []*uuid.UUID{head.ActiveGenerationID, head.CandidateGenerationID} {
			if generationID == nil || *generationID == uuid.Nil {
				continue
			}
			result := tx.Exec(`INSERT INTO feed_generation_memberships (generation_id, member_type, member_id, attached_at)
				SELECT ?, 'story', story_id, NOW()
				FROM content_items
				WHERE tenant_id=? AND type='NEWS' AND status='READY' AND story_id IS NOT NULL
				  AND (COALESCE(news_retention_state, 'full')='full' OR news_feed_role IN ('lead','representative'))
				GROUP BY story_id
				ON CONFLICT DO NOTHING`, *generationID, tenant)
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
	for _, generationID := range []*uuid.UUID{head.ActiveGenerationID, head.CandidateGenerationID} {
		if generationID == nil || *generationID == uuid.Nil {
			continue
		}
		if eligible {
			membership := models.FeedGenerationMembership{GenerationID: *generationID, MemberType: "feed_unit", MemberID: item.PublicID, AttachedAt: time.Now().UTC()}
			if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&membership).Error; err != nil {
				return err
			}
		} else if err := tx.Where("generation_id=? AND member_type=? AND member_id=?", *generationID, "feed_unit", item.PublicID).Delete(&models.FeedGenerationMembership{}).Error; err != nil {
			return err
		}
	}
	return nil
}
