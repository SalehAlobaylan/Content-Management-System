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
