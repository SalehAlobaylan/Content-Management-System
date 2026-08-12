package feedstate

import (
	"fmt"
	"hash/fnv"

	"content-management-system/src/models"

	"gorm.io/gorm"
)

// ReconcileMediaMembership is bounded and tenant-scoped. Advisory locking
// prevents two CMS replicas from racing generation-head membership changes;
// another tenant can reconcile independently.
func ReconcileMediaMembership(db *gorm.DB, tenant string, limit int) (int, error) {
	if db == nil || tenant == "" {
		return 0, fmt.Errorf("media membership reconciliation requires a tenant")
	}
	if limit <= 0 || limit > 250 {
		limit = 100
	}
	var processed int
	err := db.Transaction(func(tx *gorm.DB) error {
		key := advisoryKey("wahb:feed-membership:" + tenant)
		var locked bool
		if err := tx.Raw("SELECT pg_try_advisory_xact_lock(?)", key).Scan(&locked).Error; err != nil {
			return err
		}
		if !locked {
			return nil
		}
		if !tx.Migrator().HasTable(&models.FeedGenerationHead{}) || !tx.Migrator().HasTable(&models.FeedGenerationMembership{}) {
			return nil
		}
		var items []models.ContentItem
		if err := tx.Where("tenant_id=? AND type IN ?", tenant, []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}).Order("updated_at ASC, id ASC").Limit(limit).Find(&items).Error; err != nil {
			return err
		}
		for _, item := range items {
			if err := SyncMediaMembership(tx, item); err != nil {
				return err
			}
			processed++
		}
		return nil
	})
	return processed, err
}

func advisoryKey(value string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(value))
	return int64(h.Sum64())
}
