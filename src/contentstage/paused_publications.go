package contentstage

import (
	"content-management-system/src/models"
	"gorm.io/gorm"
)

// Final stage receipts keep verifying while paused. After resume, reconcile
// raw episodes whose last proof arrived during that pause, without rerunning
// any provider effect. Long-form generations have their own activation sweep.
func reconcileHeldPodsPublications(db *gorm.DB) error {
	var items []models.ContentItem
	if err := db.Table("content_items i").Select("i.*").Where(`i.type IN ('VIDEO','PODCAST') AND i.parent_content_item_id IS NULL AND i.is_feed_unit=TRUE AND i.feed_visibility='visible' AND i.status NOT IN ('READY','ARCHIVED')
	 AND NOT EXISTS(SELECT 1 FROM content_stage_controls c WHERE c.tenant_id=i.tenant_id AND c.lane='pods' AND (NOT c.scheduling_enabled OR NOT c.execution_enabled))
	 AND EXISTS(SELECT 1 FROM content_stage_requests r WHERE r.tenant_id=i.tenant_id AND r.content_item_id=i.public_id AND r.processing_generation=i.processing_generation AND r.blocking_scope='content_ready')
	 AND NOT EXISTS(SELECT 1 FROM content_stage_requests r WHERE r.tenant_id=i.tenant_id AND r.content_item_id=i.public_id AND r.processing_generation=i.processing_generation AND r.blocking_scope='content_ready' AND r.state<>'verified')`).Order("i.updated_at ASC").Limit(16).Find(&items).Error; err != nil {
		return err
	}
	for _, item := range items {
		if err := db.Transaction(func(tx *gorm.DB) error {
			return reduceReadiness(tx, item.TenantID, item.PublicID, item.ProcessingGeneration)
		}); err != nil {
			return err
		}
	}
	return nil
}
