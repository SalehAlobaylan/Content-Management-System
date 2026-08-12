package controllers

import (
	"time"

	"content-management-system/src/models"
	"content-management-system/src/supply"

	"gorm.io/gorm"
)

// Register the exact anonymous Pods assembly path for the Supply supervisor.
// This is an in-process, tenant-explicit read; it creates no session, does not
// consult seen state or preferences, and never records serve telemetry.
func init() {
	supply.RegisterPodsReturnProbe(buildPodsSupplyReturnProbe)
}

func buildPodsSupplyReturnProbe(db *gorm.DB, tenantID string, limit int) ([]supply.PodsReturnedItem, error) {
	if db == nil || tenantID == "" || limit < 1 || limit > 100 {
		return nil, gorm.ErrInvalidDB
	}
	config := loadTenantConfig(db, tenantID)
	atomizedFeedSchema := supportsAtomizedPodsSchema(db)
	items := make([]models.ContentItem, 0, limit)
	if config.IsActive {
		base := podsEligibleMediaQuery(db, tenantID, atomizedFeedSchema)
		windowDays := config.FreshnessDecayHours / 24
		if windowDays < 30 {
			windowDays = 30
		}
		var candidates []models.ContentItem
		if err := base.Session(&gorm.Session{}).
			Where("COALESCE(published_at, created_at) > ?", time.Now().UTC().AddDate(0, 0, -windowDays)).
			Order("COALESCE(published_at, created_at) DESC").Limit(200).Find(&candidates).Error; err != nil {
			return nil, err
		}
		if len(candidates) < 200 {
			if err := base.Session(&gorm.Session{}).Order("COALESCE(published_at, created_at) DESC").Limit(200).Find(&candidates).Error; err != nil {
				return nil, err
			}
		}
		candidates = excludeCollapsedRedundancyMembers(db, tenantID, candidates)
		ids := extractPublicIDs(candidates)
		flags := LoadContentFlags(db, tenantID, ids)
		velocity := LoadVelocityData(db, ids, config.VelocityWindowHours, time.Now().UTC())
		scored := ScoreItems(candidates, config, flags, velocity, time.Now().UTC())
		scored = applyIntelligenceFeedHooks(db, tenantID, scored)
		scored = spaceScoredSiblingChapters(scored)
		if len(scored) > limit {
			scored = scored[:limit]
		}
		for _, scoredItem := range scored {
			items = append(items, scoredItem.Item)
		}
	} else {
		if err := podsEligibleMediaQuery(db, tenantID, atomizedFeedSchema).
			Order("COALESCE(published_at, created_at) DESC, public_id DESC").Limit((limit * 3) + 1).Find(&items).Error; err != nil {
			return nil, err
		}
		items = excludeCollapsedRedundancyMembers(db, tenantID, items)
		items = spaceSiblingChapters(items)
		if len(items) > limit {
			items = items[:limit]
		}
	}
	returned := make([]supply.PodsReturnedItem, 0, len(items))
	for _, item := range items {
		at := item.CreatedAt.UTC()
		if item.PublishedAt != nil {
			at = item.PublishedAt.UTC()
		}
		returned = append(returned, supply.PodsReturnedItem{ID: item.PublicID, PublishedAt: at, SourceRunRequestID: item.SourceRunRequestID})
	}
	return returned, nil
}
