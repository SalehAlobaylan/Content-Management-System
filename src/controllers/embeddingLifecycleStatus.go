package controllers

import (
	"content-management-system/src/feedcontract"
	"content-management-system/src/models"
	"fmt"
	"gorm.io/gorm"
)

func embeddingEligibility(s EmbeddingSurface) string {
	switch s.Key {
	case "content_text", "content_image":
		return `status='READY' AND ((type='NEWS' AND COALESCE(news_retention_state,'full')='full') OR (type IN ('VIDEO','PODCAST') AND is_feed_unit AND feed_visibility='visible' AND duration_sec BETWEEN 270 AND 2400))`
	case "story_centroid":
		return `COALESCE(news_retention_state,'full')='full' AND (last_member_at IS NULL OR last_member_at>=now()-interval '7 days')`
	case "topic_centroid":
		return "active=true"
	case "topic_proposal":
		return "status='pending'"
	case "discovery_profile":
		return "enabled=true"
	}
	return "false"
}

func embeddingAvailability(db *gorm.DB, s EmbeddingSurface, spaceID string) (eligible, current int64, err error) {
	var row struct{ Eligible, Current int64 }
	source := s.Table
	if s.Key == "story_centroid" && feedcontract.HasNewsStoryProjection(db) {
		source = `(SELECT (jsonb_populate_record(canonical,COALESCE(p.projection,'{}'::jsonb))).* FROM stories canonical JOIN feed_generation_heads h ON h.tenant_id=canonical.tenant_id AND h.lane='news' LEFT JOIN news_feed_story_projections p ON p.tenant_id=canonical.tenant_id AND p.story_id=canonical.public_id AND p.generation_id=h.active_generation_id WHERE EXISTS (SELECT 1 FROM feed_generation_memberships m WHERE m.generation_id=h.active_generation_id AND m.member_type='story' AND m.member_id=canonical.public_id)) AS serving_stories`
	}
	err = db.Raw(fmt.Sprintf(`SELECT COUNT(*) AS eligible, COUNT(*) FILTER (WHERE %s IS NOT NULL AND %s=?) AS current FROM %s WHERE %s`, s.VecCol, s.SpaceIDCol, source, embeddingEligibility(s)), spaceID).Scan(&row).Error
	return row.Eligible, row.Current, err
}

func embeddingHeldWork(db *gorm.DB) (map[string]int64, error) {
	space := currentTextSpaceIDForSimilarity()
	result := map[string]int64{"classification": 0, "topic_remap": 0, "relations": 0}
	var n int64
	if err := db.Model(&models.ContentItem{}).Where("type='NEWS' AND status='READY' AND story_id IS NULL AND embedding_space_id=? AND COALESCE(news_retention_state,'full')='full'", space).Count(&n).Error; err != nil {
		return nil, err
	}
	result["classification"] = n
	if err := db.Model(&models.Topic{}).Where("active AND needs_remap").Count(&n).Error; err != nil {
		return nil, err
	}
	result["topic_remap"] = n
	var campaigns []models.EmbeddingCampaign
	if err := db.Where("space=? AND state IN ?", EmbeddingSpaceText, []string{models.EmbeddingCampaignRunning, models.EmbeddingCampaignVerifying, models.EmbeddingCampaignPaused, models.EmbeddingCampaignBlocked}).Find(&campaigns).Error; err != nil {
		return nil, err
	}
	for i := range campaigns {
		pending, err := countCampaignPendingStoryRelations(db, &campaigns[i])
		if err != nil {
			return nil, err
		}
		result["relations"] += pending
	}

	return result, nil
}
