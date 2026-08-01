package controllers

import (
	"context"
	"time"

	"content-management-system/src/models"
	operatorpkg "content-management-system/src/operator"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// executeOperatorDomainPausePlan is an intentionally closed set of
// tenant-local, disable-only brakes.  It does not call an admin HTTP handler,
// change an enabled/mode/capacity field, or reach an external service.  Every
// case locks one known policy row, records its prior expiry, and verifies the
// persisted effective expiry before the plan worker can succeed.
func executeOperatorDomainPausePlan(ctx context.Context, db *gorm.DB, tenantID, toolKey string, canonical operatorpkg.CanonicalPlan) (bool, map[string]any, map[string]any, map[string]any) {
	if canonical.NormalizedArguments["duration_minutes"] != 24*60 {
		return false, nil, map[string]any{"error_class": "invalid_registered_arguments"}, nil
	}
	now := time.Now().UTC()
	until := now.Add(24 * time.Hour)
	before := map[string]any{"paused": false}
	after := map[string]any{}
	verified := map[string]any{}
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		switch toolKey {
		case "feed_integrity.pause.24h":
			var row models.FeedIntegrityPolicy
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=?", tenantID).First(&row).Error; err != nil {
				return err
			}
			before["paused_until"] = row.AutopilotPausedUntil
			if row.AutopilotPausedUntil == nil || row.AutopilotPausedUntil.Before(until) {
				if err := tx.Model(&row).Update("autopilot_paused_until", until).Error; err != nil {
					return err
				}
			}
			if err := tx.Where("id=? AND tenant_id=?", row.ID, tenantID).First(&row).Error; err != nil {
				return err
			}
			after["paused_until"], verified["paused_until"] = row.AutopilotPausedUntil, row.AutopilotPausedUntil != nil && !row.AutopilotPausedUntil.Before(until)
		case "real_experience.pause.24h":
			var row models.ExperiencePolicy
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=?", tenantID).First(&row).Error; err != nil {
				return err
			}
			before["paused_until"] = row.PausedUntil
			if row.PausedUntil == nil || row.PausedUntil.Before(until) {
				if err := tx.Model(&row).Update("paused_until", until).Error; err != nil {
					return err
				}
			}
			if err := tx.Where("id=? AND tenant_id=?", row.ID, tenantID).First(&row).Error; err != nil {
				return err
			}
			after["paused_until"], verified["paused_until"] = row.PausedUntil, row.PausedUntil != nil && !row.PausedUntil.Before(until)
		case "retention.pause.24h":
			var row models.RetentionPolicy
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=?", tenantID).First(&row).Error; err != nil {
				return err
			}
			before["paused_until"] = row.PausedUntil
			if row.PausedUntil == nil || row.PausedUntil.Before(until) {
				if err := tx.Model(&row).Update("paused_until", until).Error; err != nil {
					return err
				}
			}
			if err := tx.Where("id=? AND tenant_id=?", row.ID, tenantID).First(&row).Error; err != nil {
				return err
			}
			after["paused_until"], verified["paused_until"] = row.PausedUntil, row.PausedUntil != nil && !row.PausedUntil.Before(until)
		case "ai_economics.pause.24h":
			var row models.AISpendPolicy
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=?", tenantID).First(&row).Error; err != nil {
				return err
			}
			before["paused_until"] = row.PausedUntil
			if row.PausedUntil == nil || row.PausedUntil.Before(until) {
				if err := tx.Model(&row).Update("paused_until", until).Error; err != nil {
					return err
				}
			}
			if err := tx.Where("id=? AND tenant_id=?", row.ID, tenantID).First(&row).Error; err != nil {
				return err
			}
			after["paused_until"], verified["paused_until"] = row.PausedUntil, row.PausedUntil != nil && !row.PausedUntil.Before(until)
		case "news_circulation.pause.24h":
			var row models.NewsCirculationPolicy
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=?", tenantID).First(&row).Error; err != nil {
				return err
			}
			before["paused_until"] = row.AutopilotPausedUntil
			if row.AutopilotPausedUntil == nil || row.AutopilotPausedUntil.Before(until) {
				if err := tx.Model(&row).Update("autopilot_paused_until", until).Error; err != nil {
					return err
				}
			}
			if err := tx.Where("id=? AND tenant_id=?", row.ID, tenantID).First(&row).Error; err != nil {
				return err
			}
			after["paused_until"], verified["paused_until"] = row.AutopilotPausedUntil, row.AutopilotPausedUntil != nil && !row.AutopilotPausedUntil.Before(until)
		case "media_circulation.pause.24h":
			var row models.MediaCirculationPolicy
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=?", tenantID).First(&row).Error; err != nil {
				return err
			}
			before["paused_until"] = row.AutopilotPausedUntil
			if row.AutopilotPausedUntil == nil || row.AutopilotPausedUntil.Before(until) {
				if err := tx.Model(&row).Update("autopilot_paused_until", until).Error; err != nil {
					return err
				}
			}
			if err := tx.Where("id=? AND tenant_id=?", row.ID, tenantID).First(&row).Error; err != nil {
				return err
			}
			after["paused_until"], verified["paused_until"] = row.AutopilotPausedUntil, row.AutopilotPausedUntil != nil && !row.AutopilotPausedUntil.Before(until)
		case "redundancy.pause.24h":
			var row models.RedundancyPolicy
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=?", tenantID).First(&row).Error; err != nil {
				return err
			}
			before["paused_until"] = row.PausedUntil
			if row.PausedUntil == nil || row.PausedUntil.Before(until) {
				if err := tx.Model(&row).Update("paused_until", until).Error; err != nil {
					return err
				}
			}
			if err := tx.Where("id=? AND tenant_id=?", row.ID, tenantID).First(&row).Error; err != nil {
				return err
			}
			after["paused_until"], verified["paused_until"] = row.PausedUntil, row.PausedUntil != nil && !row.PausedUntil.Before(until)
		case "pipeline.pause.24h":
			var row models.PipelineAutopilotPolicy
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=?", tenantID).First(&row).Error; err != nil {
				return err
			}
			before["paused_until"] = row.PausedUntil
			if row.PausedUntil == nil || row.PausedUntil.Before(until) {
				if err := tx.Model(&row).Update("paused_until", until).Error; err != nil {
					return err
				}
			}
			if err := tx.Where("id=? AND tenant_id=?", row.ID, tenantID).First(&row).Error; err != nil {
				return err
			}
			after["paused_until"], verified["paused_until"] = row.PausedUntil, row.PausedUntil != nil && !row.PausedUntil.Before(until)
		case "enrichment.pause.24h":
			var row models.EnrichmentAutopilotPolicy
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=?", tenantID).First(&row).Error; err != nil {
				return err
			}
			before["paused_until"] = row.PausedUntil
			if row.PausedUntil == nil || row.PausedUntil.Before(until) {
				if err := tx.Model(&row).Update("paused_until", until).Error; err != nil {
					return err
				}
			}
			if err := tx.Where("id=? AND tenant_id=?", row.ID, tenantID).First(&row).Error; err != nil {
				return err
			}
			after["paused_until"], verified["paused_until"] = row.PausedUntil, row.PausedUntil != nil && !row.PausedUntil.Before(until)
		case "embeddings.pause_campaigns.24h":
			var row models.EmbeddingLifecyclePolicy
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=?", tenantID).First(&row).Error; err != nil {
				return err
			}
			before["paused_until"] = row.CampaignsPausedUntil
			if row.CampaignsPausedUntil == nil || row.CampaignsPausedUntil.Before(until) {
				if err := tx.Model(&row).Update("campaigns_paused_until", until).Error; err != nil {
					return err
				}
			}
			if err := tx.Where("id=? AND tenant_id=?", row.ID, tenantID).First(&row).Error; err != nil {
				return err
			}
			after["paused_until"], verified["paused_until"] = row.CampaignsPausedUntil, row.CampaignsPausedUntil != nil && !row.CampaignsPausedUntil.Before(until)
		case "topics_preferences.pause.24h":
			var row models.PreferenceAutopilotPolicy
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=?", tenantID).First(&row).Error; err != nil {
				return err
			}
			before["paused_until"] = row.PausedUntil
			if row.PausedUntil == nil || row.PausedUntil.Before(until) {
				if err := tx.Model(&row).Update("paused_until", until).Error; err != nil {
					return err
				}
			}
			if err := tx.Where("id=? AND tenant_id=?", row.ID, tenantID).First(&row).Error; err != nil {
				return err
			}
			after["paused_until"], verified["paused_until"] = row.PausedUntil, row.PausedUntil != nil && !row.PausedUntil.Before(until)
		case "media_library.pause.24h":
			var row models.MediaStudioAutopilotPolicy
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=?", tenantID).First(&row).Error; err != nil {
				return err
			}
			before["paused_until"] = row.PausedUntil
			if row.PausedUntil == nil || row.PausedUntil.Before(until) {
				if err := tx.Model(&row).Update("paused_until", until).Error; err != nil {
					return err
				}
			}
			if err := tx.Where("id=? AND tenant_id=?", row.ID, tenantID).First(&row).Error; err != nil {
				return err
			}
			after["paused_until"], verified["paused_until"] = row.PausedUntil, row.PausedUntil != nil && !row.PausedUntil.Before(until)
		default:
			return gorm.ErrRecordNotFound
		}
		return nil
	})
	if err != nil {
		return false, before, map[string]any{"error_class": "domain_pause_preflight_failed"}, map[string]any{"paused_until": false}
	}
	paused, _ := verified["paused_until"].(bool)
	verified["duration_minutes"] = 24 * 60
	return paused, before, after, verified
}
