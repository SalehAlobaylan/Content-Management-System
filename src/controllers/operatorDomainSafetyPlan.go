package controllers

import (
	"context"
	"time"

	"content-management-system/src/models"
	operatorpkg "content-management-system/src/operator"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// executeOperatorDomainSafetyPlan contains named, bounded suppress/revoke
// operations owned by CMS. It does not call legacy HTTP handlers and it never
// reaches a queue, provider, or arbitrary service endpoint.
func executeOperatorDomainSafetyPlan(ctx context.Context, db *gorm.DB, tenantID, toolKey string, plan models.OperatorActionPlan, canonical operatorpkg.CanonicalPlan) (bool, map[string]any, map[string]any, map[string]any) {
	switch toolKey {
	case "feed_integrity.suppress_episode.1h":
		return executeFeedIntegrityEpisodeSuppression(ctx, db, tenantID, plan, canonical)
	case "feed_integrity.revoke_suppression":
		return executeFeedIntegritySuppressionRevoke(ctx, db, tenantID, canonical)
	case "real_experience.suppress_incident.1h":
		return executeExperienceIncidentSuppression(ctx, db, tenantID, plan, canonical)
	case "real_experience.revoke_suppression":
		return executeExperienceSuppressionRevoke(ctx, db, tenantID, canonical)
	default:
		return false, nil, map[string]any{"error_class": "unregistered_domain_safety_executor"}, nil
	}
}

func operatorSafetyUUID(arguments map[string]any, key string) (uuid.UUID, error) {
	return governanceUUID(arguments, key)
}

func executeFeedIntegrityEpisodeSuppression(ctx context.Context, db *gorm.DB, tenantID string, plan models.OperatorActionPlan, canonical operatorpkg.CanonicalPlan) (bool, map[string]any, map[string]any, map[string]any) {
	id, err := operatorSafetyUUID(canonical.NormalizedArguments, "episode_id")
	if err != nil || canonical.NormalizedArguments["ttl_minutes"] != 60 {
		return false, nil, map[string]any{"error_class": "invalid_registered_arguments"}, nil
	}
	var episode models.FeedIntegrityEpisode
	if err := db.WithContext(ctx).Where("public_id=? AND tenant_id=? AND status IN ?", id, tenantID, []string{models.FeedIntegrityEpisodeOpen, models.FeedIntegrityEpisodeRecovering}).First(&episode).Error; err != nil {
		return false, map[string]any{"episode_id": id.String(), "suppressible": false}, map[string]any{"error_class": "episode_not_open"}, nil
	}
	now := time.Now().UTC()
	before := map[string]any{"episode_id": episode.PublicID.String(), "check_key": episode.CheckKey, "suppressed": false}
	suppression := models.FeedIntegritySuppression{TenantID: tenantID, CheckKey: episode.CheckKey, Feed: episode.Feed, Variant: episode.Variant, Scope: episode.Scope, Reason: "operator_signed_plan", StartsAt: now, ExpiresAt: now.Add(time.Hour), CreatedBy: plan.ActorID}
	if err := db.WithContext(ctx).Create(&suppression).Error; err != nil {
		return false, before, map[string]any{"error_class": "suppression_create_failed"}, nil
	}
	var verifiedRow models.FeedIntegritySuppression
	err = db.WithContext(ctx).Where("id=? AND tenant_id=? AND revoked_at IS NULL AND expires_at>?", suppression.ID, tenantID, now).First(&verifiedRow).Error
	verified := map[string]any{"active": err == nil, "duration_minutes": 60, "check_matches": err == nil && verifiedRow.CheckKey == episode.CheckKey}
	return err == nil && verifiedRow.CheckKey == episode.CheckKey, before, map[string]any{"suppression_id": suppression.PublicID.String(), "check_key": suppression.CheckKey, "expires_at": suppression.ExpiresAt}, verified
}

func executeFeedIntegritySuppressionRevoke(ctx context.Context, db *gorm.DB, tenantID string, canonical operatorpkg.CanonicalPlan) (bool, map[string]any, map[string]any, map[string]any) {
	id, err := operatorSafetyUUID(canonical.NormalizedArguments, "suppression_id")
	if err != nil {
		return false, nil, map[string]any{"error_class": "invalid_registered_arguments"}, nil
	}
	var row models.FeedIntegritySuppression
	if err := db.WithContext(ctx).Where("public_id=? AND tenant_id=? AND revoked_at IS NULL", id, tenantID).First(&row).Error; err != nil {
		return false, map[string]any{"suppression_id": id.String(), "active": false}, map[string]any{"error_class": "suppression_not_active"}, nil
	}
	before := map[string]any{"suppression_id": row.PublicID.String(), "active": true}
	now := time.Now().UTC()
	if err := db.WithContext(ctx).Model(&row).Updates(map[string]any{"revoked_at": now, "revoked_by": canonical.ActorID}).Error; err != nil {
		return false, before, map[string]any{"error_class": "suppression_revoke_failed"}, nil
	}
	var verifiedRow models.FeedIntegritySuppression
	err = db.WithContext(ctx).Where("id=? AND tenant_id=?", row.ID, tenantID).First(&verifiedRow).Error
	verified := map[string]any{"revoked": err == nil && verifiedRow.RevokedAt != nil}
	return err == nil && verifiedRow.RevokedAt != nil, before, map[string]any{"suppression_id": row.PublicID.String(), "revoked_at": now}, verified
}

func executeExperienceIncidentSuppression(ctx context.Context, db *gorm.DB, tenantID string, plan models.OperatorActionPlan, canonical operatorpkg.CanonicalPlan) (bool, map[string]any, map[string]any, map[string]any) {
	id, err := operatorSafetyUUID(canonical.NormalizedArguments, "incident_id")
	if err != nil || canonical.NormalizedArguments["ttl_minutes"] != 60 {
		return false, nil, map[string]any{"error_class": "invalid_registered_arguments"}, nil
	}
	var incident models.ExperienceIncident
	if err := db.WithContext(ctx).Where("public_id=? AND tenant_id=? AND status NOT IN ?", id, tenantID, []string{"closed", "resolved"}).First(&incident).Error; err != nil {
		return false, map[string]any{"incident_id": id.String(), "suppressible": false}, map[string]any{"error_class": "incident_not_open"}, nil
	}
	now := time.Now().UTC()
	before := map[string]any{"incident_id": incident.PublicID.String(), "metric_key": incident.MetricKey, "suppressed": false}
	suppression := models.ExperienceSuppression{TenantID: tenantID, MetricKey: incident.MetricKey, Surface: incident.Surface, CohortDim: incident.CohortDim, CohortVal: incident.CohortVal, Reason: "operator_signed_plan", StartsAt: now, ExpiresAt: now.Add(time.Hour), CreatedBy: plan.ActorID}
	if err := db.WithContext(ctx).Create(&suppression).Error; err != nil {
		return false, before, map[string]any{"error_class": "suppression_create_failed"}, nil
	}
	var verifiedRow models.ExperienceSuppression
	err = db.WithContext(ctx).Where("id=? AND tenant_id=? AND revoked_at IS NULL AND expires_at>?", suppression.ID, tenantID, now).First(&verifiedRow).Error
	verified := map[string]any{"active": err == nil, "duration_minutes": 60, "metric_matches": err == nil && verifiedRow.MetricKey == incident.MetricKey}
	return err == nil && verifiedRow.MetricKey == incident.MetricKey, before, map[string]any{"suppression_id": suppression.PublicID.String(), "metric_key": suppression.MetricKey, "expires_at": suppression.ExpiresAt}, verified
}

func executeExperienceSuppressionRevoke(ctx context.Context, db *gorm.DB, tenantID string, canonical operatorpkg.CanonicalPlan) (bool, map[string]any, map[string]any, map[string]any) {
	id, err := operatorSafetyUUID(canonical.NormalizedArguments, "suppression_id")
	if err != nil {
		return false, nil, map[string]any{"error_class": "invalid_registered_arguments"}, nil
	}
	var row models.ExperienceSuppression
	if err := db.WithContext(ctx).Where("public_id=? AND tenant_id=? AND revoked_at IS NULL", id, tenantID).First(&row).Error; err != nil {
		return false, map[string]any{"suppression_id": id.String(), "active": false}, map[string]any{"error_class": "suppression_not_active"}, nil
	}
	before := map[string]any{"suppression_id": row.PublicID.String(), "active": true}
	now := time.Now().UTC()
	if err := db.WithContext(ctx).Model(&row).Updates(map[string]any{"revoked_at": now, "revoked_by": canonical.ActorID}).Error; err != nil {
		return false, before, map[string]any{"error_class": "suppression_revoke_failed"}, nil
	}
	var verifiedRow models.ExperienceSuppression
	err = db.WithContext(ctx).Where("id=? AND tenant_id=?", row.ID, tenantID).First(&verifiedRow).Error
	verified := map[string]any{"revoked": err == nil && verifiedRow.RevokedAt != nil}
	return err == nil && verifiedRow.RevokedAt != nil, before, map[string]any{"suppression_id": row.PublicID.String(), "revoked_at": now}, verified
}

// executeOperatorSourceStatePlan is the CMS-native source pause/resume
// boundary.  It accepts only an exact source UUID already signed into the
// plan, rechecks the tenant and the Console domain category while locked, and
// verifies the resulting state.  It never pushes a queue command directly.
func executeOperatorSourceStatePlan(ctx context.Context, db *gorm.DB, tenantID, toolKey string, canonical operatorpkg.CanonicalPlan) (bool, map[string]any, map[string]any, map[string]any) {
	id, err := operatorSafetyUUID(canonical.NormalizedArguments, "source_id")
	if err != nil {
		return false, nil, map[string]any{"error_class": "invalid_registered_arguments"}, nil
	}
	category, targetActive := "news", false
	switch toolKey {
	case "sources.pause":
		category, targetActive = "news", false
	case "sources.resume":
		category, targetActive = "news", true
	case "media_sources.pause":
		category, targetActive = "media", false
	case "media_sources.resume":
		category, targetActive = "media", true
	default:
		return false, nil, map[string]any{"error_class": "unregistered_source_state_executor"}, nil
	}
	var source models.ContentSource
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id=? AND tenant_id=? AND category=?", id, tenantID, category).First(&source).Error; err != nil {
			return err
		}
		if source.IsActive == targetActive {
			return nil
		}
		return tx.Model(&source).Update("is_active", targetActive).Error
	})
	if err != nil {
		return false, map[string]any{"source_id": id.String(), "eligible": false}, map[string]any{"error_class": "source_missing_or_category_changed"}, nil
	}
	before := map[string]any{"source_id": source.PublicID.String(), "category": source.Category, "is_active": source.IsActive}
	var verifiedSource models.ContentSource
	err = db.WithContext(ctx).Where("public_id=? AND tenant_id=? AND category=?", id, tenantID, category).First(&verifiedSource).Error
	verified := map[string]any{"state_matches": err == nil && verifiedSource.IsActive == targetActive, "category_matches": err == nil && verifiedSource.Category == category}
	return err == nil && verifiedSource.IsActive == targetActive && verifiedSource.Category == category, before, map[string]any{"source_id": id.String(), "category": category, "is_active": targetActive}, verified
}
