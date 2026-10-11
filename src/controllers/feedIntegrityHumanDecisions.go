package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"fmt"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"time"
)

func lockIntegrityPolicy(tx *gorm.DB, tenant string) error {
	var p models.FeedIntegrityPolicy
	return tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=?", tenant).First(&p).Error
}

// Policy effects and their success audit commit together under the same lock
// used by repair admission. Update only requested fields, never a stale model.
func updateIntegrityPolicyAudited(db *gorm.DB, p utils.AdminPrincipal, action string, updates map[string]interface{}) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := lockIntegrityPolicy(tx, p.TenantID); err != nil {
			return err
		}
		if err := tx.Model(&models.FeedIntegrityPolicy{}).Where("tenant_id=?", p.TenantID).Updates(updates).Error; err != nil {
			return err
		}
		return tx.Create(&models.AuditLog{TenantID: p.TenantID, UserID: p.UserID, UserEmail: p.Email, Action: action, TargetService: "cms", TargetResource: p.TenantID, Status: "success", Payload: integrityJSON(updates)}).Error
	})
}
func skipUnstartedIntegrityActions(tx *gorm.DB, tenant string, episodeID uint, reason string) error {
	return tx.Model(&models.FeedIntegrityAction{}).Where("tenant_id=? AND episode_id=? AND outcome IN ?", tenant, episodeID, []string{"approval_required", "approved", "ready", "claimed"}).Updates(map[string]interface{}{"outcome": "skipped", "guardrail": "human_stop", "reason": reason, "finished_at": time.Now().UTC(), "updated_at": time.Now().UTC()}).Error
}
func closeIntegrityEpisode(db *gorm.DB, p utils.AdminPrincipal, id uuid.UUID, reason, notes string) (models.FeedIntegrityEpisode, error) {
	var result models.FeedIntegrityEpisode
	err := db.Transaction(func(tx *gorm.DB) error {
		if err := lockIntegrityPolicy(tx, p.TenantID); err != nil {
			return err
		}
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=?", p.TenantID, id).First(&result).Error; err != nil {
			return err
		}
		if result.Status != "open" && result.Status != "recovering" {
			return fmt.Errorf("episode is no longer open")
		}
		now := time.Now().UTC()
		if err := tx.Model(&result).Updates(map[string]interface{}{"status": models.FeedIntegrityEpisodeClosed, "closed_by": p.Email, "close_reason_class": reason, "close_notes": notes, "resolved_at": now, "clean_streak": 0, "violation_streak": 0, "updated_at": now}).Error; err != nil {
			return err
		}
		if err := skipUnstartedIntegrityActions(tx, p.TenantID, result.ID, "Episode closed by operator"); err != nil {
			return err
		}
		if err := tx.Create(&models.AuditLog{TenantID: p.TenantID, UserID: p.UserID, UserEmail: p.Email, Action: "feed_integrity.episode.close", TargetService: "cms", TargetResource: result.PublicID.String(), Status: "success", Payload: integrityJSON(map[string]interface{}{"reason_class": reason, "notes": notes})}).Error; err != nil {
			return err
		}
		return tx.First(&result, result.ID).Error
	})
	return result, err
}
func createIntegritySuppression(db *gorm.DB, p utils.AdminPrincipal, row *models.FeedIntegritySuppression) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := lockIntegrityPolicy(tx, row.TenantID); err != nil {
			return err
		}
		var episodes []models.FeedIntegrityEpisode
		q := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND check_key=? AND status IN ?", row.TenantID, row.CheckKey, []string{"open", "recovering"})
		if row.Feed != "" {
			q = q.Where("feed=?", row.Feed)
		}
		if row.Variant != "" {
			q = q.Where("variant=?", row.Variant)
		}
		if row.Scope != "" {
			q = q.Where("scope=?", row.Scope)
		}
		if err := q.Order("id ASC").Find(&episodes).Error; err != nil {
			return err
		}
		for _, ep := range episodes {
			if err := skipUnstartedIntegrityActions(tx, row.TenantID, ep.ID, "Check suppressed by operator"); err != nil {
				return err
			}
		}
		if err := tx.Create(row).Error; err != nil {
			return err
		}
		return tx.Create(&models.AuditLog{TenantID: p.TenantID, UserID: p.UserID, UserEmail: p.Email, Action: "feed_integrity.suppression.create", TargetService: "cms", TargetResource: row.PublicID.String(), Status: "success", Payload: integrityJSON(map[string]interface{}{"check": row.CheckKey, "scope": row.Scope, "expires_at": row.ExpiresAt})}).Error
	})
}
