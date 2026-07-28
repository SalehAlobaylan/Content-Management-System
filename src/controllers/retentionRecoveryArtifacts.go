package controllers

import (
	"time"

	"content-management-system/src/models"

	"gorm.io/gorm"
)

// sweepExpiredRetentionRecoveryArtifacts is deliberately a lifecycle-only
// background task: it can delete only an already-expired object named in the
// durable ledger. It never discovers content and never changes canonical News.
func sweepExpiredRetentionRecoveryArtifacts(db *gorm.DB) {
	now := time.Now().UTC()
	var artifacts []models.RetentionRecoveryArtifact
	if err := db.Where("state IN ? AND expires_at <= ?", []string{"verified", "expired", "delete_failed"}, now).
		Order("expires_at ASC").Limit(20).Find(&artifacts).Error; err != nil {
		return
	}
	for _, artifact := range artifacts {
		if artifact.State == "verified" {
			_ = db.Model(&models.RetentionRecoveryArtifact{}).Where("id = ?", artifact.ID).Update("state", "expired").Error
		}
		if err := deleteRecoveryArtifact(artifact.ArtifactKey); err != nil {
			_ = db.Model(&models.RetentionRecoveryArtifact{}).Where("id = ?", artifact.ID).Updates(map[string]interface{}{"state": "delete_failed", "last_error": err.Error()}).Error
			continue
		}
		_ = db.Model(&models.RetentionRecoveryArtifact{}).Where("id = ?", artifact.ID).Updates(map[string]interface{}{"state": "deleted", "deleted_at": now, "last_error": ""}).Error
	}
}
