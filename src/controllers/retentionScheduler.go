package controllers

import (
	"time"

	"content-management-system/src/models"

	"gorm.io/gorm"
)

// StartRetentionHeartbeat polls persisted policy state. The runner is
// intentionally observe-only in this slice; scheduled work can write samples
// and ledgers but cannot mutate canonical content.
func StartRetentionHeartbeat(db *gorm.DB) {
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		runRetentionDue(db)
		for range ticker.C {
			runRetentionDue(db)
		}
	}()
}

func runRetentionDue(db *gorm.DB) {
	var policies []models.RetentionPolicy
	if err := db.Where("enabled = TRUE").
		Order("tenant_id ASC").Limit(100).Find(&policies).Error; err != nil {
		return
	}
	now := time.Now().UTC()
	for _, policy := range policies {
		if policy.TenantID != retentionV1Tenant ||
			(policy.PausedUntil != nil && policy.PausedUntil.After(now)) {
			continue
		}
		interval := time.Duration(policy.ScheduleIntervalMinutes) * time.Minute
		if interval < 15*time.Minute {
			interval = 6 * time.Hour
		}
		if policy.LastRunAt != nil && now.Sub(*policy.LastRunAt) < interval {
			continue
		}
		_, _ = runRetention(db, policy.TenantID, "scheduled", "automation")
	}
}
