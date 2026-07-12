package controllers

import (
	"log"
	"time"

	"content-management-system/src/models"

	"gorm.io/gorm"
)

// Real User Experience — retention sweeper. Raw events are short-lived evidence;
// rollups, incidents, and the ledger are the durable record. Deleting raw events
// must never touch rollups (plan §14 + validation criterion 10).

// SweepExperienceRetention deletes expired raw events and high-resolution
// rollups per each tenant's policy. Bounded, idempotent, safe to run often.
func SweepExperienceRetention(db *gorm.DB) {
	var policies []models.ExperiencePolicy
	if err := db.Find(&policies).Error; err != nil {
		return
	}
	now := time.Now()
	for _, p := range policies {
		rawCutoff := now.AddDate(0, 0, -maxInt(p.RawRetentionDays, 1))
		if r := db.Where("tenant_id = ? AND received_at < ?", p.TenantID, rawCutoff).
			Delete(&models.ExperienceEvent{}); r.Error != nil {
			log.Printf("[experience] raw retention sweep error (tenant=%s): %v", p.TenantID, r.Error)
		}

		// Minute-resolution rollups are pruned aggressively; hourly rollups are
		// the durable aggregate history.
		minuteCutoff := now.Add(-time.Duration(maxInt(p.MinuteRollupRetentionHours, 1)) * time.Hour)
		db.Where("tenant_id = ? AND resolution = ? AND bucket_start < ?", p.TenantID, "minute", minuteCutoff).
			Delete(&models.ExperienceMetricRollup{})

		hourCutoff := now.AddDate(0, 0, -maxInt(p.HourRollupRetentionDays, 1))
		db.Where("tenant_id = ? AND resolution = ? AND bucket_start < ?", p.TenantID, "hour", hourCutoff).
			Delete(&models.ExperienceMetricRollup{})

		// Expired suppressions and resolved-long-ago incidents are kept
		// indefinitely as record (like the sibling systems' episodes); only raw
		// evidence and high-res rollups are swept.
	}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
