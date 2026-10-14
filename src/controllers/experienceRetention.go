package controllers

import (
	"content-management-system/src/models"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"log"
	"time"
)

const experienceRetentionBatch = 2000
const experienceRetentionPasses = 5

// Raw retention never exempts stalled evaluators indefinitely. A durable gap
// precedes any deletion of unevaluated evidence, under the checkpoint lock.
func sweepExperienceTenant(db *gorm.DB, p models.ExperiencePolicy, now time.Time) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=?", p.TenantID).First(&p).Error; err != nil {
			return err
		}
		cutoff := now.AddDate(0, 0, -maxInt(p.RawRetentionDays, 1))
		for pass := 0; pass < experienceRetentionPasses; pass++ {
			var events []models.ExperienceEvent
			if err := tx.Select("id,received_at").Where("tenant_id=? AND received_at<?", p.TenantID, cutoff).Order("id ASC").Limit(experienceRetentionBatch).Find(&events).Error; err != nil {
				return err
			}
			if len(events) == 0 {
				break
			}
			var ids []uint
			gap := false
			for _, ev := range events {
				ids = append(ids, ev.ID)
				if p.LastEvaluatedBucket == nil || floorBucket(ev.ReceivedAt).After(*p.LastEvaluatedBucket) {
					gap = true
				}
			}
			if gap {
				if err := tx.Create(&models.ExperienceAction{TenantID: p.TenantID, ActionClass: "evidence_gap", Label: "Expired unevaluated raw evidence removed; recovery interrupted", Guardrail: "raw_retention_horizon", Evidence: jsonBytes(map[string]any{"cutoff": cutoff, "events": len(ids), "window_start": events[0].ReceivedAt})}).Error; err != nil {
					return err
				}
				if err := interruptIncidents(tx, p.TenantID, "", floorBucket(cutoff)); err != nil {
					return err
				}
			}
			if err := tx.Where("id IN ?", ids).Delete(&models.ExperienceEvent{}).Error; err != nil {
				return err
			}
		}
		for _, bound := range []struct {
			resolution string
			cutoff     time.Time
		}{{"minute", now.Add(-time.Duration(maxInt(p.MinuteRollupRetentionHours, 1)) * time.Hour)}, {"hour", now.AddDate(0, 0, -maxInt(p.HourRollupRetentionDays, 1))}} {
			for pass := 0; pass < experienceRetentionPasses; pass++ {
				var ids []uint
				if err := tx.Model(&models.ExperienceMetricRollup{}).Where("tenant_id=? AND resolution=? AND bucket_start<?", p.TenantID, bound.resolution, bound.cutoff).Order("id ASC").Limit(experienceRetentionBatch).Pluck("id", &ids).Error; err != nil {
					return err
				}
				if len(ids) == 0 {
					break
				}
				if err := tx.Where("id IN ?", ids).Delete(&models.ExperienceMetricRollup{}).Error; err != nil {
					return err
				}
			}
		}
		return nil
	})
}
func SweepExperienceRetention(db *gorm.DB) {
	var policies []models.ExperiencePolicy
	if err := db.Find(&policies).Error; err != nil {
		log.Print("[experience] retention policy read failed")
		return
	}
	for _, p := range policies {
		if err := sweepExperienceTenant(db, p, time.Now().UTC()); err != nil {
			log.Printf("[experience] retention failed for tenant %s; will retry", p.TenantID)
		}
	}
}
func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
