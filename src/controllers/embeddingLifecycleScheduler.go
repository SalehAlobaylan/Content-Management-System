package controllers

import (
	"log"
	"time"

	"content-management-system/src/models"

	"gorm.io/gorm"
)

// StartEmbeddingLifecycleHeartbeat starts the audit scheduler — a once-a-minute
// tick that fires a scheduled audit when `audit_interval_minutes` has elapsed
// and `audit_enabled` is true. Observation only; the campaign tick (Slice 3) is
// separate. Mirrors the family heartbeat pattern (System Health / Media Studio).
func StartEmbeddingLifecycleHeartbeat(db *gorm.DB) {
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			tickEmbeddingLifecycle(db)
			tickEmbeddingCampaigns(db)
		}
	}()
	log.Println("Embedding Lifecycle heartbeat started")
}

// tickEmbeddingCampaigns advances any running campaign by one bounded batch,
// unless campaigns are paused by policy. Separate from the audit tick so a
// campaign never blinds observation and vice versa.
func tickEmbeddingCampaigns(db *gorm.DB) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("embedding campaign tick panic: %v", r)
		}
	}()

	var policy models.EmbeddingLifecyclePolicy
	if err := db.Where("tenant_id = ?", embeddingLifecycleTenant).First(&policy).Error; err != nil {
		return
	}
	if policy.CampaignsPausedUntil != nil && time.Now().Before(*policy.CampaignsPausedUntil) {
		return
	}
	var running []models.EmbeddingCampaign
	db.Where("state = ?", models.EmbeddingCampaignRunning).Find(&running)
	for i := range running {
		passes := running[i].BatchesPerRun
		if passes < 1 {
			passes = 1
		}
		for pass := 0; pass < passes && running[i].State == models.EmbeddingCampaignRunning; pass++ {
			if _, err := executeCampaignBatch(db, &running[i]); err != nil {
				log.Printf("campaign %d batch: %v", running[i].ID, err)
				break
			}
		}
	}

	// Advance campaigns in the verification phase — the owner centroid/cache
	// handshake and completion invariants (Slice 4).
	var verifying []models.EmbeddingCampaign
	db.Where("state = ?", models.EmbeddingCampaignVerifying).Find(&verifying)
	for i := range verifying {
		advanceCampaignVerification(db, &verifying[i])
	}
}

func tickEmbeddingLifecycle(db *gorm.DB) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("embedding lifecycle heartbeat panic: %v", r)
		}
	}()

	var policy models.EmbeddingLifecyclePolicy
	if err := db.Where("tenant_id = ?", embeddingLifecycleTenant).First(&policy).Error; err != nil {
		return // no policy yet — nothing scheduled until first cockpit visit
	}
	if !policy.AuditEnabled {
		return
	}
	interval := time.Duration(policy.AuditIntervalMinutes) * time.Minute
	if interval <= 0 {
		interval = 6 * time.Hour
	}
	if policy.LastAuditAt != nil && time.Since(*policy.LastAuditAt) < interval {
		return
	}
	if _, _, err := runEmbeddingAudit(db, models.EmbeddingRunTriggerScheduled); err != nil {
		log.Printf("scheduled embedding audit failed: %v", err)
	}
	// Retention sweep piggybacks on the scheduled audit cadence — cheap and
	// idempotent, so no separate timer.
	sweepEmbeddingLifecycleRetention(db)
}
