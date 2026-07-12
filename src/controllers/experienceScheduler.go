package controllers

import (
	"log"
	"sync"
	"time"

	"content-management-system/src/models"

	"gorm.io/gorm"
)

// Real User Experience — Observe scheduler. A single CMS heartbeat evaluates
// closed buckets on cadence. Single-flight across manual + scheduled triggers.
// No BullMQ, no cross-service calls (plan §14).

var experienceRunMu sync.Mutex

// tryStartExperienceRun enforces single-flight. Returns false if a run is in
// progress (manual callers map this to 409).
func tryStartExperienceRun() bool {
	return experienceRunMu.TryLock()
}

func finishExperienceRunLock() {
	experienceRunMu.Unlock()
}

// StartExperienceHeartbeat ticks every minute and evaluates when the tenant's
// policy has evaluation enabled and is not paused. Hourly buckets mean most
// ticks are no-ops (no newly-closed bucket), which is cheap. Retention sweeps
// run at most once per hour, independent of evaluation (raw events accumulate
// even when evaluation is off but ingestion is on).
func StartExperienceHeartbeat(db *gorm.DB) {
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		runExperienceDue(db)
		SweepExperienceRetention(db)
		lastSweep := time.Now()
		for range ticker.C {
			runExperienceDue(db)
			if time.Since(lastSweep) >= time.Hour {
				SweepExperienceRetention(db)
				lastSweep = time.Now()
			}
		}
	}()
}

func runExperienceDue(db *gorm.DB) {
	var policies []models.ExperiencePolicy
	if err := db.Where("evaluation_enabled = ?", true).Find(&policies).Error; err != nil {
		return
	}
	now := time.Now()
	for _, p := range policies {
		if p.PausedUntil != nil && p.PausedUntil.After(now) {
			continue
		}
		if !tryStartExperienceRun() {
			return // another run in flight; try next tick
		}
		func() {
			defer finishExperienceRunLock()
			defer func() {
				if r := recover(); r != nil {
					log.Printf("[experience] scheduled run panic (tenant=%s): %v", p.TenantID, r)
				}
			}()
			if _, err := RunExperienceEvaluation(db, p.TenantID, "scheduled"); err != nil {
				log.Printf("[experience] scheduled run error (tenant=%s): %v", p.TenantID, err)
			}
		}()
	}
}
