package supply

import (
	"log"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

const (
	projectionWorkerInterval = 5 * time.Second
	projectionWorkerLease    = 45 * time.Second
)

var projectionWorkerLastHeartbeat atomic.Int64

// StartProjectionWorker starts CMS-only current-state reduction. It owns no
// provider effects and does not dispatch or inspect an Aggregation queue.
func StartProjectionWorker(db *gorm.DB) {
	owner := "cms-source-run-projection-" + uuid.NewString()
	runProjectionWorkerOnce(db, owner)
	go func() {
		ticker := time.NewTicker(projectionWorkerInterval)
		defer ticker.Stop()
		for range ticker.C {
			runProjectionWorkerOnce(db, owner)
		}
	}()
}

func ProjectionWorkerHealthy(now time.Time) bool {
	last := projectionWorkerLastHeartbeat.Load()
	return last > 0 && now.UTC().Sub(time.Unix(0, last)) <= projectionWorkerLease*2
}

func runProjectionWorkerOnce(db *gorm.DB, owner string) {
	if err := AdvanceSourceRunManifests(db, 64); err != nil {
		log.Printf("source-run manifest advancement failed: %v", err)
	}
	lease, claimed, err := ClaimNextProjectionWork(db, owner, projectionWorkerLease)
	if err != nil {
		log.Printf("source-run projection claim failed: %v", err)
		return
	}
	projectionWorkerLastHeartbeat.Store(time.Now().UTC().UnixNano())
	if !claimed {
		return
	}
	if err := ApplyProjectionLease(db, lease, owner); err != nil {
		log.Printf("source-run projection %s failed: %v", lease.Work.PublicID, err)
		_ = FailProjectionWork(db, lease.Work.TenantID, lease.Work.PublicID.String(), owner, lease.ClaimToken.String(), err.Error())
	}
}
