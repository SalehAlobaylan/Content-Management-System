package pipeline

import (
	"log"
	"sync/atomic"
	"time"

	"gorm.io/gorm"
)

var workerHeartbeat atomic.Int64

// StartWorker only recovers leases and verifies persisted CMS evidence. It
// never calls Aggregation, changes content status, or replays an owner effect.
func StartWorker(db *gorm.DB) {
	runWorkerOnce(db)
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			runWorkerOnce(db)
		}
	}()
}
func WorkerHealthy(now time.Time) bool {
	at := workerHeartbeat.Load()
	return at > 0 && now.UTC().Sub(time.Unix(0, at).UTC()) <= 90*time.Second
}
func runWorkerOnce(db *gorm.DB) {
	if err := RecoverExpired(db); err != nil {
		log.Printf("pipeline repair recovery failed: %v", err)
		return
	}
	if _, err := VerifyOne(db); err != nil {
		log.Printf("pipeline repair verification failed: %v", err)
		return
	}
	workerHeartbeat.Store(time.Now().UTC().UnixNano())
}
