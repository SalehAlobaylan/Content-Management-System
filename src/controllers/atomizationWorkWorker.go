package controllers

import (
	"log"
	"sync/atomic"
	"time"

	"content-management-system/src/atomizationwork"
	"gorm.io/gorm"
)

var atomizationWorkHeartbeat atomic.Int64

func StartAtomizationWorkVerifier(db *gorm.DB) {
	runAtomizationWorkVerifier(db)
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			runAtomizationWorkVerifier(db)
		}
	}()
}
func runAtomizationWorkVerifier(db *gorm.DB) {
	if err := atomizationwork.RecoverExpired(db); err != nil {
		log.Printf("atomization work recovery failed: %v", err)
		return
	}
	if _, err := atomizationwork.VerifyOne(db); err != nil {
		log.Printf("atomization work verification failed: %v", err)
		return
	}
	atomizationWorkHeartbeat.Store(time.Now().UTC().UnixNano())
}
func AtomizationWorkVerifierHealthy(now time.Time) bool {
	last := atomizationWorkHeartbeat.Load()
	return last > 0 && now.UTC().Sub(time.Unix(0, last).UTC()) <= 90*time.Second
}
