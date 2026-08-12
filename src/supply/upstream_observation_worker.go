package supply

import (
	"log"
	"sync/atomic"
	"time"

	"content-management-system/src/models"

	"gorm.io/gorm"
)

const upstreamObservationWorkerInterval = time.Minute

var upstreamObservationWorkerHeartbeat atomic.Int64

func StartUpstreamObservationWorker(db *gorm.DB) {
	runUpstreamObservationWorkerOnce(db)
	go func() {
		ticker := time.NewTicker(upstreamObservationWorkerInterval)
		defer ticker.Stop()
		for range ticker.C {
			runUpstreamObservationWorkerOnce(db)
		}
	}()
}

func UpstreamObservationWorkerHealthy(now time.Time) bool {
	last := upstreamObservationWorkerHeartbeat.Load()
	return last > 0 && !now.UTC().Before(time.Unix(0, last)) && now.UTC().Sub(time.Unix(0, last)) <= 3*upstreamObservationWorkerInterval
}

func runUpstreamObservationWorkerOnce(db *gorm.DB) {
	if db == nil {
		return
	}
	now := time.Now().UTC()
	var observations []models.SourceUpstreamObservation
	err := db.
		Where("replay_until IS NOT NULL AND replay_until <= ?", now.Add(time.Hour)).
		Where(`NOT EXISTS (
			SELECT 1 FROM source_upstream_observation_events expiry_event
			WHERE expiry_event.observation_id = source_upstream_observations.public_id
			  AND expiry_event.event_type = CASE
				WHEN source_upstream_observations.replay_until <= ? THEN 'replay_expired'
				ELSE 'replay_expiring'
			  END
		)`, now).
		Order("replay_until ASC").Limit(50).Find(&observations).Error
	if err != nil {
		log.Printf("upstream observation expiry scan failed: %v", err)
		return
	}
	for _, observation := range observations {
		eventType := "replay_expiring"
		if observation.ReplayUntil != nil && !observation.ReplayUntil.After(now) {
			eventType = "replay_expired"
		}
		if _, err := AppendUpstreamObservationEvent(db, observation, eventType, "cms-upstream-observation-expiry", now); err != nil {
			log.Printf("upstream observation %s expiry event failed: %v", observation.PublicID, err)
			return
		}
	}
	upstreamObservationWorkerHeartbeat.Store(now.UnixNano())
}
