package contentstage

import (
	"log"
	"sync/atomic"
	"time"

	"content-management-system/src/models"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var workerHeartbeat atomic.Int64

const (
	verificationBatchPerTick   = 32
	classificationBatchPerTick = 1
	staleCMSStageWindow        = 10 * time.Minute
)

type ClassifyFunc func(*gorm.DB, uuid.UUID)

// StartWorker performs deterministic CMS-owned classification, lease recovery,
// and artifact verification. It never invokes an external model directly and
// never dispatches Aggregation/Media effects.
func StartWorker(db *gorm.DB, classify ClassifyFunc) {
	runWorkerOnce(db, nil)
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			runWorkerOnce(db, nil)
		}
	}()
	if classify != nil {
		go func() {
			ticker := time.NewTicker(2 * time.Second)
			defer ticker.Stop()
			for {
				if err := recoverStaleCMSStages(db); err != nil {
					log.Printf("content-stage CMS recovery failed: %v", err)
				} else if _, err := runCMSStageOne(db, classify); err != nil {
					log.Printf("content-stage CMS execution failed: %v", err)
				}
				<-ticker.C
			}
		}()
	}
}

func recoverStaleCMSStages(db *gorm.DB) error {
	return db.Transaction(func(tx *gorm.DB) error {
		var requests []models.ContentStageRequest
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Where("owner=? AND state=? AND updated_at<=?", models.ContentStageOwnerCMS, models.ContentStageRunning, time.Now().UTC().Add(-staleCMSStageWindow)).
			Order("updated_at ASC").Limit(16).Find(&requests).Error; err != nil {
			return err
		}
		now := time.Now().UTC()
		for _, request := range requests {
			if err := tx.Model(&request).Updates(map[string]any{"state": models.ContentStageQueued, "claim_owner": "", "failure_class": "cms_execution_interrupted", "not_before_at": now, "updated_at": now}).Error; err != nil {
				return err
			}
			request.State, request.ClaimOwner, request.FailureClass = models.ContentStageQueued, "", "cms_execution_interrupted"
			if err := appendEvent(tx, request, nil, "cms_execution_recovered", map[string]any{"previous_effect": "unknown", "reentry": "idempotent_classification"}); err != nil {
				return err
			}
		}
		return nil
	})
}

func WorkerHealthy(now time.Time) bool {
	at := workerHeartbeat.Load()
	return at > 0 && now.UTC().Sub(time.Unix(0, at).UTC()) <= 90*time.Second
}

func runWorkerOnce(db *gorm.DB, classify ClassifyFunc) {
	if err := RecoverExpired(db); err != nil {
		log.Printf("content-stage lease recovery failed: %v", err)
		return
	}
	for index := 0; index < verificationBatchPerTick; index++ {
		found, err := VerifyShadowOne(db)
		if err != nil {
			log.Printf("content-stage shadow verification failed: %v", err)
			return
		}
		if !found {
			break
		}
	}
	for index := 0; index < verificationBatchPerTick; index++ {
		found, err := VerifyOne(db)
		if err != nil {
			log.Printf("content-stage verification failed: %v", err)
			return
		}
		if !found {
			break
		}
	}
	if classify != nil {
		for index := 0; index < classificationBatchPerTick; index++ {
			found, err := runCMSStageOne(db, classify)
			if err != nil {
				log.Printf("content-stage CMS execution failed: %v", err)
				return
			}
			if !found {
				break
			}
		}
	}
	workerHeartbeat.Store(time.Now().UTC().UnixNano())
}

func runCMSStageOne(db *gorm.DB, classify ClassifyFunc) (bool, error) {
	var request models.ContentStageRequest
	claimed := false
	err := db.Transaction(func(tx *gorm.DB) error {
		var candidates []models.ContentStageRequest
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Where("owner=? AND stage=? AND state=?", models.ContentStageOwnerCMS, models.ContentStageNewsStoryClassification, models.ContentStageQueued).
			Order("created_at ASC").Limit(16).Find(&candidates).Error; err != nil {
			return err
		}
		for _, candidate := range candidates {
			allowed, err := executionAllowed(tx, candidate.TenantID, candidate.Lane, candidate.Stage)
			if err != nil {
				return err
			}
			if !allowed {
				continue
			}
			ready, err := dependenciesVerified(tx, candidate)
			if err != nil {
				return err
			}
			if !ready {
				continue
			}
			now := time.Now().UTC()
			// Classification is a replaceable projection of the current text
			// generation. Remove any predecessor pointer before execution so a
			// no-op/failed classifier cannot make an old story look like proof for
			// the new request.
			if err := tx.Model(&models.ContentItem{}).Where("tenant_id=? AND public_id=? AND processing_generation=?", candidate.TenantID, candidate.ContentItemID, candidate.ProcessingGeneration).Update("story_id", nil).Error; err != nil {
				return err
			}
			if err := tx.Model(&candidate).Updates(map[string]any{"state": models.ContentStageRunning, "claim_owner": "cms-content-stage-worker", "claim_epoch": gorm.Expr("claim_epoch + 1"), "updated_at": now}).Error; err != nil {
				return err
			}
			candidate.State, candidate.ClaimOwner, candidate.ClaimEpoch = models.ContentStageRunning, "cms-content-stage-worker", candidate.ClaimEpoch+1
			if err := appendEvent(tx, candidate, nil, "cms_stage_began", map[string]any{"stage": candidate.Stage}); err != nil {
				return err
			}
			if err := reduceReadiness(tx, candidate.TenantID, candidate.ContentItemID, candidate.ProcessingGeneration); err != nil {
				return err
			}
			request, claimed = candidate, true
			return nil
		}
		return nil
	})
	if err != nil || !claimed {
		return false, err
	}
	stopHeartbeat := make(chan struct{})
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				_ = db.Model(&models.ContentStageRequest{}).Where("public_id=? AND state=? AND claim_owner=?", request.PublicID, models.ContentStageRunning, "cms-content-stage-worker").Update("updated_at", time.Now().UTC()).Error
			case <-stopHeartbeat:
				return
			}
		}
	}()
	func() {
		defer close(stopHeartbeat)
		classify(db, request.ContentItemID)
	}()
	if err := db.Model(&models.ContentStageRequest{}).Where("public_id=? AND state=?", request.PublicID, models.ContentStageRunning).Updates(map[string]any{"state": models.ContentStageVerifying, "updated_at": time.Now().UTC()}).Error; err != nil {
		return true, err
	}
	return true, VerifyRequest(db, request.PublicID)
}
