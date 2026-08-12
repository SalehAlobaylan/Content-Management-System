package supply

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"content-management-system/src/models"

	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	sourceRunSchedulerInterval       = time.Minute
	sourceRunSchedulerBatch          = 32
	sourceRunSchedulerHeartbeatGrace = 3 * time.Minute
)

var sourceRunSchedulerHeartbeat atomic.Int64

func SourceRunSchedulerHealthy(now time.Time) bool {
	last := sourceRunSchedulerHeartbeat.Load()
	return last > 0 && now.UTC().Sub(time.Unix(0, last).UTC()) <= sourceRunSchedulerHeartbeatGrace
}

func markSourceRunSchedulerHeartbeat(now time.Time) {
	sourceRunSchedulerHeartbeat.Store(now.UTC().UnixNano())
}

// AdmitDueSourceRuns turns already-due source state into immutable CMS
// requests. It is admission only: no queue, HTTP executor, or provider is
// invoked here. A source without an explicit next_due_at is intentionally not
// guessed into a default schedule.
func AdmitDueSourceRuns(db *gorm.DB, now time.Time, limit int) ([]models.SourceRunRequest, error) {
	if db == nil || limit < 1 || limit > sourceRunSchedulerBatch {
		return nil, fmt.Errorf("source-run admission requires a bounded batch")
	}
	now = now.UTC()
	var sources []models.ContentSource
	// Two passes per tenant prevents a single cold-start tenant from consuming
	// the entire global batch. The unique request identity remains the final
	// concurrency fence, while the per-source advisory lock below serializes
	// budget/control/source-version rechecks across replicas.
	if err := db.Raw(`SELECT ranked.* FROM (
		SELECT content_sources.*, ROW_NUMBER() OVER (PARTITION BY tenant_id ORDER BY next_due_at ASC, public_id ASC) AS tenant_rank
		FROM content_sources
		WHERE tenant_id <> '' AND is_active = TRUE AND next_due_at IS NOT NULL AND next_due_at <= ?
		  AND (intake_circuit_until IS NULL OR intake_circuit_until <= ?)
	) ranked WHERE ranked.tenant_rank <= 2
	ORDER BY ranked.tenant_rank ASC, ranked.next_due_at ASC, ranked.tenant_id ASC, ranked.public_id ASC LIMIT ?`, now, now, limit).Scan(&sources).Error; err != nil {
		return nil, err
	}
	admitted := make([]models.SourceRunRequest, 0, len(sources))
	for _, selected := range sources {
		var admittedRequest models.SourceRunRequest
		created := false
		err := db.Transaction(func(tx *gorm.DB) error {
			var acquired bool
			lockKey := "source-run-admission/v1/" + selected.TenantID + "/" + selected.PublicID.String()
			if err := tx.Raw("SELECT pg_try_advisory_xact_lock(hashtextextended(?, 0))", lockKey).Scan(&acquired).Error; err != nil || !acquired {
				return err
			}
			var source models.ContentSource
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=? AND is_active=TRUE AND next_due_at IS NOT NULL AND next_due_at<=? AND (intake_circuit_until IS NULL OR intake_circuit_until<=?)", selected.TenantID, selected.PublicID, now, now).First(&source).Error; err != nil {
				if err == gorm.ErrRecordNotFound {
					return nil
				}
				return err
			}
			intakeEnabled, err := MayScheduleNormalIntake(tx, source.TenantID)
			if err != nil || !intakeEnabled {
				return err
			}
			if err := RequireDurableAdmission(tx, source.TenantID, source.Category); err != nil {
				return nil
			}
			var active int64
			if err := tx.Model(&models.SourceRunRequest{}).Where("tenant_id=? AND content_source_id=? AND state IN ?", source.TenantID, source.PublicID, []string{string(RequestRequested), string(RequestAccepted), string(RequestRunning), string(RequestVerificationRequired)}).Count(&active).Error; err != nil {
				return err
			}
			if active > 0 {
				return nil
			}
			identity, err := scheduledRequestIdentity(source)
			if err != nil {
				return err
			}
			admittedRequest, created, err = CreateRequest(tx, CreateRequestInput{Source: source, Identity: identity, RequestedBy: "schedule", EvidenceFingerprint: "source-next-due:" + source.NextDueAt.UTC().Format(time.RFC3339Nano), Metadata: []byte(`{"schema_version":"source-run/v1","max_results":50}`)})
			return err
		})
		if err != nil {
			return admitted, err
		}
		if created {
			admitted = append(admitted, admittedRequest)
		}
	}
	return admitted, nil
}

// AdmitDeferredObservationRuns drains retained upstream identities before
// baseline polling. Each request owns exactly one observation so Aggregation
// never receives a browser/provider-selected replay target or an ambiguous
// batch. The observation reservation and request are committed atomically.
func AdmitDeferredObservationRuns(db *gorm.DB, now time.Time, limit int) ([]models.SourceRunRequest, error) {
	if db == nil || limit < 1 || limit > sourceRunSchedulerBatch {
		return nil, fmt.Errorf("deferred observation admission requires a bounded batch")
	}
	now = now.UTC()
	var observations []models.SourceUpstreamObservation
	if err := db.Raw(`SELECT ranked.* FROM (
		SELECT o.*, ROW_NUMBER() OVER (PARTITION BY o.tenant_id ORDER BY o.replay_until ASC, o.observed_at ASC, o.public_id ASC) tenant_rank
		FROM source_upstream_observations o
		LEFT JOIN source_upstream_observation_dispositions d
		  ON d.tenant_id=o.tenant_id AND d.observation_id=o.public_id
		WHERE o.replay_until IS NOT NULL AND o.replay_until > ?
		  AND COALESCE(d.disposition, 'deferred') IN ('deferred','replay_expiring')
		  AND NOT EXISTS (
			SELECT 1 FROM source_upstream_observation_events e
			WHERE e.tenant_id=o.tenant_id AND e.observation_id=o.public_id
			  AND e.event_type='materialization_reserved'
		  )
	) ranked WHERE ranked.tenant_rank <= 2
	ORDER BY ranked.tenant_rank ASC, ranked.replay_until ASC, ranked.observed_at ASC, ranked.tenant_id ASC LIMIT ?`, now, limit).Scan(&observations).Error; err != nil {
		return nil, err
	}
	admitted := make([]models.SourceRunRequest, 0, len(observations))
	for _, selected := range observations {
		var request models.SourceRunRequest
		created := false
		err := db.Transaction(func(tx *gorm.DB) error {
			var acquired bool
			lockKey := "source-run-deferred-drain/v1/" + selected.TenantID + "/" + selected.PublicID.String()
			if err := tx.Raw("SELECT pg_try_advisory_xact_lock(hashtextextended(?, 0))", lockKey).Scan(&acquired).Error; err != nil || !acquired {
				return err
			}
			var observation models.SourceUpstreamObservation
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=? AND replay_until>?", selected.TenantID, selected.PublicID, now).First(&observation).Error; err != nil {
				if err == gorm.ErrRecordNotFound {
					return nil
				}
				return err
			}
			var disposition models.SourceUpstreamObservationDisposition
			if err := tx.Where("tenant_id=? AND observation_id=?", observation.TenantID, observation.PublicID).First(&disposition).Error; err == nil {
				if disposition.Disposition != "deferred" && disposition.Disposition != "replay_expiring" {
					return nil
				}
			} else if err != gorm.ErrRecordNotFound {
				return err
			}
			var source models.ContentSource
			if err := tx.Where("tenant_id=? AND public_id=? AND is_active=TRUE", observation.TenantID, observation.ContentSourceID).First(&source).Error; err != nil {
				if err == gorm.ErrRecordNotFound {
					return nil
				}
				return err
			}
			enabled, err := MayScheduleNormalIntake(tx, source.TenantID)
			if err != nil || !enabled {
				return err
			}
			if err := RequireDurableAdmission(tx, source.TenantID, source.Category); err != nil {
				return nil
			}
			var active int64
			if err := tx.Model(&models.SourceRunRequest{}).Where("tenant_id=? AND content_source_id=? AND state IN ?", source.TenantID, source.PublicID, []string{string(RequestRequested), string(RequestAccepted), string(RequestRunning), string(RequestVerificationRequired)}).Count(&active).Error; err != nil {
				return err
			}
			if active > 0 {
				return nil
			}
			identity, err := deferredObservationRequestIdentity(source, observation)
			if err != nil {
				return err
			}
			metadata, _ := json.Marshal(map[string]any{
				"schema_version": "source-run/v1", "max_results": 1,
				"deferred_observation_ids":   []string{observation.PublicID.String()},
				"deferred_upstream_item_ids": []string{observation.UpstreamItemID},
				"deferred_observation_map":   map[string]string{observation.UpstreamItemID: observation.PublicID.String()},
			})
			request, created, err = CreateRequest(tx, CreateRequestInput{Source: source, Identity: identity, RequestedBy: "schedule", EvidenceFingerprint: "upstream-observation:" + observation.UpstreamFingerprint, Metadata: datatypes.JSON(metadata), ExpiresAt: observation.ReplayUntil, DeadlineAt: observation.ReplayUntil})
			if err != nil || !created {
				return err
			}
			payload, _ := json.Marshal(map[string]any{"schema_version": "source-run-upstream-observation-event/v1", "drain_request_id": request.PublicID, "upstream_fingerprint": observation.UpstreamFingerprint})
			event := models.SourceUpstreamObservationEvent{PublicID: uuid.New(), TenantID: observation.TenantID, EventKey: observationCausationEventKey(observation.TenantID, observation.PublicID, "materialization_reserved", request.PublicID.String()), ObservationID: observation.PublicID, EventType: "materialization_reserved", CausationID: request.PublicID.String(), Payload: datatypes.JSON(payload), OccurredAt: now}
			if err := tx.Create(&event).Error; err != nil {
				return err
			}
			return tx.Create(&models.SourceRunProjectionWork{PublicID: uuid.New(), TenantID: observation.TenantID, EvidenceKind: "upstream_observation_event", EvidenceID: event.PublicID, ReducerVersion: "source-run-upstream-observation/v1", State: "queued"}).Error
		})
		if err != nil {
			return admitted, err
		}
		if created {
			admitted = append(admitted, request)
		}
	}
	return admitted, nil
}

func deferredObservationRequestIdentity(source models.ContentSource, observation models.SourceUpstreamObservation) (RequestIdentity, error) {
	if observation.PublicID == uuid.Nil || observation.ContentSourceID != source.PublicID || observation.TenantID != source.TenantID {
		return RequestIdentity{}, fmt.Errorf("deferred observation does not match its source")
	}
	lane := strings.TrimSpace(source.Category)
	if lane != models.SourceCategoryNews && lane != models.SourceCategoryMedia {
		return RequestIdentity{}, fmt.Errorf("source category is not an admitted drain lane")
	}
	version := source.SourceConfigVersion
	if version < 1 {
		version = 1
	}
	argument := sha256.Sum256([]byte(strings.Join([]string{"deferred-drain/v1", observation.PublicID.String(), observation.UpstreamItemID, observation.UpstreamFingerprint, observation.ProviderVersion}, "\n")))
	policy := sha256.Sum256([]byte(strings.Join([]string{"source-deferred-drain/v1", lane, fmt.Sprintf("%d", version)}, "\n")))
	return RequestIdentity{TenantID: source.TenantID, ContentSourceID: source.PublicID.String(), Lane: lane, Purpose: "deferred_drain", CadenceWindowStart: observation.ObservedAt.UTC(), SourceConfigVersion: version, PolicyFingerprint: hex.EncodeToString(policy[:]), ArgumentFingerprint: hex.EncodeToString(argument[:])}, nil
}

func scheduledRequestIdentity(source models.ContentSource) (RequestIdentity, error) {
	if strings.TrimSpace(source.TenantID) == "" || source.PublicID.String() == "" || source.NextDueAt == nil {
		return RequestIdentity{}, fmt.Errorf("scheduled source is missing explicit tenant or due evidence")
	}
	version := source.SourceConfigVersion
	if version < 1 {
		version = 1
	}
	lane := strings.TrimSpace(source.Category)
	if lane != models.SourceCategoryNews && lane != models.SourceCategoryMedia {
		return RequestIdentity{}, fmt.Errorf("source category is not an admitted schedule lane")
	}
	argument := sha256.Sum256([]byte(strings.Join([]string{string(source.Type), lane, derefSourceURL(source.FeedURL), string(source.APIConfig)}, "\n")))
	policy := sha256.Sum256([]byte(strings.Join([]string{"source-schedule/v1", lane, fmt.Sprintf("%d", source.FetchIntervalMinutes), fmt.Sprintf("%d", version)}, "\n")))
	return RequestIdentity{TenantID: source.TenantID, ContentSourceID: source.PublicID.String(), Lane: lane, Purpose: "baseline", CadenceWindowStart: source.NextDueAt.UTC(), SourceConfigVersion: version, PolicyFingerprint: hex.EncodeToString(policy[:]), ArgumentFingerprint: hex.EncodeToString(argument[:])}, nil
}

func derefSourceURL(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

// StartSourceRunScheduler runs bounded CMS-only admission. It does not make
// source work executable; that requires the later Aggregation dispatcher
// cutover and its independently qualified receipt durability contract.
func StartSourceRunScheduler(db *gorm.DB) {
	runSourceRunSchedulerOnce(db)
	go func() {
		ticker := time.NewTicker(sourceRunSchedulerInterval)
		defer ticker.Stop()
		for range ticker.C {
			runSourceRunSchedulerOnce(db)
		}
	}()
}

func runSourceRunSchedulerOnce(db *gorm.DB) {
	now := time.Now().UTC()
	drained, err := AdmitDeferredObservationRuns(db, now, sourceRunSchedulerBatch)
	if err != nil {
		log.Printf("source-run deferred observation admission failed: %v", err)
		return
	}
	remaining := sourceRunSchedulerBatch - len(drained)
	if remaining > 0 {
		_, err = AdmitDueSourceRuns(db, now, remaining)
	}
	if err != nil {
		log.Printf("source-run scheduler admission failed: %v", err)
		return
	}
	markSourceRunSchedulerHeartbeat(time.Now().UTC())
}
