package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/pipeline"
	"content-management-system/src/supply"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"golang.org/x/sync/singleflight"
	"gorm.io/gorm"
)

const (
	mediaSupplyStatusCacheTTL        = 10 * time.Second
	mediaSupplyStatusCacheMaxTenants = 128
)

type cachedMediaSupplyStatus struct {
	response  mediaSupplyStatusResponse
	expiresAt time.Time
}

var mediaSupplyStatusCache struct {
	sync.Mutex
	entries map[string]cachedMediaSupplyStatus
	group   singleflight.Group
}

// mediaSupplyStatusResponse is the first read-only Supply Continuity API. Its
// input is the same bounded CMS schedule and delivery proof used by the Media
// Circulation cockpit; it never reads Redis, calls a provider, or offers a
// retry/action shortcut.
type mediaSupplyStatusResponse struct {
	SupplyEvaluation supply.SupplyEvaluation             `json:"supply_evaluation"`
	Schedules        mediaCirculationSourceScheduleProof `json:"schedules"`
	Delivery         mediaCirculationDeliveryProof       `json:"delivery"`
	Evaluator        mediaSupplyEvaluatorStatus          `json:"evaluator"`
	Operational      mediaSupplyOperationalHealth        `json:"operational"`
	Exposure         supply.PodsExposureProof            `json:"exposure"`
}

// mediaSupplyOperationalHealth is a bounded dashboard read model, not an
// automation trigger. Worker readiness answers whether CMS can safely accept
// its durable protocol; backlog counts answer whether operators should look
// closer. Neither result ever starts, retries, or cancels work.
type mediaSupplyOperationalHealth struct {
	State     string                                 `json:"state"`
	Workers   map[string]string                      `json:"workers"`
	Owners    map[string]supply.SupplyOwnerReadiness `json:"owners"`
	Backlogs  map[string]int64                       `json:"backlogs"`
	Metrics   supply.SupplyOperationalMetrics        `json:"metrics"`
	Unknowns  []string                               `json:"unknowns"`
	Generated time.Time                              `json:"generated_at"`
}

// mediaSupplyEvaluatorStatus is the narrow liveness proof for the scheduled
// evaluator. It never turns a current headline into an action capability; it
// only says whether CMS has recently persisted an observation checkpoint.
type mediaSupplyEvaluatorStatus struct {
	RecordingEnabled      *bool      `json:"recording_enabled"`
	WorkerState           string     `json:"worker_state"`
	WorkerLastHeartbeatAt *time.Time `json:"worker_last_heartbeat_at,omitempty"`
	WorkerStaleAfterAt    *time.Time `json:"worker_stale_after_at,omitempty"`
	LastOutcome           string     `json:"last_outcome,omitempty"`
	LastTrigger           string     `json:"last_trigger,omitempty"`
	LastObservedAt        *time.Time `json:"last_observed_at,omitempty"`
	LastEvaluatedAt       *time.Time `json:"last_evaluated_at,omitempty"`
	EvaluationDigest      *string    `json:"evaluation_digest,omitempty"`
	Unknowns              []string   `json:"unknowns"`
}

// GetMediaSupplyStatus returns one tenant-scoped, deterministic headline over
// the current CMS evidence. It is intentionally a separate endpoint so future
// episode/action work cannot accidentally turn the cockpit economics response
// into an execution surface.
func GetMediaSupplyStatus(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	c.JSON(http.StatusOK, getCachedMediaSupplyStatus(c.MustGet("db").(*gorm.DB), principal.TenantID))
}

// getCachedMediaSupplyStatus coalesces concurrent dashboard reads per tenant.
// The response is evidence-only and explicitly carries its generation time, so
// a short TTL is safer than allowing multiple Console tabs to repeat the same
// expensive bounded joins against Neon. No action or worker state is cached as
// authority; plan/claim paths always re-read CMS state.
func getCachedMediaSupplyStatus(db *gorm.DB, tenantID string) mediaSupplyStatusResponse {
	now := time.Now().UTC()
	mediaSupplyStatusCache.Lock()
	if mediaSupplyStatusCache.entries == nil {
		mediaSupplyStatusCache.entries = make(map[string]cachedMediaSupplyStatus)
	}
	if cached, ok := mediaSupplyStatusCache.entries[tenantID]; ok && now.Before(cached.expiresAt) {
		mediaSupplyStatusCache.Unlock()
		return cached.response
	}
	mediaSupplyStatusCache.Unlock()

	value, _, _ := mediaSupplyStatusCache.group.Do(tenantID, func() (any, error) {
		// Another request may have filled the cache while this caller waited for
		// the singleflight slot; check again before doing the database work.
		checkNow := time.Now().UTC()
		mediaSupplyStatusCache.Lock()
		if cached, ok := mediaSupplyStatusCache.entries[tenantID]; ok && checkNow.Before(cached.expiresAt) {
			mediaSupplyStatusCache.Unlock()
			return cached.response, nil
		}
		mediaSupplyStatusCache.Unlock()

		response := buildMediaSupplyStatus(db, tenantID)
		mediaSupplyStatusCache.Lock()
		for key, cached := range mediaSupplyStatusCache.entries {
			if time.Now().UTC().After(cached.expiresAt) {
				delete(mediaSupplyStatusCache.entries, key)
			}
		}
		if len(mediaSupplyStatusCache.entries) >= mediaSupplyStatusCacheMaxTenants {
			for key := range mediaSupplyStatusCache.entries {
				delete(mediaSupplyStatusCache.entries, key)
				break
			}
		}
		mediaSupplyStatusCache.entries[tenantID] = cachedMediaSupplyStatus{response: response, expiresAt: time.Now().UTC().Add(mediaSupplyStatusCacheTTL)}
		mediaSupplyStatusCache.Unlock()
		return response, nil
	})
	return value.(mediaSupplyStatusResponse)
}

func buildMediaSupplyStatus(db *gorm.DB, tenantID string) mediaSupplyStatusResponse {
	schedules := loadMediaCirculationSourceScheduleProof(db, tenantID)
	delivery := loadMediaCirculationDeliveryProof(db, tenantID)
	exposure := supply.BuildPodsExposureProof(db, tenantID, time.Now().UTC())

	input := supply.SupplyEvaluationInput{
		TenantID:          tenantID,
		EvaluatedAt:       schedules.GeneratedAt,
		ScheduleAvailable: schedules.Available,
		ScheduleTruncated: len(schedules.Items) == mediaCirculationSourceScheduleLimit,
		Schedule:          make([]supply.SupplyScheduleObservation, 0, len(schedules.Items)),
		Delivery:          make([]supply.SupplyDeliveryObservation, 0, len(delivery.Items)),
		Exposure:          &exposure,
	}
	for _, item := range schedules.Items {
		input.Schedule = append(input.Schedule, supply.SupplyScheduleObservation{
			SourceID: item.SourceID.String(),
			State:    item.ScheduleState,
		})
	}
	for _, item := range delivery.Items {
		input.Delivery = append(input.Delivery, supply.SupplyDeliveryObservation{
			RequestID:       item.RequestID.String(),
			SourceID:        item.SourceID.String(),
			RequestState:    item.RequestState,
			State:           item.DeliveryState,
			TerminalOutcome: item.TerminalOutcome,
		})
	}

	return mediaSupplyStatusResponse{
		SupplyEvaluation: supply.EvaluateMediaSupply(input),
		Schedules:        schedules,
		Delivery:         delivery,
		Evaluator:        loadMediaSupplyEvaluatorStatus(db, tenantID),
		Operational:      loadMediaSupplyOperationalHealth(db, tenantID),
		Exposure:         exposure,
	}
}

func loadMediaSupplyOperationalHealth(db *gorm.DB, tenantID string) mediaSupplyOperationalHealth {
	now := time.Now().UTC()
	workers := map[string]string{
		"source_run_scheduler": workerState(supply.SourceRunSchedulerHealthy(now)),
		"receipt_projection":   workerState(supply.ProjectionWorkerHealthy(now)),
		"source_recovery":      workerState(supply.RecoveryWorkerHealthy(now)),
		"source_reconciler":    workerState(supply.ReconcilerWorkerHealthy(now)),
		"supply_action":        workerState(supply.SupplyActionWorkerHealthy(now)),
		"supply_evaluator":     workerState(supply.MediaSupplyEvaluatorWorkerHealthy(now)),
		"pipeline_repair":      workerState(pipeline.WorkerHealthy(now)),
		"artifact_coverage":    workerState(ArtifactCoverageWorkerHealthy(now)),
		"atomization_work":     workerState(AtomizationWorkVerifierHealthy(now)),
		"studio_clearance":     workerState(StudioClearanceWorkerHealthy(now)),
		"upstream_observation": workerState(supply.UpstreamObservationWorkerHealthy(now)),
	}
	owners := supply.SupplyOwnerReadinessAt(now)
	backlogs := map[string]int64{}
	unknowns := make([]string, 0, 7)
	for owner, readiness := range owners {
		if readiness.State != "ready" {
			unknowns = append(unknowns, "Supply owner "+owner+" is unavailable: "+readiness.Detail)
		}
	}
	for key, query := range map[string]func(*int64) error{
		"projection_pending": func(count *int64) error {
			return db.Model(&models.SourceRunProjectionWork{}).Where("state IN ?", []string{"queued", "claimed", "running"}).Count(count).Error
		},
		"retained_receipts": func(count *int64) error {
			return db.Model(&models.SourceRunRetainedReceipt{}).Where("tenant_id=? AND state=?", tenantID, "retained").Count(count).Error
		},
		"actions_in_flight": func(count *int64) error {
			return db.Model(&models.MediaSupplyActionRequest{}).Where("tenant_id=? AND state IN ?", tenantID, []string{"queued", "claimed", "running", "verifying", "uncertain"}).Count(count).Error
		},
		"episodes_open": func(count *int64) error {
			return db.Model(&models.MediaSupplyEpisode{}).Where("tenant_id=? AND state IN ?", tenantID, []string{models.MediaSupplyEpisodeOpen, models.MediaSupplyEpisodeRecovering}).Count(count).Error
		},
	} {
		var count int64
		if err := query(&count); err != nil {
			unknowns = append(unknowns, "CMS could not read "+strings.ReplaceAll(key, "_", " ")+".")
			continue
		}
		backlogs[key] = count
	}
	state := "ready"
	for _, value := range workers {
		if value != "ready" {
			state = "degraded"
			break
		}
	}
	if state == "ready" {
		for _, readiness := range owners {
			if readiness.State != "ready" {
				state = "degraded"
				break
			}
		}
	}
	if state == "ready" && (len(unknowns) > 0 || backlogs["projection_pending"] > 0 || backlogs["retained_receipts"] > 0 || backlogs["episodes_open"] > 0) {
		state = "attention"
	}
	return mediaSupplyOperationalHealth{State: state, Workers: workers, Owners: owners, Backlogs: backlogs, Metrics: supply.BuildSupplyOperationalMetrics(db, tenantID, now), Unknowns: unknowns, Generated: now}
}

func workerState(healthy bool) string {
	if healthy {
		return "ready"
	}
	return "stale"
}

func loadMediaSupplyEvaluatorStatus(db *gorm.DB, tenantID string) mediaSupplyEvaluatorStatus {
	worker := supply.MediaSupplyEvaluatorWorkerStatusAt(time.Now().UTC())
	result := mediaSupplyEvaluatorStatus{
		WorkerState: worker.State, WorkerLastHeartbeatAt: worker.LastHeartbeat,
		WorkerStaleAfterAt: worker.StaleAfterAt, Unknowns: []string{},
	}
	if worker.State == "not_started" {
		result.Unknowns = append(result.Unknowns, "CMS has not completed a Supply evaluator scheduler pass in this process.")
	}
	if worker.State == "stale" {
		result.Unknowns = append(result.Unknowns, "The Supply evaluator scheduler heartbeat is stale; current checkpoint freshness cannot be assumed.")
	}
	enabled, err := mediaSupplyEvaluationRecordingEnabled(db, tenantID)
	if err != nil {
		result.Unknowns = append(result.Unknowns, "CMS could not read the Supply evaluator control state.")
	} else {
		result.RecordingEnabled = &enabled
	}

	var checkpoint models.MediaSupplyEvaluationCheckpoint
	err = db.Where("tenant_id = ?", strings.TrimSpace(tenantID)).First(&checkpoint).Error
	if err == gorm.ErrRecordNotFound {
		result.Unknowns = append(result.Unknowns, "CMS has not yet persisted a Supply evaluator checkpoint for this tenant.")
		return result
	}
	if err != nil {
		result.Unknowns = append(result.Unknowns, "CMS could not read the Supply evaluator checkpoint.")
		return result
	}

	result.LastOutcome = checkpoint.LastOutcome
	result.LastTrigger = checkpoint.LastTrigger
	result.LastObservedAt = &checkpoint.LastObservedAt
	result.LastEvaluatedAt = checkpoint.LastEvaluatedAt
	result.EvaluationDigest = checkpoint.EvaluationDigest
	return result
}
