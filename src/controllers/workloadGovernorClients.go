package controllers

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/workload"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type workloadDeferredError struct{ Reason string }

func (e *workloadDeferredError) Error() string { return "workload_governor: " + e.Reason }
func isWorkloadDeferred(err error) bool {
	var deferred *workloadDeferredError
	return errors.As(err, &deferred)
}

// These fields are never serialized. Only a CMS execution owner may choose the
// origin or attach a database/permit; neither browser nor model input can do so.
func withWorkloadContext(db *gorm.DB, context AISpendContext, origin string) AISpendContext {
	context.workloadDB, context.workloadOrigin = db, origin
	return context
}

func enrichmentWorkloadContext(db *gorm.DB, tenant, trigger, run string) AISpendContext {
	origin := "automation"
	// These are fixed owner-selected triggers, not a request-supplied origin.
	if trigger == "admin_enrichment" || trigger == "manual" {
		origin = "human"
	} else if trigger == "enrichment_campaign" {
		origin = "campaign"
	}
	return withWorkloadContext(db, spendContext(tenant, trigger, run), origin)
}

func enrichmentNativeTranscriptionTrigger(trigger string) string {
	if trigger == "admin_enrichment" || trigger == "enrichment_campaign" {
		return models.TranscriptionTriggerManual
	}
	return trigger
}

type controllerWorkloadLease struct {
	db       *gorm.DB
	decision workload.Decision
	begun    bool
}

func acquireControllerWorkload(db *gorm.DB, tenant, system, class, run, origin string) (*controllerWorkloadLease, error) {
	system = workloadSystemForTrigger(system)
	if run == "" {
		run = uuid.NewString()
	}
	owner := uuid.NewString()
	decision := workload.RequestPermit(db, workload.Request{TenantID: tenant, System: system, Class: class, Units: 1, RunID: run,
		Origin: origin, OwnerKind: "controller_call", OwnerID: owner, RequestKey: owner})
	if !decision.Allowed() {
		return nil, &workloadDeferredError{Reason: fmt.Sprint(decision.Reasons)}
	}
	return &controllerWorkloadLease{db: db, decision: decision}, nil
}

func workloadSystemForTrigger(trigger string) string {
	switch trigger {
	case "topics_maintenance":
		return "preferences_autopilot"
	case "ingest_story_label":
		return "classification_backfill"
	default:
		return trigger
	}
}

func (l *controllerWorkloadLease) cancelBeforeEffect() {
	if l != nil && !l.begun && l.decision.PermitID != "" {
		_ = workload.ReleasePermit(l.db, l.decision.PermitID, 0)
	}
}

func beginWorkloadSweep(db *gorm.DB, tenant, system, trigger string) (func(), error) {
	origin := "automation"
	if trigger == "manual" {
		origin = "human"
	} else if trigger == "campaign" {
		origin = "campaign"
	}
	lease, err := acquireControllerWorkload(db, tenant, system, "heavy_sweep", uuid.NewString(), origin)
	if err != nil {
		return nil, err
	}
	if lease.decision.PermitID == "" {
		return func() {}, nil
	}
	if err = workload.BeginPermit(db, lease.decision.PermitID); err != nil {
		lease.cancelBeforeEffect()
		if workloadKnownStop(err) {
			return nil, &workloadDeferredError{Reason: "Capacity changed before the database sweep"}
		}
		log.Print("workload governor begin unavailable; native sweep guard remains authoritative")
		return func() {}, nil
	}
	lease.begun = true
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				_ = workload.RenewPermit(db, lease.decision.PermitID)
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() { close(done); <-finished; _ = workload.ReleasePermit(db, lease.decision.PermitID, 1) })
	}, nil
}

// Model endpoints used here finish their synchronous effect before returning a
// response. A transport failure has no terminal proof and retains uncertainty.
// Service-local gates and native financial admission remain authoritative.
func workloadModelDo(client *http.Client, request *http.Request, context AISpendContext, class string) (*http.Response, error) {
	if context.workloadDB == nil || (context.ServingExempt && context.TriggerSource == "serving_news_summary") {
		response, err := client.Do(request)
		if err == nil && modelRejectedForCapacity(response) {
			return nil, &workloadDeferredError{Reason: "Model service deferred this operation before execution"}
		}
		return response, err
	}
	lease := context.workloadLease
	if lease == nil {
		var err error
		origin := context.workloadOrigin
		if origin == "" {
			origin = workload.OriginAutomation
		}
		lease, err = acquireControllerWorkload(context.workloadDB, context.TenantID, context.TriggerSource, class, context.SystemRunID, origin)
		if err != nil {
			return nil, err
		}
	}
	defer lease.cancelBeforeEffect()
	if lease.decision.PermitID != "" {
		if err := workload.BeginPermit(lease.db, lease.decision.PermitID); err != nil {
			if workloadKnownStop(err) {
				return nil, &workloadDeferredError{Reason: "Capacity changed before the model call; retry on the next owner tick"}
			}
			log.Print("workload governor begin unavailable; native model admission remains authoritative")
		}
	}
	lease.begun = true
	response, err := client.Do(request)
	capacityDeferred := err == nil && modelRejectedForCapacity(response)
	if lease.decision.PermitID != "" {
		if err != nil {
			_ = workload.MarkUncertain(lease.db, lease.decision.PermitID, "Direct model request has no terminal response; inspect the native operation before clearing capacity")
		} else {
			actual := 1
			if capacityDeferred {
				actual = 0
			}
			_ = workload.ReleasePermit(lease.db, lease.decision.PermitID, actual)
		}
	}
	if capacityDeferred {
		return nil, &workloadDeferredError{Reason: "Model service deferred this operation before execution"}
	}
	return response, err
}

// Only the typed owner rejection proves that no effect started. An arbitrary
// 429/503 or transport failure cannot be reclassified as a safe capacity retry.
func modelRejectedForCapacity(response *http.Response) bool {
	if response == nil || response.Body == nil || response.StatusCode != http.StatusTooManyRequests {
		return false
	}
	prefix, err := io.ReadAll(io.LimitReader(response.Body, 8193))
	if err != nil {
		response.Body = &workloadReplayBody{Reader: io.MultiReader(bytes.NewReader(prefix), response.Body), Closer: response.Body}
		return false
	}
	var body struct {
		ErrorCode string `json:"error_code"`
	}
	if len(prefix) <= 8192 && json.Unmarshal(prefix, &body) == nil && body.ErrorCode == "WORKLOAD_OVERLOADED" {
		_ = response.Body.Close()
		return true
	}
	response.Body = &workloadReplayBody{Reader: io.MultiReader(bytes.NewReader(prefix), response.Body), Closer: response.Body}
	return false
}

type workloadReplayBody struct {
	io.Reader
	io.Closer
}

func workloadKnownStop(err error) bool {
	return errors.Is(err, workload.ErrNotAdmitted) || errors.Is(err, workload.ErrUnsafeRevoke) || errors.Is(err, workload.ErrConflict)
}
