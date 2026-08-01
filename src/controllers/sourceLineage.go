package controllers

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"content-management-system/src/models"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

const (
	lineageStageSourceRun = "source_run"
	lineageStageIngest    = "content_ingest"
)

type SourceRunCorrelation struct {
	OperatorPlanID *uuid.UUID
	OperatorStepID *uuid.UUID
	IdempotencyKey string
}

func createSourceRunRequest(db *gorm.DB, source models.ContentSource, requestedBy, actorID string, suggestionID *uuid.UUID) (models.SourceRunRequest, error) {
	return createSourceRunRequestWithCorrelation(db, source, requestedBy, actorID, suggestionID, SourceRunCorrelation{})
}

func createSourceRunRequestWithCorrelation(db *gorm.DB, source models.ContentSource, requestedBy, actorID string, suggestionID *uuid.UUID, correlationInput SourceRunCorrelation) (models.SourceRunRequest, error) {
	requestedBy = strings.TrimSpace(requestedBy)
	if requestedBy != "approval_handoff" && requestedBy != "manual" && requestedBy != "operator" && requestedBy != "schedule" && requestedBy != "system" {
		return models.SourceRunRequest{}, fmt.Errorf("invalid source-run requester")
	}
	correlation := uuid.NewString()
	idempotencyKey := strings.TrimSpace(correlationInput.IdempotencyKey)
	if idempotencyKey == "" {
		idempotencyKey = "source-run:" + correlation
	}
	request := models.SourceRunRequest{
		TenantID: source.TenantID, ContentSourceID: source.PublicID, SourceSuggestionID: suggestionID,
		RequestedBy: requestedBy, RequestedByActorID: strings.TrimSpace(actorID), State: models.SourceRunRequested,
		OperatorPlanID: correlationInput.OperatorPlanID, OperatorStepID: correlationInput.OperatorStepID,
		CorrelationID: correlation, IdempotencyKey: idempotencyKey, RequestedAt: time.Now().UTC(), Metadata: datatypes.JSON([]byte(`{}`)),
	}
	err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&request).Error; err != nil {
			return err
		}
		return appendContentProcessingEvent(tx, models.ContentProcessingEvent{
			TenantID: request.TenantID, ContentSourceID: &request.ContentSourceID, SourceRunRequestID: &request.ID,
			Stage: lineageStageSourceRun, State: models.SourceRunRequested, Producer: "cms", CorrelationID: request.CorrelationID,
			IdempotencyKey: request.IdempotencyKey, EventClass: "source_run_requested", Payload: datatypes.JSON([]byte(`{}`)), OccurredAt: request.RequestedAt,
		})
	})
	return request, err
}

func markSourceRunAccepted(db *gorm.DB, requestID uuid.UUID, aggregationJobID string) error {
	if strings.TrimSpace(aggregationJobID) == "" {
		return fmt.Errorf("aggregation job id is required")
	}
	now := time.Now().UTC()
	return db.Transaction(func(tx *gorm.DB) error {
		var request models.SourceRunRequest
		if err := tx.Where("public_id=?", requestID).First(&request).Error; err != nil {
			return err
		}
		if request.State != models.SourceRunRequested && request.State != models.SourceRunAccepted {
			return fmt.Errorf("source run request cannot be accepted from %s", request.State)
		}
		if err := tx.Model(&request).Updates(map[string]any{"state": models.SourceRunAccepted, "aggregation_job_id": aggregationJobID, "accepted_at": now}).Error; err != nil {
			return err
		}
		request.State, request.AggregationJobID, request.AcceptedAt = models.SourceRunAccepted, aggregationJobID, &now
		return appendContentProcessingEvent(tx, models.ContentProcessingEvent{
			TenantID: request.TenantID, ContentSourceID: &request.ContentSourceID, SourceRunRequestID: &request.ID,
			Stage: lineageStageSourceRun, State: models.SourceRunAccepted, Producer: "cms", JobID: aggregationJobID,
			CorrelationID: request.CorrelationID, IdempotencyKey: request.IdempotencyKey, EventClass: "aggregation_job_accepted", Payload: datatypes.JSON([]byte(`{}`)), OccurredAt: now,
		})
	})
}

// InternalAcceptSourceRunRequest records BullMQ acceptance as a CMS fact.
// Aggregation calls it immediately after enqueueing a CMS-owned request; CMS
// never interrogates the queue to fill this transition in later.
func InternalAcceptSourceRunRequest(c *gin.Context) {
	requestID, err := uuid.Parse(strings.TrimSpace(c.Param("id")))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid source-run request ID"})
		return
	}
	var body struct {
		JobID string `json:"job_id"`
	}
	if err := c.ShouldBindJSON(&body); err != nil || strings.TrimSpace(body.JobID) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"message": "job_id is required"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	if err := markSourceRunAccepted(db, requestID, body.JobID); err != nil {
		if err == gorm.ErrRecordNotFound {
			c.JSON(http.StatusNotFound, gin.H{"message": "Source-run request not found"})
			return
		}
		c.JSON(http.StatusConflict, gin.H{"message": "Source-run request cannot be accepted"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func markSourceRunDispatchFailed(db *gorm.DB, requestID uuid.UUID, err error) {
	if err == nil {
		return
	}
	now := time.Now().UTC()
	_ = db.Transaction(func(tx *gorm.DB) error {
		var request models.SourceRunRequest
		if lookupErr := tx.Where("public_id=?", requestID).First(&request).Error; lookupErr != nil {
			return lookupErr
		}
		summary := strings.TrimSpace(err.Error())
		if len(summary) > 1000 {
			summary = summary[:1000]
		}
		if updateErr := tx.Model(&request).Updates(map[string]any{"state": models.SourceRunFailed, "finished_at": now, "failure_class": "dispatch_failed", "failure_summary": summary}).Error; updateErr != nil {
			return updateErr
		}
		return appendContentProcessingEvent(tx, models.ContentProcessingEvent{
			TenantID: request.TenantID, ContentSourceID: &request.ContentSourceID, SourceRunRequestID: &request.ID,
			Stage: lineageStageSourceRun, State: models.SourceRunFailed, Producer: "cms", CorrelationID: request.CorrelationID,
			IdempotencyKey: request.IdempotencyKey, EventClass: "aggregation_dispatch_failed", ErrorClass: "dispatch_failed",
			Payload: datatypes.JSON([]byte(`{"safe_summary":"dispatch failed"}`)), OccurredAt: now,
		})
	})
}

func appendContentProcessingEvent(db *gorm.DB, event models.ContentProcessingEvent) error {
	if strings.TrimSpace(event.TenantID) == "" || strings.TrimSpace(event.Stage) == "" || strings.TrimSpace(event.State) == "" || strings.TrimSpace(event.Producer) == "" || strings.TrimSpace(event.EventClass) == "" {
		return fmt.Errorf("invalid content processing event")
	}
	if event.OccurredAt.IsZero() {
		event.OccurredAt = time.Now().UTC()
	}
	if len(event.Payload) == 0 {
		event.Payload = datatypes.JSON([]byte(`{}`))
	}
	return db.Create(&event).Error
}

func appendItemProcessingEvent(db *gorm.DB, item models.ContentItem, stage, state, producer, eventClass string, payload map[string]interface{}) error {
	if item.ContentSourceID == nil {
		return nil
	}
	return appendContentProcessingEvent(db, models.ContentProcessingEvent{
		TenantID: item.TenantID, ContentSourceID: item.ContentSourceID, SourceRunRequestID: item.SourceRunRequestID, ContentItemID: &item.PublicID,
		Stage: stage, State: state, Producer: producer, EventClass: eventClass, IdempotencyKey: derefStr(item.IdempotencyKey),
		Payload: lineagePayload(payload), OccurredAt: time.Now().UTC(),
	})
}

func sourceRunRequestByPublicID(db *gorm.DB, tenantID string, value string) (*models.SourceRunRequest, error) {
	id, err := uuid.Parse(strings.TrimSpace(value))
	if err != nil {
		return nil, fmt.Errorf("invalid source_run_request_id")
	}
	var request models.SourceRunRequest
	if err := db.Where("public_id=? AND tenant_id=?", id, tenantID).First(&request).Error; err != nil {
		return nil, err
	}
	return &request, nil
}

func lineagePayload(value map[string]interface{}) datatypes.JSON {
	raw, err := json.Marshal(value)
	if err != nil {
		return datatypes.JSON([]byte(`{}`))
	}
	return datatypes.JSON(raw)
}
