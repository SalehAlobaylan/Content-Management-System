package controllers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"content-management-system/src/models"
	operatorpkg "content-management-system/src/operator"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// operatorSourceRunResult is deliberately small. Aggregation owns queue work;
// CMS owns the handoff record and all state exposed to Operator.
type operatorSourceRunResult struct {
	RequestID string
	JobID     string
}

type operatorSourceRunDispatchResponse struct {
	Data struct {
		JobID string `json:"job_id"`
	} `json:"data"`
}

// runOperatorSourceOnce is the tenant-aware mutation extracted from the legacy
// source controller. It accepts only a persisted plan/step and re-loads the
// source under the asserted tenant before it creates a durable handoff.
func runOperatorSourceOnce(db *gorm.DB, tenantID, toolKey string, plan models.OperatorActionPlan, step models.OperatorActionStep, canonical operatorpkg.CanonicalPlan) (operatorSourceRunResult, map[string]any, error) {
	if len(canonical.TargetIDs) != 1 {
		return operatorSourceRunResult{}, nil, fmt.Errorf("operator source run requires one target")
	}
	sourceID, err := uuid.Parse(canonical.TargetIDs[0])
	if err != nil {
		return operatorSourceRunResult{}, nil, fmt.Errorf("operator source target is invalid")
	}
	var source models.ContentSource
	if err := db.Where("public_id=? AND tenant_id=?", sourceID, tenantID).First(&source).Error; err != nil {
		return operatorSourceRunResult{}, nil, err
	}
	expectedCategory := models.SourceCategoryNews
	if toolKey == "media_sources.run_once" {
		expectedCategory = models.SourceCategoryMedia
	}
	if toolKey != "sources.run_once" && toolKey != "media_sources.run_once" {
		return operatorSourceRunResult{}, nil, fmt.Errorf("operator source tool is not registered")
	}
	if source.Category != expectedCategory || !source.IsActive {
		return operatorSourceRunResult{}, map[string]any{"source_id": source.PublicID.String(), "category": source.Category, "active": source.IsActive}, fmt.Errorf("source no longer passes the registered preflight")
	}
	sourceURL, err := extractSourceRunURL(source)
	if err != nil {
		return operatorSourceRunResult{}, nil, err
	}
	settings, err := parseSourceAPIConfig(source.APIConfig)
	if err != nil {
		return operatorSourceRunResult{}, nil, err
	}
	if plan.PublicID == uuid.Nil || step.PublicID == uuid.Nil || strings.TrimSpace(plan.IdempotencyKey) == "" {
		return operatorSourceRunResult{}, nil, fmt.Errorf("operator plan correlation is incomplete")
	}
	lineage, err := createSourceRunRequestWithCorrelation(db, source, "operator", plan.ActorID, nil, SourceRunCorrelation{
		OperatorPlanID: &plan.PublicID, OperatorStepID: &step.PublicID, IdempotencyKey: plan.IdempotencyKey,
	})
	if err != nil {
		return operatorSourceRunResult{}, nil, err
	}
	before := map[string]any{"source_id": source.PublicID.String(), "category": source.Category, "active": source.IsActive, "source_run_request_id": lineage.PublicID.String(), "state": lineage.State}
	payload := map[string]any{
		"source_type": string(source.Type), "url": sourceURL, "name": source.Name, "settings": settings,
		"source_id": source.PublicID.String(), "source_run_request_id": lineage.PublicID.String(), "tenant_id": tenantID,
		"operator_plan_id": plan.PublicID.String(), "operator_step_id": step.PublicID.String(), "idempotency_key": plan.IdempotencyKey,
	}
	body, status, err := callAggregationInternal(http.MethodPost, "/internal/operator/source-runs", payload)
	if err != nil {
		markSourceRunDispatchFailed(db, lineage.PublicID, err)
		return operatorSourceRunResult{}, before, err
	}
	var dispatched operatorSourceRunDispatchResponse
	if err := json.Unmarshal(body, &dispatched); err != nil || status < http.StatusOK || status >= http.StatusMultipleChoices || strings.TrimSpace(dispatched.Data.JobID) == "" {
		dispatchErr := fmt.Errorf("aggregation rejected registered source run")
		markSourceRunDispatchFailed(db, lineage.PublicID, dispatchErr)
		return operatorSourceRunResult{}, before, dispatchErr
	}
	if err := markSourceRunAccepted(db, lineage.PublicID, dispatched.Data.JobID); err != nil {
		return operatorSourceRunResult{}, before, err
	}
	now := time.Now().UTC()
	_ = db.Model(&source).Update("last_fetched_at", now).Error
	return operatorSourceRunResult{RequestID: lineage.PublicID.String(), JobID: dispatched.Data.JobID}, before, nil
}
