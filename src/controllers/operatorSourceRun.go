package controllers

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"

	"content-management-system/src/models"
	operatorpkg "content-management-system/src/operator"
	"content-management-system/src/supply"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// operatorSourceRunResult is deliberately small. Aggregation owns queue work;
// CMS owns the handoff record and all state exposed to Operator.
type operatorSourceRunResult struct {
	RequestID      string
	IdempotencyKey string
	Created        bool
}

// runOperatorSourceOnce is a durable CMS admission, not a queue handoff. The
// signed plan creates one exact source-run request; the CMS-claimed dispatcher
// later decides whether and when its coordinator can be enqueued. This keeps
// browser approval, plan execution, queue acceptance, provider I/O, and
// downstream delivery as distinct evidence boundaries.
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
	if plan.PublicID == uuid.Nil || step.PublicID == uuid.Nil || strings.TrimSpace(plan.IdempotencyKey) == "" {
		return operatorSourceRunResult{}, nil, fmt.Errorf("operator plan correlation is incomplete")
	}
	identity, err := operatorSourceRunIdentity(source, plan, step, toolKey)
	if err != nil {
		return operatorSourceRunResult{}, nil, err
	}
	metadata, err := json.Marshal(map[string]any{
		"schema_version":   supply.ContractVersion,
		"origin":           "operator_plan",
		"operator_plan_id": plan.PublicID.String(),
		"operator_step_id": step.PublicID.String(),
		"tool_key":         toolKey,
	})
	if err != nil {
		return operatorSourceRunResult{}, nil, err
	}
	request, created, err := supply.CreateRequest(db, supply.CreateRequestInput{
		Source: source, Identity: identity, RequestedBy: "approval_handoff", RequestedByActorID: plan.ActorID,
		OperatorPlanID: &plan.PublicID, OperatorStepID: &step.PublicID,
		EvidenceFingerprint: "operator-plan:" + plan.PublicID.String() + ":" + step.PublicID.String(), Metadata: metadata,
	})
	if err != nil {
		return operatorSourceRunResult{}, nil, err
	}
	before := map[string]any{"source_id": source.PublicID.String(), "category": source.Category, "active": source.IsActive}
	return operatorSourceRunResult{RequestID: request.PublicID.String(), IdempotencyKey: request.IdempotencyKey, Created: created}, before, nil
}

func operatorSourceRunIdentity(source models.ContentSource, plan models.OperatorActionPlan, step models.OperatorActionStep, toolKey string) (supply.RequestIdentity, error) {
	if source.PublicID == uuid.Nil || strings.TrimSpace(source.TenantID) == "" || plan.PublicID == uuid.Nil || step.PublicID == uuid.Nil {
		return supply.RequestIdentity{}, fmt.Errorf("operator source-run identity is incomplete")
	}
	version := source.SourceConfigVersion
	if version < 1 {
		version = 1
	}
	window := plan.CreatedAt.UTC()
	if window.IsZero() {
		return supply.RequestIdentity{}, fmt.Errorf("operator plan has no durable creation time")
	}
	arguments := sha256.Sum256([]byte(strings.Join([]string{string(source.Type), source.Category, operatorSourceFeedURL(source.FeedURL), string(source.APIConfig)}, "\n")))
	policy := sha256.Sum256([]byte(strings.Join([]string{supply.ContractVersion, "operator_source_run", toolKey, source.Category, fmt.Sprintf("%d", version)}, "\n")))
	return supply.RequestIdentity{
		TenantID: source.TenantID, ContentSourceID: source.PublicID.String(), Lane: source.Category, Purpose: "operator_run_once",
		CadenceWindowStart: window, SourceConfigVersion: version,
		PolicyFingerprint: fmt.Sprintf("%x", policy[:]), ArgumentFingerprint: fmt.Sprintf("%x", arguments[:]),
		PlanStepToolTarget: strings.Join([]string{plan.PublicID.String(), step.PublicID.String(), toolKey, source.PublicID.String()}, ":"),
	}, nil
}

func operatorSourceFeedURL(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
