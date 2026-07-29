package controllers

// Retention coordinates registered media owners; it never becomes a competing
// candidate selector or artifact executor.

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"content-management-system/src/models"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	retentionOwnerStorage          = "storage"
	retentionOwnerMediaCirculation = "media_circulation"
)

type retentionOwnerPrepareRequest struct {
	MaxBytes int64 `json:"max_bytes"`
}

const retentionOwnerRequestTTL = 24 * time.Hour

func retentionOwnerActionClass(owner string) (string, error) {
	switch owner {
	case retentionOwnerStorage:
		return models.RetentionActionRequestStorageRun, nil
	case retentionOwnerMediaCirculation:
		return models.RetentionActionRequestMediaRun, nil
	default:
		return "", errors.New("unknown retention owner")
	}
}

func retentionOwnerPreview(db *gorm.DB, tenant, owner string, maxBytes int64) (map[string]interface{}, error) {
	switch owner {
	case retentionOwnerStorage:
		policy := loadEffectiveStoragePolicy(db, tenant)
		health := buildStorageHealthResponseCached(db, tenant, "")
		return map[string]interface{}{
			"owner": owner, "state": health.State, "summary": health.Summary,
			"candidate_count": health.Proof.CandidateCount, "candidate_bytes": health.Proof.CandidateBytes,
			"requested_max_bytes": maxBytes, "owner_policy_enabled": policy.Enabled,
			"archive_action": policy.ArchiveAction, "recovery_required": true,
		}, nil
	case retentionOwnerMediaCirculation:
		policy := loadEffectiveMediaCirculationPolicy(db, tenant)
		health := buildMediaCirculationHealth(db, tenant, "")
		return map[string]interface{}{
			"owner": owner, "headline": health.Headline, "summary": health.Summary,
			"score": health.Score, "owner_policy_enabled": policy.Enabled,
			"autopilot_enabled": policy.AutopilotEnabled, "mode": policy.AutopilotMode,
			"requested_max_bytes": maxBytes,
		}, nil
	default:
		return nil, errors.New("unknown retention owner")
	}
}

// PrepareRetentionOwnerRequest records an immutable, approval-bound request.
// The owner will recompute candidates and all owner-specific guards later.
func PrepareRetentionOwnerRequest(c *gin.Context) {
	principal, ok := requireRetentionTenant(c)
	if !ok {
		return
	}
	owner := strings.TrimSpace(c.Param("owner"))
	class, err := retentionOwnerActionClass(owner)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	var req retentionOwnerPrepareRequest
	_ = c.ShouldBindJSON(&req)
	db := c.MustGet("db").(*gorm.DB)
	policy := loadRetentionPolicy(db, principal.TenantID)
	if req.MaxBytes <= 0 || req.MaxBytes > policy.MaxBytesPerRun {
		req.MaxBytes = policy.MaxBytesPerRun
	}
	preview, err := retentionOwnerPreview(db, principal.TenantID, owner, req.MaxBytes)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	run, err := runRetention(db, principal.TenantID, "manual", principal.Email)
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	maxActions := policy.MaxActionsPerRun
	if maxActions < 1 {
		maxActions = 1
	}
	maxItems := maxActions
	expiresAt := time.Now().UTC().Add(retentionOwnerRequestTTL)
	correlationID := uuid.New()
	allowedClasses := []string{class}
	raw, _ := json.Marshal(map[string]interface{}{"v": 2, "tenant": principal.TenantID, "owner": owner, "allowed_action_classes": allowedClasses, "max_bytes": req.MaxBytes, "max_items": maxItems, "max_actions": maxActions, "correlation_id": correlationID.String(), "expires_at": expiresAt, "preview": preview})
	hash := retentionSHA256(string(raw))
	action := models.RetentionAction{RunID: run.ID, TenantID: principal.TenantID, ActionClass: class, OwnerSystem: owner, TargetScope: "owner-reselected:" + owner, Mode: models.RetentionModeAssist, Decision: "approval_required", Outcome: models.RetentionActionApprovalRequired, IdempotencyKey: "retention-owner:" + owner + ":" + hash, EvidenceFingerprint: hash, TargetCount: 0, EstimatedBytes: req.MaxBytes, Guardrail: "owner_reselects_tenant_budget_recovery_candidates", Evidence: retentionActionEvidence(map[string]interface{}{"preview": preview, "request_hash": hash, "max_items": maxItems, "max_actions": maxActions, "expires_at": expiresAt})}
	allowedJSON, _ := json.Marshal(allowedClasses)
	ownerReq := models.RetentionOwnerRequest{TenantID: principal.TenantID, OwnerSystem: owner, IdempotencyKey: action.IdempotencyKey, RequestHash: hash, AllowedActionClasses: datatypes.JSON(allowedJSON), MaxBytes: req.MaxBytes, MaxItems: maxItems, MaxActions: maxActions, CorrelationID: &correlationID, ExpiresAt: expiresAt, Status: "approval_required", Result: datatypes.JSON([]byte(`{}`))}
	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&action).Error; err != nil {
			return err
		}
		ownerReq.ActionID = action.ID
		return tx.Create(&ownerReq).Error
	}); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	actionID := action.PublicID
	ownerReq.ActionPublicID = &actionID
	retentionAudit(db, principal, "retention.owner.prepare", ownerReq.PublicID.String(), "success", map[string]interface{}{"owner": owner})
	c.JSON(http.StatusCreated, gin.H{"data": gin.H{"request": ownerReq, "action": action, "preview": preview}})
}

func executeRetentionStorageOwner(tenant string, ownerReq models.RetentionOwnerRequest, action models.RetentionAction) (map[string]interface{}, *uuid.UUID, error) {
	payload := map[string]interface{}{"tenant_id": tenant, "owner": retentionOwnerStorage, "action_class": action.ActionClass, "allowed_action_classes": ownerReq.AllowedActionClasses, "max_bytes": ownerReq.MaxBytes, "max_items": ownerReq.MaxItems, "max_actions": ownerReq.MaxActions, "expires_at": ownerReq.ExpiresAt, "idempotency_key": ownerReq.IdempotencyKey, "manifest_hash": ownerReq.RequestHash, "owner_request_id": ownerReq.PublicID.String(), "correlation_id": ownerReq.CorrelationID}
	body, status, err := callAggregationInternal(http.MethodPost, "/internal/retention/storage/sweep", payload)
	if err != nil {
		return nil, nil, fmt.Errorf("storage owner request failed: %w", err)
	}
	if status < 200 || status >= 300 {
		return nil, nil, fmt.Errorf("storage owner request returned status %d", status)
	}
	var envelope struct {
		Data struct {
			TenantID     string  `json:"tenantId"`
			DeletedCount int     `json:"deletedCount"`
			FreedBytes   float64 `json:"freedBytes"`
			RequestHash  string  `json:"request_hash"`
			Owner        string  `json:"owner"`
			OwnerRequest string  `json:"owner_request_id"`
			ResultHash   string  `json:"result_hash"`
			Skipped      bool    `json:"skipped"`
			Reason       string  `json:"reason,omitempty"`
			ActionCount  int     `json:"action_count"`
			ActionClass  string  `json:"action_class"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, nil, err
	}
	if envelope.Data.Owner != retentionOwnerStorage || envelope.Data.ActionClass != action.ActionClass || envelope.Data.RequestHash != ownerReq.RequestHash || envelope.Data.OwnerRequest != ownerReq.PublicID.String() {
		return nil, nil, errors.New("storage owner result envelope does not match the approved request")
	}
	expectedHash := retentionSHA256(fmt.Sprintf("%s|%s|%d|%d|%d|%s", ownerReq.RequestHash, tenant, envelope.Data.DeletedCount, int64(envelope.Data.FreedBytes), envelope.Data.ActionCount, ownerReq.PublicID.String()))
	if envelope.Data.ResultHash == "" || envelope.Data.ResultHash != expectedHash {
		return nil, nil, errors.New("storage owner result hash did not match the approved request")
	}
	if ownerReq.MaxBytes > 0 && int64(envelope.Data.FreedBytes) > ownerReq.MaxBytes {
		return nil, nil, fmt.Errorf("storage owner exceeded Retention byte bound (%d > %d)", int64(envelope.Data.FreedBytes), ownerReq.MaxBytes)
	}
	if ownerReq.MaxItems > 0 && envelope.Data.ActionCount > ownerReq.MaxItems {
		return nil, nil, fmt.Errorf("storage owner exceeded Retention item bound (%d > %d)", envelope.Data.ActionCount, ownerReq.MaxItems)
	}
	if ownerReq.MaxActions > 0 && envelope.Data.ActionCount > ownerReq.MaxActions {
		return nil, nil, fmt.Errorf("storage owner exceeded Retention action bound (%d > %d)", envelope.Data.ActionCount, ownerReq.MaxActions)
	}
	return map[string]interface{}{"tenant_id": envelope.Data.TenantID, "deleted_count": envelope.Data.DeletedCount, "action_count": envelope.Data.ActionCount, "freed_bytes": int64(envelope.Data.FreedBytes), "request_hash": envelope.Data.RequestHash, "owner": envelope.Data.Owner, "action_class": envelope.Data.ActionClass, "owner_request_id": envelope.Data.OwnerRequest, "result_hash": envelope.Data.ResultHash, "skipped": envelope.Data.Skipped, "reason": envelope.Data.Reason}, nil, nil
}

func executeRetentionMediaOwner(db *gorm.DB, tenant, actor string, ownerReq models.RetentionOwnerRequest, action models.RetentionAction) (map[string]interface{}, *uuid.UUID, error) {
	run, actions, err := runMediaCirculationAutopilot(db, tenant, mediaAutopilotRunOptions{Trigger: "retention", CreatedBy: actor, MaxBytes: ownerReq.MaxBytes, MaxItems: ownerReq.MaxItems, MaxActions: ownerReq.MaxActions, RequestHash: ownerReq.RequestHash})
	if err != nil {
		return nil, nil, err
	}
	ids := make([]string, 0, len(actions))
	var usedBytes int64
	for _, action := range actions {
		ids = append(ids, action.PublicID.String())
		if action.ByteImpact > 0 {
			usedBytes += action.ByteImpact
		}
	}
	if ownerReq.MaxBytes > 0 && usedBytes > ownerReq.MaxBytes {
		return nil, nil, fmt.Errorf("media owner exceeded Retention byte bound (%d > %d)", usedBytes, ownerReq.MaxBytes)
	}
	if ownerReq.MaxItems > 0 && len(ids) > ownerReq.MaxItems {
		return nil, nil, fmt.Errorf("media owner exceeded Retention item bound (%d > %d)", len(ids), ownerReq.MaxItems)
	}
	if ownerReq.MaxActions > 0 && len(ids) > ownerReq.MaxActions {
		return nil, nil, fmt.Errorf("media owner exceeded Retention action bound (%d > %d)", len(ids), ownerReq.MaxActions)
	}
	resultHash := retentionSHA256(fmt.Sprintf("%s|%s|%s|%d|%d", ownerReq.RequestHash, action.ActionClass, run.PublicID.String(), len(ids), usedBytes))
	return map[string]interface{}{"run_id": run.PublicID.String(), "status": run.Status, "summary": run.Summary, "action_ids": ids, "used_bytes": usedBytes, "max_bytes": ownerReq.MaxBytes, "max_items": ownerReq.MaxItems, "max_actions": ownerReq.MaxActions, "action_count": len(ids), "request_hash": ownerReq.RequestHash, "action_class": action.ActionClass, "result_hash": resultHash}, &run.PublicID, nil
}

func ExecuteRetentionOwnerRequest(c *gin.Context) {
	principal, ok := requireRetentionTenant(c)
	if !ok {
		return
	}
	requestID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid owner request id"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	policy := loadRetentionPolicy(db, principal.TenantID)
	if !policy.Enabled || policy.Mode != models.RetentionModeAssist {
		c.JSON(http.StatusConflict, gin.H{"error": "owner request execution requires enabled Assist mode"})
		return
	}
	if err := requireRetentionCapability(db, principal.TenantID, retentionCapabilityOwnerRuns); err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	var ownerReq models.RetentionOwnerRequest
	var action models.RetentionAction
	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=?", principal.TenantID, requestID).First(&ownerReq).Error; err != nil {
			return err
		}
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=?", ownerReq.ActionID).First(&action).Error; err != nil {
			return err
		}
		if action.Outcome != models.RetentionActionApproved || ownerReq.Status != "approved" {
			return errors.New("approved owner request is required")
		}
		if !ownerReq.ExpiresAt.After(time.Now().UTC()) {
			return errors.New("owner request has expired; prepare a new bounded request")
		}
		return tx.Model(&models.RetentionOwnerRequest{}).Where("id=?", ownerReq.ID).Update("status", "running").Error
	}); err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	var result map[string]interface{}
	var ownerRunID *uuid.UUID
	switch ownerReq.OwnerSystem {
	case retentionOwnerStorage:
		result, ownerRunID, err = executeRetentionStorageOwner(principal.TenantID, ownerReq, action)
	case retentionOwnerMediaCirculation:
		result, ownerRunID, err = executeRetentionMediaOwner(db, principal.TenantID, principal.Email, ownerReq, action)
	default:
		err = errors.New("unregistered owner")
	}
	finished := time.Now().UTC()
	if err != nil {
		// Keep the approved envelope retryable. The owner endpoint is
		// idempotency-bound, so a transient failure or ambiguous response must
		// not force a new, potentially wider request.
		_ = db.Model(&models.RetentionOwnerRequest{}).Where("id=?", ownerReq.ID).Updates(map[string]interface{}{"status": "approved", "result": retentionActionEvidence(map[string]interface{}{"error": err.Error(), "retryable": true})}).Error
		_ = db.Model(&models.RetentionAction{}).Where("id=?", action.ID).Updates(map[string]interface{}{"outcome": models.RetentionActionApproved, "finished_at": finished, "verification": retentionActionEvidence(map[string]interface{}{"owner_error": err.Error(), "retryable": true})}).Error
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	requestHash, _ := result["request_hash"].(string)
	resultActionClass, _ := result["action_class"].(string)
	ownerResultHash, _ := result["result_hash"].(string)
	var allowed []string
	_ = json.Unmarshal(ownerReq.AllowedActionClasses, &allowed)
	allowedClass := false
	for _, candidate := range allowed {
		if candidate == action.ActionClass {
			allowedClass = true
			break
		}
	}
	if !allowedClass || (resultActionClass != "" && resultActionClass != action.ActionClass) || requestHash != ownerReq.RequestHash || ownerResultHash == "" {
		_ = db.Model(&models.RetentionOwnerRequest{}).Where("id=?", ownerReq.ID).Updates(map[string]interface{}{"status": "approved", "result": retentionActionEvidence(map[string]interface{}{"error": "owner result hash/request mismatch", "retryable": true})}).Error
		_ = db.Model(&models.RetentionAction{}).Where("id=?", action.ID).Updates(map[string]interface{}{"outcome": models.RetentionActionApproved, "finished_at": finished, "verification": retentionActionEvidence(map[string]interface{}{"owner_error": "owner result hash/request mismatch", "retryable": true})}).Error
		c.JSON(http.StatusConflict, gin.H{"error": "owner result hash/request mismatch"})
		return
	}
	if ownerReq.OwnerSystem == retentionOwnerMediaCirculation {
		runID, _ := result["run_id"].(string)
		actionCount, _ := result["action_count"].(int)
		usedBytes, _ := result["used_bytes"].(int64)
		if actionCount == 0 {
			if value, ok := result["action_count"].(float64); ok {
				actionCount = int(value)
			}
		}
		if usedBytes == 0 {
			if value, ok := result["used_bytes"].(float64); ok {
				usedBytes = int64(value)
			}
		}
		expected := retentionSHA256(fmt.Sprintf("%s|%s|%s|%d|%d", ownerReq.RequestHash, action.ActionClass, runID, actionCount, usedBytes))
		if expected != ownerResultHash {
			_ = db.Model(&models.RetentionOwnerRequest{}).Where("id=?", ownerReq.ID).Updates(map[string]interface{}{"status": "approved", "result": retentionActionEvidence(map[string]interface{}{"error": "media owner result hash mismatch", "retryable": true})}).Error
			_ = db.Model(&models.RetentionAction{}).Where("id=?", action.ID).Updates(map[string]interface{}{"outcome": models.RetentionActionApproved, "finished_at": finished, "verification": retentionActionEvidence(map[string]interface{}{"owner_error": "media owner result hash mismatch", "retryable": true})}).Error
			c.JSON(http.StatusConflict, gin.H{"error": "media owner result hash mismatch"})
			return
		}
	}
	raw, _ := json.Marshal(result)
	resultHash := ownerResultHash
	resultEnvelopeHash := retentionSHA256(string(raw))
	requestStatus := "succeeded"
	actionOutcome := models.RetentionActionVerified
	// Storage can enqueue asynchronous re-encodes. Its own sweep/artifact
	// ledgers are authoritative for completion, so Retention records submission
	// rather than falsely certifying the media-side outcome.
	if ownerReq.OwnerSystem == retentionOwnerStorage {
		requestStatus = "submitted"
		actionOutcome = models.RetentionActionToolSucceeded
	}
	_ = db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&models.RetentionOwnerRequest{}).Where("id=?", ownerReq.ID).Updates(map[string]interface{}{"status": requestStatus, "owner_run_id": ownerRunID, "result_hash": resultHash, "result": datatypes.JSON(raw)}).Error; err != nil {
			return err
		}
		return tx.Model(&models.RetentionAction{}).Where("id=?", action.ID).Updates(map[string]interface{}{"outcome": actionOutcome, "started_at": finished, "finished_at": finished, "verification": retentionActionEvidence(map[string]interface{}{"owner_request_id": ownerReq.PublicID.String(), "owner_result_hash": resultHash, "owner_result_envelope_hash": resultEnvelopeHash, "owner_completion": requestStatus})}).Error
	})
	retentionAudit(db, principal, "retention.owner.execute", ownerReq.PublicID.String(), "success", map[string]interface{}{"owner": ownerReq.OwnerSystem})
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"request_id": ownerReq.PublicID, "result": result}})
}

func ListRetentionOwnerRequests(c *gin.Context) {
	principal, ok := requireRetentionTenant(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var rows []models.RetentionOwnerRequest
	if err := db.Where("tenant_id=?", principal.TenantID).Order("created_at DESC").Limit(100).Find(&rows).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if len(rows) > 0 {
		actionIDs := make([]uint, 0, len(rows))
		for _, row := range rows {
			actionIDs = append(actionIDs, row.ActionID)
		}
		var actions []models.RetentionAction
		if err := db.Where("id IN ?", actionIDs).Find(&actions).Error; err == nil {
			public := map[uint]uuid.UUID{}
			for _, action := range actions {
				public[action.ID] = action.PublicID
			}
			for index := range rows {
				if id, ok := public[rows[index].ActionID]; ok {
					rows[index].ActionPublicID = &id
				}
			}
		}
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"items": rows}})
}
