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
	raw, _ := json.Marshal(map[string]interface{}{"v": 1, "tenant": principal.TenantID, "owner": owner, "max_bytes": req.MaxBytes, "preview": preview})
	hash := retentionSHA256(string(raw))
	action := models.RetentionAction{RunID: run.ID, TenantID: principal.TenantID, ActionClass: class, OwnerSystem: owner, TargetScope: "owner-reselected:" + owner, Mode: models.RetentionModeAssist, Decision: "approval_required", Outcome: models.RetentionActionApprovalRequired, IdempotencyKey: "retention-owner:" + owner + ":" + hash, EvidenceFingerprint: hash, TargetCount: 0, EstimatedBytes: req.MaxBytes, Guardrail: "owner_reselects_tenant_budget_recovery_candidates", Evidence: retentionActionEvidence(map[string]interface{}{"preview": preview, "request_hash": hash})}
	ownerReq := models.RetentionOwnerRequest{TenantID: principal.TenantID, OwnerSystem: owner, IdempotencyKey: action.IdempotencyKey, RequestHash: hash, Status: "approval_required", Result: datatypes.JSON([]byte(`{}`))}
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
	payload := map[string]interface{}{"tenant_id": tenant, "max_bytes": action.EstimatedBytes, "idempotency_key": ownerReq.IdempotencyKey, "manifest_hash": action.ManifestHash, "owner_request_id": ownerReq.PublicID.String(), "correlation_id": action.PublicID.String()}
	body, status, err := callAggregationInternal(http.MethodPost, "/internal/retention/storage/sweep", payload)
	if err != nil {
		return nil, nil, fmt.Errorf("storage owner request failed: %w", err)
	}
	if status < 200 || status >= 300 {
		return nil, nil, fmt.Errorf("storage owner request returned status %d", status)
	}
	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, nil, err
	}
	return result, nil, nil
}

func executeRetentionMediaOwner(db *gorm.DB, tenant, actor string) (map[string]interface{}, *uuid.UUID, error) {
	run, actions, err := runMediaCirculationAutopilot(db, tenant, mediaAutopilotRunOptions{Trigger: "retention", CreatedBy: actor})
	if err != nil {
		return nil, nil, err
	}
	ids := make([]string, 0, len(actions))
	for _, action := range actions {
		ids = append(ids, action.PublicID.String())
	}
	return map[string]interface{}{"run_id": run.PublicID.String(), "status": run.Status, "summary": run.Summary, "action_ids": ids}, &run.PublicID, nil
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
		result, ownerRunID, err = executeRetentionMediaOwner(db, principal.TenantID, principal.Email)
	default:
		err = errors.New("unregistered owner")
	}
	finished := time.Now().UTC()
	if err != nil {
		_ = db.Model(&models.RetentionOwnerRequest{}).Where("id=?", ownerReq.ID).Updates(map[string]interface{}{"status": "failed", "result": retentionActionEvidence(map[string]interface{}{"error": err.Error()})}).Error
		_ = db.Model(&models.RetentionAction{}).Where("id=?", action.ID).Updates(map[string]interface{}{"outcome": models.RetentionActionToolFailed, "finished_at": finished, "verification": retentionActionEvidence(map[string]interface{}{"owner_error": err.Error()})}).Error
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	raw, _ := json.Marshal(result)
	resultHash := retentionSHA256(string(raw))
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
		return tx.Model(&models.RetentionAction{}).Where("id=?", action.ID).Updates(map[string]interface{}{"outcome": actionOutcome, "started_at": finished, "finished_at": finished, "verification": retentionActionEvidence(map[string]interface{}{"owner_request_id": ownerReq.PublicID.String(), "owner_result_hash": resultHash, "owner_completion": requestStatus})}).Error
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
