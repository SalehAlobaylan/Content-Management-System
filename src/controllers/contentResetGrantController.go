package controllers

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"content-management-system/src/sourceidentity"
	"content-management-system/src/supply"
	"content-management-system/src/utils"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type issueContentResetGrantRequest struct {
	TenantID            string `json:"tenant_id"`
	CampaignID          string `json:"campaign_id"`
	RevisionID          string `json:"revision_id"`
	TargetContentItemID string `json:"target_content_item_id"`
	SourceObservationID string `json:"source_observation_id"`
	UnitJobID           string `json:"unit_job_id"`
	AttemptFenceToken   string `json:"attempt_fence_token"`
	ExecutionLeaseToken string `json:"execution_lease_token"`
	PageID              string `json:"page_id"`
	BatchID             string `json:"batch_id"`
	ExpiresAt           string `json:"expires_at,omitempty"`
}

// InternalIssueContentResetReconstructionGrant issues a one-use capability
// only to the current Aggregation normalization unit of an approved replay.
func InternalIssueContentResetReconstructionGrant(c *gin.Context) {
	if !requireAggregationSourceRunPrincipal(c) {
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 32*1024)
	var body issueContentResetGrantRequest
	if err := decodeStrictJSON(c, &body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid Content Reset reconstruction request"})
		return
	}
	tenantID := strings.TrimSpace(body.TenantID)
	if tenantID == "" || strings.TrimSpace(body.UnitJobID) == "" || strings.TrimSpace(body.PageID) == "" || strings.TrimSpace(body.BatchID) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Missing Content Reset replay execution identity"})
		return
	}
	parseID := func(raw string) (uuid.UUID, error) { return uuid.Parse(strings.TrimSpace(raw)) }
	campaignID, err := parseID(body.CampaignID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid Content Reset campaign"})
		return
	}
	revisionID, err := parseID(body.RevisionID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid Content Reset revision"})
		return
	}
	targetID := uuid.Nil
	if strings.TrimSpace(body.TargetContentItemID) != "" {
		targetID, err = parseID(body.TargetContentItemID)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid selected content identity"})
			return
		}
	}
	observationID, err := parseID(body.SourceObservationID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid replay observation identity"})
		return
	}
	fenceToken, err := parseID(body.AttemptFenceToken)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid source-run fence"})
		return
	}
	leaseToken, err := parseID(body.ExecutionLeaseToken)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid source-run execution lease"})
		return
	}
	requestID, err := parseID(c.Param("request"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid replay request identity"})
		return
	}
	attemptID, err := parseID(c.Param("attempt"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid replay attempt identity"})
		return
	}
	unitID, err := parseID(c.Param("unit"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid normalization unit identity"})
		return
	}
	expiresAt := time.Now().UTC().Add(10 * time.Minute)
	if strings.TrimSpace(body.ExpiresAt) != "" {
		expiresAt, err = time.Parse(time.RFC3339, strings.TrimSpace(body.ExpiresAt))
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid reconstruction grant expiry"})
			return
		}
	}
	secret, err := utils.GetJWTSecret()
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Content Reset grant signing is unavailable"})
		return
	}
	credentialID, _ := utils.GetMachineCredentialID(c)
	createdBy := string(utils.MachinePrincipalAggregation)
	if strings.TrimSpace(credentialID) != "" {
		createdBy += "/" + strings.TrimSpace(credentialID)
	}

	db := c.MustGet("db").(*gorm.DB)
	var issued sourceidentity.IssuedReconstructionGrant
	err = db.Transaction(func(tx *gorm.DB) error {
		unit, verifyErr := supply.VerifyExecutionEnvelope(tx, tenantID, c.Param("request"), c.Param("attempt"), c.Param("unit"), body.UnitJobID, body.AttemptFenceToken)
		if verifyErr != nil {
			return verifyErr
		}
		if unit.UnitType != "normalize_batch" || unit.PageID != strings.TrimSpace(body.PageID) || unit.BatchID != strings.TrimSpace(body.BatchID) ||
			unit.State != string(supply.UnitRunning) || unit.ExecutionOwner != string(utils.MachinePrincipalAggregation) ||
			unit.ExecutionLeaseToken == nil || *unit.ExecutionLeaseToken != leaseToken || unit.ExecutionLeaseExpiresAt == nil || !unit.ExecutionLeaseExpiresAt.After(time.Now().UTC()) {
			return sourceidentity.ErrReconstructionGrantScope
		}
		issued, verifyErr = sourceidentity.IssueReconstructionGrant(tx, sourceidentity.ReconstructionGrantIssueInput{
			TenantID: tenantID, CampaignID: campaignID, RevisionID: revisionID, TargetContentID: targetID,
			ObservationID: observationID, ReplayRequestID: requestID, SourceRunAttemptID: attemptID,
			ExecutionUnitID: unitID, ExecutionFenceToken: fenceToken, UnitJobID: body.UnitJobID,
			ExecutionLeaseToken: leaseToken, PageID: strings.TrimSpace(body.PageID), BatchID: strings.TrimSpace(body.BatchID),
			CreatedBy: createdBy, ExpiresAt: expiresAt, TokenDerivationKey: sourceidentity.GrantTokenDerivationKey(secret),
		})
		return verifyErr
	})
	if err != nil {
		if errors.Is(err, sourceidentity.ErrInvalidReconstructionGrant) || errors.Is(err, sourceidentity.ErrReconstructionGrantScope) || errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusConflict, gin.H{"error": "Replay unit or target is outside the approved Content Reset scope", "code": "CONTENT_RESET_RECONSTRUCTION_GRANT_REJECTED"})
			return
		}
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Content Reset reconstruction grant could not be issued"})
		return
	}
	if issued.ExistingContentItemID != nil {
		c.JSON(http.StatusOK, gin.H{"reference_id": issued.ReferenceID, "grant_kind": "existing_instance", "existing_content_item_id": *issued.ExistingContentItemID, "source_observation_id": observationID, "replacement_instance_generation": issued.Grant.ReplacementInstanceGeneration})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"grant_id": issued.Grant.PublicID, "grant": issued.Token,
		"grant_kind":                      issued.Grant.GrantKind,
		"target_content_item_id":          issued.Grant.TargetContentItemID,
		"source_observation_id":           issued.Grant.SourceObservationID,
		"replacement_instance_generation": issued.Grant.ReplacementInstanceGeneration,
		"expires_at":                      issued.Grant.ExpiresAt.UTC(),
	})
}
