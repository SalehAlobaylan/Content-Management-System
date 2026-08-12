package controllers

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"content-management-system/src/supply"
	"content-management-system/src/utils"

	"github.com/gin-gonic/gin"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

const sourceRunInternalLease = 2 * time.Minute

type sourceRunEnvelopeBody struct {
	TenantID          string `json:"tenant_id"`
	UnitJobID         string `json:"unit_job_id"`
	AttemptFenceToken string `json:"attempt_fence_token"`
	ExecutionLease    string `json:"execution_lease_token"`
}

func InternalClaimNextSourceRun(c *gin.Context) {
	if !requireAggregationSourceRunPrincipal(c) {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	claim, found, err := supply.ClaimNextDispatchableRequest(db, string(utils.MachinePrincipalAggregation), sourceRunInternalLease, sourceRunInternalLease)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "CMS source-run admission is unavailable"})
		return
	}
	if !found {
		c.Status(http.StatusNoContent)
		return
	}
	c.JSON(http.StatusOK, dispatchClaimResponse(claim))
}

func InternalAuthorizeSourceRunUnit(c *gin.Context) {
	if !requireAggregationSourceRunPrincipal(c) {
		return
	}
	var body struct {
		TenantID     string `json:"tenant_id"`
		ParentUnitID string `json:"parent_unit_id"`
		UnitType     string `json:"unit_type"`
		UnitKey      string `json:"unit_key"`
		PageID       string `json:"page_id"`
		BatchID      string `json:"batch_id"`
	}
	if err := decodeStrictJSON(c, &body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid source-run unit authorization"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	unit, created, err := supply.AuthorizeChildUnit(db, supply.ChildUnitInput{TenantID: strings.TrimSpace(body.TenantID), RequestID: c.Param("request"), AttemptID: c.Param("attempt"), ParentUnitID: strings.TrimSpace(body.ParentUnitID), UnitType: strings.TrimSpace(body.UnitType), UnitKey: strings.TrimSpace(body.UnitKey), PageID: strings.TrimSpace(body.PageID), BatchID: strings.TrimSpace(body.BatchID)})
	if err != nil {
		writeSourceRunContractError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"id": unit.PublicID, "job_id": unit.JobID, "attempt_fence_token": unit.AttemptFenceToken, "state": unit.State, "duplicate": !created})
}

// InternalAcceptSourceRunUnit returns the sole current execution token for an
// authorized unit. It never accepts a queue name or lets an executor select a
// sibling unit.
func InternalAcceptSourceRunUnit(c *gin.Context) {
	if !requireAggregationSourceRunPrincipal(c) {
		return
	}
	var body sourceRunEnvelopeBody
	if err := decodeStrictJSON(c, &body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid source-run unit acceptance"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	unit, err := supply.VerifyExecutionEnvelope(db, strings.TrimSpace(body.TenantID), c.Param("request"), c.Param("attempt"), c.Param("unit"), strings.TrimSpace(body.UnitJobID), strings.TrimSpace(body.AttemptFenceToken))
	if err != nil {
		writeSourceRunContractError(c, err)
		return
	}
	if unit.State == string(supply.UnitAccepted) && unit.ExecutionOwner == string(utils.MachinePrincipalAggregation) && unit.ExecutionLeaseToken != nil && unit.ExecutionLeaseExpiresAt != nil && unit.ExecutionLeaseExpiresAt.After(time.Now().UTC()) {
		c.JSON(http.StatusOK, gin.H{"execution_lease_token": unit.ExecutionLeaseToken, "execution_lease_expires_at": unit.ExecutionLeaseExpiresAt, "state": unit.State, "reused": true})
		return
	}
	// A dispatcher can safely reacquire a pre-effect accepted unit after an
	// expired lease. BeginUnitEffect separately refuses to replay a unit that
	// already crossed its effect boundary.
	if unit.State != string(supply.UnitAuthorized) && unit.State != string(supply.UnitAccepted) {
		c.JSON(http.StatusConflict, gin.H{"message": "Source-run unit cannot be accepted"})
		return
	}
	lease, err := supply.AcquireUnitExecution(db, strings.TrimSpace(body.TenantID), unit.PublicID.String(), string(utils.MachinePrincipalAggregation), sourceRunInternalLease)
	if err != nil {
		writeSourceRunContractError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"execution_lease_token": lease.LeaseToken, "execution_lease_expires_at": lease.Unit.ExecutionLeaseExpiresAt, "state": lease.Unit.State, "reused": false})
}

func InternalBeginSourceRunUnit(c *gin.Context) {
	if !requireAggregationSourceRunPrincipal(c) {
		return
	}
	var body sourceRunEnvelopeBody
	if err := decodeStrictJSON(c, &body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid source-run unit begin"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	if _, err := supply.VerifyExecutionEnvelope(db, strings.TrimSpace(body.TenantID), c.Param("request"), c.Param("attempt"), c.Param("unit"), strings.TrimSpace(body.UnitJobID), strings.TrimSpace(body.AttemptFenceToken)); err != nil {
		writeSourceRunContractError(c, err)
		return
	}
	unit, err := supply.BeginUnitEffect(db, supply.UnitLeaseInput{TenantID: strings.TrimSpace(body.TenantID), UnitID: c.Param("unit"), Owner: string(utils.MachinePrincipalAggregation), LeaseToken: strings.TrimSpace(body.ExecutionLease)})
	if err != nil {
		writeSourceRunContractError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"id": unit.PublicID, "state": unit.State, "effect_started_at": unit.EffectStartedAt})
}

// InternalFreezeSourceRunPage records the finite set of normalization units
// authorized by one fetch page. Aggregation cannot seal the overall manifest
// or invent a child outside this declaration.
func InternalFreezeSourceRunPage(c *gin.Context) {
	if !requireAggregationSourceRunPrincipal(c) {
		return
	}
	var body struct {
		TenantID            string `json:"tenant_id"`
		DeclaredChildCount  int    `json:"declared_child_count"`
		DeclaredChildDigest string `json:"declared_child_digest"`
	}
	if err := decodeStrictJSON(c, &body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid source-run page declaration"})
		return
	}
	if err := supply.FreezeFetchPageChildren(c.MustGet("db").(*gorm.DB), strings.TrimSpace(body.TenantID), c.Param("unit"), body.DeclaredChildCount, strings.TrimSpace(body.DeclaredChildDigest)); err != nil {
		writeSourceRunContractError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func InternalRecordSourceRunUpstreamObservations(c *gin.Context) {
	if !requireAggregationSourceRunPrincipal(c) {
		return
	}
	var body struct {
		sourceRunEnvelopeBody
		ProviderCapability string `json:"provider_capability"`
		ProviderVersion    string `json:"provider_version"`
		ProviderPageID     string `json:"provider_page_id"`
		ProviderCursor     string `json:"provider_cursor"`
		Items              []struct {
			UpstreamItemID      string `json:"upstream_item_id"`
			UpstreamFingerprint string `json:"upstream_fingerprint"`
		} `json:"items"`
	}
	if err := decodeStrictJSON(c, &body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid source-run upstream observations"})
		return
	}
	items := make([]supply.UpstreamObservationItem, 0, len(body.Items))
	for _, item := range body.Items {
		items = append(items, supply.UpstreamObservationItem{UpstreamItemID: item.UpstreamItemID, UpstreamFingerprint: item.UpstreamFingerprint})
	}
	created, err := supply.RecordUpstreamObservations(c.MustGet("db").(*gorm.DB), supply.RecordUpstreamObservationsInput{
		TenantID: body.TenantID, RequestID: c.Param("request"), AttemptID: c.Param("attempt"), UnitID: c.Param("unit"),
		UnitJobID: body.UnitJobID, AttemptFenceToken: body.AttemptFenceToken, ExecutionLeaseToken: body.ExecutionLease,
		ProviderCapability: body.ProviderCapability, ProviderVersion: body.ProviderVersion, ProviderPageID: body.ProviderPageID,
		ProviderCursor: body.ProviderCursor, Items: items,
	})
	if err != nil {
		writeSourceRunContractError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "created": created})
}

func InternalRecordSourceRunUpstreamObservationDisposition(c *gin.Context) {
	if !requireAggregationSourceRunPrincipal(c) {
		return
	}
	var body struct {
		sourceRunEnvelopeBody
		Disposition   string `json:"disposition"`
		ContentItemID string `json:"content_item_id"`
		FilterClass   string `json:"filter_class"`
	}
	if err := decodeStrictJSON(c, &body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid source-run upstream observation disposition"})
		return
	}
	created, err := supply.RecordUpstreamObservationDisposition(c.MustGet("db").(*gorm.DB), supply.MaterializeUpstreamObservationInput{
		TenantID: body.TenantID, RequestID: c.Param("request"), AttemptID: c.Param("attempt"), UnitID: c.Param("unit"),
		UnitJobID: body.UnitJobID, AttemptFenceToken: body.AttemptFenceToken, ExecutionLeaseToken: body.ExecutionLease,
		ObservationID: c.Param("observation"), Disposition: strings.TrimSpace(body.Disposition), ContentItemID: strings.TrimSpace(body.ContentItemID), FilterClass: strings.TrimSpace(body.FilterClass),
	})
	if err != nil {
		writeSourceRunContractError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "created": created})
}

// InternalSealSourceRunManifest is deliberately idempotent. Workers may ask
// CMS to seal after their final page, but only CMS can decide that all pages
// are terminal and every child declaration agrees with its authorized units.
func InternalSealSourceRunManifest(c *gin.Context) {
	if !requireAggregationSourceRunPrincipal(c) {
		return
	}
	var body struct {
		TenantID string `json:"tenant_id"`
	}
	if err := decodeStrictJSON(c, &body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid source-run manifest seal"})
		return
	}
	request, err := supply.SealManifest(c.MustGet("db").(*gorm.DB), strings.TrimSpace(body.TenantID), c.Param("request"))
	if err != nil {
		writeSourceRunContractError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"id": request.PublicID, "manifest_state": request.ManifestState, "manifest_version": request.ManifestVersion})
}

func InternalHeartbeatSourceRunUnit(c *gin.Context) {
	if !requireAggregationSourceRunPrincipal(c) {
		return
	}
	var body sourceRunEnvelopeBody
	if err := decodeStrictJSON(c, &body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid source-run unit heartbeat"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	if _, err := supply.VerifyExecutionEnvelope(db, strings.TrimSpace(body.TenantID), c.Param("request"), c.Param("attempt"), c.Param("unit"), strings.TrimSpace(body.UnitJobID), strings.TrimSpace(body.AttemptFenceToken)); err != nil {
		writeSourceRunContractError(c, err)
		return
	}
	if err := supply.RenewUnitLease(db, supply.UnitLeaseInput{TenantID: strings.TrimSpace(body.TenantID), UnitID: c.Param("unit"), Owner: string(utils.MachinePrincipalAggregation), LeaseToken: strings.TrimSpace(body.ExecutionLease)}, sourceRunInternalLease); err != nil {
		writeSourceRunContractError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "execution_lease_expires_at": time.Now().UTC().Add(sourceRunInternalLease)})
}

func InternalClaimSourceRunVerification(c *gin.Context) {
	if !requireAggregationSourceRunPrincipal(c) {
		return
	}
	var body struct {
		TenantID string `json:"tenant_id"`
		TaskID   string `json:"task_id"`
	}
	if err := decodeStrictJSON(c, &body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid verification claim"})
		return
	}
	lease, err := supply.ClaimVerificationTask(c.MustGet("db").(*gorm.DB), strings.TrimSpace(body.TenantID), strings.TrimSpace(body.TaskID), string(utils.MachinePrincipalAggregation), sourceRunInternalLease)
	if err != nil {
		writeSourceRunContractError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"id": lease.Task.PublicID, "claim_token": lease.ClaimToken, "claim_expires_at": lease.Task.ClaimExpiresAt, "state": lease.Task.State})
}

// InternalClaimNextSourceRunVerification lets Aggregation ask CMS for one
// verification task. The caller cannot nominate a tenant, unit, or provider.
func InternalClaimNextSourceRunVerification(c *gin.Context) {
	if !requireAggregationSourceRunPrincipal(c) {
		return
	}
	lease, found, err := supply.ClaimNextVerificationTask(c.MustGet("db").(*gorm.DB), string(utils.MachinePrincipalAggregation), sourceRunInternalLease)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "CMS source-run verification is unavailable"})
		return
	}
	if !found {
		c.Status(http.StatusNoContent)
		return
	}
	c.JSON(http.StatusOK, gin.H{"id": lease.Task.PublicID, "tenant_id": lease.Task.TenantID, "source_run_request_id": lease.Task.SourceRunRequestID, "source_run_attempt_id": lease.Task.SourceRunAttemptID, "execution_unit_id": lease.Task.ExecutionUnitID, "content_source_id": lease.Task.ContentSourceID, "stage": lease.Task.Stage, "claim_token": lease.ClaimToken, "claim_expires_at": lease.Task.ClaimExpiresAt})
}

func InternalHeartbeatSourceRunVerification(c *gin.Context) {
	if !requireAggregationSourceRunPrincipal(c) {
		return
	}
	var body struct {
		TenantID   string `json:"tenant_id"`
		ClaimToken string `json:"claim_token"`
	}
	if err := decodeStrictJSON(c, &body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid verification heartbeat"})
		return
	}
	if err := supply.RenewVerificationLease(c.MustGet("db").(*gorm.DB), strings.TrimSpace(body.TenantID), c.Param("task"), string(utils.MachinePrincipalAggregation), strings.TrimSpace(body.ClaimToken), sourceRunInternalLease); err != nil {
		writeSourceRunContractError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func InternalCompleteSourceRunVerification(c *gin.Context) {
	if !requireAggregationSourceRunPrincipal(c) {
		return
	}
	var body struct {
		TenantID         string          `json:"tenant_id"`
		ClaimToken       string          `json:"claim_token"`
		EventKey         string          `json:"event_key"`
		EvidenceSnapshot string          `json:"evidence_snapshot"`
		ProvenanceDigest string          `json:"provenance_digest"`
		Verdict          string          `json:"verdict"`
		Payload          json.RawMessage `json:"payload"`
	}
	if err := decodeStrictJSON(c, &body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid verification completion"})
		return
	}
	if len(body.Payload) == 0 {
		body.Payload = json.RawMessage(`{}`)
	}
	var payload map[string]any
	if json.Unmarshal(body.Payload, &payload) != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Verification payload must be an object"})
		return
	}
	event, created, err := supply.CompleteVerification(c.MustGet("db").(*gorm.DB), supply.VerificationResultInput{TenantID: strings.TrimSpace(body.TenantID), TaskID: c.Param("task"), Owner: string(utils.MachinePrincipalAggregation), ClaimToken: strings.TrimSpace(body.ClaimToken), EventKey: strings.TrimSpace(body.EventKey), EvidenceSnapshot: strings.TrimSpace(body.EvidenceSnapshot), ProvenanceDigest: strings.TrimSpace(body.ProvenanceDigest), Verdict: supply.VerificationVerdict(strings.TrimSpace(body.Verdict)), Payload: datatypes.JSON(bytes.TrimSpace(body.Payload))})
	if err != nil {
		writeSourceRunContractError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"id": event.PublicID, "duplicate": !created, "verdict": event.Verdict})
}

// InternalObserveSourceRunVerification has no caller-provided verdict or
// evidence. CMS rebuilds its bounded authoritative observation and completes
// the current task through the existing fenced reconciliation path.
func InternalObserveSourceRunVerification(c *gin.Context) {
	if !requireAggregationSourceRunPrincipal(c) {
		return
	}
	var body struct {
		TenantID   string `json:"tenant_id"`
		ClaimToken string `json:"claim_token"`
	}
	if err := decodeStrictJSON(c, &body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid verification observation"})
		return
	}
	event, created, err := supply.ObserveAndCompleteVerification(c.MustGet("db").(*gorm.DB), strings.TrimSpace(body.TenantID), c.Param("task"), string(utils.MachinePrincipalAggregation), strings.TrimSpace(body.ClaimToken))
	if err != nil {
		writeSourceRunContractError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"id": event.PublicID, "duplicate": !created, "verdict": event.Verdict, "evidence_snapshot": event.EvidenceSnapshot})
}

func requireAggregationSourceRunPrincipal(c *gin.Context) bool {
	principal, ok := utils.GetMachinePrincipal(c)
	if ok && principal == utils.MachinePrincipalAggregation {
		return true
	}
	c.JSON(http.StatusForbidden, gin.H{"message": "Aggregation source-run capability is required"})
	return false
}

func dispatchClaimResponse(claim supply.DispatchClaim) gin.H {
	settings := map[string]any{}
	_ = json.Unmarshal(claim.Source.APIConfig, &settings)
	metadata := map[string]any{}
	_ = json.Unmarshal(claim.Request.Metadata, &metadata)
	url := ""
	if claim.Source.FeedURL != nil {
		url = *claim.Source.FeedURL
	}
	return gin.H{"request": gin.H{"id": claim.Request.PublicID, "tenant_id": claim.Request.TenantID, "source_id": claim.Request.ContentSourceID, "lane": claim.Request.Lane, "purpose": claim.Request.Purpose, "correlation_id": claim.Request.CorrelationID, "metadata": metadata, "item_cap": claim.Request.ItemCap, "byte_cap": claim.Request.ByteCap, "provider_call_cap": claim.Request.ProviderCallCap, "workload_cap": claim.Request.WorkloadCap}, "source": gin.H{"id": claim.Source.PublicID, "type": claim.Source.Type, "name": claim.Source.Name, "url": url, "settings": settings, "fetch_interval_minutes": claim.Source.FetchIntervalMinutes, "source_config_version": claim.Source.SourceConfigVersion}, "attempt": gin.H{"id": claim.Attempt.PublicID, "fence_token": claim.Attempt.FenceToken, "dispatcher_token": claim.DispatcherToken, "dispatcher_lease_expires_at": claim.Attempt.DispatcherLeaseExpiresAt}, "unit": gin.H{"id": claim.RootUnit.PublicID, "job_id": claim.RootUnit.JobID, "execution_lease_token": claim.ExecutionToken, "execution_lease_expires_at": claim.RootUnit.ExecutionLeaseExpiresAt, "unit_type": claim.RootUnit.UnitType}}
}

func decodeStrictJSON(c *gin.Context, target any) error {
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("request has trailing data")
	}
	return nil
}

func writeSourceRunContractError(c *gin.Context, err error) {
	if errors.Is(err, gorm.ErrRecordNotFound) {
		c.JSON(http.StatusNotFound, gin.H{"message": "Source-run target was not found"})
		return
	}
	c.JSON(http.StatusConflict, gin.H{"message": "Source-run preconditions changed"})
}
