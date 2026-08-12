package controllers

import (
	"net/http"
	"strings"
	"time"

	"content-management-system/src/supply"
	"content-management-system/src/utils"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

const supplyActionOwnerLease = 2 * time.Minute

// InternalClaimUnitAdoptionAction gives the Aggregation dispatcher one
// fenced, CMS-selected action. It accepts no tenant, episode, source, unit,
// job, queue, or provider fields from the caller.
func InternalClaimUnitAdoptionAction(c *gin.Context) {
	if !requireAggregationSourceRunPrincipal(c) {
		return
	}
	lease, found, err := supply.ClaimNextSupplyActionForOwner(c.MustGet("db").(*gorm.DB), supply.SupplyActionOwnerAggregationDispatcher, string(utils.MachinePrincipalAggregation), supplyActionOwnerLease)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Supply action claim is unavailable"})
		return
	}
	if !found {
		c.Status(http.StatusNoContent)
		return
	}
	c.JSON(http.StatusOK, gin.H{"id": lease.Request.PublicID, "claim_token": lease.ClaimToken})
}

// InternalPrepareUnitAdoption turns the claimed opaque action into exactly
// one CMS-derived coordinator envelope. The static route is deliberately not
// shared with receipt recovery or ordinary source-run dispatch.
func InternalPrepareUnitAdoption(c *gin.Context) {
	if !requireAggregationSourceRunPrincipal(c) {
		return
	}
	var body struct {
		ClaimToken string `json:"claim_token"`
	}
	if err := decodeStrictJSON(c, &body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid unit adoption claim"})
		return
	}
	claim, _, err := supply.PrepareUnitAdoption(c.MustGet("db").(*gorm.DB), c.Param("action"), string(utils.MachinePrincipalAggregation), strings.TrimSpace(body.ClaimToken), supplyActionOwnerLease, supplyActionOwnerLease)
	if err != nil {
		writeSourceRunContractError(c, err)
		return
	}
	c.JSON(http.StatusOK, dispatchClaimResponse(claim))
}

// InternalAcknowledgeUnitAdoption records only that Aggregation durably
// accepted the fixed coordinator job. CMS retains the action in verification
// until a separately retained dispatch receipt proves the handoff executed.
func InternalAcknowledgeUnitAdoption(c *gin.Context) {
	if !requireAggregationSourceRunPrincipal(c) {
		return
	}
	var body struct {
		ClaimToken string `json:"claim_token"`
	}
	if err := decodeStrictJSON(c, &body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid unit adoption acknowledgement"})
		return
	}
	if err := supply.MarkUnitAdoptionQueued(c.MustGet("db").(*gorm.DB), c.Param("action"), string(utils.MachinePrincipalAggregation), strings.TrimSpace(body.ClaimToken)); err != nil {
		writeSourceRunContractError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "state": "verifying"})
}

func InternalClaimReceiptRedeliveryAction(c *gin.Context) {
	if !requireAggregationSourceRunPrincipal(c) {
		return
	}
	lease, found, err := supply.ClaimNextSupplyActionForOwner(c.MustGet("db").(*gorm.DB), supply.SupplyActionOwnerAggregationReceipt, string(utils.MachinePrincipalAggregation), supplyActionOwnerLease)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Supply receipt action claim is unavailable"})
		return
	}
	if !found {
		c.Status(http.StatusNoContent)
		return
	}
	c.JSON(http.StatusOK, gin.H{"id": lease.Request.PublicID, "claim_token": lease.ClaimToken})
}

func InternalPrepareReceiptRedelivery(c *gin.Context) {
	if !requireAggregationSourceRunPrincipal(c) {
		return
	}
	var body struct {
		ClaimToken string `json:"claim_token"`
	}
	if err := decodeStrictJSON(c, &body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid receipt redelivery claim"})
		return
	}
	receipt, err := supply.PrepareReceiptRedelivery(c.MustGet("db").(*gorm.DB), c.Param("action"), string(utils.MachinePrincipalAggregation), strings.TrimSpace(body.ClaimToken))
	if err != nil {
		writeSourceRunContractError(c, err)
		return
	}
	c.Data(http.StatusOK, "application/json", receipt)
}

func InternalCompleteReceiptRedelivery(c *gin.Context) {
	if !requireAggregationSourceRunPrincipal(c) {
		return
	}
	var body struct {
		ClaimToken string `json:"claim_token"`
	}
	if err := decodeStrictJSON(c, &body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid receipt redelivery completion"})
		return
	}
	if err := supply.CompleteReceiptRedelivery(c.MustGet("db").(*gorm.DB), c.Param("action"), string(utils.MachinePrincipalAggregation), strings.TrimSpace(body.ClaimToken)); err != nil {
		writeSourceRunContractError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "state": "succeeded"})
}
