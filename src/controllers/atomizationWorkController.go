package controllers

import (
	"net/http"
	"strings"
	"time"

	"content-management-system/src/atomizationwork"
	"content-management-system/src/models"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type atomizationWorkStep struct {
	ClaimToken string         `json:"claim_token"`
	FenceToken string         `json:"fence_token"`
	Phase      string         `json:"phase,omitempty"`
	Proof      map[string]any `json:"proof,omitempty"`
}

func InternalClaimAtomizationWork(c *gin.Context) {
	claim, found, err := atomizationwork.ClaimNext(c.MustGet("db").(*gorm.DB), "aggregation-atomization")
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Atomization claim unavailable"})
		return
	}
	if !found {
		c.Status(http.StatusNoContent)
		return
	}
	c.JSON(http.StatusOK, gin.H{"id": claim.Request.PublicID, "attempt_id": claim.Attempt.PublicID, "claim_token": claim.Attempt.ClaimToken, "fence_token": claim.Attempt.FenceToken, "deterministic_job_id": claim.Attempt.DeterministicJobID, "input_fingerprint": claim.Request.InputFingerprint, "parent_content_item_id": claim.Parent.PublicID})
}

func bindAtomizationStep(c *gin.Context) (atomizationWorkStep, uuid.UUID, uuid.UUID, bool) {
	var body atomizationWorkStep
	if decodeStrictJSON(c, &body) != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid atomization work step"})
		return body, uuid.Nil, uuid.Nil, false
	}
	token, err := uuid.Parse(strings.TrimSpace(body.ClaimToken))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid atomization claim token"})
		return body, uuid.Nil, uuid.Nil, false
	}
	fence, err := uuid.Parse(strings.TrimSpace(body.FenceToken))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid atomization fence token"})
		return body, uuid.Nil, uuid.Nil, false
	}
	return body, token, fence, true
}
func InternalBeginAtomizationWork(c *gin.Context) {
	_, token, fence, ok := bindAtomizationStep(c)
	if !ok {
		return
	}
	if err := atomizationwork.Begin(c.MustGet("db").(*gorm.DB), c.Param("id"), "aggregation-atomization", token, fence); err != nil {
		c.JSON(http.StatusConflict, gin.H{"message": "Atomization claim cannot begin"})
		return
	}
	c.Status(http.StatusNoContent)
}
func InternalHeartbeatAtomizationWork(c *gin.Context) {
	_, token, fence, ok := bindAtomizationStep(c)
	if !ok {
		return
	}
	if err := atomizationwork.Heartbeat(c.MustGet("db").(*gorm.DB), c.Param("id"), "aggregation-atomization", token, fence); err != nil {
		c.JSON(http.StatusConflict, gin.H{"message": "Atomization heartbeat rejected"})
		return
	}
	var attempt models.AtomizationWorkAttempt
	if err := c.MustGet("db").(*gorm.DB).Where("claim_token=? AND fence_token=?", token, fence).First(&attempt).Error; err != nil {
		c.JSON(503, gin.H{"error": "lease readback unavailable"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"lease_expires_at": attempt.LeaseExpiresAt})
}

func InternalDeferAtomizationWork(c *gin.Context) {
	var body struct {
		ClaimToken    string `json:"claim_token"`
		FenceToken    string `json:"fence_token"`
		RetryAfterSec int    `json:"retry_after_sec"`
		Summary       string `json:"summary"`
	}
	if err := decodeStrictJSON(c, &body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid atomization deferral"})
		return
	}
	token, err := uuid.Parse(strings.TrimSpace(body.ClaimToken))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid atomization claim token"})
		return
	}
	fence, err := uuid.Parse(strings.TrimSpace(body.FenceToken))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid atomization fence token"})
		return
	}
	if err := atomizationwork.Defer(c.MustGet("db").(*gorm.DB), c.Param("id"), "aggregation-atomization", token, fence, time.Duration(body.RetryAfterSec)*time.Second, body.Summary); err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "atomization deferral rejected", "reason": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "state": "queued"})
}
func InternalCheckpointAtomizationWork(c *gin.Context) {
	body, token, fence, ok := bindAtomizationStep(c)
	if !ok {
		return
	}
	if err := atomizationwork.Checkpoint(c.MustGet("db").(*gorm.DB), c.Param("id"), token, fence, strings.TrimSpace(body.Phase), body.Proof); err != nil {
		c.JSON(http.StatusConflict, gin.H{"message": "Atomization checkpoint rejected"})
		return
	}
	c.Status(http.StatusNoContent)
}
