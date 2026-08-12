package controllers

import (
	"net/http"
	"strings"

	"content-management-system/src/atomizationwork"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type atomizationWorkStep struct {
	ClaimToken string         `json:"claim_token"`
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

func bindAtomizationStep(c *gin.Context) (atomizationWorkStep, uuid.UUID, bool) {
	var body atomizationWorkStep
	if decodeStrictJSON(c, &body) != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid atomization work step"})
		return body, uuid.Nil, false
	}
	token, err := uuid.Parse(strings.TrimSpace(body.ClaimToken))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid atomization claim token"})
		return body, uuid.Nil, false
	}
	return body, token, true
}
func InternalBeginAtomizationWork(c *gin.Context) {
	_, token, ok := bindAtomizationStep(c)
	if !ok {
		return
	}
	if err := atomizationwork.Begin(c.MustGet("db").(*gorm.DB), c.Param("id"), "aggregation-atomization", token); err != nil {
		c.JSON(http.StatusConflict, gin.H{"message": "Atomization claim cannot begin"})
		return
	}
	c.Status(http.StatusNoContent)
}
func InternalHeartbeatAtomizationWork(c *gin.Context) {
	_, token, ok := bindAtomizationStep(c)
	if !ok {
		return
	}
	if err := atomizationwork.Heartbeat(c.MustGet("db").(*gorm.DB), c.Param("id"), "aggregation-atomization", token); err != nil {
		c.JSON(http.StatusConflict, gin.H{"message": "Atomization heartbeat rejected"})
		return
	}
	c.Status(http.StatusNoContent)
}
func InternalCheckpointAtomizationWork(c *gin.Context) {
	body, token, ok := bindAtomizationStep(c)
	if !ok {
		return
	}
	if err := atomizationwork.Checkpoint(c.MustGet("db").(*gorm.DB), c.Param("id"), token, strings.TrimSpace(body.Phase), body.Proof); err != nil {
		c.JSON(http.StatusConflict, gin.H{"message": "Atomization checkpoint rejected"})
		return
	}
	c.Status(http.StatusNoContent)
}
