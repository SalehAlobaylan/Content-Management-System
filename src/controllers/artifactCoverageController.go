package controllers

import (
	"net/http"
	"strings"

	"content-management-system/src/artifacts"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

func InternalClaimMediaArtifactCoverage(c *gin.Context) {
	internalClaimArtifactCoverage(c, artifacts.MediaOwner)
}
func internalClaimArtifactCoverage(c *gin.Context, owner string) {
	claim, found, err := artifacts.ClaimNext(c.MustGet("db").(*gorm.DB), owner)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "artifact coverage claim unavailable"})
		return
	}
	if !found {
		c.Status(http.StatusNoContent)
		return
	}
	item := claim.Item
	c.JSON(http.StatusOK, gin.H{"id": claim.Request.PublicID, "attempt_id": claim.Attempt.PublicID, "claim_token": claim.ClaimToken, "fence_token": claim.Attempt.FenceToken, "input_digest": claim.Request.InputDigest, "artifact": claim.Request.Artifact, "deterministic_job_id": claim.Attempt.DeterministicJobID, "content": gin.H{"id": item.PublicID, "type": item.Type, "original_url": item.OriginalURL, "media_url": item.MediaURL, "thumbnail_url": item.ThumbnailURL, "title": item.Title, "excerpt": item.Excerpt, "body_text": item.BodyText, "duration_sec": item.DurationSec}})
}
func artifactClaimToken(c *gin.Context) (uuid.UUID, bool) {
	var body struct {
		ClaimToken string         `json:"claim_token"`
		Proof      map[string]any `json:"proof"`
	}
	if c.ShouldBindJSON(&body) != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "artifact claim token is required"})
		return uuid.Nil, false
	}
	token, err := uuid.Parse(strings.TrimSpace(body.ClaimToken))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "artifact claim token is invalid"})
		return uuid.Nil, false
	}
	c.Set("artifact_acceptance_proof", body.Proof)
	return token, true
}
func InternalBeginMediaArtifactCoverage(c *gin.Context) {
	token, ok := artifactClaimToken(c)
	if !ok {
		return
	}
	if _, err := artifacts.Begin(c.MustGet("db").(*gorm.DB), c.Param("id"), artifacts.MediaOwner, token); err != nil {
		c.JSON(http.StatusConflict, gin.H{"message": "artifact claim cannot begin"})
		return
	}
	c.Status(http.StatusNoContent)
}
func InternalHeartbeatMediaArtifactCoverage(c *gin.Context) {
	token, ok := artifactClaimToken(c)
	if !ok {
		return
	}
	if _, err := artifacts.Heartbeat(c.MustGet("db").(*gorm.DB), c.Param("id"), artifacts.MediaOwner, token); err != nil {
		c.JSON(http.StatusConflict, gin.H{"message": "artifact heartbeat rejected"})
		return
	}
	c.Status(http.StatusNoContent)
}
func InternalAcceptMediaArtifactCoverage(c *gin.Context) {
	token, ok := artifactClaimToken(c)
	if !ok {
		return
	}
	proof, _ := c.Get("artifact_acceptance_proof")
	value, _ := proof.(map[string]any)
	if err := artifacts.MarkAccepted(c.MustGet("db").(*gorm.DB), c.Param("id"), artifacts.MediaOwner, token, value); err != nil {
		c.JSON(http.StatusConflict, gin.H{"message": "artifact acceptance rejected"})
		return
	}
	c.Status(http.StatusNoContent)
}

func InternalUncertainMediaArtifactCoverage(c *gin.Context) {
	token, ok := artifactClaimToken(c)
	if !ok {
		return
	}
	if err := artifacts.MarkUncertain(c.MustGet("db").(*gorm.DB), c.Param("id"), artifacts.MediaOwner, token, "media_owner_effect_uncertain"); err != nil {
		c.JSON(http.StatusConflict, gin.H{"message": "artifact uncertainty receipt rejected"})
		return
	}
	c.Status(http.StatusNoContent)
}
