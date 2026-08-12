package controllers

import (
	"encoding/json"
	"net/http"
	"strings"

	"content-management-system/src/models"
	"content-management-system/src/pipeline"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// These endpoints intentionally expose only a CMS-selected repair and its
// opaque claim token. Aggregation cannot submit a content ID, tenant, stage,
// queue, arbitrary job identity, or retry payload.
func InternalClaimPipelineRepair(c *gin.Context) {
	claim, found, err := pipeline.ClaimNext(c.MustGet("db").(*gorm.DB), "aggregation-pipeline-repair-dispatcher")
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "pipeline repair claim unavailable"})
		return
	}
	if !found {
		c.Status(http.StatusNoContent)
		return
	}
	var item struct {
		Type        string
		Source      string
		OriginalURL *string
		MediaURL    *string
		Title       *string
		Excerpt     *string
		BodyText    *string
		Metadata    datatypes.JSON
	}
	if err := c.MustGet("db").(*gorm.DB).Model(&models.ContentItem{}).Where("public_id = ? AND tenant_id = ? AND updated_at = ?", claim.Request.ContentItemID, claim.Request.TenantID, claim.Request.ExpectedItemUpdatedAt).First(&item).Error; err != nil {
		c.JSON(http.StatusConflict, gin.H{"message": "pipeline repair target changed"})
		return
	}
	metadata := map[string]any{}
	_ = json.Unmarshal(item.Metadata, &metadata)
	c.JSON(http.StatusOK, gin.H{"id": claim.Request.PublicID, "attempt_id": claim.Attempt.PublicID, "tenant_id": claim.Request.TenantID, "claim_token": claim.ClaimToken, "deterministic_job_id": claim.Request.DeterministicJobID, "stage": claim.Request.Stage, "content_item_id": claim.Request.ContentItemID, "item_version": claim.Request.ExpectedItemUpdatedAt, "source_run_request_id": claim.Request.SourceRunRequestID, "fence_token": claim.Attempt.FenceToken, "lease_expires_at": claim.Lease.LeaseExpiresAt, "lease_epoch": claim.Lease.LeaseEpoch, "effect_input_digest": claim.Request.EffectInputDigest, "content": gin.H{"type": item.Type, "source": item.Source, "original_url": item.OriginalURL, "media_url": item.MediaURL, "title": item.Title, "excerpt": item.Excerpt, "body_text": item.BodyText, "metadata": metadata}})
}

func pipelineRepairToken(c *gin.Context) (uuid.UUID, bool) {
	raw := strings.TrimSpace(c.GetHeader("X-Pipeline-Repair-Claim"))
	if raw == "" && c.Request.Method == http.MethodPost {
		var body struct {
			ClaimToken string `json:"claim_token"`
		}
		// This is an opaque CMS-issued lease token, not a caller-selected
		// target or command. The shared service client keeps custom headers
		// private, so the fixed contract carries it in the typed body.
		if c.ShouldBindJSON(&body) == nil {
			raw = strings.TrimSpace(body.ClaimToken)
		}
	}
	token, err := uuid.Parse(raw)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "pipeline repair claim token is required"})
		return uuid.Nil, false
	}
	return token, true
}
func InternalBeginPipelineRepair(c *gin.Context) {
	token, ok := pipelineRepairToken(c)
	if !ok {
		return
	}
	if err := pipeline.Begin(c.MustGet("db").(*gorm.DB), c.Param("id"), "aggregation-pipeline-repair-dispatcher", token); err != nil {
		c.JSON(http.StatusConflict, gin.H{"message": "pipeline repair begin preconditions changed"})
		return
	}
	c.Status(http.StatusNoContent)
}
func InternalHeartbeatPipelineRepair(c *gin.Context) {
	token, ok := pipelineRepairToken(c)
	if !ok {
		return
	}
	if err := pipeline.Heartbeat(c.MustGet("db").(*gorm.DB), c.Param("id"), "aggregation-pipeline-repair-dispatcher", token); err != nil {
		c.JSON(http.StatusConflict, gin.H{"message": "pipeline repair heartbeat rejected"})
		return
	}
	c.Status(http.StatusNoContent)
}

// InternalCompletePipelineRepair is the fenced terminal receipt from the
// dedicated Aggregation stage executor. It cannot name a target, stage, queue
// or provider request: those identities were frozen by the CMS claim.
func InternalCompletePipelineRepair(c *gin.Context) {
	var body struct {
		ClaimToken      string         `json:"claim_token"`
		ProducerEventID string         `json:"producer_event_id"`
		OutputDigest    string         `json:"output_digest"`
		Output          map[string]any `json:"output"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "pipeline repair terminal receipt is invalid"})
		return
	}
	token, err := uuid.Parse(strings.TrimSpace(body.ClaimToken))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "pipeline repair claim token is required"})
		return
	}
	producerEventID, err := uuid.Parse(strings.TrimSpace(body.ProducerEventID))
	if err != nil || strings.TrimSpace(body.OutputDigest) == "" || len(body.OutputDigest) > 128 {
		c.JSON(http.StatusBadRequest, gin.H{"message": "pipeline repair terminal receipt is invalid"})
		return
	}
	if body.Output == nil {
		body.Output = map[string]any{}
	}
	encoded, err := json.Marshal(body.Output)
	if err != nil || len(encoded) > 8<<10 {
		c.JSON(http.StatusBadRequest, gin.H{"message": "pipeline repair terminal proof exceeds bounds"})
		return
	}
	if err := pipeline.CompleteEffect(c.MustGet("db").(*gorm.DB), c.Param("id"), "aggregation-pipeline-repair-dispatcher", token, pipeline.EffectReceipt{ProducerEventID: producerEventID, OutputDigest: strings.TrimSpace(body.OutputDigest), Output: body.Output}); err != nil {
		c.JSON(http.StatusConflict, gin.H{"message": "pipeline repair terminal receipt rejected"})
		return
	}
	c.Status(http.StatusNoContent)
}

// Cancellation observation has no effect-side input. It only lets a worker
// stop safely before a subsequent phase boundary.
func InternalObservePipelineRepairCancellation(c *gin.Context) {
	token, ok := pipelineRepairToken(c)
	if !ok {
		return
	}
	var state struct {
		CancellationRequestedAt *string `json:"cancellation_requested_at"`
	}
	if err := c.MustGet("db").(*gorm.DB).Raw("SELECT cancellation_requested_at FROM pipeline_repair_requests WHERE public_id = ? AND claim_token = ?", c.Param("id"), token).Scan(&state).Error; err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "pipeline repair cancellation state unavailable"})
		return
	}
	c.JSON(http.StatusOK, state)
}
