package controllers

import (
	"net/http"
	"strings"
	"time"

	"content-management-system/src/contentstage"
	"content-management-system/src/models"
	"content-management-system/src/utils"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type contentStageTransitionRequest struct {
	contentstage.Correlation
	RetryAfterSec int    `json:"retry_after_sec,omitempty"`
	FailureClass  string `json:"failure_class,omitempty"`
	Summary       string `json:"summary,omitempty"`
}

func claimContentStage(c *gin.Context, lane string, media bool) {
	db := c.MustGet("db").(*gorm.DB)
	// The current rollout baseline is the default tenant. Derive both scope and
	// worker role server-side so an internal caller cannot widen a claim by
	// submitting a different tenant or owner label.
	tenantID, claimOwner := "default", "aggregation-"+lane+"-dispatcher"
	if media {
		claimOwner = "media-content-stage-worker"
	}
	var claim contentstage.ClaimEnvelope
	var found bool
	var err error
	if media {
		claim, found, err = contentstage.ClaimMediaNext(db, tenantID, claimOwner)
	} else {
		claim, found, err = contentstage.ClaimNext(db, tenantID, lane, claimOwner)
	}
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "content-stage claim unavailable", "reason": err.Error()})
		return
	}
	if !found {
		c.Status(http.StatusNoContent)
		return
	}
	c.JSON(http.StatusOK, claim)
}

func InternalClaimNewsContentStage(c *gin.Context) {
	claimContentStage(c, models.ContentStageLaneNews, false)
}
func InternalClaimPodsContentStage(c *gin.Context) {
	claimContentStage(c, models.ContentStageLanePods, false)
}
func InternalClaimMediaContentStage(c *gin.Context) {
	claimContentStage(c, models.ContentStageLanePods, true)
}

func authorizeStageTransition(c *gin.Context, db *gorm.DB, requestID uuid.UUID) error {
	var request models.ContentStageRequest
	if err := db.Where("public_id=?", requestID).First(&request).Error; err != nil {
		return err
	}
	principal, ok := utils.GetMachinePrincipal(c)
	if !ok {
		return gorm.ErrRecordNotFound
	}
	if request.Owner == models.ContentStageOwnerMedia && principal != utils.MachinePrincipalMedia {
		return gorm.ErrRecordNotFound
	}
	if (request.Owner == models.ContentStageOwnerAggregationNews || request.Owner == models.ContentStageOwnerAggregationPods) && principal != utils.MachinePrincipalAggregation {
		return gorm.ErrRecordNotFound
	}
	return nil
}

func contentStageTransition(c *gin.Context, action string) {
	db := c.MustGet("db").(*gorm.DB)
	requestID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid content-stage request id"})
		return
	}
	if err := authorizeStageTransition(c, db, requestID); err != nil {
		c.JSON(http.StatusForbidden, gin.H{"error": "content-stage transition is not owned by this service"})
		return
	}
	var req contentStageTransitionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid content-stage transition"})
		return
	}
	switch action {
	case "begin":
		err = contentstage.Begin(db, requestID, req.Correlation)
	case "heartbeat":
		err = contentstage.Heartbeat(db, requestID, req.Correlation)
	case "accepted":
		err = contentstage.MarkAccepted(db, requestID, req.Correlation)
	case "deferred":
		delay := req.RetryAfterSec
		if delay < 1 {
			delay = 1
		}
		if delay > 300 {
			delay = 300
		}
		err = contentstage.MarkDeferred(db, requestID, req.Correlation, time.Now().UTC().Add(time.Duration(delay)*time.Second), req.Summary)
	case "uncertain":
		err = contentstage.MarkUncertain(db, requestID, req.Correlation, req.Summary)
	case "failed":
		if strings.TrimSpace(req.FailureClass) == "" {
			req.FailureClass = "execution_failed"
		}
		err = contentstage.MarkFailed(db, requestID, req.Correlation, req.FailureClass, req.Summary)
	}
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "content-stage transition rejected", "reason": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "state": action})
}

func InternalBeginContentStage(c *gin.Context)     { contentStageTransition(c, "begin") }
func InternalHeartbeatContentStage(c *gin.Context) { contentStageTransition(c, "heartbeat") }
func InternalAcceptContentStage(c *gin.Context)    { contentStageTransition(c, "accepted") }
func InternalDeferContentStage(c *gin.Context)     { contentStageTransition(c, "deferred") }
func InternalUncertainContentStage(c *gin.Context) { contentStageTransition(c, "uncertain") }
func InternalFailContentStage(c *gin.Context)      { contentStageTransition(c, "failed") }

func InternalSettleAtomizationNotRequired(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	requestID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid content-stage request id"})
		return
	}
	if err := authorizeStageTransition(c, db, requestID); err != nil {
		c.JSON(http.StatusForbidden, gin.H{"error": "content-stage transition is not owned by this service"})
		return
	}
	var req contentStageTransitionRequest
	if c.ShouldBindJSON(&req) != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid content-stage transition"})
		return
	}
	var request models.ContentStageRequest
	if err := db.Where("public_id=? AND stage=?", requestID, models.ContentStagePodsAtomization).First(&request).Error; err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "request is not an atomization stage"})
		return
	}
	var item models.ContentItem
	if err := db.Where("tenant_id=? AND public_id=? AND processing_generation=?", request.TenantID, request.ContentItemID, request.ProcessingGeneration).First(&item).Error; err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "atomization target generation changed"})
		return
	}
	policy := atomizationPolicyForItem(db, &item)
	if !canSettleAtomizationNotRequired(item, policy) {
		c.JSON(http.StatusConflict, gin.H{"error": "atomization remains required by current CMS policy"})
		return
	}
	proof := map[string]any{"not_required": true, "chaptering_enabled": policy.ChapteringEnabled, "duration_sec": item.DurationSec, "reason": req.Summary}
	if err := contentstage.MarkNotRequired(db, requestID, req.Correlation, proof); err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "content-stage transition rejected", "reason": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "state": models.ContentStageVerified})
}

func canSettleAtomizationNotRequired(item models.ContentItem, policy atomizationPolicy) bool {
	return !policy.ChapteringEnabled || (item.DurationSec != nil && *item.DurationSec <= atomizationMinParentDurationSec)
}

func InternalGetContentStageTrace(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	contentID, err := uuid.Parse(c.Param("id"))
	if err != nil || strings.TrimSpace(c.Query("tenant_id")) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "valid content id and tenant_id are required"})
		return
	}
	trace, err := contentstage.Trace(db, c.Query("tenant_id"), contentID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "content-stage trace not found"})
		return
	}
	c.JSON(http.StatusOK, trace)
}

func AdminGetContentStageHealth(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	health, err := contentstage.Health(c.MustGet("db").(*gorm.DB), principal.TenantID)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "content-stage health unavailable"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"tenant_id": principal.TenantID, "lanes": health, "worker_healthy": contentstage.WorkerHealthy(time.Now().UTC())})
}

func AdminGetContentStageTrace(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	contentID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid content id"})
		return
	}
	trace, err := contentstage.Trace(c.MustGet("db").(*gorm.DB), principal.TenantID, contentID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "content-stage trace not found"})
		return
	}
	c.JSON(http.StatusOK, trace)
}

func AdminUpdateContentStageControl(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	var req struct {
		SchedulingEnabled          *bool  `json:"scheduling_enabled"`
		ExecutionEnabled           *bool  `json:"execution_enabled"`
		OptionalMetadataEnabled    *bool  `json:"optional_metadata_enabled"`
		TranscriptExecutionEnabled *bool  `json:"transcript_execution_enabled"`
		Reason                     string `json:"reason"`
	}
	if c.ShouldBindJSON(&req) != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid content-stage control"})
		return
	}
	value, err := contentstage.UpdateControl(c.MustGet("db").(*gorm.DB), principal.TenantID, c.Param("lane"), principal.Email, req.SchedulingEnabled, req.ExecutionEnabled, req.OptionalMetadataEnabled, req.TranscriptExecutionEnabled, req.Reason)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, value)
}

func AdminPromoteContentStageLane(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	var req struct {
		Mode               string `json:"mode"`
		VerificationDigest string `json:"verification_digest"`
	}
	if c.ShouldBindJSON(&req) != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid content-stage cutover"})
		return
	}
	value, err := contentstage.Promote(c.MustGet("db").(*gorm.DB), principal.TenantID, c.Param("lane"), req.Mode, principal.Email, req.VerificationDigest)
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, value)
}

func AdminGetContentStageQualification(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	digest, err := contentstage.QualificationDigest(c.MustGet("db").(*gorm.DB), principal.TenantID, c.Param("lane"))
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"lane": c.Param("lane"), "verification_digest": digest})
}

func AdminBackfillContentStages(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	var req struct {
		Limit int `json:"limit"`
	}
	_ = c.ShouldBindJSON(&req)
	count, err := contentstage.BackfillCurrent(c.MustGet("db").(*gorm.DB), principal.TenantID, c.Param("lane"), req.Limit)
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"backfilled_items": count})
}

func AdminCancelContentStage(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	requestID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid content-stage request id"})
		return
	}
	var req struct {
		Reason string `json:"reason"`
	}
	_ = c.ShouldBindJSON(&req)
	if err := contentstage.Cancel(c.MustGet("db").(*gorm.DB), principal.TenantID, requestID, req.Reason); err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"id": requestID, "state": models.ContentStageCancelled})
}
