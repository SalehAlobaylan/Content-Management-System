package controllers

import (
	"encoding/json"
	"io"
	"math"
	"net/http"
	"strings"
	"time"

	"content-management-system/src/models"
	operatorpkg "content-management-system/src/operator"
	"content-management-system/src/utils"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type operatorScheduleCreateRequest struct {
	InvestigationID string `json:"investigation_id"`
	CadenceMinutes  int    `json:"cadence_minutes"`
	Confirmation    string `json:"confirmation"`
}
type operatorConfirmationRequest struct {
	Confirmation string `json:"confirmation"`
	Reason       string `json:"reason,omitempty"`
}
type operatorShareRequest struct {
	RecipientID  string `json:"recipient_id"`
	Confirmation string `json:"confirmation"`
}
type operatorFeedbackRequest struct {
	Confirmation  string `json:"confirmation"`
	Reason        string `json:"reason,omitempty"`
	SnoozeMinutes int    `json:"snooze_minutes,omitempty"`
}
type operatorControlDisableRequest struct {
	Kind         string `json:"kind"`
	Key          string `json:"key,omitempty"`
	Reason       string `json:"reason"`
	Confirmation string `json:"confirmation"`
}

func operatorGovernancePrincipal(c *gin.Context) (utils.AdminPrincipal, string, *gorm.DB, bool) {
	principal, ok := utils.GetAdminPrincipal(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"message": "Authentication required"})
		return utils.AdminPrincipal{}, "", nil, false
	}
	tenantID, err := operatorpkg.RequireExplicitTenant(principal)
	if err != nil {
		c.JSON(http.StatusForbidden, gin.H{"message": "Explicit tenant claim required"})
		return utils.AdminPrincipal{}, "", nil, false
	}
	db := c.MustGet("db").(*gorm.DB)
	_, policy, err := operatorExecutionPolicy(db, tenantID)
	if err != nil || !policy.LaunchMode.AdminSurfaceEnabled() {
		c.Status(http.StatusNotFound)
		return utils.AdminPrincipal{}, "", nil, false
	}
	return principal, tenantID, db, true
}

func operatorFreshAccess(c *gin.Context, userID, tenantID string) (operatorpkg.AccessSnapshot, bool) {
	client, err := operatorpkg.NewIAMAccessClientFromEnv()
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Current Operator access cannot be verified"})
		return operatorpkg.AccessSnapshot{}, false
	}
	access, err := client.Snapshot(c.Request.Context(), userID, tenantID)
	if err != nil || !access.IsAdmin {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Current Operator access cannot be verified"})
		return operatorpkg.AccessSnapshot{}, false
	}
	return access, true
}

func CreateOperatorSchedule(c *gin.Context) {
	principal, tenantID, db, ok := operatorGovernancePrincipal(c)
	if !ok {
		return
	}
	_, policy, err := operatorExecutionPolicy(db, tenantID)
	if err != nil || !policy.SchedulesEnabled || !policy.ReadEnabled {
		c.Status(http.StatusNotFound)
		return
	}
	var body operatorScheduleCreateRequest
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil || decoder.Decode(&struct{}{}) != io.EOF || body.Confirmation != "SCHEDULE" {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Confirm this read-only schedule with SCHEDULE"})
		return
	}
	access, ok := operatorFreshAccess(c, principal.UserID, tenantID)
	if !ok {
		return
	}
	publicID, err := uuid.Parse(strings.TrimSpace(body.InvestigationID))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid Operator investigation"})
		return
	}
	var investigation models.OperatorInvestigation
	if err := db.Where("public_id=? AND tenant_id=? AND actor_id=? AND state=?", publicID, tenantID, principal.UserID, "completed").First(&investigation).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"message": "Completed creator-owned investigation not found"})
		return
	}
	input, err := operatorpkg.DecodeStoredInvestigationInput(investigation)
	if err != nil || input.Intent == operatorpkg.IntentResolve {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"message": "Only a completed read investigation can be scheduled"})
		return
	}
	var evidence []models.OperatorEvidence
	if err := db.Where("investigation_id=? AND tenant_id=?", investigation.ID, tenantID).Find(&evidence).Error; err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Schedule evidence is unavailable"})
		return
	}
	permissions := make([]string, 0, len(evidence))
	for _, row := range evidence {
		permissions = append(permissions, row.RequiredPermission)
	}
	template := operatorpkg.ScheduledTemplate{VisibleContext: input.VisibleContext, Request: operatorpkg.InvestigationRequest{Intent: input.Intent, Message: input.Message, Tier: input.Tier}, AdapterKeys: []string{operatorpkg.AdapterKeyForVisibleContext(input.VisibleContext)}}
	schedule, err := operatorpkg.NewScheduleStore(db).Create(c.Request.Context(), access, template, permissions, input.Locale, body.CadenceMinutes)
	if err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"message": "Schedule could not be created from current access and read context"})
		return
	}
	c.JSON(http.StatusCreated, operatorScheduleResponse(schedule))
}

func ListOperatorSchedules(c *gin.Context) {
	_, tenantID, db, ok := operatorGovernancePrincipal(c)
	if !ok {
		return
	}
	schedules, err := operatorpkg.NewScheduleStore(db).List(c.Request.Context(), tenantID, 100)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Schedules are unavailable"})
		return
	}
	out := make([]gin.H, 0, len(schedules))
	for _, schedule := range schedules {
		out = append(out, operatorScheduleResponse(schedule))
	}
	c.JSON(http.StatusOK, gin.H{"items": out})
}

func TakeoverOperatorSchedule(c *gin.Context) {
	principal, tenantID, db, ok := operatorGovernancePrincipal(c)
	if !ok {
		return
	}
	_, policy, err := operatorExecutionPolicy(db, tenantID)
	if err != nil || !policy.SchedulesEnabled {
		c.Status(http.StatusNotFound)
		return
	}
	var body operatorConfirmationRequest
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil || decoder.Decode(&struct{}{}) != io.EOF || body.Confirmation != "TAKEOVER" {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Confirm takeover with TAKEOVER"})
		return
	}
	access, ok := operatorFreshAccess(c, principal.UserID, tenantID)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid schedule"})
		return
	}
	schedule, err := operatorpkg.NewScheduleStore(db).Load(c.Request.Context(), tenantID, id.String())
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"message": "Schedule not found"})
		return
	}
	updated, err := operatorpkg.NewScheduleStore(db).Takeover(c.Request.Context(), schedule.ID, access)
	if err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"message": "Fresh access does not permit takeover"})
		return
	}
	c.JSON(http.StatusOK, operatorScheduleResponse(updated))
}

func PauseOperatorSchedule(c *gin.Context) {
	principal, tenantID, db, ok := operatorGovernancePrincipal(c)
	if !ok {
		return
	}
	var body operatorConfirmationRequest
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil || decoder.Decode(&struct{}{}) != io.EOF || body.Confirmation != "PAUSE" {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Confirm pause with PAUSE"})
		return
	}
	if _, ok := operatorFreshAccess(c, principal.UserID, tenantID); !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid schedule"})
		return
	}
	schedule, err := operatorpkg.NewScheduleStore(db).Load(c.Request.Context(), tenantID, id.String())
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"message": "Schedule not found"})
		return
	}
	if err := operatorpkg.NewScheduleStore(db).Pause(c.Request.Context(), schedule.ID, tenantID, principal.UserID, body.Reason); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Schedule could not be paused"})
		return
	}
	c.Status(http.StatusNoContent)
}

func ResumeOperatorSchedule(c *gin.Context) {
	principal, tenantID, db, ok := operatorGovernancePrincipal(c)
	if !ok {
		return
	}
	_, policy, err := operatorExecutionPolicy(db, tenantID)
	if err != nil || !policy.SchedulesEnabled {
		c.Status(http.StatusNotFound)
		return
	}
	var body operatorConfirmationRequest
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil || decoder.Decode(&struct{}{}) != io.EOF || body.Confirmation != "RESUME" {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Confirm resume with RESUME"})
		return
	}
	access, ok := operatorFreshAccess(c, principal.UserID, tenantID)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid schedule"})
		return
	}
	schedule, err := operatorpkg.NewScheduleStore(db).Load(c.Request.Context(), tenantID, id.String())
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"message": "Schedule not found"})
		return
	}
	updated, err := operatorpkg.NewScheduleStore(db).Resume(c.Request.Context(), schedule.ID, access)
	if err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"message": "Schedule cannot be resumed with current ownership and access"})
		return
	}
	c.JSON(http.StatusOK, operatorScheduleResponse(updated))
}

func GetOperatorSchedule(c *gin.Context) {
	_, tenantID, db, ok := operatorGovernancePrincipal(c)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid schedule"})
		return
	}
	schedule, err := operatorpkg.NewScheduleStore(db).Load(c.Request.Context(), tenantID, id.String())
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"message": "Schedule not found"})
		return
	}
	c.JSON(http.StatusOK, operatorScheduleResponse(schedule))
}

func ListOperatorScheduleRuns(c *gin.Context) {
	_, tenantID, db, ok := operatorGovernancePrincipal(c)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid schedule"})
		return
	}
	schedule, err := operatorpkg.NewScheduleStore(db).Load(c.Request.Context(), tenantID, id.String())
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"message": "Schedule not found"})
		return
	}
	var runs []models.OperatorScheduleRun
	if err := db.Where("schedule_id=? AND tenant_id=?", schedule.ID, tenantID).Order("created_at DESC").Limit(100).Find(&runs).Error; err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Schedule runs unavailable"})
		return
	}
	out := make([]gin.H, 0, len(runs))
	for _, run := range runs {
		out = append(out, gin.H{"id": run.PublicID, "state": run.State, "pause_reason": run.PauseReason, "started_at": run.StartedAt, "finished_at": run.FinishedAt})
	}
	c.JSON(http.StatusOK, gin.H{"items": out})
}

func GetOperatorControls(c *gin.Context) {
	_, tenantID, db, ok := operatorGovernancePrincipal(c)
	if !ok {
		return
	}
	var row models.OperatorPolicy
	_ = db.Where("tenant_id=?", tenantID).First(&row).Error
	_, policy, err := operatorExecutionPolicy(db, tenantID)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Operator controls unavailable"})
		return
	}
	var controls []models.OperatorCapabilityControl
	if err := db.Where("tenant_id=?", tenantID).Order("capability_kind, capability_key").Find(&controls).Error; err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Operator capability controls unavailable"})
		return
	}
	soft, hard := operatorSpendStatus(db, tenantID, row.InteractiveSoftSpendLimit, row.DeepHardSpendLimit)
	c.JSON(http.StatusOK, gin.H{"operational": true, "controls": gin.H{"read_enabled": policy.ReadEnabled, "llm_enabled": policy.LLMEnabled, "execution_enabled": policy.ExecutionEnabled, "schedules_enabled": policy.SchedulesEnabled, "adapters": controls}, "spend": gin.H{"interactive": soft, "scheduled_hard_stop": hard}, "metrics": operatorOutcomeMetrics(db, tenantID)})
}

func DisableOperatorControl(c *gin.Context) {
	principal, tenantID, db, ok := operatorGovernancePrincipal(c)
	if !ok {
		return
	}
	var body operatorControlDisableRequest
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil || decoder.Decode(&struct{}{}) != io.EOF || body.Confirmation != "DISABLE" || strings.TrimSpace(body.Reason) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Confirm a non-empty disable request with DISABLE"})
		return
	}
	if _, ok := operatorFreshAccess(c, principal.UserID, tenantID); !ok {
		return
	}
	if body.Kind == "adapter" || body.Kind == "tool" {
		if strings.TrimSpace(body.Key) == "" {
			c.JSON(http.StatusBadRequest, gin.H{"message": "Capability key is required"})
			return
		}
		control := models.OperatorCapabilityControl{TenantID: tenantID, CapabilityKind: body.Kind, CapabilityKey: body.Key, Disabled: true, Reason: body.Reason, ActorID: principal.UserID}
		if err := db.Where("tenant_id=? AND capability_kind=? AND capability_key=?", tenantID, body.Kind, body.Key).Assign(control).FirstOrCreate(&control).Error; err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Capability could not be disabled"})
			return
		}
		c.Status(http.StatusNoContent)
		return
	}
	if body.Kind != "read" && body.Kind != "llm" && body.Kind != "execution" && body.Kind != "schedules" {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Unsupported disable-only control"})
		return
	}
	control := models.OperatorCapabilityControl{TenantID: tenantID, CapabilityKind: "system", CapabilityKey: body.Kind, Disabled: true, Reason: body.Reason, ActorID: principal.UserID}
	if err := db.Where("tenant_id=? AND capability_kind=? AND capability_key=?", tenantID, "system", body.Kind).Assign(control).FirstOrCreate(&control).Error; err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Control could not be disabled"})
		return
	}
	c.Status(http.StatusNoContent)
}

func CreateOperatorInvestigationShare(c *gin.Context) {
	principal, tenantID, db, ok := operatorGovernancePrincipal(c)
	if !ok {
		return
	}
	var body operatorShareRequest
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil || decoder.Decode(&struct{}{}) != io.EOF || body.Confirmation != "SHARE" || strings.TrimSpace(body.RecipientID) == "" || body.RecipientID == principal.UserID {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Confirm a different tenant recipient with SHARE"})
		return
	}
	if _, ok := operatorFreshAccess(c, principal.UserID, tenantID); !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid Operator investigation"})
		return
	}
	var investigation models.OperatorInvestigation
	if err := db.Where("public_id=? AND tenant_id=? AND actor_id=?", id, tenantID, principal.UserID).First(&investigation).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"message": "Creator-owned investigation not found"})
		return
	}
	share := models.OperatorInvestigationShare{InvestigationID: investigation.ID, TenantID: tenantID, RecipientID: strings.TrimSpace(body.RecipientID), CreatedBy: principal.UserID, State: "active"}
	if err := db.Where("investigation_id=? AND recipient_id=?", investigation.ID, share.RecipientID).Assign(models.OperatorInvestigationShare{State: "active", CreatedBy: principal.UserID, RevokedAt: nil}).FirstOrCreate(&share).Error; err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Investigation share could not be saved"})
		return
	}
	c.JSON(http.StatusCreated, gin.H{"id": share.PublicID, "investigation_id": id, "recipient_id": share.RecipientID, "state": "active", "non_transferable": true})
}

func ApplyOperatorRecommendationFeedback(c *gin.Context) {
	principal, tenantID, db, ok := operatorGovernancePrincipal(c)
	if !ok {
		return
	}
	var body operatorFeedbackRequest
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid recommendation feedback"})
		return
	}
	kind := c.Param("kind")
	required := ""
	switch kind {
	case "snooze":
		required = "SNOOZE"
	case "dismiss":
		required = "DISMISS"
	case "subject-override":
		required = "OVERRIDE"
	default:
		c.JSON(http.StatusNotFound, gin.H{"message": "Feedback action not found"})
		return
	}
	if body.Confirmation != required {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Confirmation does not match this feedback action"})
		return
	}
	if _, ok := operatorFreshAccess(c, principal.UserID, tenantID); !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid recommendation"})
		return
	}
	var recommendation models.OperatorRecommendation
	if err := db.Where("public_id=? AND tenant_id=?", id, tenantID).First(&recommendation).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"message": "Recommendation not found"})
		return
	}
	if time.Now().UTC().After(recommendation.ExpiresAt) {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"message": "Recommendation has expired"})
		return
	}
	var expires *time.Time
	state := "dismissed"
	if kind == "snooze" {
		if body.SnoozeMinutes < 15 || body.SnoozeMinutes > 7*24*60 {
			c.JSON(http.StatusBadRequest, gin.H{"message": "Snooze must be 15 minutes through 7 days"})
			return
		}
		next := time.Now().UTC().Add(time.Duration(body.SnoozeMinutes) * time.Minute)
		expires = &next
		state = "snoozed"
	}
	if kind == "subject-override" {
		state = "blocked"
	}
	feedback := models.OperatorRecommendationFeedback{RecommendationID: recommendation.ID, TenantID: tenantID, ActorID: principal.UserID, FeedbackKind: strings.ReplaceAll(kind, "-", "_"), Reason: body.Reason, ExpiresAt: expires}
	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&feedback).Error; err != nil {
			return err
		}
		return tx.Model(&recommendation).Updates(map[string]any{"state": state}).Error
	}); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Recommendation feedback could not be recorded"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"id": recommendation.PublicID, "state": state, "expires_at": expires})
}

// ListSharedOperatorInvestigationEvents is a deliberately reduced read model:
// the invitation is checked first, then every response block is redacted unless
// the receiver's *current* IAM snapshot independently covers its evidence.
// Plans, approvals, executor events, and the creator's authority never cross
// this boundary.
func ListSharedOperatorInvestigationEvents(c *gin.Context) {
	principal, tenantID, db, ok := operatorGovernancePrincipal(c)
	if !ok {
		return
	}
	_, policy, err := operatorExecutionPolicy(db, tenantID)
	if err != nil || !policy.ReadEnabled {
		c.Status(http.StatusNotFound)
		return
	}
	access, ok := operatorFreshAccess(c, principal.UserID, tenantID)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid Operator investigation"})
		return
	}
	var investigation models.OperatorInvestigation
	if err := db.Where("public_id=? AND tenant_id=?", id, tenantID).First(&investigation).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"message": "Investigation not found"})
		return
	}
	var share models.OperatorInvestigationShare
	if err := db.Where("investigation_id=? AND tenant_id=? AND recipient_id=? AND state=?", investigation.ID, tenantID, principal.UserID, "active").First(&share).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"message": "Shared investigation not found"})
		return
	}
	var evidence []models.OperatorEvidence
	if err := db.Where("investigation_id=? AND tenant_id=?", investigation.ID, tenantID).Find(&evidence).Error; err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Evidence unavailable"})
		return
	}
	allowed := map[string]bool{}
	for _, row := range evidence {
		allowed[row.EvidenceID] = access.HasPermission(row.RequiredPermission)
	}
	var events []models.OperatorInvestigationEvent
	if err := db.Where("investigation_id=? AND tenant_id=? AND event_type=?", investigation.ID, tenantID, "response_block").Order("sequence ASC").Find(&events).Error; err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Shared investigation unavailable"})
		return
	}
	out := make([]gin.H, 0, len(events))
	for _, event := range events {
		var payload map[string]any
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			continue
		}
		ids, _ := payload["evidence_ids"].([]any)
		// A shared explanation is safe only when it names evidence that the
		// recipient can currently read. Empty or malformed citations fail closed.
		visible := len(ids) > 0
		for _, raw := range ids {
			if evidenceID, ok := raw.(string); !ok || !allowed[evidenceID] {
				visible = false
				break
			}
		}
		if !visible {
			out = append(out, gin.H{"sequence": event.Sequence, "event_type": "response_block", "payload": gin.H{"kind": "redacted", "text": "Evidence is restricted by your current permissions."}, "created_at": event.CreatedAt})
			continue
		}
		out = append(out, gin.H{"sequence": event.Sequence, "event_type": "response_block", "payload": payload, "created_at": event.CreatedAt})
	}
	c.JSON(http.StatusOK, gin.H{"investigation_id": id, "state": investigation.State, "events": out, "non_transferable": true})
}

// ListOperatorInvestigationShares is the creator-owned governance read model.
// It exposes invitation state only; it never turns a share into evidence or
// leaks another administrator's access information.
func ListOperatorInvestigationShares(c *gin.Context) {
	principal, tenantID, db, ok := operatorGovernancePrincipal(c)
	if !ok {
		return
	}
	_, policy, err := operatorExecutionPolicy(db, tenantID)
	if err != nil || !policy.ReadEnabled {
		c.Status(http.StatusNotFound)
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid Operator investigation"})
		return
	}
	var investigation models.OperatorInvestigation
	if err := db.Where("public_id=? AND tenant_id=? AND actor_id=?", id, tenantID, principal.UserID).First(&investigation).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"message": "Creator-owned investigation not found"})
		return
	}
	var shares []models.OperatorInvestigationShare
	if err := db.Where("investigation_id=? AND tenant_id=?", investigation.ID, tenantID).Order("created_at DESC").Find(&shares).Error; err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Investigation shares are unavailable"})
		return
	}
	out := make([]gin.H, 0, len(shares))
	for _, share := range shares {
		out = append(out, gin.H{"id": share.PublicID, "recipient_id": share.RecipientID, "state": share.State, "created_at": share.CreatedAt, "revoked_at": share.RevokedAt, "non_transferable": true, "recipient_reauthorized_on_read": true})
	}
	c.JSON(http.StatusOK, gin.H{"investigation_id": investigation.PublicID, "items": out})
}

func operatorScheduleResponse(schedule models.OperatorSchedule) gin.H {
	return gin.H{"id": schedule.PublicID, "creator_id": schedule.CreatorID, "owner_id": schedule.OwnerID, "state": schedule.State, "locale": schedule.Locale, "cadence": schedule.Cadence, "next_run_at": schedule.NextRunAt, "paused_reason": schedule.PausedReason, "created_at": schedule.CreatedAt, "updated_at": schedule.UpdatedAt, "read_only": true}
}
func operatorSpendStatus(db *gorm.DB, tenantID string, softLimit, hardLimit int64) (bool, bool) {
	var spend float64
	db.Model(&models.AISpendEvent{}).Where("tenant_id=? AND occurred_at>=?", tenantID, time.Now().UTC().AddDate(0, 0, -30)).Select("COALESCE(sum(cost_usd),0)").Scan(&spend)
	micro := int64(math.Round(spend * 1_000_000))
	return softLimit > 0 && micro >= softLimit, hardLimit > 0 && micro >= hardLimit
}
func operatorOutcomeMetrics(db *gorm.DB, tenantID string) gin.H {
	var investigations, completed, plans, succeeded, schedules, paused int64
	db.Model(&models.OperatorInvestigation{}).Where("tenant_id=?", tenantID).Count(&investigations)
	db.Model(&models.OperatorInvestigation{}).Where("tenant_id=? AND state=?", tenantID, "completed").Count(&completed)
	db.Model(&models.OperatorActionPlan{}).Where("tenant_id=?", tenantID).Count(&plans)
	db.Model(&models.OperatorActionPlan{}).Where("tenant_id=? AND state=?", tenantID, "succeeded").Count(&succeeded)
	db.Model(&models.OperatorSchedule{}).Where("tenant_id=?", tenantID).Count(&schedules)
	db.Model(&models.OperatorSchedule{}).Where("tenant_id=? AND state=?", tenantID, "paused").Count(&paused)
	return gin.H{"investigations": investigations, "completed_investigations": completed, "plans": plans, "succeeded_plans": succeeded, "schedules": schedules, "paused_schedules": paused}
}
