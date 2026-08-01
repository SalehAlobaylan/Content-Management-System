package controllers

import (
	"context"
	"fmt"
	"strings"
	"time"

	"content-management-system/src/models"
	operatorpkg "content-management-system/src/operator"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// executeOperatorGovernancePlan contains only individually registered
// Operator-native state transitions. It accepts the fresh worker IAM snapshot
// rather than a browser credential and re-reads every target before mutation.
func executeOperatorGovernancePlan(ctx context.Context, db *gorm.DB, tenantID, toolKey string, plan models.OperatorActionPlan, canonical operatorpkg.CanonicalPlan, access operatorpkg.AccessSnapshot) (bool, map[string]any, map[string]any, map[string]any) {
	switch toolKey {
	case "operator.schedule.create.hourly", "operator.schedule.create.daily", "operator.schedule.create.weekly":
		return executeOperatorScheduleCreate(ctx, db, tenantID, plan, canonical, access)
	case "operator.schedule.pause":
		return executeOperatorSchedulePause(ctx, db, tenantID, canonical)
	case "operator.schedule.resume":
		return executeOperatorScheduleResume(ctx, db, tenantID, canonical, access)
	case "operator.schedule.takeover":
		return executeOperatorScheduleTakeover(ctx, db, tenantID, canonical, access)
	case "operator.share.create":
		return executeOperatorShareCreate(ctx, db, tenantID, plan, canonical)
	case "operator.share.revoke":
		return executeOperatorShareRevoke(ctx, db, tenantID, plan, canonical)
	case "operator.recommendation.snooze.15m", "operator.recommendation.snooze.1h", "operator.recommendation.snooze.1d", "operator.recommendation.snooze.7d", "operator.recommendation.dismiss", "operator.recommendation.subject_override":
		return executeOperatorRecommendationFeedback(ctx, db, tenantID, toolKey, canonical, access)
	case "operator.control.disable.read", "operator.control.disable.llm", "operator.control.disable.execution", "operator.control.disable.schedules", "operator.control.disable.adapter", "operator.control.disable.tool":
		return executeOperatorControlDisable(ctx, db, tenantID, toolKey, canonical, access)
	default:
		return false, nil, map[string]any{"error_class": "unregistered_governance_executor"}, nil
	}
}

func executeOperatorShareCreate(ctx context.Context, db *gorm.DB, tenantID string, plan models.OperatorActionPlan, canonical operatorpkg.CanonicalPlan) (bool, map[string]any, map[string]any, map[string]any) {
	investigationID, err := governanceUUID(canonical.NormalizedArguments, "investigation_id")
	recipientID, recipientOK := canonical.NormalizedArguments["recipient_id"].(string)
	if err != nil || !recipientOK || recipientID == "" || recipientID == plan.ActorID || len(canonical.TargetIDs) != 1 {
		return false, nil, map[string]any{"error_class": "invalid_registered_arguments"}, nil
	}
	var investigation models.OperatorInvestigation
	if err := db.WithContext(ctx).Where("public_id=? AND tenant_id=? AND actor_id=? AND state=?", investigationID, tenantID, plan.ActorID, "completed").First(&investigation).Error; err != nil {
		return false, map[string]any{"investigation_id": investigationID.String(), "shareable": false}, map[string]any{"error_class": "investigation_not_shareable"}, nil
	}
	var prior models.OperatorInvestigationShare
	found := db.WithContext(ctx).Where("investigation_id=? AND recipient_id=?", investigation.ID, recipientID).First(&prior).Error == nil
	before := map[string]any{"investigation_id": investigationID.String(), "recipient_id": recipientID, "active": found && prior.State == "active"}
	share := models.OperatorInvestigationShare{InvestigationID: investigation.ID, TenantID: tenantID, RecipientID: recipientID, CreatedBy: plan.ActorID, State: "active"}
	if err := db.WithContext(ctx).Where("investigation_id=? AND recipient_id=?", investigation.ID, recipientID).Assign(models.OperatorInvestigationShare{State: "active", CreatedBy: plan.ActorID, RevokedAt: nil}).FirstOrCreate(&share).Error; err != nil {
		return false, before, map[string]any{"error_class": "share_create_failed"}, nil
	}
	var verifiedShare models.OperatorInvestigationShare
	err = db.WithContext(ctx).Where("investigation_id=? AND tenant_id=? AND recipient_id=? AND state=?", investigation.ID, tenantID, recipientID, "active").First(&verifiedShare).Error
	verified := map[string]any{"active": err == nil, "non_transferable": true, "recipient_reauthorized_on_read": true}
	return err == nil, before, map[string]any{"share_id": verifiedShare.PublicID.String(), "investigation_id": investigationID.String(), "recipient_id": recipientID, "state": verifiedShare.State}, verified
}

func executeOperatorShareRevoke(ctx context.Context, db *gorm.DB, tenantID string, plan models.OperatorActionPlan, canonical operatorpkg.CanonicalPlan) (bool, map[string]any, map[string]any, map[string]any) {
	shareID, err := governanceUUID(canonical.NormalizedArguments, "share_id")
	if err != nil {
		return false, nil, map[string]any{"error_class": "invalid_registered_arguments"}, nil
	}
	var share models.OperatorInvestigationShare
	if err := db.WithContext(ctx).Joins("JOIN operator_investigations ON operator_investigations.id = operator_investigation_shares.investigation_id").Where("operator_investigation_shares.public_id=? AND operator_investigation_shares.tenant_id=? AND operator_investigations.actor_id=?", shareID, tenantID, plan.ActorID).First(&share).Error; err != nil {
		return false, map[string]any{"share_id": shareID.String(), "exists": false}, map[string]any{"error_class": "share_not_found"}, nil
	}
	before := map[string]any{"share_id": share.PublicID.String(), "state": share.State, "recipient_id": share.RecipientID}
	now := time.Now().UTC()
	if err := db.WithContext(ctx).Model(&models.OperatorInvestigationShare{}).Where("id=? AND tenant_id=?", share.ID, tenantID).Updates(map[string]any{"state": "revoked", "revoked_at": now}).Error; err != nil {
		return false, before, map[string]any{"error_class": "share_revoke_failed"}, nil
	}
	var verifiedShare models.OperatorInvestigationShare
	err = db.WithContext(ctx).Where("id=? AND tenant_id=?", share.ID, tenantID).First(&verifiedShare).Error
	verified := map[string]any{"revoked": err == nil && verifiedShare.State == "revoked" && verifiedShare.RevokedAt != nil}
	return verified["revoked"].(bool), before, map[string]any{"share_id": share.PublicID.String(), "state": verifiedShare.State, "recipient_id": verifiedShare.RecipientID}, verified
}

func governanceUUID(arguments map[string]any, key string) (uuid.UUID, error) {
	raw, ok := arguments[key].(string)
	if !ok {
		return uuid.Nil, fmt.Errorf("missing %s", key)
	}
	id, err := uuid.Parse(raw)
	if err != nil || id == uuid.Nil {
		return uuid.Nil, fmt.Errorf("invalid %s", key)
	}
	return id, nil
}

func executeOperatorScheduleCreate(ctx context.Context, db *gorm.DB, tenantID string, plan models.OperatorActionPlan, canonical operatorpkg.CanonicalPlan, access operatorpkg.AccessSnapshot) (bool, map[string]any, map[string]any, map[string]any) {
	id, err := governanceUUID(canonical.NormalizedArguments, "investigation_id")
	if err != nil {
		return false, nil, map[string]any{"error_class": "invalid_registered_arguments"}, nil
	}
	cadence, ok := canonical.NormalizedArguments["cadence_minutes"].(int)
	if !ok || (cadence != 60 && cadence != 24*60 && cadence != 7*24*60) {
		return false, nil, map[string]any{"error_class": "invalid_registered_arguments"}, nil
	}
	var investigation models.OperatorInvestigation
	if err := db.WithContext(ctx).Where("public_id=? AND tenant_id=? AND actor_id=? AND state=?", id, tenantID, plan.ActorID, "completed").First(&investigation).Error; err != nil {
		return false, map[string]any{"investigation_id": id.String(), "eligible": false}, map[string]any{"error_class": "schedule_investigation_not_eligible"}, nil
	}
	input, err := operatorpkg.DecodeStoredInvestigationInput(investigation)
	if err != nil || input.Intent == operatorpkg.IntentResolve {
		return false, map[string]any{"investigation_id": id.String(), "eligible": false}, map[string]any{"error_class": "schedule_template_not_read_only"}, nil
	}
	var evidence []models.OperatorEvidence
	if err := db.WithContext(ctx).Where("investigation_id=? AND tenant_id=?", investigation.ID, tenantID).Find(&evidence).Error; err != nil {
		return false, nil, map[string]any{"error_class": "schedule_evidence_unavailable"}, nil
	}
	permissions := make([]string, 0, len(evidence))
	for _, row := range evidence {
		permissions = append(permissions, row.RequiredPermission)
	}
	template := operatorpkg.ScheduledTemplate{VisibleContext: input.VisibleContext, Request: operatorpkg.InvestigationRequest{Intent: input.Intent, Message: input.Message, Tier: input.Tier}, AdapterKeys: []string{operatorpkg.AdapterKeyForVisibleContext(input.VisibleContext)}}
	schedule, err := operatorpkg.NewScheduleStore(db).Create(ctx, access, template, permissions, input.Locale, cadence)
	if err != nil {
		return false, map[string]any{"investigation_id": id.String(), "eligible": true}, map[string]any{"error_class": "schedule_create_failed"}, nil
	}
	verified := map[string]any{"created": schedule.ID != 0, "owner_matches": schedule.OwnerID == plan.ActorID, "read_only": true, "cadence": schedule.Cadence}
	return verified["created"].(bool) && verified["owner_matches"].(bool), map[string]any{"investigation_id": id.String(), "schedule_exists": false}, map[string]any{"schedule_id": schedule.PublicID.String(), "state": schedule.State, "cadence": schedule.Cadence}, verified
}

func loadGovernanceSchedule(ctx context.Context, db *gorm.DB, tenantID string, canonical operatorpkg.CanonicalPlan) (models.OperatorSchedule, map[string]any, error) {
	id, err := governanceUUID(canonical.NormalizedArguments, "schedule_id")
	if err != nil {
		return models.OperatorSchedule{}, nil, err
	}
	schedule, err := operatorpkg.NewScheduleStore(db).Load(ctx, tenantID, id.String())
	if err != nil {
		return models.OperatorSchedule{}, map[string]any{"schedule_id": id.String(), "exists": false}, err
	}
	return schedule, map[string]any{"schedule_id": schedule.PublicID.String(), "state": schedule.State, "owner_id": schedule.OwnerID, "cadence": schedule.Cadence}, nil
}

func executeOperatorSchedulePause(ctx context.Context, db *gorm.DB, tenantID string, canonical operatorpkg.CanonicalPlan) (bool, map[string]any, map[string]any, map[string]any) {
	schedule, before, err := loadGovernanceSchedule(ctx, db, tenantID, canonical)
	if err != nil {
		return false, before, map[string]any{"error_class": "schedule_not_found"}, nil
	}
	if err := operatorpkg.NewScheduleStore(db).Pause(ctx, schedule.ID, tenantID, canonical.ActorID, "operator_signed_plan"); err != nil {
		return false, before, map[string]any{"error_class": "schedule_pause_failed"}, nil
	}
	updated, err := operatorpkg.NewScheduleStore(db).Load(ctx, tenantID, schedule.PublicID.String())
	verified := map[string]any{"paused": err == nil && updated.State == "paused" && strings.TrimSpace(updated.PausedReason) != ""}
	return verified["paused"].(bool), before, map[string]any{"schedule_id": schedule.PublicID.String(), "state": updated.State, "paused_reason": updated.PausedReason}, verified
}

func executeOperatorScheduleResume(ctx context.Context, db *gorm.DB, tenantID string, canonical operatorpkg.CanonicalPlan, access operatorpkg.AccessSnapshot) (bool, map[string]any, map[string]any, map[string]any) {
	schedule, before, err := loadGovernanceSchedule(ctx, db, tenantID, canonical)
	if err != nil {
		return false, before, map[string]any{"error_class": "schedule_not_found"}, nil
	}
	updated, err := operatorpkg.NewScheduleStore(db).Resume(ctx, schedule.ID, access)
	if err != nil {
		return false, before, map[string]any{"error_class": "schedule_resume_failed"}, nil
	}
	verified := map[string]any{"resumed": updated.State == "active" && updated.OwnerID == canonical.ActorID}
	return verified["resumed"].(bool), before, map[string]any{"schedule_id": updated.PublicID.String(), "state": updated.State, "owner_id": updated.OwnerID}, verified
}

func executeOperatorScheduleTakeover(ctx context.Context, db *gorm.DB, tenantID string, canonical operatorpkg.CanonicalPlan, access operatorpkg.AccessSnapshot) (bool, map[string]any, map[string]any, map[string]any) {
	schedule, before, err := loadGovernanceSchedule(ctx, db, tenantID, canonical)
	if err != nil {
		return false, before, map[string]any{"error_class": "schedule_not_found"}, nil
	}
	updated, err := operatorpkg.NewScheduleStore(db).Takeover(ctx, schedule.ID, access)
	if err != nil {
		return false, before, map[string]any{"error_class": "schedule_takeover_failed"}, nil
	}
	verified := map[string]any{"owner_changed": updated.OwnerID == canonical.ActorID, "access_version_refreshed": updated.AccessVersion == access.AccessVersion}
	return verified["owner_changed"].(bool) && verified["access_version_refreshed"].(bool), before, map[string]any{"schedule_id": updated.PublicID.String(), "owner_id": updated.OwnerID, "state": updated.State}, verified
}

func executeOperatorRecommendationFeedback(ctx context.Context, db *gorm.DB, tenantID, toolKey string, canonical operatorpkg.CanonicalPlan, access operatorpkg.AccessSnapshot) (bool, map[string]any, map[string]any, map[string]any) {
	id, err := governanceUUID(canonical.NormalizedArguments, "recommendation_id")
	if err != nil {
		return false, nil, map[string]any{"error_class": "invalid_registered_arguments"}, nil
	}
	var recommendation models.OperatorRecommendation
	if err := db.WithContext(ctx).Where("public_id=? AND tenant_id=?", id, tenantID).First(&recommendation).Error; err != nil || time.Now().UTC().After(recommendation.ExpiresAt) {
		return false, map[string]any{"recommendation_id": id.String(), "eligible": false}, map[string]any{"error_class": "recommendation_not_eligible"}, nil
	}
	before := map[string]any{"recommendation_id": recommendation.PublicID.String(), "state": recommendation.State}
	state, feedbackKind := "dismissed", "dismiss"
	var expires *time.Time
	if strings.Contains(toolKey, ".snooze.") {
		minutes, ok := canonical.NormalizedArguments["snooze_minutes"].(int)
		if !ok || minutes < 15 || minutes > 7*24*60 {
			return false, before, map[string]any{"error_class": "invalid_registered_arguments"}, nil
		}
		next := time.Now().UTC().Add(time.Duration(minutes) * time.Minute)
		expires, state, feedbackKind = &next, "snoozed", "snooze"
	} else if toolKey == "operator.recommendation.subject_override" {
		state, feedbackKind = "blocked", "subject_override"
	}
	feedback := models.OperatorRecommendationFeedback{RecommendationID: recommendation.ID, TenantID: tenantID, ActorID: access.UserID, FeedbackKind: feedbackKind, Reason: "operator_signed_plan", ExpiresAt: expires}
	if err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&feedback).Error; err != nil {
			return err
		}
		return tx.Model(&recommendation).Update("state", state).Error
	}); err != nil {
		return false, before, map[string]any{"error_class": "recommendation_feedback_failed"}, nil
	}
	var verifiedRow models.OperatorRecommendation
	err = db.WithContext(ctx).Where("id=? AND tenant_id=?", recommendation.ID, tenantID).First(&verifiedRow).Error
	verified := map[string]any{"state_matches": err == nil && verifiedRow.State == state, "feedback_recorded": feedback.ID != 0}
	return verified["state_matches"].(bool) && verified["feedback_recorded"].(bool), before, map[string]any{"recommendation_id": recommendation.PublicID.String(), "state": state, "expires_at": expires}, verified
}

func executeOperatorControlDisable(ctx context.Context, db *gorm.DB, tenantID, toolKey string, canonical operatorpkg.CanonicalPlan, access operatorpkg.AccessSnapshot) (bool, map[string]any, map[string]any, map[string]any) {
	kind := strings.TrimPrefix(toolKey, "operator.control.disable.")
	if kind == "adapter" || kind == "tool" {
		capabilityKey, ok := canonical.NormalizedArguments["capability_key"].(string)
		if !ok || capabilityKey != canonical.TargetIDs[0] || (kind == "adapter" && !operatorpkg.HasRegisteredAdapterKey(capabilityKey)) || (kind == "tool" && func() bool {
			_, registered := operatorpkg.DefaultToolCatalog().Lookup(capabilityKey)
			return !registered
		}()) {
			return false, nil, map[string]any{"error_class": "invalid_registered_arguments"}, nil
		}
		var existing models.OperatorCapabilityControl
		found := db.WithContext(ctx).Where("tenant_id=? AND capability_kind=? AND capability_key=?", tenantID, kind, capabilityKey).First(&existing).Error == nil
		before := map[string]any{"capability_kind": kind, "capability_key": capabilityKey, "disabled": found && existing.Disabled}
		control := models.OperatorCapabilityControl{TenantID: tenantID, CapabilityKind: kind, CapabilityKey: capabilityKey, Disabled: true, Reason: "operator_signed_plan", ActorID: access.UserID}
		if err := db.WithContext(ctx).Where("tenant_id=? AND capability_kind=? AND capability_key=?", tenantID, kind, capabilityKey).Assign(control).FirstOrCreate(&control).Error; err != nil {
			return false, before, map[string]any{"error_class": "capability_disable_failed"}, nil
		}
		var verifiedControl models.OperatorCapabilityControl
		err := db.WithContext(ctx).Where("tenant_id=? AND capability_kind=? AND capability_key=? AND disabled=?", tenantID, kind, capabilityKey, true).First(&verifiedControl).Error
		verified := map[string]any{"disabled": err == nil, "kind": kind, "capability_key": capabilityKey}
		return err == nil, before, map[string]any{"capability_kind": kind, "capability_key": capabilityKey, "disabled": true}, verified
	}
	if kind != "read" && kind != "llm" && kind != "execution" && kind != "schedules" {
		return false, nil, map[string]any{"error_class": "invalid_registered_arguments"}, nil
	}
	var existing models.OperatorCapabilityControl
	found := db.WithContext(ctx).Where("tenant_id=? AND capability_kind=? AND capability_key=?", tenantID, "system", kind).First(&existing).Error == nil
	before := map[string]any{"capability_kind": "system", "capability_key": kind, "disabled": found && existing.Disabled}
	control := models.OperatorCapabilityControl{TenantID: tenantID, CapabilityKind: "system", CapabilityKey: kind, Disabled: true, Reason: "operator_signed_plan", ActorID: access.UserID}
	if err := db.WithContext(ctx).Where("tenant_id=? AND capability_kind=? AND capability_key=?", tenantID, "system", kind).Assign(control).FirstOrCreate(&control).Error; err != nil {
		return false, before, map[string]any{"error_class": "control_disable_failed"}, nil
	}
	err := operatorpkg.EnsureSystemCapabilityEnabled(db, tenantID, kind, time.Now().UTC())
	verified := map[string]any{"disabled": err != nil, "capability_kind": "system", "capability_key": kind}
	return verified["disabled"].(bool), before, map[string]any{"capability_kind": "system", "capability_key": kind, "disabled": true}, verified
}
