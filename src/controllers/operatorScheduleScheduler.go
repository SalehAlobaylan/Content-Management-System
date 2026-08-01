package controllers

import (
	"context"
	"encoding/json"
	"math"
	"time"

	"content-management-system/src/models"
	operatorpkg "content-management-system/src/operator"

	"gorm.io/gorm"
)

const operatorScheduleHeartbeatInterval = time.Minute

// StartOperatorScheduleHeartbeat runs only persisted, read-only templates.
// It obtains a live IAM snapshot at every run and pauses instead of reusing a
// stale browser credential, prior approval, or historical permission set.
func StartOperatorScheduleHeartbeat(db *gorm.DB) {
	go func() {
		runOperatorScheduleHeartbeat(db)
		ticker := time.NewTicker(operatorScheduleHeartbeatInterval)
		defer ticker.Stop()
		for range ticker.C {
			runOperatorScheduleHeartbeat(db)
		}
	}()
}

func runOperatorScheduleHeartbeat(db *gorm.DB) {
	accessClient, accessClientErr := operatorpkg.NewIAMAccessClientFromEnv()
	ctx, cancel := context.WithTimeout(context.Background(), 55*time.Second)
	defer cancel()
	var due []models.OperatorSchedule
	if err := db.WithContext(ctx).Where("state=? AND next_run_at<=?", "active", time.Now().UTC()).Order("next_run_at ASC").Limit(10).Find(&due).Error; err != nil {
		return
	}
	for _, candidate := range due {
		scheduleStore := operatorpkg.NewScheduleStore(db)
		schedule, run, template, err := scheduleStore.ClaimDue(ctx, candidate.TenantID, candidate.ID)
		if err != nil {
			continue
		}
		if accessClientErr != nil {
			scheduleStore.PauseDueFailure(ctx, schedule, run, "access_unavailable")
			continue
		}
		access, err := accessClient.Snapshot(ctx, schedule.OwnerID, schedule.TenantID)
		if err != nil || !access.IsAdmin {
			scheduleStore.PauseDueFailure(ctx, schedule, run, "access_unavailable")
			continue
		}
		permissions, err := operatorSchedulePermissions(schedule.RequiredPermissions)
		if err != nil {
			scheduleStore.PauseDueFailure(ctx, schedule, run, "invalid_required_permissions")
			continue
		}
		permitted := true
		for _, permission := range permissions {
			if !access.HasPermission(permission) {
				permitted = false
				break
			}
		}
		if !permitted {
			scheduleStore.PauseDueFailure(ctx, schedule, run, "permission_lost")
			continue
		}
		for _, adapterKey := range template.AdapterKeys {
			if operatorpkg.EnsureAdapterCapabilityEnabled(db, schedule.TenantID, adapterKey, time.Now().UTC()) != nil {
				permitted = false
				break
			}
		}
		if !permitted {
			scheduleStore.PauseDueFailure(ctx, schedule, run, "adapter_disabled")
			continue
		}
		var persisted models.OperatorPolicy
		_ = db.WithContext(ctx).Where("tenant_id=?", schedule.TenantID).First(&persisted).Error
		_, policy, err := operatorExecutionPolicy(db, schedule.TenantID)
		if err != nil || !policy.ReadEnabled || !policy.SchedulesEnabled {
			scheduleStore.PauseDueFailure(ctx, schedule, run, "schedules_disabled")
			continue
		}
		if operatorScheduledHardSpendStop(db, schedule.TenantID, persisted.DeepHardSpendLimit) {
			scheduleStore.PauseDueFailure(ctx, schedule, run, "hard_spend_stop")
			continue
		}
		var reasoner operatorpkg.Reasoner
		if policy.LLMEnabled {
			reasoner, _ = operatorpkg.NewHTTPReasonerFromEnv()
		}
		coordinator := operatorpkg.NewInvestigationCoordinator(operatorpkg.NewContextFabric(db, operatorpkg.DefaultAdapterRegistry()), operatorpkg.NewInvestigationStore(db), reasoner)
		input := operatorpkg.InvestigationInput{VisibleContext: template.VisibleContext, Intent: template.Request.Intent, Locale: schedule.Locale, Message: template.Request.Message, Tier: template.Request.Tier}
		result, err := coordinator.Run(ctx, access, policy, input)
		if err != nil {
			_ = scheduleStore.FinishRun(ctx, run.ID, schedule.TenantID, "failed", "investigation_failed", nil)
			continue
		}
		_ = scheduleStore.FinishRun(ctx, run.ID, schedule.TenantID, "completed", "", &result.Investigation.ID)
	}
}

func operatorSchedulePermissions(raw []byte) ([]string, error) {
	var permissions []string
	if err := json.Unmarshal(raw, &permissions); err != nil {
		return nil, err
	}
	return permissions, nil
}
func operatorScheduledHardSpendStop(db *gorm.DB, tenantID string, limit int64) bool {
	if limit <= 0 {
		return false
	}
	var spend float64
	db.Model(&models.AISpendEvent{}).Where("tenant_id=? AND occurred_at>=?", tenantID, time.Now().UTC().AddDate(0, 0, -30)).Select("COALESCE(sum(cost_usd),0)").Scan(&spend)
	return int64(math.Round(spend*1_000_000)) >= limit
}
