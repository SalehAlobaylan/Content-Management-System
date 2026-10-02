package controllers

import (
	"context"
	"log"
	"strconv"
	"time"

	"content-management-system/src/contentreset"
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"gorm.io/gorm"
)

// The Content Reset outbox engine treats an admitted delegated Pods retirement
// batch as a command whose continuation lives in the Plan 119 executor: the
// outbox only reconciles it while this worker advances the delegated run. Plan
// 119 processes a bounded number of items per pass and requires a delayed second
// object-absence probe, so a one-shot owner invocation can never finish it.
func StartContentResetPodsRetirementWorker(db *gorm.DB) {
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			err := advanceContentResetPodsRetirement(ctx, db)
			cancel()
			if err != nil {
				log.Print("Content Reset delegated Pods retirement pass requires attention")
			}
		}
	}()
}

func advanceContentResetPodsRetirement(ctx context.Context, db *gorm.DB) error {
	if db == nil || !db.Migrator().HasTable(&models.PodsResetRun{}) ||
		!db.Migrator().HasColumn(&models.PodsResetRun{}, "content_reset_campaign_id") {
		return nil
	}
	fence, err := utils.ReadWriterFence(db.WithContext(ctx))
	if err != nil {
		return err
	}
	if fence.State != "open" && fence.State != "successor_open" {
		return nil
	}
	// Eligibility is decided in SQL before the bounded limit so paused,
	// terminal or step-less runs cannot occupy the pass and starve later
	// eligible runs. Authority is still rechecked at the effect boundary.
	var runs []models.PodsResetRun
	if err := db.WithContext(ctx).Table("pods_reset_runs AS pr").
		Select("pr.*").
		Joins("JOIN content_reset_campaigns c ON c.id = pr.content_reset_campaign_id AND c.tenant_id = pr.tenant_id").
		Joins("JOIN content_reset_executions e ON e.campaign_id = c.id AND e.tenant_id = c.tenant_id AND e.revision_id = pr.content_reset_revision_id").
		Where("pr.content_reset_campaign_id IS NOT NULL AND pr.state IN ?", []string{"approved", "executing", "partial"}).
		Where("c.state IN ?", []string{"executing", "published", "cleanup_pending", "partial"}).
		Where("e.pause_requested = FALSE AND e.completed_at IS NULL AND e.rolled_back_at IS NULL").
		Where(`EXISTS (SELECT 1 FROM content_reset_steps s
			WHERE s.tenant_id = pr.tenant_id AND s.campaign_id = c.id AND s.revision_id = e.revision_id
			  AND s.step_key = ('retire/' || pr.content_reset_lane || '/' || pr.content_reset_batch::text)
			  AND s.state IN ('pending','claimed','waiting','deferred','outcome_unknown'))`).
		Order("pr.updated_at ASC, pr.public_id ASC").Limit(8).Find(&runs).Error; err != nil {
		return err
	}
	for _, run := range runs {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if run.ContentResetCampaignID == nil || run.ContentResetLane == nil || run.ContentResetBatch == nil {
			continue
		}
		var execution models.ContentResetExecution
		if err := db.WithContext(ctx).Where("tenant_id=? AND campaign_id=?", run.TenantID, *run.ContentResetCampaignID).First(&execution).Error; err != nil {
			continue
		}
		if err := contentreset.ValidateExecutionEnvironment(db.WithContext(ctx), execution); err != nil {
			continue
		}
		// Continuation is only authorized while its exact owner command is
		// still admitted. A terminal step means this worker must stop.
		stepKey := contentResetRetireKey(*run.ContentResetLane, *run.ContentResetBatch)
		var steps int64
		if err := db.WithContext(ctx).Model(&models.ContentResetStep{}).
			Where("tenant_id=? AND campaign_id=? AND revision_id=? AND step_key=? AND state IN ?",
				run.TenantID, *run.ContentResetCampaignID, execution.RevisionID, stepKey,
				[]string{"pending", "claimed", "waiting", "deferred", "outcome_unknown"}).
			Count(&steps).Error; err != nil {
			return err
		}
		if steps != 1 {
			continue
		}
		var campaign models.ContentResetCampaign
		if err := db.WithContext(ctx).Where("tenant_id=? AND id=?", run.TenantID, *run.ContentResetCampaignID).First(&campaign).Error; err != nil {
			continue
		}
		principal := utils.AdminPrincipal{
			UserID: "cms/content-reset", Email: "content-reset+" + campaign.PublicID.String() + "@cms.internal",
			TenantID: run.TenantID, TenantClaimed: true, Role: "admin",
		}
		outcome, execErr := executePodsResetRunInProcess(db.WithContext(ctx), principal, run)
		if execErr != nil {
			log.Printf("Content Reset delegated Pods retirement batch %s stopped: %s", run.PublicID, execErr.Error())
			continue
		}
		_ = outcome
	}
	return nil
}

func contentResetRetireKey(lane string, ordinal int) string {
	return "retire/" + lane + "/" + strconv.Itoa(ordinal)
}
