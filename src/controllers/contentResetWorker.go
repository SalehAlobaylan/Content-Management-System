package controllers

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"content-management-system/src/contentreset"
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"gorm.io/gorm"
)

// StartContentResetWorker resumes the persisted outbox independently of an
// operator's browser session. New effects require release qualification;
// installed readback adapters remain available if qualification is revoked.
func StartContentResetWorker(db *gorm.DB) {
	owners := make([]contentreset.Owner, 0, len(contentResetOwners))
	for contract, owner := range contentResetOwners {
		if owner != nil && owner.Contract() == contract {
			owners = append(owners, owner)
		}
	}
	if len(owners) == 0 {
		return
	}
	refreshContentResetQualifications(db)
	engine, err := contentreset.NewEngineWithAdmission(contentResetEffectAdmission, owners...)
	if err != nil {
		log.Print("Content Reset owner registry is invalid")
		return
	}
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		var cursor uint
		for range ticker.C {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			cursor, err = advanceContentResetWorker(ctx, db, engine, cursor)
			cancel()
			if err != nil {
				log.Print("Content Reset coordinator pass requires attention")
			}
		}
	}()
}

func contentResetEffectAdmission(candidate contentreset.Contract, raw []byte) bool {
	var contract contentResetExecutionContract
	if json.Unmarshal(raw, &contract) != nil || len(contract.Owners) != len(contract.Qualifications) {
		return false
	}
	qualified := contentResetQualificationValue(candidate)
	if qualified == "" {
		return false
	}
	for i, owner := range contract.Owners {
		if owner == candidate {
			return contract.Qualifications[i] == qualified
		}
	}
	return false
}

func advanceContentResetWorker(ctx context.Context, db *gorm.DB, engine *contentreset.Engine, cursor uint) (uint, error) {
	if !db.Migrator().HasTable(&models.ContentResetExecution{}) {
		return 0, nil
	}
	fence, err := utils.ReadWriterFence(db.WithContext(ctx))
	if err != nil {
		return cursor, err
	}
	if fence.State != "open" && fence.State != "successor_open" {
		return cursor, nil
	}
	// Qualification rows are reviewed release evidence; pick up changes without
	// requiring a CMS restart while keeping the code-owned version gate.
	refreshContentResetQualifications(db)
	// Terminal campaigns are scanned only while an already-admitted command
	// still needs a readback receipt (a crash between the owner commit and the
	// finish transaction). They can never admit a new effect.
	var campaigns []models.ContentResetCampaign
	if err := db.WithContext(ctx).
		Where("id>?", cursor).
		Where(`(state IN ? OR (state IN ('complete','closed_partial') AND EXISTS (
			SELECT 1 FROM content_reset_steps terminal_step
			WHERE terminal_step.tenant_id = content_reset_campaigns.tenant_id
			  AND terminal_step.campaign_id = content_reset_campaigns.id
			  AND terminal_step.state IN ('claimed','waiting','outcome_unknown'))))`,
			[]string{"executing", "partial", "published", "cleanup_pending"}).
		Order("id ASC").Limit(8).Find(&campaigns).Error; err != nil {
		return cursor, err
	}
	if len(campaigns) == 0 {
		return 0, nil
	}
	for _, campaign := range campaigns {
		if ctx.Err() != nil {
			return cursor, ctx.Err()
		}
		cursor = campaign.ID
		if err := planContentResetWorkflow(ctx, db, engine, campaign.TenantID, campaign.ID); err != nil {
			return cursor, err
		}
		if _, err := engine.Advance(ctx, db, campaign.TenantID, campaign.ID); err != nil {
			return cursor, err
		}
	}
	return cursor, nil
}
