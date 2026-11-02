package controllers

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"

	"content-management-system/src/models"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Calls are synchronous and bounded to at most one minute. An expired attempt
// is fenced out before it is retried, so a delayed old response cannot overwrite it.
const embeddingAttemptLease = 2 * time.Minute

type embeddingCampaignCorrelation struct {
	CampaignID uint   `json:"campaign_id"`
	ActionID   uint   `json:"action_id"`
	RequestID  string `json:"request_id"`
}

func campaignCanContinue(db *gorm.DB, c *models.EmbeddingCampaign) bool {
	var current models.EmbeddingCampaign
	if db.First(&current, c.ID).Error != nil {
		return false
	}
	*c = current
	if current.State != models.EmbeddingCampaignRunning && current.State != models.EmbeddingCampaignVerifying {
		return false
	}
	var p models.EmbeddingLifecyclePolicy
	return db.Where("tenant_id = ?", embeddingLifecycleTenant).First(&p).Error == nil &&
		(p.CampaignsPausedUntil == nil || !p.CampaignsPausedUntil.After(time.Now()))
}

// Compare-and-set only state fields; never save a stale counter or operator state.
func campaignSetState(db *gorm.DB, c *models.EmbeddingCampaign, state, reason string) bool {
	updates := map[string]any{"state": state, "blocked_reason": reason}
	if state == models.EmbeddingCampaignCompleted || state == models.EmbeddingCampaignCompletedWithWaiver {
		updates["completed_at"] = time.Now()
	}
	r := db.Model(&models.EmbeddingCampaign{}).Where("id = ? AND state = ?", c.ID, c.State).Updates(updates)
	if r.Error != nil || r.RowsAffected != 1 {
		return false
	}
	c.State, c.BlockedReason = state, reason
	return true
}

func campaignBudgetRemaining(db *gorm.DB, c *models.EmbeddingCampaign) (int, error) {
	var used int64
	err := db.Model(&models.EmbeddingCampaignAction{}).Where("campaign_id = ? AND status <> ? AND created_at > ?", c.ID, models.EmbeddingActionSkipped, time.Now().Add(-24*time.Hour)).Count(&used).Error
	if err != nil {
		return 0, err
	}
	remaining := c.ItemsPerBatch
	if c.DailyItemCap > 0 && c.DailyItemCap-int(used) < remaining {
		remaining = c.DailyItemCap - int(used)
	}
	if remaining < 0 {
		remaining = 0
	}
	return remaining, nil
}

func claimCampaignAction(db *gorm.DB, c *models.EmbeddingCampaign, action *models.EmbeddingCampaignAction) error {
	return db.Transaction(func(tx *gorm.DB) error {
		var current models.EmbeddingCampaign
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&current, c.ID).Error; err != nil {
			return err
		}
		if !campaignCanContinue(tx, &current) {
			return fmt.Errorf("campaign is paused, stopped, or unavailable")
		}
		remaining, err := campaignBudgetRemaining(tx, &current)
		if err != nil {
			return err
		}
		if remaining == 0 {
			return fmt.Errorf("campaign daily budget reached")
		}
		if action.RequestID == "" {
			action.RequestID = uuid.NewString()
		}
		if err := tx.Create(action).Error; err != nil {
			return err
		}
		expires := time.Now().Add(embeddingAttemptLease)
		return tx.Model(&current).UpdateColumn("lease_expires_at", expires).Error
	})
}

func authorizeEmbeddingCampaignWrite(tx *gorm.DB, item models.ContentItem, surface string, correlation *embeddingCampaignCorrelation, spaceID, producerID string) error {
	if correlation == nil {
		return nil
	}
	var action models.EmbeddingCampaignAction
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=? AND campaign_id=? AND request_id=? AND target_id=? AND surface_key=? AND status=? AND created_at>?",
		correlation.ActionID, correlation.CampaignID, correlation.RequestID, item.PublicID.String(), surface, models.EmbeddingActionAttempted, time.Now().Add(-embeddingAttemptLease)).First(&action).Error; err != nil {
		return fmt.Errorf("campaign attempt is stale or invalid")
	}
	var c models.EmbeddingCampaign
	if err := tx.First(&c, correlation.CampaignID).Error; err != nil {
		return err
	}
	if c.TargetSpaceID != spaceID || action.ExpectedProducerID != producerID || action.TenantID != item.TenantID {
		return fmt.Errorf("campaign writeback target does not match its claim")
	}
	return nil
}

func recoverCampaignActions(db *gorm.DB, c *models.EmbeddingCampaign) error {
	var actions []models.EmbeddingCampaignAction
	if err := db.Where("campaign_id=? AND status=? AND created_at<=?", c.ID, models.EmbeddingActionAttempted, time.Now().Add(-embeddingAttemptLease)).Find(&actions).Error; err != nil {
		return err
	}
	for i := range actions {
		a := &actions[i]
		s, known := EmbeddingSurfaceByKey(a.SurfaceKey)
		if known && ownerReadback(db, s, a.TargetID, c.TargetSpaceID, a.ExpectedProducerID) == nil {
			a.Status = models.EmbeddingActionCompleted
			a.ObservedProducerID = a.ExpectedProducerID
			resolveCampaignException(db, c.ID, a.SurfaceKey, a.TargetID)
		} else {
			a.Status, a.Reason = models.EmbeddingActionFailed, "runner interrupted; expired attempt fenced before retry"
			if !known {
				s.Key = a.SurfaceKey
			}
			recordAdapterFailure(db, c, s, a.TargetID, a.Reason)
		}
		if err := db.Save(a).Error; err != nil {
			return err
		}
	}
	return nil
}

func campaignInputFingerprint(db *gorm.DB, item *models.ContentItem, surface string) (string, error) {
	var input string
	if surface == "content_image" {
		if item.ThumbnailURL != nil {
			input = *item.ThumbnailURL
		}
	} else {
		var err error
		input, err = buildLifecycleEmbeddingText(db, item)
		if err != nil {
			return "", err
		}
	}
	return fmt.Sprintf("%x", sha256.Sum256([]byte(input))), nil
}

// Called after the content row is locked. Reject an in-flight result if an edit,
// transcript update, or re-ingest changed the canonical inference input.
func validateCampaignEmbeddingInput(tx *gorm.DB, item *models.ContentItem, surface string, correlation *embeddingCampaignCorrelation) error {
	if correlation == nil {
		return nil
	}
	var action models.EmbeddingCampaignAction
	if err := tx.First(&action, correlation.ActionID).Error; err != nil {
		return err
	}
	var evidence struct {
		InputFingerprint string `json:"input_fingerprint"`
	}
	if json.Unmarshal(action.Evidence, &evidence) != nil || evidence.InputFingerprint == "" {
		return fmt.Errorf("campaign input evidence missing")
	}
	observed, err := campaignInputFingerprint(tx, item, surface)
	if err != nil {
		return err
	}
	if observed != evidence.InputFingerprint {
		return fmt.Errorf("campaign input changed during inference")
	}
	return nil
}

func lockEmbeddingWrite(tx *gorm.DB, space string) error {
	return tx.Exec("SELECT pg_advisory_xact_lock_shared(hashtext(?))", "embedding-write:"+space).Error
}

func recordEmbeddingWriterRegression(db *gorm.DB, space, target, reason string) {
	_ = db.Create(&models.AuditLog{TenantID: embeddingLifecycleTenant, UserID: "machine:" + space, Action: "embedding_writer_regression", TargetService: "cms", TargetResource: target, Status: "failure", Payload: jsonEvidence(map[string]any{"space": space, "reason": reason})}).Error
}
