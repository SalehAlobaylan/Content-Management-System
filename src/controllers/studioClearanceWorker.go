package controllers

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"sync/atomic"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/supply"

	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	studioClearanceLease                = 60 * time.Second
	studioClearanceVerificationDeadline = 10 * time.Minute
	studioClearanceMaxAttempts          = 2
)

var studioClearanceHeartbeat atomic.Int64

type studioClearanceClaim struct {
	Request models.StudioClearanceRequest
	Attempt models.StudioClearanceAttempt
}

type studioClearanceDecision struct {
	ChildID   string `json:"child_id"`
	ChapterID string `json:"chapter_id,omitempty"`
	Decision  string `json:"decision"`
	Reason    string `json:"reason"`
}

// StartStudioClearanceWorker consumes only exact child sets emitted by the
// atomization verifier. It never scans the global Studio queue or derives work
// from timestamps.
func StartStudioClearanceWorker(db *gorm.DB) {
	runStudioClearanceWorker(db)
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			runStudioClearanceWorker(db)
		}
	}()
}

func StudioClearanceWorkerHealthy(now time.Time) bool {
	last := studioClearanceHeartbeat.Load()
	return last > 0 && now.UTC().Sub(time.Unix(0, last).UTC()) <= 2*studioClearanceLease
}

func runStudioClearanceWorker(db *gorm.DB) {
	if err := recoverExpiredStudioClearance(db); err != nil {
		log.Printf("studio clearance recovery failed: %v", err)
		return
	}
	if verified, err := verifyOneStudioClearance(db); err != nil {
		log.Printf("studio clearance verification failed: %v", err)
		return
	} else if verified {
		studioClearanceHeartbeat.Store(time.Now().UTC().UnixNano())
		return
	}
	claim, found, err := claimOneStudioClearance(db)
	studioClearanceHeartbeat.Store(time.Now().UTC().UnixNano())
	if err != nil || !found {
		if err != nil {
			log.Printf("studio clearance claim failed: %v", err)
		}
		return
	}
	if err := executeStudioClearance(db, claim); err != nil {
		log.Printf("studio clearance %s failed: %v", claim.Request.PublicID, err)
		_ = markStudioClearanceUncertain(db, claim, "clearance_execution_interrupted")
	}
}

func claimOneStudioClearance(db *gorm.DB) (studioClearanceClaim, bool, error) {
	var claim studioClearanceClaim
	now := time.Now().UTC()
	err := db.Transaction(func(tx *gorm.DB) error {
		var rows []models.StudioClearanceRequest
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).Where("state=?", "queued").Order("created_at").Limit(16).Find(&rows).Error; err != nil {
			return err
		}
		for _, request := range rows {
			allowed, _, err := supply.MayExecuteSupplyAction(tx, request.TenantID, supply.SupplyActionStudioClearExactChildren)
			if err != nil {
				return err
			}
			if !allowed {
				continue
			}
			ids, err := exactStudioChildIDs(tx, request)
			if err != nil {
				continue
			}
			if studioChildSetDigest(ids) != request.ChildSetDigest {
				continue
			}
			var attempts int64
			if err := tx.Model(&models.StudioClearanceAttempt{}).Where("tenant_id=? AND request_id=?", request.TenantID, request.PublicID).Count(&attempts).Error; err != nil {
				return err
			}
			if attempts >= studioClearanceMaxAttempts {
				proof := datatypes.JSON([]byte(`{"verified":"absent","reason":"attempt_budget_exhausted"}`))
				if err := tx.Model(&request).Updates(map[string]any{"state": "failed", "failure_class": "attempt_budget_exhausted", "terminal_proof": proof, "finished_at": now}).Error; err != nil {
					return err
				}
				if err := appendStudioClearanceEvent(tx, request, "failed", map[string]any{"failure_class": "attempt_budget_exhausted"}); err != nil {
					return err
				}
				continue
			}
			token, fence, expires := uuid.New(), uuid.New(), now.Add(studioClearanceLease)
			attempt := models.StudioClearanceAttempt{PublicID: uuid.New(), TenantID: request.TenantID, RequestID: request.PublicID, AttemptNumber: int(attempts) + 1, State: "claimed", ClaimToken: token, FenceToken: fence, LeaseExpiresAt: expires, HeartbeatAt: now}
			if err := tx.Create(&attempt).Error; err != nil {
				return err
			}
			if err := tx.Model(&request).Updates(map[string]any{"state": "claimed", "claim_owner": "cms-studio-clearance", "claim_token": token, "fence_token": fence, "claim_epoch": gorm.Expr("claim_epoch+1"), "claim_expires_at": expires}).Error; err != nil {
				return err
			}
			request.State, request.ClaimOwner, request.ClaimToken, request.FenceToken, request.ClaimExpiresAt = "claimed", "cms-studio-clearance", &token, &fence, &expires
			claim = studioClearanceClaim{Request: request, Attempt: attempt}
			return appendStudioClearanceEvent(tx, request, "claimed", map[string]any{"attempt_id": attempt.PublicID, "child_count": len(ids)})
		}
		return nil
	})
	return claim, claim.Request.PublicID != uuid.Nil, err
}

func executeStudioClearance(db *gorm.DB, claim studioClearanceClaim) error {
	now := time.Now().UTC()
	if err := db.Transaction(func(tx *gorm.DB) error {
		var request models.StudioClearanceRequest
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=? AND state=? AND claim_token=?", claim.Request.TenantID, claim.Request.PublicID, "claimed", claim.Attempt.ClaimToken).First(&request).Error; err != nil {
			return err
		}
		allowed, _, err := supply.MayExecuteSupplyAction(tx, request.TenantID, supply.SupplyActionStudioClearExactChildren)
		if err != nil || !allowed || request.ClaimExpiresAt == nil || !request.ClaimExpiresAt.After(now) || request.CancellationRequestedAt != nil {
			return fmt.Errorf("studio clearance effect is not authorized")
		}
		if err := tx.Model(&request).Updates(map[string]any{"state": "running", "effect_started_at": now}).Error; err != nil {
			return err
		}
		if err := tx.Model(&models.StudioClearanceAttempt{}).Where("tenant_id=? AND public_id=? AND claim_token=?", request.TenantID, claim.Attempt.PublicID, claim.Attempt.ClaimToken).Updates(map[string]any{"state": "running", "effect_started_at": now}).Error; err != nil {
			return err
		}
		return appendStudioClearanceEvent(tx, request, "effect_started", map[string]any{})
	}); err != nil {
		return err
	}

	ids, err := exactStudioChildIDs(db, claim.Request)
	if err != nil {
		return err
	}
	policy := loadEffectiveMediaStudioAutopilotPolicy(db, claim.Request.TenantID)
	autoEnabled := policy.AutopilotEnabled && policy.AutopilotMode == models.StudioAutopilotModeSafeAuto && (policy.PausedUntil == nil || !policy.PausedUntil.After(time.Now().UTC()))
	if autoEnabled {
		versions, registered := supply.QualificationVersions(supply.SupplyActionStudioClearExactChildren)
		if !registered {
			autoEnabled = false
		} else if _, promotionErr := supply.RequireActiveSupplyPromotion(db, claim.Request.TenantID, supply.SupplyActionStudioClearExactChildren, versions.ActionVersion, versions.AdapterVersion, versions.VerifierVersion, versions.SchemaVersion, versions.PolicyVersion); promotionErr != nil {
			autoEnabled = false
		}
	}
	decisions := make([]studioClearanceDecision, 0, len(ids))
	for _, childID := range ids {
		if err := renewStudioClearance(db, claim); err != nil {
			return err
		}
		decision, err := clearExactStudioChild(db, claim.Request.TenantID, childID, policy, autoEnabled)
		if err != nil {
			return err
		}
		decisions = append(decisions, decision)
		if err := persistStudioClearanceDecisions(db, claim, decisions); err != nil {
			return err
		}
	}
	return db.Transaction(func(tx *gorm.DB) error {
		var request models.StudioClearanceRequest
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=? AND state=? AND claim_token=?", claim.Request.TenantID, claim.Request.PublicID, "running", claim.Attempt.ClaimToken).First(&request).Error; err != nil {
			return err
		}
		if err := tx.Model(&request).Updates(map[string]any{"state": "verifying", "claim_expires_at": nil}).Error; err != nil {
			return err
		}
		if err := tx.Model(&models.StudioClearanceAttempt{}).Where("tenant_id=? AND public_id=?", request.TenantID, claim.Attempt.PublicID).Update("state", "verifying").Error; err != nil {
			return err
		}
		return appendStudioClearanceEvent(tx, request, "verification_requested", map[string]any{"decision_count": len(decisions)})
	})
}

func clearExactStudioChild(db *gorm.DB, tenant string, childID uuid.UUID, policy models.MediaStudioAutopilotPolicy, autoEnabled bool) (studioClearanceDecision, error) {
	decision := studioClearanceDecision{ChildID: childID.String(), Decision: "manual_attention", Reason: "editorial_or_unqualified"}
	var child models.ContentItem
	if err := db.Where("tenant_id=? AND public_id=?", tenant, childID).First(&child).Error; err != nil {
		return decision, err
	}
	var chapter models.Chapter
	if err := db.Where("tenant_id=? AND child_content_item_id=?", tenant, childID).First(&chapter).Error; err != nil {
		return decision, err
	}
	decision.ChapterID = chapter.PublicID.String()
	if chapter.Status != chapterStatusReview {
		decision.Decision, decision.Reason = "already_terminal", chapter.Status
		return decision, nil
	}
	if !autoEnabled {
		decision.Reason = "studio_safe_auto_not_authorized"
		return decision, nil
	}
	cutoff := time.Now().UTC().Add(-time.Duration(policy.DirtyWorkbenchMinutes) * time.Minute)
	resources := []string{child.PublicID.String()}
	if child.ParentContentItemID != nil {
		resources = append(resources, child.ParentContentItemID.String())
	}
	var edits int64
	if err := db.Model(&models.AuditLog{}).Where("tenant_id=? AND target_resource IN ? AND user_email<>? AND action IN ? AND created_at>?", tenant, resources, models.StudioAuditPrincipal, []string{"media_studio.chapters_save", "media_studio.transcript_edit", "media_studio.transcript_approve"}, cutoff).Count(&edits).Error; err != nil {
		return decision, err
	}
	if edits > 0 {
		decision.Reason = "recent_human_edit"
		return decision, nil
	}
	codes := []string(chapter.NeedsReviewCodes)
	primary := ""
	if chapter.NeedsReviewCode != nil {
		primary = *chapter.NeedsReviewCode
	}
	duration := 0
	if child.DurationSec != nil {
		duration = *child.DurationSec
	}
	switch decideStudioChapterPath(primary, codes, duration) {
	case studioPathAutoReject:
		_, reviewErr := applyAtomizedChapterReviewWithOptions(db, tenant, chapter.PublicID, false, chapterReviewActor{Email: models.StudioAuditPrincipal}, chapterReviewApplyOptions{RequireNeedsReview: true, ExpectedChildID: chapter.ChildContentItemID, ExpectedReviewCodes: []string{models.StudioReviewCodeShortUnmergeable}, RequireNoBlockingOverride: true})
		if reviewErr != nil {
			decision.Reason = "reject_guard:" + reviewErr.code
			return decision, nil
		}
		decision.Decision, decision.Reason = "auto_rejected_structural", "verified_short_unmergeable"
	case studioPathAutoPublish:
		upstream, resolved := studioParentAutoPublishPolicy(db, tenant, &child)
		if !resolved || !upstream || !studioReasonCodeTrustEarned(db, tenant, models.StudioReviewCodeMergedShort, policy) {
			decision.Reason = "publish_authority_not_earned"
			return decision, nil
		}
		_, reviewErr := applyAtomizedChapterReviewWithOptions(db, tenant, chapter.PublicID, true, chapterReviewActor{Email: models.StudioAuditPrincipal}, chapterReviewApplyOptions{RequireNeedsReview: true, RequireParentAutoPublish: true, ExpectedChildID: chapter.ChildContentItemID, ExpectedReviewCodes: []string{models.StudioReviewCodeMergedShort}, RequireMergeProvenance: true, RequireNoSponsor: true, RequireNoBlockingOverride: true})
		if reviewErr != nil {
			decision.Reason = "publish_guard:" + reviewErr.code
			return decision, nil
		}
		decision.Decision, decision.Reason = "auto_published_mechanical", "trusted_merged_short"
	}
	return decision, nil
}

func exactStudioChildIDs(db *gorm.DB, request models.StudioClearanceRequest) ([]uuid.UUID, error) {
	var raw []string
	if err := json.Unmarshal(request.ChildIDs, &raw); err != nil || len(raw) == 0 || len(raw) > 64 {
		return nil, fmt.Errorf("studio clearance child set is invalid")
	}
	var atomization models.AtomizationWorkRequest
	if err := db.Where("tenant_id=? AND public_id=? AND state=?", request.TenantID, request.AtomizationRequestID, "succeeded").First(&atomization).Error; err != nil {
		return nil, err
	}
	ids := make([]uuid.UUID, 0, len(raw))
	seen := map[uuid.UUID]bool{}
	for _, value := range raw {
		id, err := uuid.Parse(value)
		if err != nil || seen[id] {
			return nil, fmt.Errorf("studio clearance child identity is invalid")
		}
		var count int64
		if err := db.Model(&models.ContentItem{}).Where("tenant_id=? AND public_id=? AND parent_content_item_id=?", request.TenantID, id, atomization.ParentContentItemID).Count(&count).Error; err != nil || count != 1 {
			return nil, fmt.Errorf("studio clearance child escaped its atomization result")
		}
		seen[id] = true
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })
	return ids, nil
}

func studioChildSetDigest(ids []uuid.UUID) string {
	values := make([]string, 0, len(ids))
	for _, id := range ids {
		values = append(values, id.String())
	}
	sort.Strings(values)
	sum := sha256.Sum256([]byte(joinStudioValues(values)))
	return hex.EncodeToString(sum[:])
}

func joinStudioValues(values []string) string {
	result := ""
	for index, value := range values {
		if index > 0 {
			result += "\n"
		}
		result += value
	}
	return result
}

func renewStudioClearance(db *gorm.DB, claim studioClearanceClaim) error {
	now, expires := time.Now().UTC(), time.Now().UTC().Add(studioClearanceLease)
	return db.Transaction(func(tx *gorm.DB) error {
		var request models.StudioClearanceRequest
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=? AND state=? AND claim_token=?", claim.Request.TenantID, claim.Request.PublicID, "running", claim.Attempt.ClaimToken).First(&request).Error; err != nil {
			return err
		}
		allowed, _, err := supply.MayExecuteSupplyAction(tx, request.TenantID, supply.SupplyActionStudioClearExactChildren)
		if err != nil || !allowed || request.ClaimExpiresAt == nil || !request.ClaimExpiresAt.After(now) || request.CancellationRequestedAt != nil {
			return fmt.Errorf("studio clearance claim is no longer authorized")
		}
		if err := tx.Model(&request).Update("claim_expires_at", expires).Error; err != nil {
			return err
		}
		return tx.Model(&models.StudioClearanceAttempt{}).Where("tenant_id=? AND public_id=? AND claim_token=?", request.TenantID, claim.Attempt.PublicID, claim.Attempt.ClaimToken).Updates(map[string]any{"lease_expires_at": expires, "heartbeat_at": now}).Error
	})
}

func persistStudioClearanceDecisions(db *gorm.DB, claim studioClearanceClaim, decisions []studioClearanceDecision) error {
	bytes, _ := json.Marshal(decisions)
	return db.Model(&models.StudioClearanceRequest{}).Where("tenant_id=? AND public_id=? AND state=? AND claim_token=?", claim.Request.TenantID, claim.Request.PublicID, "running", claim.Attempt.ClaimToken).Update("decisions", datatypes.JSON(bytes)).Error
}

func verifyOneStudioClearance(db *gorm.DB) (bool, error) {
	var request models.StudioClearanceRequest
	if err := db.Where("state IN ?", []string{"verifying", "uncertain"}).Order("updated_at").First(&request).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return false, nil
		}
		return false, err
	}
	ids, err := exactStudioChildIDs(db, request)
	if err != nil {
		return true, failStudioClearanceVerificationIfExpired(db, request, "child_set_unverifiable", err)
	}
	var decisions []studioClearanceDecision
	if err := json.Unmarshal(request.Decisions, &decisions); err != nil || len(decisions) != len(ids) {
		return true, failStudioClearanceVerificationIfExpired(db, request, "decision_set_unverifiable", err)
	}
	decisionByChild := make(map[string]studioClearanceDecision, len(decisions))
	for _, decision := range decisions {
		if _, duplicate := decisionByChild[decision.ChildID]; duplicate {
			return true, failStudioClearanceVerificationIfExpired(db, request, "duplicate_decision", nil)
		}
		decisionByChild[decision.ChildID] = decision
	}
	states := map[string]int{}
	for _, id := range ids {
		decision, exists := decisionByChild[id.String()]
		if !exists {
			return true, failStudioClearanceVerificationIfExpired(db, request, "missing_decision", nil)
		}
		var chapter models.Chapter
		if err := db.Where("tenant_id=? AND child_content_item_id=?", request.TenantID, id).First(&chapter).Error; err != nil {
			return true, failStudioClearanceVerificationIfExpired(db, request, "chapter_unverifiable", err)
		}
		switch decision.Decision {
		case "auto_rejected_structural", "auto_published_mechanical", "already_terminal":
			if chapter.Status == chapterStatusReview {
				return true, failStudioClearanceVerificationIfExpired(db, request, "declared_effect_absent", nil)
			}
		case "manual_attention":
			// Manual attention is itself a verified terminal routing decision;
			// this worker never converts editorial ambiguity into approval.
		default:
			return true, failStudioClearanceVerificationIfExpired(db, request, "unknown_decision", nil)
		}
		states[chapter.Status]++
	}
	now := time.Now().UTC()
	proof, _ := json.Marshal(map[string]any{"schema_version": "studio-clearance-proof/v1", "child_set_digest": request.ChildSetDigest, "child_count": len(ids), "states": states, "manual_attention_count": states[chapterStatusReview]})
	err = db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&request).Updates(map[string]any{"state": "succeeded", "terminal_proof": datatypes.JSON(proof), "finished_at": now, "claim_expires_at": nil}).Error; err != nil {
			return err
		}
		if err := tx.Model(&models.StudioClearanceAttempt{}).Where("tenant_id=? AND request_id=?", request.TenantID, request.PublicID).Order("attempt_number DESC").Updates(map[string]any{"state": "succeeded", "finished_at": now}).Error; err != nil {
			return err
		}
		return appendStudioClearanceEvent(tx, request, "verified", map[string]any{"states": states, "manual_attention_count": states[chapterStatusReview]})
	})
	return true, err
}

func failStudioClearanceVerificationIfExpired(db *gorm.DB, request models.StudioClearanceRequest, failure string, cause error) error {
	if time.Since(request.UpdatedAt) < studioClearanceVerificationDeadline {
		// Evidence can arrive after the owner checkpoint. Keep the request in
		// verifier-led uncertainty rather than turning a transient read failure
		// into a retryable effect failure.
		return nil
	}
	now := time.Now().UTC()
	proof, _ := json.Marshal(map[string]any{"schema_version": "studio-clearance-proof/v1", "verified": "absent", "failure_class": failure})
	return db.Transaction(func(tx *gorm.DB) error {
		result := tx.Model(&models.StudioClearanceRequest{}).Where("tenant_id=? AND public_id=? AND state IN ?", request.TenantID, request.PublicID, []string{"verifying", "uncertain"}).Updates(map[string]any{"state": "failed", "failure_class": failure, "terminal_proof": datatypes.JSON(proof), "finished_at": now, "claim_expires_at": nil})
		if result.Error != nil || result.RowsAffected == 0 {
			return result.Error
		}
		if err := tx.Model(&models.StudioClearanceAttempt{}).Where("tenant_id=? AND request_id=?", request.TenantID, request.PublicID).Order("attempt_number DESC").Updates(map[string]any{"state": "failed", "finished_at": now}).Error; err != nil {
			return err
		}
		return appendStudioClearanceEvent(tx, request, "verification_failed", map[string]any{"failure_class": failure})
	})
}

func recoverExpiredStudioClearance(db *gorm.DB) error {
	now := time.Now().UTC()
	return db.Transaction(func(tx *gorm.DB) error {
		var rows []models.StudioClearanceRequest
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).Where("state IN ? AND claim_expires_at<?", []string{"claimed", "running"}, now).Limit(16).Find(&rows).Error; err != nil {
			return err
		}
		for _, request := range rows {
			next := "queued"
			if request.EffectStartedAt != nil {
				next = "uncertain"
			}
			if err := tx.Model(&request).Updates(map[string]any{"state": next, "claim_owner": "", "claim_token": nil, "fence_token": nil, "claim_expires_at": nil, "failure_class": "expired_claim"}).Error; err != nil {
				return err
			}
			if err := appendStudioClearanceEvent(tx, request, "claim_expired", map[string]any{"next": next}); err != nil {
				return err
			}
		}
		return nil
	})
}

func markStudioClearanceUncertain(db *gorm.DB, claim studioClearanceClaim, failure string) error {
	return db.Model(&models.StudioClearanceRequest{}).Where("tenant_id=? AND public_id=? AND state IN ?", claim.Request.TenantID, claim.Request.PublicID, []string{"running", "claimed"}).Updates(map[string]any{"state": "uncertain", "claim_expires_at": nil, "failure_class": failure}).Error
}

func appendStudioClearanceEvent(tx *gorm.DB, request models.StudioClearanceRequest, kind string, payload map[string]any) error {
	var sequence int64
	if err := tx.Model(&models.StudioClearanceEvent{}).Where("tenant_id=? AND request_id=?", request.TenantID, request.PublicID).Select("COALESCE(MAX(sequence),0)").Scan(&sequence).Error; err != nil {
		return err
	}
	bytes, _ := json.Marshal(payload)
	return tx.Create(&models.StudioClearanceEvent{PublicID: uuid.New(), TenantID: request.TenantID, RequestID: request.PublicID, Sequence: sequence + 1, EventType: kind, Payload: datatypes.JSON(bytes), OccurredAt: time.Now().UTC()}).Error
}
