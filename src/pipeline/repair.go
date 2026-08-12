// Package pipeline owns admission and verification of one exact Aggregation
// stage repair. It never resets a content row or selects a queue.
package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/supply"

	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const OwnerProtocol = "aggregation_pipeline"
const maxAttempts = 2
const leaseDuration = 45 * time.Second

type Candidate struct {
	Item           models.ContentItem
	Stage          string
	EvidenceDigest string
	LastEvent      models.ContentProcessingEvent
}
type Claim struct {
	Request    models.PipelineRepairRequest `json:"request"`
	Attempt    models.PipelineRepairAttempt `json:"attempt"`
	Lease      models.PipelineStageLease    `json:"lease"`
	ClaimToken uuid.UUID                    `json:"claim_token"`
}

// EffectReceipt is the only owner-written completion record for a pipeline
// repair. It deliberately contains proof identifiers, never an arbitrary
// target, queue name, provider URL, or caller-selected stage.
type EffectReceipt struct {
	ProducerEventID uuid.UUID
	OutputDigest    string
	Output          map[string]any
}

// TextEmbeddingWriteback is passed only between Aggregation, Enrichment and
// the CMS internal embedding endpoint. It is an opaque proof of the current
// exact repair lease, not a caller-selected target or stage.
type TextEmbeddingWriteback struct {
	RepairID            string
	AttemptID           string
	ClaimToken          string
	FenceToken          string
	ExpectedItemVersion string
	InputDigest         string
}

// AuthorizeTextEmbeddingWriteback fences Enrichment's persistence step to the
// CMS-issued Pipeline repair. The actual vector remains Enrichment-owned; CMS
// only admits it while the exact Aggregation repair lease is current.
func AuthorizeTextEmbeddingWriteback(tx *gorm.DB, contentItemID uuid.UUID, input TextEmbeddingWriteback) error {
	repairID, err := uuid.Parse(strings.TrimSpace(input.RepairID))
	if err != nil {
		return fmt.Errorf("pipeline repair writeback id is invalid")
	}
	attemptID, err := uuid.Parse(strings.TrimSpace(input.AttemptID))
	if err != nil {
		return fmt.Errorf("pipeline repair writeback attempt is invalid")
	}
	token, err := uuid.Parse(strings.TrimSpace(input.ClaimToken))
	if err != nil {
		return fmt.Errorf("pipeline repair writeback token is invalid")
	}
	fence, err := uuid.Parse(strings.TrimSpace(input.FenceToken))
	if err != nil {
		return fmt.Errorf("pipeline repair writeback fence is invalid")
	}
	expected, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(input.ExpectedItemVersion))
	if err != nil {
		return fmt.Errorf("pipeline repair writeback item version is invalid")
	}
	if strings.TrimSpace(input.InputDigest) == "" {
		return fmt.Errorf("pipeline repair writeback digest is invalid")
	}
	if err := recheckPipelineRepairActionAccess(tx, repairID.String()); err != nil {
		return err
	}
	var request models.PipelineRepairRequest
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id=? AND content_item_id=? AND stage=? AND state=? AND claim_owner=? AND claim_token=? AND expected_item_updated_at=? AND effect_input_digest=? AND cancellation_requested_at IS NULL AND claim_expires_at>?", repairID, contentItemID, models.PipelineStageTextEmbedding, models.PipelineRepairRunning, "aggregation-pipeline-repair-dispatcher", token, expected, input.InputDigest, time.Now().UTC()).First(&request).Error; err != nil {
		return fmt.Errorf("pipeline repair writeback is stale: %w", err)
	}
	var attempt models.PipelineRepairAttempt
	if err := tx.Where("public_id=? AND tenant_id=? AND repair_request_id=? AND fence_token=? AND state=?", attemptID, request.TenantID, request.PublicID, fence, models.PipelineRepairRunning).First(&attempt).Error; err != nil {
		return fmt.Errorf("pipeline repair writeback attempt is stale: %w", err)
	}
	var item models.ContentItem
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id=? AND tenant_id=? AND updated_at=?", contentItemID, request.TenantID, expected).First(&item).Error; err != nil {
		return fmt.Errorf("pipeline repair target changed before writeback: %w", err)
	}
	return nil
}

func isStage(stage string) bool {
	switch stage {
	case models.PipelineStageMediaDownload, models.PipelineStageMediaTranscode, models.PipelineStageMediaThumbnail, models.PipelineStageTextEmbedding:
		return true
	}
	return false
}

// CandidateForItem preserves unknown as unknown: no normalized item event
// means an old row cannot acquire repair authority merely because it is old.
func CandidateForItem(db *gorm.DB, tenantID string, itemID uuid.UUID) (Candidate, error) {
	if db == nil || strings.TrimSpace(tenantID) == "" || itemID == uuid.Nil {
		return Candidate{}, fmt.Errorf("pipeline repair requires tenant-scoped content item")
	}
	var item models.ContentItem
	if err := db.Where("public_id = ? AND tenant_id = ?", itemID, tenantID).First(&item).Error; err != nil {
		return Candidate{}, err
	}
	if item.Status == models.ContentStatusReady || item.Status == models.ContentStatusArchived {
		return Candidate{}, fmt.Errorf("content item is not a repair candidate")
	}
	var event models.ContentProcessingEvent
	if err := db.Where("tenant_id = ? AND content_item_id = ?", tenantID, itemID).Order("occurred_at DESC, id DESC").First(&event).Error; err != nil {
		return Candidate{}, fmt.Errorf("pipeline evidence is unknown: %w", err)
	}
	stage := ""
	switch event.Stage {
	case "media_download", "media_transcode", "media_thumbnail":
		stage = event.Stage
	case "text_embedding":
		stage = models.PipelineStageTextEmbedding
	// Media artefact and atomization gaps deliberately have later owners.
	case "media_artifacts", "transcript", "image_embedding", "atomization":
		return Candidate{}, fmt.Errorf("pipeline evidence belongs to a different owner")
	default:
		return Candidate{}, fmt.Errorf("pipeline stage evidence is not repairable")
	}
	if event.State == "completed" {
		return Candidate{}, fmt.Errorf("last exact stage is already verified")
	}
	digest := sha256.Sum256([]byte(strings.Join([]string{"pipeline-repair-evidence/v1", tenantID, itemID.String(), item.UpdatedAt.UTC().Format(time.RFC3339Nano), stage, event.PublicID.String(), event.State, event.OccurredAt.UTC().Format(time.RFC3339Nano)}, "\n")))
	return Candidate{Item: item, Stage: stage, EvidenceDigest: hex.EncodeToString(digest[:]), LastEvent: event}, nil
}

func CheckCandidate(db *gorm.DB, c Candidate) error {
	return checkCandidate(db, c, nil)
}

func checkCandidate(db *gorm.DB, c Candidate, currentRepair *uuid.UUID) error {
	if !isStage(c.Stage) {
		return fmt.Errorf("pipeline stage is not admitted")
	}
	allowed, _, err := supply.MayExecuteSupplyAction(db, c.Item.TenantID, supply.SupplyActionPipelineResumeExactStage)
	if err != nil {
		return err
	}
	if !allowed {
		return fmt.Errorf("pipeline repair is disabled")
	}
	// A stage repair consumes only a preceding persisted artifact. Historical
	// rows without this proof remain visible as attention, rather than
	// downloading a fresh arbitrary source while claiming to resume a stage.
	if c.Stage == models.PipelineStageMediaTranscode && pipelineRepairArtifactURL(c.Item, "pipeline_repair_original_url") == "" {
		return fmt.Errorf("pipeline repair has no verified original artifact")
	}
	if c.Stage == models.PipelineStageMediaThumbnail && pipelineRepairArtifactURL(c.Item, "pipeline_repair_processed_url") == "" {
		return fmt.Errorf("pipeline repair has no verified processed rendition")
	}
	var live models.PipelineStageLease
	err = db.Where("tenant_id = ? AND content_item_id = ? AND item_updated_at = ? AND stage = ? AND state IN ? AND lease_expires_at > ?", c.Item.TenantID, c.Item.PublicID, c.Item.UpdatedAt, c.Stage, []string{"claimed", "running", "verifying"}, time.Now().UTC()).First(&live).Error
	if err == nil {
		return fmt.Errorf("an authoritative pipeline worker is live")
	}
	if err != gorm.ErrRecordNotFound {
		return err
	}
	var active models.PipelineRepairRequest
	activeQuery := db.Where("tenant_id = ? AND content_item_id = ? AND expected_item_updated_at = ? AND stage = ? AND state IN ?", c.Item.TenantID, c.Item.PublicID, c.Item.UpdatedAt, c.Stage, []string{models.PipelineRepairAwaitingApproval, models.PipelineRepairQueued, models.PipelineRepairClaimed, models.PipelineRepairRunning, models.PipelineRepairVerifying, models.PipelineRepairUncertain})
	if currentRepair != nil {
		activeQuery = activeQuery.Where("public_id <> ?", *currentRepair)
	}
	err = activeQuery.First(&active).Error
	if err == nil {
		return fmt.Errorf("an exact repair is already active")
	}
	if err != gorm.ErrRecordNotFound {
		return err
	}
	return nil
}

func pipelineRepairArtifactURL(item models.ContentItem, key string) string {
	if len(item.Metadata) == 0 {
		return ""
	}
	var metadata map[string]any
	if err := json.Unmarshal(item.Metadata, &metadata); err != nil {
		return ""
	}
	value, _ := metadata[key].(string)
	return strings.TrimSpace(value)
}

// CreateApprovedRepair is only called after the existing action preview was
// approved. Its key is server-derived from the immutable action request.
func CreateApprovedRepair(db *gorm.DB, action models.MediaSupplyActionRequest) (models.PipelineRepairRequest, error) {
	if action.ActionKey != supply.SupplyActionPipelineResumeExactStage || action.TargetType != "content_item" || action.State != models.MediaSupplyActionRequestQueued {
		return models.PipelineRepairRequest{}, fmt.Errorf("pipeline repair action is invalid")
	}
	tenantID, itemID, approvedBy := action.TenantID, action.TargetID, action.ApprovedBy
	c, err := CandidateForItem(db, tenantID, itemID)
	if err != nil {
		return models.PipelineRepairRequest{}, err
	}
	if err := CheckCandidate(db, c); err != nil {
		return models.PipelineRepairRequest{}, err
	}
	idemSum := sha256.Sum256([]byte(strings.Join([]string{"pipeline-repair/v1", tenantID, itemID.String(), c.Item.UpdatedAt.UTC().Format(time.RFC3339Nano), c.Stage, c.EvidenceDigest}, "\n")))
	idem := hex.EncodeToString(idemSum[:])
	now := time.Now().UTC()
	effectDigest := sha256.Sum256([]byte(strings.Join([]string{"pipeline-repair-effect/v1", tenantID, itemID.String(), c.Item.UpdatedAt.UTC().Format(time.RFC3339Nano), c.Stage, c.EvidenceDigest, idem}, "\n")))
	request := models.PipelineRepairRequest{PublicID: uuid.New(), TenantID: tenantID, ContentItemID: itemID, ActionRequestID: &action.PublicID, ExpectedItemUpdatedAt: c.Item.UpdatedAt, ExpectedStatus: string(c.Item.Status), Stage: c.Stage, SourceRunRequestID: c.Item.SourceRunRequestID, PriorStageEvidenceDigest: c.EvidenceDigest, RepairClass: "approval_required", IdempotencyKey: idem, DeterministicJobID: "pipeline-repair:" + idem, EffectInputDigest: hex.EncodeToString(effectDigest[:]), State: models.PipelineRepairQueued, ApprovedBy: approvedBy, ApprovedAt: &now, PlannedEffects: jsonValue(map[string]any{"stage": c.Stage, "effect": "resume_exact_stage", "rollback": "forward_only"}), AffectedSubjects: jsonValue([]map[string]string{{"type": "content_item", "id": itemID.String()}}), DeepLinks: datatypes.JSON([]byte(`["/platform/media/circulation"]`))}
	err = db.Transaction(func(tx *gorm.DB) error {
		if err := CheckCandidate(tx, c); err != nil {
			return err
		}
		if err := tx.Create(&request).Error; err != nil {
			return err
		}
		return appendEvent(tx, request, nil, "queued", map[string]any{"stage": c.Stage, "evidence_digest": c.EvidenceDigest})
	})
	return request, err
}

func ClaimNext(db *gorm.DB, owner string) (Claim, bool, error) {
	if strings.TrimSpace(owner) == "" {
		return Claim{}, false, fmt.Errorf("pipeline repair owner is required")
	}
	now := time.Now().UTC()
	if !supply.SupplyActionOwnerReady("aggregation_pipeline", now) {
		return Claim{}, false, fmt.Errorf("pipeline repair owner readiness is unavailable")
	}
	var claim Claim
	err := db.Transaction(func(tx *gorm.DB) error {
		var requests []models.PipelineRepairRequest
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).Where("state = ?", models.PipelineRepairQueued).Order("created_at ASC").Limit(16).Find(&requests).Error; err != nil {
			return err
		}
		for _, r := range requests {
			c, err := CandidateForItem(tx, r.TenantID, r.ContentItemID)
			if err != nil || c.Stage != r.Stage || !c.Item.UpdatedAt.Equal(r.ExpectedItemUpdatedAt) || c.EvidenceDigest != r.PriorStageEvidenceDigest {
				continue
			}
			if err = checkCandidate(tx, c, &r.PublicID); err != nil {
				continue
			}
			// A claim that expired before Begin is proven not to have crossed the
			// effect boundary. Reclaim its same attempt/fence/job identity instead
			// of allocating a second repair attempt. The dispatcher replaces the
			// waiting BullMQ envelope with this new lease epoch.
			reclaimed, reused, reclaimErr := reclaimUnstartedAttempt(tx, r, owner, now)
			if reclaimErr != nil {
				return reclaimErr
			}
			if reused {
				claim = reclaimed
				return nil
			}
			var n int64
			if err := tx.Model(&models.PipelineRepairAttempt{}).Where("tenant_id=? AND repair_request_id=?", r.TenantID, r.PublicID).Count(&n).Error; err != nil {
				return err
			}
			if n >= maxAttempts {
				continue
			}
			token, fence := uuid.New(), uuid.New()
			expires := now.Add(leaseDuration)
			attempt := models.PipelineRepairAttempt{PublicID: uuid.New(), TenantID: r.TenantID, RepairRequestID: r.PublicID, AttemptNumber: int(n) + 1, State: models.PipelineRepairClaimed, FenceToken: fence, OwnerProtocol: OwnerProtocol}
			lease := models.PipelineStageLease{PublicID: uuid.New(), TenantID: r.TenantID, ContentItemID: r.ContentItemID, ItemUpdatedAt: r.ExpectedItemUpdatedAt, Stage: r.Stage, ExecutionOwner: OwnerProtocol, RepairRequestID: &r.PublicID, DeterministicJobID: r.DeterministicJobID, State: "claimed", LeaseToken: token, FenceToken: fence, LeaseEpoch: 1, LeaseExpiresAt: expires, HeartbeatAt: now}
			if err := tx.Create(&attempt).Error; err != nil {
				return err
			}
			if err := tx.Create(&lease).Error; err != nil {
				return err
			}
			if err := claimLinkedAction(tx, r, attempt, token, expires); err != nil {
				return err
			}
			if err := tx.Model(&r).Updates(map[string]any{"state": models.PipelineRepairClaimed, "claim_owner": owner, "claim_token": token, "claim_epoch": gorm.Expr("claim_epoch + 1"), "claim_expires_at": expires}).Error; err != nil {
				return err
			}
			r.State = models.PipelineRepairClaimed
			r.ClaimOwner = owner
			r.ClaimToken = &token
			r.ClaimEpoch++
			r.ClaimExpiresAt = &expires
			claim = Claim{Request: r, Attempt: attempt, Lease: lease, ClaimToken: token}
			return appendEvent(tx, r, &attempt, "claimed", map[string]any{"stage": r.Stage, "fence": fence.String()})
		}
		return nil
	})
	if err != nil || claim.Request.PublicID == uuid.Nil {
		return claim, false, err
	}
	if accessErr := recheckLinkedSupplyActionAccess(db, claim.Request.TenantID, claim.Request.ActionRequestID); accessErr != nil {
		return Claim{}, false, accessErr
	}
	return claim, true, nil
}

func reclaimUnstartedAttempt(tx *gorm.DB, request models.PipelineRepairRequest, owner string, now time.Time) (Claim, bool, error) {
	var attempt models.PipelineRepairAttempt
	err := tx.Where("tenant_id=? AND repair_request_id=?", request.TenantID, request.PublicID).Order("attempt_number DESC").First(&attempt).Error
	if err == gorm.ErrRecordNotFound || attempt.EffectStartedAt != nil || attempt.State != models.PipelineRepairClaimed {
		return Claim{}, false, nil
	}
	if err != nil {
		return Claim{}, false, err
	}
	var lease models.PipelineStageLease
	err = tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND repair_request_id=? AND fence_token=? AND state=? AND effect_started_at IS NULL", request.TenantID, request.PublicID, attempt.FenceToken, "claimed").Order("lease_epoch DESC").First(&lease).Error
	if err == gorm.ErrRecordNotFound {
		return Claim{}, false, nil
	}
	if err != nil {
		return Claim{}, false, err
	}
	token := uuid.New()
	expires := now.Add(leaseDuration)
	if err := tx.Model(&lease).Updates(map[string]any{
		"lease_token": token, "lease_epoch": gorm.Expr("lease_epoch + 1"), "lease_expires_at": expires, "heartbeat_at": now,
	}).Error; err != nil {
		return Claim{}, false, err
	}
	if err := tx.Model(&attempt).Update("state", models.PipelineRepairClaimed).Error; err != nil {
		return Claim{}, false, err
	}
	if err := claimLinkedAction(tx, request, attempt, token, expires); err != nil {
		return Claim{}, false, err
	}
	if err := tx.Model(&request).Updates(map[string]any{
		"state": models.PipelineRepairClaimed, "claim_owner": owner, "claim_token": token,
		"claim_epoch": gorm.Expr("claim_epoch + 1"), "claim_expires_at": expires,
	}).Error; err != nil {
		return Claim{}, false, err
	}
	request.State, request.ClaimOwner, request.ClaimToken, request.ClaimExpiresAt = models.PipelineRepairClaimed, owner, &token, &expires
	request.ClaimEpoch++
	lease.LeaseToken, lease.LeaseExpiresAt, lease.HeartbeatAt, lease.LeaseEpoch = token, expires, now, lease.LeaseEpoch+1
	if err := appendEvent(tx, request, &attempt, "reclaimed_unstarted", map[string]any{"stage": request.Stage, "fence": attempt.FenceToken.String(), "lease_epoch": lease.LeaseEpoch}); err != nil {
		return Claim{}, false, err
	}
	return Claim{Request: request, Attempt: attempt, Lease: lease, ClaimToken: token}, true, nil
}

func Begin(db *gorm.DB, requestID, owner string, token uuid.UUID) error {
	if !supply.SupplyActionOwnerReady("aggregation_pipeline", time.Now().UTC()) {
		return fmt.Errorf("pipeline repair owner readiness is unavailable")
	}
	if err := recheckPipelineRepairActionAccess(db, requestID); err != nil {
		return err
	}
	return transition(db, requestID, owner, token, "begin")
}
func Heartbeat(db *gorm.DB, requestID, owner string, token uuid.UUID) error {
	if !supply.SupplyActionOwnerReady("aggregation_pipeline", time.Now().UTC()) {
		return fmt.Errorf("pipeline repair owner readiness is unavailable")
	}
	if err := recheckPipelineRepairActionAccess(db, requestID); err != nil {
		return err
	}
	return transition(db, requestID, owner, token, "heartbeat")
}

func recheckPipelineRepairActionAccess(db *gorm.DB, requestID string) error {
	var request models.PipelineRepairRequest
	if err := db.Select("tenant_id", "action_request_id").Where("public_id = ?", requestID).First(&request).Error; err != nil {
		return err
	}
	return recheckLinkedSupplyActionAccess(db, request.TenantID, request.ActionRequestID)
}

func recheckLinkedSupplyActionAccess(db *gorm.DB, tenantID string, actionID *uuid.UUID) error {
	if actionID == nil {
		return fmt.Errorf("pipeline repair has no signed supply action")
	}
	var action models.MediaSupplyActionRequest
	if err := db.Where("tenant_id = ? AND public_id = ?", tenantID, *actionID).First(&action).Error; err != nil {
		return err
	}
	if err := supply.RecheckSupplyActionAccess(context.Background(), action); err != nil {
		return err
	}
	return supply.RecheckSupplyActionExecutionAuthority(db, action)
}

func transition(db *gorm.DB, requestID, owner string, token uuid.UUID, kind string) error {
	now := time.Now().UTC()
	return db.Transaction(func(tx *gorm.DB) error {
		var r models.PipelineRepairRequest
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id=? AND claim_owner=? AND claim_token=?", requestID, owner, token).First(&r).Error; err != nil {
			return err
		}
		if r.CancellationRequestedAt != nil {
			return fmt.Errorf("pipeline repair is cancelled")
		}
		if r.ClaimExpiresAt == nil || !r.ClaimExpiresAt.After(now) {
			return fmt.Errorf("pipeline repair claim is stale")
		}
		var a models.PipelineRepairAttempt
		if err := tx.Where("tenant_id=? AND repair_request_id=?", r.TenantID, r.PublicID).Order("attempt_number DESC").First(&a).Error; err != nil {
			return err
		}
		var l models.PipelineStageLease
		if err := tx.Where("tenant_id=? AND repair_request_id=? AND lease_token=? AND fence_token=?", r.TenantID, r.PublicID, token, a.FenceToken).First(&l).Error; err != nil {
			return err
		}
		allowed, _, err := supply.MayExecuteSupplyAction(tx, r.TenantID, supply.SupplyActionPipelineResumeExactStage)
		if err != nil {
			return err
		}
		if !allowed {
			return fmt.Errorf("pipeline repair control is disabled")
		}
		expires := now.Add(leaseDuration)
		if kind == "begin" {
			if r.State != models.PipelineRepairClaimed {
				return fmt.Errorf("pipeline repair cannot begin")
			}
			current, candidateErr := CandidateForItem(tx, r.TenantID, r.ContentItemID)
			if candidateErr != nil || current.Stage != r.Stage || !current.Item.UpdatedAt.Equal(r.ExpectedItemUpdatedAt) || current.EvidenceDigest != r.PriorStageEvidenceDigest {
				return fmt.Errorf("pipeline repair target evidence changed before effect")
			}
			if err := tx.Model(&r).Updates(map[string]any{"state": models.PipelineRepairRunning, "claim_expires_at": expires, "before_effects": jsonValue(map[string]any{"status": r.ExpectedStatus, "stage": r.Stage})}).Error; err != nil {
				return err
			}
			if err := tx.Model(&a).Updates(map[string]any{"state": models.PipelineRepairRunning, "started_at": now, "effect_started_at": now}).Error; err != nil {
				return err
			}
			if err := tx.Model(&l).Updates(map[string]any{"state": "running", "lease_expires_at": expires, "heartbeat_at": now, "effect_started_at": now}).Error; err != nil {
				return err
			}
			if err := updateLinkedActionLease(tx, r, a, token, expires, true, now); err != nil {
				return err
			}
			return appendEvent(tx, r, &a, "effect_started", map[string]any{"stage": r.Stage})
		}
		if err := tx.Model(&r).Update("claim_expires_at", expires).Error; err != nil {
			return err
		}
		if err := tx.Model(&l).Updates(map[string]any{"lease_expires_at": expires, "heartbeat_at": now}).Error; err != nil {
			return err
		}
		return updateLinkedActionLease(tx, r, a, token, expires, false, now)
	})
}

// CompleteEffect records a fenced owner receipt after the actual exact stage
// executor returns. This is intentionally not a generic processing-event API:
// the repair, attempt, fence, item version and input digest are all derived
// from the current CMS claim and must match together.
func CompleteEffect(db *gorm.DB, requestID, owner string, token uuid.UUID, receipt EffectReceipt) error {
	if receipt.ProducerEventID == uuid.Nil || strings.TrimSpace(receipt.OutputDigest) == "" || len(receipt.OutputDigest) > 128 {
		return fmt.Errorf("pipeline repair effect receipt is invalid")
	}
	now := time.Now().UTC()
	return db.Transaction(func(tx *gorm.DB) error {
		var r models.PipelineRepairRequest
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id=? AND claim_owner=? AND claim_token=? AND state=?", requestID, owner, token, models.PipelineRepairRunning).First(&r).Error; err != nil {
			return err
		}
		// Cancellation is advisory after Begin. A terminal receipt still has to
		// be retained and independently verified so a completed effect is never
		// relabelled as cancelled.
		if r.ClaimExpiresAt == nil || !r.ClaimExpiresAt.After(now) {
			return fmt.Errorf("pipeline repair claim is stale")
		}
		var attempt models.PipelineRepairAttempt
		if err := tx.Where("tenant_id=? AND repair_request_id=?", r.TenantID, r.PublicID).Order("attempt_number DESC").First(&attempt).Error; err != nil {
			return err
		}
		var lease models.PipelineStageLease
		if err := tx.Where("tenant_id=? AND repair_request_id=? AND lease_token=? AND fence_token=? AND state=?", r.TenantID, r.PublicID, token, attempt.FenceToken, "running").First(&lease).Error; err != nil {
			return fmt.Errorf("pipeline repair lease is stale: %w", err)
		}
		if r.EffectProducerEventID != nil && *r.EffectProducerEventID != receipt.ProducerEventID {
			return fmt.Errorf("pipeline repair already has a different producer receipt")
		}
		// Unique producer_event_id also makes a lost HTTP acknowledgement safe to
		// redeliver without duplicating evidence.
		var existing models.ContentProcessingEvent
		err := tx.Where("tenant_id=? AND producer_event_id=?", r.TenantID, receipt.ProducerEventID).First(&existing).Error
		if err == nil {
			if existing.PipelineRepairRequestID == nil || *existing.PipelineRepairRequestID != r.PublicID || existing.PipelineRepairAttemptID == nil || *existing.PipelineRepairAttemptID != attempt.PublicID || existing.PipelineRepairFenceToken == nil || *existing.PipelineRepairFenceToken != attempt.FenceToken {
				return fmt.Errorf("pipeline repair producer receipt identity conflicts")
			}
		} else if err != gorm.ErrRecordNotFound {
			return err
		} else {
			expected := r.ExpectedItemUpdatedAt
			repairID, attemptID, fence, producerID := r.PublicID, attempt.PublicID, attempt.FenceToken, receipt.ProducerEventID
			if err := tx.Create(&models.ContentProcessingEvent{
				TenantID: r.TenantID, ContentItemID: &r.ContentItemID, SourceRunRequestID: r.SourceRunRequestID,
				Stage: r.Stage, State: "completed", Producer: "aggregation", ExecutionOwner: OwnerProtocol,
				EventClass: "pipeline_repair_effect_persisted", CorrelationID: r.PublicID.String(), JobID: r.DeterministicJobID,
				PipelineRepairRequestID: &repairID, PipelineRepairAttemptID: &attemptID, PipelineRepairFenceToken: &fence,
				ExpectedItemUpdatedAt: &expected, ProducerEventID: &producerID, EffectInputDigest: r.EffectInputDigest,
				Payload: jsonValue(map[string]any{"output_digest": receipt.OutputDigest, "output": receipt.Output}), OccurredAt: now,
			}).Error; err != nil {
				return err
			}
		}
		if err := tx.Model(&r).Updates(map[string]any{"state": models.PipelineRepairVerifying, "effect_producer_event_id": receipt.ProducerEventID, "after_effects": jsonValue(map[string]any{"schema_version": "pipeline-repair-effects/v1", "stage": r.Stage, "output_digest": receipt.OutputDigest, "producer_event_id": receipt.ProducerEventID}), "claim_expires_at": nil}).Error; err != nil {
			return err
		}
		if err := tx.Model(&attempt).Updates(map[string]any{"state": models.PipelineRepairVerifying, "finished_at": now}).Error; err != nil {
			return err
		}
		if err := tx.Model(&lease).Updates(map[string]any{"state": "verifying", "terminal_at": now, "lease_expires_at": now}).Error; err != nil {
			return err
		}
		if err := markLinkedActionVerifying(tx, r, attempt, token, now); err != nil {
			return err
		}
		return appendEvent(tx, r, &attempt, "effect_receipt_recorded", map[string]any{"producer_event_id": receipt.ProducerEventID.String(), "output_digest": receipt.OutputDigest})
	})
}

func Cancel(db *gorm.DB, tenantID, requestID, actor string) (models.PipelineRepairRequest, error) {
	var result models.PipelineRepairRequest
	now := time.Now().UTC()
	err := db.Transaction(func(tx *gorm.DB) error {
		var r models.PipelineRepairRequest
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id=? AND tenant_id=?", requestID, tenantID).First(&r).Error; err != nil {
			return err
		}
		if r.State == models.PipelineRepairQueued || r.State == models.PipelineRepairClaimed {
			if err := tx.Model(&r).Updates(map[string]any{"state": models.PipelineRepairCancelled, "cancellation_requested_at": now, "finished_at": now}).Error; err != nil {
				return err
			}
		} else if r.State == models.PipelineRepairRunning || r.State == models.PipelineRepairVerifying {
			if err := tx.Model(&r).Update("cancellation_requested_at", now).Error; err != nil {
				return err
			}
		} else {
			return fmt.Errorf("pipeline repair is terminal")
		}
		result = r
		return appendEvent(tx, r, nil, "cancellation_requested", map[string]any{"actor": actor})
	})
	return result, err
}

// CancelByAction propagates cancellation to the exact pipeline ledger linked
// to one signed Supply action. It accepts no browser-derived stage or item.
func CancelByAction(db *gorm.DB, tenant string, actionID uuid.UUID, actor string) error {
	var request models.PipelineRepairRequest
	err := db.Where("tenant_id=? AND action_request_id=?", tenant, actionID).First(&request).Error
	if err == gorm.ErrRecordNotFound {
		return nil
	}
	if err != nil {
		return err
	}
	_, err = Cancel(db, tenant, request.PublicID.String(), actor)
	return err
}

func VerifyOne(db *gorm.DB) (bool, error) {
	var r models.PipelineRepairRequest
	err := db.Where("state IN ?", []string{models.PipelineRepairVerifying, models.PipelineRepairUncertain}).Order("updated_at ASC").First(&r).Error
	if err == gorm.ErrRecordNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var event models.ContentProcessingEvent
	if r.EffectProducerEventID == nil || strings.TrimSpace(r.EffectInputDigest) == "" {
		// A pre-correlation, post-effect repair is intrinsically uncertain. It
		// remains visible for human investigation rather than accepting any
		// similarly shaped historical event.
		return true, nil
	}
	var attempt models.PipelineRepairAttempt
	if err := db.Where("tenant_id=? AND repair_request_id=?", r.TenantID, r.PublicID).Order("attempt_number DESC").First(&attempt).Error; err != nil {
		return true, err
	}
	err = db.Where("tenant_id=? AND content_item_id=? AND stage=? AND state=? AND pipeline_repair_request_id=? AND pipeline_repair_attempt_id=? AND pipeline_repair_fence_token=? AND producer_event_id=? AND effect_input_digest=? AND expected_item_updated_at=?", r.TenantID, r.ContentItemID, r.Stage, "completed", r.PublicID, attempt.PublicID, attempt.FenceToken, *r.EffectProducerEventID, r.EffectInputDigest, r.ExpectedItemUpdatedAt).Order("occurred_at DESC").First(&event).Error
	now := time.Now().UTC()
	if err == gorm.ErrRecordNotFound {
		return true, nil
	}
	if err != nil {
		return true, err
	}
	if !matchesExactEffectEvent(r, attempt, event) {
		return true, nil
	}
	present, observeErr := observePersistedStageEffect(db, r, event)
	if observeErr != nil {
		return true, observeErr
	}
	if !present {
		return true, nil
	}
	return true, db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&models.PipelineRepairRequest{}).Where("public_id=? AND state IN ?", r.PublicID, []string{models.PipelineRepairVerifying, models.PipelineRepairUncertain}).Updates(map[string]any{"state": models.PipelineRepairSucceeded, "verified_effects": jsonValue(map[string]any{"stage_event_id": event.PublicID.String(), "stage": r.Stage, "verification": "persisted_processing_event"}), "terminal_proof": jsonValue(map[string]any{"evidence": "present", "event_id": event.PublicID.String()}), "finished_at": now}).Error; err != nil {
			return err
		}
		if err := completeLinkedAction(tx, r, event, now); err != nil {
			return err
		}
		return appendEvent(tx, r, nil, "verified", map[string]any{"event_id": event.PublicID.String()})
	})
}

// observePersistedStageEffect is deliberately separate from the owner receipt:
// an accepted HTTP call or a queue completion never proves a repair. The
// receipt identifies the exact effect; this observer checks the corresponding
// durable CMS state without making another effect.
func observePersistedStageEffect(db *gorm.DB, request models.PipelineRepairRequest, event models.ContentProcessingEvent) (bool, error) {
	var payload struct {
		Output map[string]any `json:"output"`
	}
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return false, fmt.Errorf("pipeline repair receipt payload is unreadable: %w", err)
	}
	var item models.ContentItem
	if err := db.Where("tenant_id=? AND public_id=?", request.TenantID, request.ContentItemID).First(&item).Error; err != nil {
		return false, err
	}
	switch request.Stage {
	case models.PipelineStageMediaDownload:
		url, _ := payload.Output["storage_url"].(string)
		if url == "" {
			return false, nil
		}
		var metadata map[string]any
		if err := json.Unmarshal(item.Metadata, &metadata); err != nil {
			return false, nil
		}
		stored, _ := metadata["pipeline_repair_original_url"].(string)
		return stored == url, nil
	case models.PipelineStageMediaTranscode:
		url, _ := payload.Output["playback_url"].(string)
		return url != "" && item.MediaURL != nil && item.PlaybackURL != nil && *item.MediaURL == url && *item.PlaybackURL == url, nil
	case models.PipelineStageMediaThumbnail:
		url, _ := payload.Output["thumbnail_url"].(string)
		return url != "" && item.ThumbnailURL != nil && *item.ThumbnailURL == url, nil
	case models.PipelineStageTextEmbedding:
		return item.Embedding != nil && item.EmbeddingModel != nil && strings.TrimSpace(*item.EmbeddingModel) != "", nil
	default:
		return false, fmt.Errorf("pipeline repair stage is not registered")
	}
}

func matchesExactEffectEvent(request models.PipelineRepairRequest, attempt models.PipelineRepairAttempt, event models.ContentProcessingEvent) bool {
	return event.TenantID == request.TenantID && event.ContentItemID != nil && *event.ContentItemID == request.ContentItemID &&
		event.Stage == request.Stage && event.State == "completed" && event.ExecutionOwner == OwnerProtocol &&
		event.PipelineRepairRequestID != nil && *event.PipelineRepairRequestID == request.PublicID &&
		event.PipelineRepairAttemptID != nil && *event.PipelineRepairAttemptID == attempt.PublicID &&
		event.PipelineRepairFenceToken != nil && *event.PipelineRepairFenceToken == attempt.FenceToken &&
		event.ProducerEventID != nil && request.EffectProducerEventID != nil && *event.ProducerEventID == *request.EffectProducerEventID &&
		event.EffectInputDigest == request.EffectInputDigest && event.ExpectedItemUpdatedAt != nil && event.ExpectedItemUpdatedAt.Equal(request.ExpectedItemUpdatedAt)
}

func RecoverExpired(db *gorm.DB) error {
	now := time.Now().UTC()
	return db.Transaction(func(tx *gorm.DB) error {
		var requests []models.PipelineRepairRequest
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).Where("state IN ? AND claim_expires_at < ?", []string{models.PipelineRepairClaimed, models.PipelineRepairRunning}, now).Find(&requests).Error; err != nil {
			return err
		}
		for _, r := range requests {
			next := models.PipelineRepairQueued
			if r.State == models.PipelineRepairRunning {
				next = models.PipelineRepairUncertain
			}
			if err := tx.Model(&r).Updates(map[string]any{"state": next, "claim_token": nil, "claim_owner": "", "claim_expires_at": nil}).Error; err != nil {
				return err
			}
			if err := recoverLinkedAction(tx, r, next, now); err != nil {
				return err
			}
			if err := appendEvent(tx, r, nil, "claim_expired", map[string]any{"next": next}); err != nil {
				return err
			}
		}
		return nil
	})
}

func claimLinkedAction(tx *gorm.DB, request models.PipelineRepairRequest, attempt models.PipelineRepairAttempt, token uuid.UUID, expires time.Time) error {
	if request.ActionRequestID == nil {
		return fmt.Errorf("pipeline repair is not linked to a signed action")
	}
	var action models.MediaSupplyActionRequest
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=? AND state=?", request.TenantID, *request.ActionRequestID, models.MediaSupplyActionRequestQueued).First(&action).Error; err != nil {
		return fmt.Errorf("linked pipeline action cannot be claimed: %w", err)
	}
	linked := models.MediaSupplyActionAttempt{}
	lookup := tx.Where("tenant_id=? AND action_request_id=? AND owner_protocol=?", request.TenantID, action.PublicID, "pipeline-repair/v1:"+attempt.PublicID.String()).First(&linked).Error
	if lookup == gorm.ErrRecordNotFound {
		linked = models.MediaSupplyActionAttempt{PublicID: uuid.New(), TenantID: request.TenantID, ActionRequestID: action.PublicID, AttemptNumber: attempt.AttemptNumber, State: models.MediaSupplyActionRequestClaimed, FenceToken: attempt.FenceToken, OwnerProtocol: "pipeline-repair/v1:" + attempt.PublicID.String()}
		if err := tx.Create(&linked).Error; err != nil {
			return err
		}
	} else if lookup != nil {
		return lookup
	} else if err := tx.Model(&linked).Updates(map[string]any{"state": models.MediaSupplyActionRequestClaimed, "finished_at": nil}).Error; err != nil {
		return err
	}
	if err := tx.Model(&action).Updates(map[string]any{"state": models.MediaSupplyActionRequestClaimed, "claim_owner": OwnerProtocol, "claim_token": token, "claim_epoch": gorm.Expr("claim_epoch+1"), "claim_expires_at": expires}).Error; err != nil {
		return err
	}
	return appendLinkedActionEvent(tx, action, &linked, "pipeline_claimed", map[string]any{"pipeline_repair_id": request.PublicID, "stage": request.Stage})
}

func linkedActionAttempt(tx *gorm.DB, request models.PipelineRepairRequest, attempt models.PipelineRepairAttempt) (models.MediaSupplyActionRequest, models.MediaSupplyActionAttempt, error) {
	if request.ActionRequestID == nil {
		return models.MediaSupplyActionRequest{}, models.MediaSupplyActionAttempt{}, fmt.Errorf("pipeline repair is not linked to a signed action")
	}
	var action models.MediaSupplyActionRequest
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=?", request.TenantID, *request.ActionRequestID).First(&action).Error; err != nil {
		return action, models.MediaSupplyActionAttempt{}, err
	}
	var linked models.MediaSupplyActionAttempt
	if err := tx.Where("tenant_id=? AND action_request_id=? AND owner_protocol=?", request.TenantID, action.PublicID, "pipeline-repair/v1:"+attempt.PublicID.String()).First(&linked).Error; err != nil {
		return action, linked, err
	}
	return action, linked, nil
}

func updateLinkedActionLease(tx *gorm.DB, request models.PipelineRepairRequest, attempt models.PipelineRepairAttempt, token uuid.UUID, expires time.Time, begin bool, now time.Time) error {
	action, linked, err := linkedActionAttempt(tx, request, attempt)
	if err != nil {
		return err
	}
	updates := map[string]any{"claim_expires_at": expires}
	if begin {
		updates["state"] = models.MediaSupplyActionRequestRunning
		updates["before_effects"] = jsonValue(map[string]any{"schema_version": "pipeline-repair-effects/v1", "content_item_id": request.ContentItemID, "item_updated_at": request.ExpectedItemUpdatedAt, "stage": request.Stage, "status": request.ExpectedStatus})
	}
	result := tx.Model(&action).Where("claim_token=?", token).Updates(updates)
	if result.Error != nil || result.RowsAffected != 1 {
		return fmt.Errorf("linked pipeline action lease is stale")
	}
	if begin {
		if err := tx.Model(&linked).Updates(map[string]any{"state": models.MediaSupplyActionRequestRunning, "started_at": now, "effect_started_at": now}).Error; err != nil {
			return err
		}
		return appendLinkedActionEvent(tx, action, &linked, "pipeline_effect_started", map[string]any{"stage": request.Stage})
	}
	return nil
}

func markLinkedActionVerifying(tx *gorm.DB, request models.PipelineRepairRequest, attempt models.PipelineRepairAttempt, token uuid.UUID, now time.Time) error {
	action, linked, err := linkedActionAttempt(tx, request, attempt)
	if err != nil {
		return err
	}
	after := jsonValue(map[string]any{"schema_version": "pipeline-repair-effects/v1", "content_item_id": request.ContentItemID, "stage": request.Stage, "handoff": "accepted", "verification": "pending"})
	result := tx.Model(&action).Where("claim_token=?", token).Updates(map[string]any{"state": models.MediaSupplyActionRequestVerifying, "after_effects": after, "claim_expires_at": nil})
	if result.Error != nil || result.RowsAffected != 1 {
		return fmt.Errorf("linked pipeline action cannot enter verification")
	}
	if err := tx.Model(&linked).Update("state", models.MediaSupplyActionRequestVerifying).Error; err != nil {
		return err
	}
	return appendLinkedActionEvent(tx, action, &linked, "pipeline_verification_pending", map[string]any{"at": now.Format(time.RFC3339Nano)})
}

func completeLinkedAction(tx *gorm.DB, request models.PipelineRepairRequest, evidence models.ContentProcessingEvent, now time.Time) error {
	if request.ActionRequestID == nil {
		return fmt.Errorf("pipeline repair is not linked to a signed action")
	}
	var action models.MediaSupplyActionRequest
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=? AND state IN ?", request.TenantID, *request.ActionRequestID, []string{models.MediaSupplyActionRequestVerifying, models.MediaSupplyActionRequestUncertain}).First(&action).Error; err != nil {
		return err
	}
	proof := jsonValue(map[string]any{"schema_version": "pipeline-repair-proof/v1", "content_item_id": request.ContentItemID, "stage": request.Stage, "processing_event_id": evidence.PublicID, "verified": "persisted_processing_event"})
	if err := tx.Model(&action).Updates(map[string]any{"state": models.MediaSupplyActionRequestSucceeded, "verified_effects": proof, "finished_at": now, "claim_expires_at": nil}).Error; err != nil {
		return err
	}
	if err := tx.Model(&models.MediaSupplyActionAttempt{}).Where("tenant_id=? AND action_request_id=?", request.TenantID, action.PublicID).Order("attempt_number DESC").Updates(map[string]any{"state": models.MediaSupplyActionRequestSucceeded, "finished_at": now}).Error; err != nil {
		return err
	}
	return appendLinkedActionEvent(tx, action, nil, "pipeline_verified", map[string]any{"processing_event_id": evidence.PublicID})
}

func recoverLinkedAction(tx *gorm.DB, request models.PipelineRepairRequest, next string, now time.Time) error {
	if request.ActionRequestID == nil {
		return fmt.Errorf("pipeline repair is not linked to a signed action")
	}
	state, attemptState, failure := models.MediaSupplyActionRequestQueued, models.MediaSupplyActionRequestCancelled, ""
	if next == models.PipelineRepairUncertain {
		state, attemptState, failure = models.MediaSupplyActionRequestUncertain, models.MediaSupplyActionRequestUncertain, "pipeline_claim_expired_after_effect"
	}
	var action models.MediaSupplyActionRequest
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=?", request.TenantID, *request.ActionRequestID).First(&action).Error; err != nil {
		return err
	}
	if err := tx.Model(&action).Updates(map[string]any{"state": state, "claim_owner": "", "claim_token": nil, "claim_expires_at": nil, "failure_class": failure}).Error; err != nil {
		return err
	}
	if err := tx.Model(&models.MediaSupplyActionAttempt{}).Where("tenant_id=? AND action_request_id=?", request.TenantID, action.PublicID).Order("attempt_number DESC").Updates(map[string]any{"state": attemptState, "finished_at": now}).Error; err != nil {
		return err
	}
	return appendLinkedActionEvent(tx, action, nil, "pipeline_claim_recovered", map[string]any{"next": state})
}

func appendLinkedActionEvent(tx *gorm.DB, action models.MediaSupplyActionRequest, attempt *models.MediaSupplyActionAttempt, eventType string, payload map[string]any) error {
	var sequence int64
	if err := tx.Model(&models.MediaSupplyActionEvent{}).Where("tenant_id=? AND action_request_id=?", action.TenantID, action.PublicID).Count(&sequence).Error; err != nil {
		return err
	}
	var attemptID *uuid.UUID
	if attempt != nil {
		value := attempt.PublicID
		attemptID = &value
	}
	return tx.Create(&models.MediaSupplyActionEvent{PublicID: uuid.New(), TenantID: action.TenantID, ActionRequestID: action.PublicID, AttemptID: attemptID, Sequence: sequence + 1, EventKey: fmt.Sprintf("pipeline:%s:%d", eventType, sequence+1), EventType: eventType, Payload: jsonValue(payload), OccurredAt: time.Now().UTC()}).Error
}

func appendEvent(db *gorm.DB, r models.PipelineRepairRequest, a *models.PipelineRepairAttempt, key string, payload map[string]any) error {
	var n int64
	if err := db.Model(&models.PipelineRepairEvent{}).Where("tenant_id=? AND repair_request_id=?", r.TenantID, r.PublicID).Count(&n).Error; err != nil {
		return err
	}
	var aid *uuid.UUID
	if a != nil {
		v := a.PublicID
		aid = &v
	}
	return db.Create(&models.PipelineRepairEvent{PublicID: uuid.New(), TenantID: r.TenantID, RepairRequestID: r.PublicID, AttemptID: aid, Sequence: n + 1, EventKey: key, EventType: "lifecycle", Payload: jsonValue(payload), OccurredAt: time.Now().UTC()}).Error
}
func jsonValue(v any) datatypes.JSON { raw, _ := json.Marshal(v); return datatypes.JSON(raw) }
