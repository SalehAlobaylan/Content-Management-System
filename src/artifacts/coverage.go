package artifacts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/supply"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	MediaOwner             = "media"
	EnrichmentOwner        = "enrichment"
	ArtifactTranscript     = "transcript"
	ArtifactImageEmbedding = "image_embedding"
	ArtifactTextEmbedding  = "text_embedding"
	ArtifactLLMMetadata    = "llm_metadata"
	leaseDuration          = 45 * time.Second
	maxAttempts            = 2
	verificationDeadline   = 10 * time.Minute
)

type Descriptor struct {
	Artifact, Owner, ActionKey, Verification string
	MaxWork                                  int
	Rollback                                 string
}

func artifactBudget(item models.ContentItem, artifact string) (string, float64, error) {
	switch artifact {
	case ArtifactTranscript:
		if item.DurationSec == nil || *item.DurationSec <= 0 {
			return "", 0, fmt.Errorf("transcript workload is unknown")
		}
		return "media_minute", math.Max(1, math.Ceil(float64(*item.DurationSec)/60)), nil
	case ArtifactImageEmbedding:
		return "image_item", 1, nil
	case ArtifactTextEmbedding:
		return "embedding_item", 1, nil
	case ArtifactLLMMetadata:
		return "llm_call", 1, nil
	default:
		return "", 0, fmt.Errorf("artifact workload is not registered")
	}
}

func artifactNativeBudgetAvailable(tx *gorm.DB, tenant, artifact string) error {
	if artifact == ArtifactTranscript {
		var config models.TranscriptionConfig
		err := tx.Where("tenant_id = ?", tenant).First(&config).Error
		if err != nil && err != gorm.ErrRecordNotFound {
			return err
		}
		if err == nil && config.MonthlyBudgetCapUsd > 0 && config.MonthlySpendUsd+config.MonthlyReservedUsd >= config.MonthlyBudgetCapUsd {
			return fmt.Errorf("transcription budget is exhausted")
		}
	}
	if artifact == ArtifactTextEmbedding || artifact == ArtifactLLMMetadata {
		var budgets []models.AISpendBudget
		if err := tx.Where("tenant_id = ?", tenant).Find(&budgets).Error; err != nil {
			return err
		}
		for _, budget := range budgets {
			if budget.PausedUntil != nil && budget.PausedUntil.After(time.Now().UTC()) {
				return fmt.Errorf("AI spend is paused")
			}
			hardPct := budget.HardPct
			if hardPct <= 0 {
				hardPct = 100
			}
			if budget.CapUSD != nil && *budget.CapUSD > 0 && budget.SpendUSD+budget.ReservedUSD >= *budget.CapUSD*hardPct/100 {
				return fmt.Errorf("AI spend hard stop is active")
			}
		}
	}
	return nil
}

var descriptors = map[string]Descriptor{
	ArtifactTranscript:     {ArtifactTranscript, MediaOwner, supply.SupplyActionArtifactRequestTranscript, "linked_transcript_with_correlated_receipt", 1, "forward_only"},
	ArtifactImageEmbedding: {ArtifactImageEmbedding, MediaOwner, supply.SupplyActionArtifactRequestImageEmbedding, "image_vector_with_correlated_provenance", 1, "forward_only"},
	ArtifactTextEmbedding:  {ArtifactTextEmbedding, EnrichmentOwner, supply.SupplyActionArtifactRequestTextEmbedding, "text_vector_with_correlated_provenance", 1, "forward_only"},
	ArtifactLLMMetadata:    {ArtifactLLMMetadata, EnrichmentOwner, supply.SupplyActionArtifactRequestLLMMetadata, "typed_metadata_with_correlated_provenance", 1, "forward_only"},
}

func DescriptorForArtifact(value string) (Descriptor, bool) {
	d, ok := descriptors[strings.TrimSpace(value)]
	return d, ok
}
func DescriptorForAction(value string) (Descriptor, bool) {
	for _, d := range descriptors {
		if d.ActionKey == strings.TrimSpace(value) {
			return d, true
		}
	}
	return Descriptor{}, false
}
func OwnerFor(value string) (string, error) {
	d, ok := DescriptorForArtifact(value)
	if !ok {
		return "", fmt.Errorf("artifact is not admitted")
	}
	return d.Owner, nil
}

type Correlation struct{ RequestID, AttemptID, ClaimToken, FenceToken, InputDigest, ProducerEventID string }
type Claim struct {
	Request    models.ArtifactCoverageRequest `json:"request"`
	Attempt    models.ArtifactCoverageAttempt `json:"attempt"`
	ClaimToken uuid.UUID                      `json:"claim_token"`
	Item       models.ContentItem             `json:"-"`
}

func Missing(item models.ContentItem, artifact string) (bool, error) {
	switch artifact {
	case ArtifactTranscript:
		return item.TranscriptID == nil, nil
	case ArtifactImageEmbedding:
		return item.ImageEmbedding == nil, nil
	case ArtifactTextEmbedding:
		return item.Embedding == nil, nil
	case ArtifactLLMMetadata:
		var metadata map[string]any
		if len(item.Metadata) == 0 {
			metadata = map[string]any{}
		} else if json.Unmarshal(item.Metadata, &metadata) != nil {
			return false, fmt.Errorf("metadata evidence is unknown")
		}
		_, summary := metadata["summary"]
		_, points := metadata["key_points"]
		return !summary && !points, nil
	default:
		return false, fmt.Errorf("artifact is not admitted")
	}
}

func CreateApproved(db *gorm.DB, action models.MediaSupplyActionRequest) (models.ArtifactCoverageRequest, error) {
	d, ok := DescriptorForAction(action.ActionKey)
	if !ok || action.TargetType != "content_item" || action.State != models.MediaSupplyActionRequestQueued {
		return models.ArtifactCoverageRequest{}, fmt.Errorf("approved artifact action is invalid")
	}
	allowed, _, err := supply.MayExecuteSupplyAction(db, action.TenantID, action.ActionKey)
	if err != nil || !allowed {
		return models.ArtifactCoverageRequest{}, fmt.Errorf("artifact action is disabled")
	}
	var item models.ContentItem
	if err := db.Where("public_id=? AND tenant_id=?", action.TargetID, action.TenantID).First(&item).Error; err != nil {
		return models.ArtifactCoverageRequest{}, err
	}
	missing, err := Missing(item, d.Artifact)
	if err != nil || !missing || item.Status == models.ContentStatusArchived {
		return models.ArtifactCoverageRequest{}, fmt.Errorf("artifact target is not currently eligible")
	}
	unit, amount, err := artifactBudget(item, d.Artifact)
	if err != nil {
		return models.ArtifactCoverageRequest{}, err
	}
	if err := artifactNativeBudgetAvailable(db, action.TenantID, d.Artifact); err != nil {
		return models.ArtifactCoverageRequest{}, err
	}
	evidence := digest("artifact-evidence/v1", action.TenantID, item.PublicID.String(), item.UpdatedAt.UTC().Format(time.RFC3339Nano), d.Artifact)
	idem := digest("artifact-request/v1", action.TenantID, action.PublicID.String(), evidence)
	now := time.Now().UTC()
	request := models.ArtifactCoverageRequest{PublicID: uuid.New(), TenantID: action.TenantID, ContentItemID: item.PublicID, ItemUpdatedAt: item.UpdatedAt, Artifact: d.Artifact, Owner: d.Owner, State: "queued", ActionRequestID: &action.PublicID, ApprovedBy: action.ApprovedBy, ApprovedAt: &now, EvidenceDigest: evidence, InputDigest: digest("artifact-input/v1", evidence, d.Artifact), IdempotencyKey: idem, AcceptanceProof: jsonValue(map[string]any{}), TerminalProof: jsonValue(map[string]any{}), AffectedSubjects: jsonValue([]map[string]string{{"type": "content_item", "id": item.PublicID.String()}}), DeepLinks: jsonValue([]string{"/platform/media/circulation"})}
	err = db.Transaction(func(tx *gorm.DB) error {
		result := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "tenant_id"}, {Name: "idempotency_key"}}, DoNothing: true}).Create(&request)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return tx.Where("tenant_id=? AND idempotency_key=?", action.TenantID, idem).First(&request).Error
		}
		reservation := models.ArtifactCoverageBudgetReservation{PublicID: uuid.New(), TenantID: action.TenantID, RequestID: request.PublicID, ActionKey: d.ActionKey, Unit: unit, ReservedAmount: amount, State: "reserved", EvidenceDigest: evidence, ReservedAt: now}
		if err := tx.Create(&reservation).Error; err != nil {
			return err
		}
		return appendEvent(tx, request, nil, "queued", map[string]any{"artifact": d.Artifact, "owner": d.Owner, "evidence_digest": evidence})
	})
	return request, err
}

func ClaimNext(db *gorm.DB, owner string) (Claim, bool, error) {
	if owner != MediaOwner && owner != EnrichmentOwner {
		return Claim{}, false, fmt.Errorf("invalid artifact owner")
	}
	now := time.Now().UTC()
	if !supply.SupplyActionOwnerReady(owner, now) {
		return Claim{}, false, fmt.Errorf("artifact coverage owner readiness is unavailable")
	}
	var claim Claim
	err := db.Transaction(func(tx *gorm.DB) error {
		var queued []models.ArtifactCoverageRequest
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).Where("owner=? AND state=?", owner, "queued").Order("created_at").Limit(16).Find(&queued).Error; err != nil {
			return err
		}
		for _, r := range queued {
			d, ok := DescriptorForArtifact(r.Artifact)
			if !ok || d.Owner != owner {
				continue
			}
			allowed, _, e := supply.MayExecuteSupplyAction(tx, r.TenantID, d.ActionKey)
			if e != nil {
				return e
			}
			if !allowed {
				continue
			}
			if e = artifactNativeBudgetAvailable(tx, r.TenantID, r.Artifact); e != nil {
				continue
			}
			var reservation models.ArtifactCoverageBudgetReservation
			if e = tx.Where("tenant_id=? AND request_id=? AND state=?", r.TenantID, r.PublicID, "reserved").First(&reservation).Error; e != nil {
				continue
			}
			var item models.ContentItem
			if e = tx.Where("public_id=? AND tenant_id=? AND updated_at=?", r.ContentItemID, r.TenantID, r.ItemUpdatedAt).First(&item).Error; e != nil {
				continue
			}
			missing, e := Missing(item, r.Artifact)
			if e != nil || !missing {
				continue
			}
			var count int64
			if e = tx.Model(&models.ArtifactCoverageAttempt{}).Where("tenant_id=? AND request_id=?", r.TenantID, r.PublicID).Count(&count).Error; e != nil {
				return e
			}
			token, fence := uuid.New(), uuid.New()
			expires := now.Add(leaseDuration)
			attempt := models.ArtifactCoverageAttempt{}
			if count > 0 {
				_ = tx.Where("tenant_id=? AND request_id=?", r.TenantID, r.PublicID).Order("attempt_number DESC").First(&attempt).Error
			}
			adopting := attempt.PublicID != uuid.Nil && attempt.State == "claimed" && attempt.EffectStartedAt == nil && !attempt.LeaseExpiresAt.After(now)
			if !adopting && count >= maxAttempts {
				proof := jsonValue(map[string]any{"evidence": "absent", "reason": "attempt_budget_exhausted", "verified_at": now.Format(time.RFC3339Nano)})
				if e = tx.Model(&r).Updates(map[string]any{"state": "failed", "failure_class": "attempt_budget_exhausted", "terminal_proof": proof, "finished_at": now}).Error; e != nil {
					return e
				}
				if e = settleArtifactBudget(tx, r, "released"); e != nil {
					return e
				}
				if e = terminalizeLinkedAction(tx, r, false, proof, "attempt_budget_exhausted"); e != nil {
					return e
				}
				if e = appendEvent(tx, r, nil, "failed", map[string]any{"failure_class": "attempt_budget_exhausted"}); e != nil {
					return e
				}
				continue
			}
			if adopting {
				fence = attempt.FenceToken
				if e = tx.Model(&attempt).Updates(map[string]any{"claim_token": token, "lease_expires_at": expires, "heartbeat_at": now, "adoption_count": gorm.Expr("adoption_count + 1"), "last_adopted_at": now}).Error; e != nil {
					return e
				}
				attempt.ClaimToken, attempt.LeaseExpiresAt, attempt.HeartbeatAt, attempt.LastAdoptedAt, attempt.AdoptionCount = token, expires, now, &now, attempt.AdoptionCount+1
				if e = adoptLinkedAction(tx, r, attempt, token, expires); e != nil {
					return e
				}
			} else {
				attempt = models.ArtifactCoverageAttempt{PublicID: uuid.New(), TenantID: r.TenantID, RequestID: r.PublicID, AttemptNumber: int(count) + 1, Owner: owner, State: "claimed", ClaimToken: token, FenceToken: fence, InputDigest: r.InputDigest, DeterministicJobID: "artifact:" + r.IdempotencyKey, LeaseExpiresAt: expires, HeartbeatAt: now}
				if e = tx.Create(&attempt).Error; e != nil {
					return e
				}
				if e = claimLinkedAction(tx, r, attempt, token, expires); e != nil {
					return e
				}
			}
			if e = tx.Model(&r).Updates(map[string]any{"state": "claimed", "claim_owner": owner, "claim_token": token, "fence_token": fence, "claim_epoch": gorm.Expr("claim_epoch+1"), "claim_expires_at": expires}).Error; e != nil {
				return e
			}
			r.State = "claimed"
			r.ClaimOwner = owner
			r.ClaimToken = &token
			r.FenceToken = &fence
			r.ClaimEpoch++
			r.ClaimExpiresAt = &expires
			claim = Claim{Request: r, Attempt: attempt, ClaimToken: token, Item: item}
			eventType := "claimed"
			if adopting {
				eventType = "claim_adopted"
			}
			return appendEvent(tx, r, &attempt, eventType, map[string]any{"owner": owner, "attempt": attempt.AttemptNumber, "deterministic_job_id": attempt.DeterministicJobID})
		}
		return nil
	})
	if err != nil || claim.Request.PublicID == uuid.Nil {
		return claim, false, err
	}
	if accessErr := recheckArtifactActionAccess(db, claim.Request.PublicID.String()); accessErr != nil {
		return Claim{}, false, accessErr
	}
	return claim, true, nil
}

func Begin(db *gorm.DB, id, owner string, token uuid.UUID) (models.ArtifactCoverageAttempt, error) {
	if !supply.SupplyActionOwnerReady(owner, time.Now().UTC()) {
		return models.ArtifactCoverageAttempt{}, fmt.Errorf("artifact coverage owner readiness is unavailable")
	}
	if err := recheckArtifactActionAccess(db, id); err != nil {
		return models.ArtifactCoverageAttempt{}, err
	}
	return updateLease(db, id, owner, token, true)
}
func Heartbeat(db *gorm.DB, id, owner string, token uuid.UUID) (models.ArtifactCoverageAttempt, error) {
	if !supply.SupplyActionOwnerReady(owner, time.Now().UTC()) {
		return models.ArtifactCoverageAttempt{}, fmt.Errorf("artifact coverage owner readiness is unavailable")
	}
	if err := recheckArtifactActionAccess(db, id); err != nil {
		return models.ArtifactCoverageAttempt{}, err
	}
	return updateLease(db, id, owner, token, false)
}

func recheckArtifactActionAccess(db *gorm.DB, requestID string) error {
	var request models.ArtifactCoverageRequest
	if err := db.Select("tenant_id", "action_request_id").Where("public_id = ?", requestID).First(&request).Error; err != nil {
		return err
	}
	if request.ActionRequestID == nil {
		return fmt.Errorf("artifact recovery has no signed supply action")
	}
	var action models.MediaSupplyActionRequest
	if err := db.Where("tenant_id = ? AND public_id = ?", request.TenantID, *request.ActionRequestID).First(&action).Error; err != nil {
		return err
	}
	if err := supply.RecheckSupplyActionAccess(context.Background(), action); err != nil {
		return err
	}
	return supply.RecheckSupplyActionExecutionAuthority(db, action)
}
func updateLease(db *gorm.DB, id, owner string, token uuid.UUID, begin bool) (models.ArtifactCoverageAttempt, error) {
	var out models.ArtifactCoverageAttempt
	now := time.Now().UTC()
	err := db.Transaction(func(tx *gorm.DB) error {
		var r models.ArtifactCoverageRequest
		if e := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id=? AND owner=? AND claim_token=?", id, owner, token).First(&r).Error; e != nil {
			return e
		}
		if r.CancellationRequestedAt != nil || r.ClaimExpiresAt == nil || !r.ClaimExpiresAt.After(now) {
			return fmt.Errorf("artifact claim is stale or cancelled")
		}
		d, _ := DescriptorForArtifact(r.Artifact)
		allowed, _, e := supply.MayExecuteSupplyAction(tx, r.TenantID, d.ActionKey)
		if e != nil || !allowed {
			return fmt.Errorf("artifact action is disabled")
		}
		if e = tx.Where("tenant_id=? AND request_id=? AND claim_token=?", r.TenantID, r.PublicID, token).Order("attempt_number DESC").First(&out).Error; e != nil {
			return e
		}
		expires := now.Add(leaseDuration)
		updates := map[string]any{"lease_expires_at": expires, "heartbeat_at": now}
		requestUpdates := map[string]any{"claim_expires_at": expires}
		if begin {
			if r.State != "claimed" {
				return fmt.Errorf("artifact request cannot begin")
			}
			var item models.ContentItem
			if e = tx.Where("tenant_id=? AND public_id=? AND updated_at=? AND status<>?", r.TenantID, r.ContentItemID, r.ItemUpdatedAt, models.ContentStatusArchived).First(&item).Error; e != nil {
				return fmt.Errorf("artifact target changed before effect: %w", e)
			}
			missing, missingErr := Missing(item, r.Artifact)
			if missingErr != nil || !missing {
				return fmt.Errorf("artifact target is no longer missing before effect")
			}
			if e = artifactNativeBudgetAvailable(tx, r.TenantID, r.Artifact); e != nil {
				return e
			}
			var reservation models.ArtifactCoverageBudgetReservation
			if e = tx.Where("tenant_id=? AND request_id=? AND state=?", r.TenantID, r.PublicID, "reserved").First(&reservation).Error; e != nil {
				return fmt.Errorf("artifact workload reservation is unavailable: %w", e)
			}
			updates["state"] = "running"
			updates["effect_started_at"] = now
			requestUpdates["state"] = "running"
			requestUpdates["effect_started_at"] = now
		}
		if e = tx.Model(&out).Updates(updates).Error; e != nil {
			return e
		}
		if e = tx.Model(&r).Updates(requestUpdates).Error; e != nil {
			return e
		}
		if e = updateLinkedActionLease(tx, r, out, token, expires, begin, now); e != nil {
			return e
		}
		if begin {
			return appendEvent(tx, r, &out, "effect_started", map[string]any{"artifact": r.Artifact})
		}
		return nil
	})
	return out, err
}

func MarkAccepted(db *gorm.DB, id, owner string, token uuid.UUID, proof map[string]any) error {
	now := time.Now().UTC()
	return db.Transaction(func(tx *gorm.DB) error {
		var r models.ArtifactCoverageRequest
		if e := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id=? AND owner=? AND claim_token=? AND state=?", id, owner, token, "running").First(&r).Error; e != nil {
			return e
		}
		var a models.ArtifactCoverageAttempt
		if e := tx.Where("tenant_id=? AND request_id=? AND claim_token=?", r.TenantID, r.PublicID, token).Order("attempt_number DESC").First(&a).Error; e != nil {
			return e
		}
		if e := tx.Model(&r).Updates(map[string]any{"state": "verifying", "accepted_at": now, "acceptance_proof": jsonValue(proof), "claim_expires_at": nil}).Error; e != nil {
			return e
		}
		if e := tx.Model(&a).Updates(map[string]any{"state": "verifying", "accepted_at": now}).Error; e != nil {
			return e
		}
		if e := markLinkedActionVerifying(tx, r, a, proof, now); e != nil {
			return e
		}
		return appendEvent(tx, r, &a, "owner_accepted", proof)
	})
}

// MarkUncertain records a failed/lost owner acknowledgement after Begin. It
// deliberately preserves the same attempt/fence and routes to verification;
// it never returns the request to the executable queue.
func MarkUncertain(db *gorm.DB, id, owner string, token uuid.UUID, failureClass string) error {
	now := time.Now().UTC()
	failureClass = strings.TrimSpace(failureClass)
	if failureClass == "" || len(failureClass) > 64 {
		failureClass = "owner_effect_uncertain"
	}
	return db.Transaction(func(tx *gorm.DB) error {
		var r models.ArtifactCoverageRequest
		if e := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id=? AND owner=? AND claim_token=? AND state=?", id, owner, token, "running").First(&r).Error; e != nil {
			return e
		}
		var a models.ArtifactCoverageAttempt
		if e := tx.Where("tenant_id=? AND request_id=? AND claim_token=?", r.TenantID, r.PublicID, token).Order("attempt_number DESC").First(&a).Error; e != nil {
			return e
		}
		if e := tx.Model(&r).Updates(map[string]any{"state": "uncertain", "failure_class": failureClass, "claim_expires_at": nil}).Error; e != nil {
			return e
		}
		if e := tx.Model(&a).Updates(map[string]any{"state": "uncertain", "finished_at": now}).Error; e != nil {
			return e
		}
		if e := settleArtifactBudget(tx, r, "retained_uncertain"); e != nil {
			return e
		}
		if e := recoverLinkedAction(tx, r, "uncertain", now); e != nil {
			return e
		}
		return appendEvent(tx, r, &a, "owner_effect_uncertain", map[string]any{"failure_class": failureClass})
	})
}

func AuthorizeWriteback(db *gorm.DB, itemID uuid.UUID, owner, artifact string, c Correlation) (models.ArtifactCoverageRequest, models.ArtifactCoverageAttempt, error) {
	var r models.ArtifactCoverageRequest
	var a models.ArtifactCoverageAttempt
	requestID, e := uuid.Parse(c.RequestID)
	if e != nil {
		return r, a, fmt.Errorf("invalid artifact request correlation")
	}
	attemptID, e := uuid.Parse(c.AttemptID)
	if e != nil {
		return r, a, fmt.Errorf("invalid artifact attempt correlation")
	}
	token, e := uuid.Parse(c.ClaimToken)
	if e != nil {
		return r, a, e
	}
	fence, e := uuid.Parse(c.FenceToken)
	if e != nil {
		return r, a, e
	}
	e = db.Where("public_id=? AND content_item_id=? AND owner=? AND artifact=? AND input_digest=? AND state IN ?", requestID, itemID, owner, artifact, c.InputDigest, []string{"running", "verifying", "uncertain"}).First(&r).Error
	if e != nil {
		return r, a, e
	}
	var current models.ContentItem
	if e = db.Select("public_id", "tenant_id", "updated_at").Where("public_id=? AND tenant_id=? AND updated_at=?", itemID, r.TenantID, r.ItemUpdatedAt).First(&current).Error; e != nil {
		return r, a, fmt.Errorf("artifact target version changed: %w", e)
	}
	e = db.Where("public_id=? AND tenant_id=? AND request_id=? AND claim_token=? AND fence_token=?", attemptID, r.TenantID, r.PublicID, token, fence).First(&a).Error
	if e != nil {
		return r, a, e
	}
	if strings.TrimSpace(c.ProducerEventID) == "" {
		return r, a, fmt.Errorf("producer event identity is required")
	}
	var duplicate int64
	if e = db.Model(&models.ArtifactCoverageReceipt{}).Where("tenant_id = ? AND owner = ? AND producer_event_id = ?", r.TenantID, owner, c.ProducerEventID).Count(&duplicate).Error; e != nil {
		return r, a, e
	}
	if duplicate != 0 {
		return r, a, fmt.Errorf("artifact producer event was already persisted")
	}
	return r, a, nil
}

func RecordPersistence(db *gorm.DB, r models.ArtifactCoverageRequest, a models.ArtifactCoverageAttempt, c Correlation, payload map[string]any) error {
	payloadBytes := jsonValue(payload)
	receipt := models.ArtifactCoverageReceipt{PublicID: uuid.New(), TenantID: r.TenantID, RequestID: r.PublicID, AttemptID: a.PublicID, Owner: r.Owner, Artifact: r.Artifact, ProducerEventID: c.ProducerEventID, FenceToken: a.FenceToken, Outcome: "persisted", PayloadDigest: digest("artifact-receipt/v1", string(payloadBytes)), ObservedAt: time.Now().UTC(), Payload: payloadBytes}
	result := db.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "tenant_id"}, {Name: "owner"}, {Name: "producer_event_id"}}, DoNothing: true}).Create(&receipt)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return nil
	}
	return appendEvent(db, r, &a, "artifact_persisted", map[string]any{"producer_event_id": c.ProducerEventID, "payload_digest": receipt.PayloadDigest})
}

func VerifyOne(db *gorm.DB) (bool, error) {
	var r models.ArtifactCoverageRequest
	e := db.Where("state IN ?", []string{"verifying", "uncertain"}).Order("updated_at").First(&r).Error
	if e == gorm.ErrRecordNotFound {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	var item models.ContentItem
	if e = db.Where("public_id=? AND tenant_id=?", r.ContentItemID, r.TenantID).First(&item).Error; e != nil {
		return true, e
	}
	missing, e := Missing(item, r.Artifact)
	if e != nil {
		return true, e
	}
	var receipts int64
	if e = db.Model(&models.ArtifactCoverageReceipt{}).Where("tenant_id=? AND request_id=? AND outcome=?", r.TenantID, r.PublicID, "persisted").Count(&receipts).Error; e != nil {
		return true, e
	}
	if missing || receipts == 0 {
		started := r.AcceptedAt
		if started == nil {
			started = r.EffectStartedAt
		}
		if started == nil || time.Since(started.UTC()) < verificationDeadline {
			return true, nil
		}
		now := time.Now().UTC()
		failure := "artifact_absent_after_owner_acceptance"
		if r.State == "uncertain" {
			failure = "artifact_absent_after_uncertain_effect"
		}
		proof := jsonValue(map[string]any{"evidence": "absent", "artifact": r.Artifact, "verified_at": now.Format(time.RFC3339Nano), "receipt_present": receipts > 0})
		e = db.Transaction(func(tx *gorm.DB) error {
			if e := tx.Model(&models.ArtifactCoverageRequest{}).Where("public_id=? AND state IN ?", r.PublicID, []string{"verifying", "uncertain"}).Updates(map[string]any{"state": "failed", "failure_class": failure, "terminal_proof": proof, "finished_at": now}).Error; e != nil {
				return e
			}
			if e := tx.Model(&models.ArtifactCoverageAttempt{}).Where("tenant_id=? AND request_id=?", r.TenantID, r.PublicID).Order("attempt_number DESC").Updates(map[string]any{"state": "failed", "finished_at": now}).Error; e != nil {
				return e
			}
			budgetState := "released"
			if r.EffectStartedAt != nil {
				budgetState = "retained_uncertain"
			}
			if e := settleArtifactBudget(tx, r, budgetState); e != nil {
				return e
			}
			if e := terminalizeLinkedAction(tx, r, false, proof, failure); e != nil {
				return e
			}
			return appendEvent(tx, r, nil, "verification_absent", map[string]any{"failure_class": failure})
		})
		return true, e
	}
	now := time.Now().UTC()
	proof := jsonValue(map[string]any{"evidence": "present", "artifact": r.Artifact, "verified_at": now.Format(time.RFC3339Nano)})
	e = db.Transaction(func(tx *gorm.DB) error {
		if e := tx.Model(&models.ArtifactCoverageRequest{}).Where("public_id=? AND state IN ?", r.PublicID, []string{"verifying", "uncertain"}).Updates(map[string]any{"state": "succeeded", "verified_at": now, "finished_at": now, "terminal_proof": proof}).Error; e != nil {
			return e
		}
		if e := tx.Model(&models.ArtifactCoverageAttempt{}).Where("tenant_id=? AND request_id=?", r.TenantID, r.PublicID).Order("attempt_number DESC").Updates(map[string]any{"state": "succeeded", "finished_at": now}).Error; e != nil {
			return e
		}
		if e := settleArtifactBudget(tx, r, "settled"); e != nil {
			return e
		}
		if e := terminalizeLinkedAction(tx, r, true, proof, ""); e != nil {
			return e
		}
		return appendEvent(tx, r, nil, "verified", map[string]any{"artifact": r.Artifact})
	})
	return true, e
}

func RecoverExpired(db *gorm.DB) error {
	now := time.Now().UTC()
	return db.Transaction(func(tx *gorm.DB) error {
		var rows []models.ArtifactCoverageRequest
		if e := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).Where("state IN ? AND claim_expires_at<?", []string{"claimed", "running"}, now).Find(&rows).Error; e != nil {
			return e
		}
		for _, r := range rows {
			next := "queued"
			if r.State == "running" {
				next = "uncertain"
			}
			if e := tx.Model(&r).Updates(map[string]any{"state": next, "claim_token": nil, "fence_token": nil, "claim_owner": "", "claim_expires_at": nil}).Error; e != nil {
				return e
			}
			if e := recoverLinkedAction(tx, r, next, now); e != nil {
				return e
			}
			if next == "uncertain" {
				if e := settleArtifactBudget(tx, r, "retained_uncertain"); e != nil {
					return e
				}
			}
			if e := appendEvent(tx, r, nil, "claim_expired", map[string]any{"next": next}); e != nil {
				return e
			}
		}
		return nil
	})
}

func Cancel(db *gorm.DB, tenant, id, actor string) (models.ArtifactCoverageRequest, error) {
	var r models.ArtifactCoverageRequest
	now := time.Now().UTC()
	e := db.Transaction(func(tx *gorm.DB) error {
		if e := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id=? AND tenant_id=?", id, tenant).First(&r).Error; e != nil {
			return e
		}
		switch r.State {
		case "queued", "claimed":
			r.State = "cancelled"
			r.FinishedAt = &now
			if e := settleArtifactBudget(tx, r, "released"); e != nil {
				return e
			}
		case "running", "verifying", "uncertain":
			r.CancellationRequestedAt = &now
		default:
			return fmt.Errorf("artifact request is terminal")
		}
		if e := tx.Save(&r).Error; e != nil {
			return e
		}
		return appendEvent(tx, r, nil, "cancellation_requested", map[string]any{"actor": actor})
	})
	return r, e
}

// CancelByAction applies cancellation to the exact artifact ledger linked to
// one signed Supply action. It never accepts an item, artifact, owner, or
// provider argument from the browser.
func CancelByAction(db *gorm.DB, tenant string, actionID uuid.UUID, actor string) error {
	var request models.ArtifactCoverageRequest
	err := db.Where("tenant_id = ? AND action_request_id = ?", tenant, actionID).First(&request).Error
	if err == gorm.ErrRecordNotFound {
		return nil
	}
	if err != nil {
		return err
	}
	_, err = Cancel(db, tenant, request.PublicID.String(), actor)
	return err
}

func claimLinkedAction(tx *gorm.DB, request models.ArtifactCoverageRequest, attempt models.ArtifactCoverageAttempt, token uuid.UUID, expires time.Time) error {
	if request.ActionRequestID == nil {
		return nil
	}
	var action models.MediaSupplyActionRequest
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id = ? AND public_id = ? AND state = ?", request.TenantID, *request.ActionRequestID, models.MediaSupplyActionRequestQueued).First(&action).Error; err != nil {
		return fmt.Errorf("linked artifact action cannot be claimed: %w", err)
	}
	linked := models.MediaSupplyActionAttempt{PublicID: uuid.New(), TenantID: request.TenantID, ActionRequestID: action.PublicID, AttemptNumber: attempt.AttemptNumber, State: models.MediaSupplyActionRequestClaimed, FenceToken: attempt.FenceToken, OwnerProtocol: "artifact-coverage/v1:" + attempt.PublicID.String()}
	if err := tx.Create(&linked).Error; err != nil {
		return err
	}
	if err := tx.Model(&action).Updates(map[string]any{"state": models.MediaSupplyActionRequestClaimed, "claim_owner": "artifact:" + request.Owner, "claim_token": token, "claim_epoch": gorm.Expr("claim_epoch + 1"), "claim_expires_at": expires}).Error; err != nil {
		return err
	}
	action.State, action.ClaimOwner, action.ClaimToken, action.ClaimExpiresAt = models.MediaSupplyActionRequestClaimed, "artifact:"+request.Owner, &token, &expires
	return appendLinkedActionEvent(tx, action, &linked, "artifact_claimed", map[string]any{"artifact_request_id": request.PublicID.String(), "owner": request.Owner})
}

func adoptLinkedAction(tx *gorm.DB, request models.ArtifactCoverageRequest, attempt models.ArtifactCoverageAttempt, token uuid.UUID, expires time.Time) error {
	if request.ActionRequestID == nil {
		return nil
	}
	action, linked, err := linkedActionAttempt(tx, request, attempt)
	if err != nil {
		return err
	}
	if action.State != models.MediaSupplyActionRequestQueued || linked.State != models.MediaSupplyActionRequestClaimed || linked.EffectStartedAt != nil {
		return fmt.Errorf("linked artifact action is not adoptable")
	}
	if err := tx.Model(&action).Updates(map[string]any{
		"state":            models.MediaSupplyActionRequestClaimed,
		"claim_owner":      "artifact:" + request.Owner,
		"claim_token":      token,
		"claim_epoch":      gorm.Expr("claim_epoch + 1"),
		"claim_expires_at": expires,
		"failure_class":    "",
	}).Error; err != nil {
		return err
	}
	action.State, action.ClaimOwner, action.ClaimToken, action.ClaimExpiresAt = models.MediaSupplyActionRequestClaimed, "artifact:"+request.Owner, &token, &expires
	return appendLinkedActionEvent(tx, action, &linked, "artifact_claim_adopted", map[string]any{
		"artifact_request_id":  request.PublicID.String(),
		"artifact_attempt_id":  attempt.PublicID.String(),
		"deterministic_job_id": attempt.DeterministicJobID,
	})
}

func linkedActionAttempt(tx *gorm.DB, request models.ArtifactCoverageRequest, attempt models.ArtifactCoverageAttempt) (models.MediaSupplyActionRequest, models.MediaSupplyActionAttempt, error) {
	if request.ActionRequestID == nil {
		return models.MediaSupplyActionRequest{}, models.MediaSupplyActionAttempt{}, nil
	}
	var action models.MediaSupplyActionRequest
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id = ? AND public_id = ?", request.TenantID, *request.ActionRequestID).First(&action).Error; err != nil {
		return action, models.MediaSupplyActionAttempt{}, err
	}
	var linked models.MediaSupplyActionAttempt
	if err := tx.Where("tenant_id = ? AND action_request_id = ? AND owner_protocol = ?", request.TenantID, action.PublicID, "artifact-coverage/v1:"+attempt.PublicID.String()).First(&linked).Error; err != nil {
		return action, linked, err
	}
	return action, linked, nil
}

func updateLinkedActionLease(tx *gorm.DB, request models.ArtifactCoverageRequest, attempt models.ArtifactCoverageAttempt, token uuid.UUID, expires time.Time, begin bool, now time.Time) error {
	if request.ActionRequestID == nil {
		return nil
	}
	action, linked, err := linkedActionAttempt(tx, request, attempt)
	if err != nil {
		return err
	}
	if action.ClaimToken == nil || *action.ClaimToken != token || action.CancellationRequestedAt != nil {
		return fmt.Errorf("linked artifact action claim is stale or cancelled")
	}
	if !begin {
		return tx.Model(&action).Update("claim_expires_at", expires).Error
	}
	if action.State != models.MediaSupplyActionRequestClaimed || linked.State != models.MediaSupplyActionRequestClaimed {
		return fmt.Errorf("linked artifact action cannot begin")
	}
	if err := tx.Model(&action).Updates(map[string]any{"state": models.MediaSupplyActionRequestRunning, "claim_expires_at": expires}).Error; err != nil {
		return err
	}
	if err := tx.Model(&linked).Updates(map[string]any{"state": models.MediaSupplyActionRequestRunning, "started_at": now, "effect_started_at": now}).Error; err != nil {
		return err
	}
	action.State, linked.State, linked.StartedAt, linked.EffectStartedAt = models.MediaSupplyActionRequestRunning, models.MediaSupplyActionRequestRunning, &now, &now
	return appendLinkedActionEvent(tx, action, &linked, "artifact_effect_started", map[string]any{"artifact_request_id": request.PublicID.String()})
}

func markLinkedActionVerifying(tx *gorm.DB, request models.ArtifactCoverageRequest, attempt models.ArtifactCoverageAttempt, proof map[string]any, now time.Time) error {
	if request.ActionRequestID == nil {
		return nil
	}
	action, linked, err := linkedActionAttempt(tx, request, attempt)
	if err != nil {
		return err
	}
	after := jsonValue(map[string]any{"schema_version": "media-supply-action-effects/v1", "artifact_request_id": request.PublicID.String(), "owner_acceptance": proof, "verification": "pending"})
	if err := tx.Model(&action).Updates(map[string]any{"state": models.MediaSupplyActionRequestVerifying, "after_effects": after, "claim_expires_at": nil}).Error; err != nil {
		return err
	}
	if err := tx.Model(&linked).Updates(map[string]any{"state": models.MediaSupplyActionRequestVerifying}).Error; err != nil {
		return err
	}
	action.State, linked.State = models.MediaSupplyActionRequestVerifying, models.MediaSupplyActionRequestVerifying
	return appendLinkedActionEvent(tx, action, &linked, "artifact_verification_pending", map[string]any{"accepted_at": now.Format(time.RFC3339Nano)})
}

func terminalizeLinkedAction(tx *gorm.DB, request models.ArtifactCoverageRequest, succeeded bool, proof datatypes.JSON, failureClass string) error {
	if request.ActionRequestID == nil {
		return nil
	}
	now := time.Now().UTC()
	state := models.MediaSupplyActionRequestSucceeded
	if !succeeded {
		state = models.MediaSupplyActionRequestFailed
	}
	var action models.MediaSupplyActionRequest
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id = ? AND public_id = ?", request.TenantID, *request.ActionRequestID).First(&action).Error; err != nil {
		return err
	}
	if err := tx.Model(&action).Updates(map[string]any{"state": state, "verified_effects": supply.VerifiedSupplyActionEffects(action, proof), "failure_class": failureClass, "finished_at": now, "claim_expires_at": nil}).Error; err != nil {
		return err
	}
	if !succeeded {
		if err := supply.DemoteBoundSupplyPromotion(tx, action, "artifact_verifier_failed"); err != nil {
			return err
		}
	}
	verdict := supply.VerdictAbsent
	if succeeded {
		verdict = supply.VerdictPresent
	}
	supply.RecordSupplyVerifierQualificationCaseBestEffort(tx, action, succeeded, verdict, false)
	var linked models.MediaSupplyActionAttempt
	if err := tx.Where("tenant_id = ? AND action_request_id = ?", request.TenantID, action.PublicID).Order("attempt_number DESC").First(&linked).Error; err == nil {
		if err := tx.Model(&linked).Updates(map[string]any{"state": state, "finished_at": now}).Error; err != nil {
			return err
		}
	}
	action.State, action.FinishedAt = state, &now
	return appendLinkedActionEvent(tx, action, nil, "artifact_"+state, map[string]any{"artifact_request_id": request.PublicID.String()})
}

func recoverLinkedAction(tx *gorm.DB, request models.ArtifactCoverageRequest, next string, now time.Time) error {
	if request.ActionRequestID == nil {
		return nil
	}
	var action models.MediaSupplyActionRequest
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id = ? AND public_id = ?", request.TenantID, *request.ActionRequestID).First(&action).Error; err != nil {
		return err
	}
	actionState := models.MediaSupplyActionRequestQueued
	failure := ""
	if next == "uncertain" {
		actionState = models.MediaSupplyActionRequestUncertain
		failure = "artifact_owner_acknowledgement_lost"
	}
	if err := tx.Model(&action).Updates(map[string]any{"state": actionState, "claim_owner": "", "claim_token": nil, "claim_expires_at": nil, "failure_class": failure}).Error; err != nil {
		return err
	}
	var linked models.MediaSupplyActionAttempt
	if err := tx.Where("tenant_id = ? AND action_request_id = ?", request.TenantID, action.PublicID).Order("attempt_number DESC").First(&linked).Error; err == nil {
		if next == "uncertain" {
			if err := tx.Model(&linked).Updates(map[string]any{"state": models.MediaSupplyActionRequestUncertain, "finished_at": now}).Error; err != nil {
				return err
			}
		}
	}
	action.State = actionState
	return appendLinkedActionEvent(tx, action, nil, "artifact_claim_recovered", map[string]any{"next": actionState})
}

func settleArtifactBudget(tx *gorm.DB, request models.ArtifactCoverageRequest, state string) error {
	if state != "settled" && state != "retained_uncertain" && state != "released" {
		return fmt.Errorf("invalid artifact budget settlement")
	}
	now := time.Now().UTC()
	updates := map[string]any{"state": state, "settled_at": now}
	if state == "settled" || state == "retained_uncertain" {
		updates["settled_amount"] = gorm.Expr("reserved_amount")
	} else {
		updates["settled_amount"] = 0
	}
	result := tx.Model(&models.ArtifactCoverageBudgetReservation{}).
		Where("tenant_id=? AND request_id=? AND state IN ?", request.TenantID, request.PublicID, []string{"reserved", "retained_uncertain"}).
		Updates(updates)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		var existing models.ArtifactCoverageBudgetReservation
		if err := tx.Where("tenant_id=? AND request_id=? AND state=?", request.TenantID, request.PublicID, state).First(&existing).Error; err != nil {
			return fmt.Errorf("artifact workload reservation cannot settle: %w", err)
		}
	}
	return nil
}

func appendLinkedActionEvent(tx *gorm.DB, action models.MediaSupplyActionRequest, attempt *models.MediaSupplyActionAttempt, eventType string, payload map[string]any) error {
	var sequence int64
	if err := tx.Model(&models.MediaSupplyActionEvent{}).Where("tenant_id = ? AND action_request_id = ?", action.TenantID, action.PublicID).Select("COALESCE(MAX(sequence), 0)").Scan(&sequence).Error; err != nil {
		return err
	}
	bytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	sequence++
	event := models.MediaSupplyActionEvent{PublicID: uuid.New(), TenantID: action.TenantID, ActionRequestID: action.PublicID, Sequence: sequence, EventType: eventType, Payload: datatypes.JSON(bytes), OccurredAt: time.Now().UTC()}
	if attempt != nil {
		event.AttemptID = &attempt.PublicID
	}
	event.EventKey = "artifact-action:" + digest(action.PublicID.String(), fmt.Sprintf("%d", sequence), eventType, string(bytes))
	return tx.Create(&event).Error
}

func appendEvent(db *gorm.DB, r models.ArtifactCoverageRequest, a *models.ArtifactCoverageAttempt, key string, payload map[string]any) error {
	var n int64
	if e := db.Model(&models.ArtifactCoverageEvent{}).Where("tenant_id=? AND request_id=?", r.TenantID, r.PublicID).Count(&n).Error; e != nil {
		return e
	}
	return db.Create(&models.ArtifactCoverageEvent{PublicID: uuid.New(), TenantID: r.TenantID, RequestID: r.PublicID, Sequence: n + 1, EventType: key, Payload: jsonValue(payload), OccurredAt: time.Now().UTC()}).Error
}
func digest(values ...string) string {
	h := sha256.Sum256([]byte(strings.Join(values, "\n")))
	return hex.EncodeToString(h[:])
}
func jsonValue(v any) datatypes.JSON { b, _ := json.Marshal(v); return datatypes.JSON(b) }
