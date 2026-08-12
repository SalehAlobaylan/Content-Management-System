package supply

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"content-management-system/src/models"

	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const supplyActionPreviewTTL = 15 * time.Minute
const supplyActionClaimLease = 45 * time.Second

const (
	SupplyExecutionApprovalRequired = "approval_required"
	SupplyExecutionSafeAuto         = "safe_auto"
)

type CreateSupplyActionPreviewInput struct {
	TenantID, ActionKey, TargetType, EvidenceDigest, PolicyDigest, CreatedBy, CreatedAccessVersion string
	TargetID                                                                                       uuid.UUID
	PreflightEvidence, PlannedEffects, AffectedSubjects, DeepLinks                                 datatypes.JSON
	ExpiresAt                                                                                      time.Time
}

// CreateSupplyActionPreview persists only server-derived evidence. Controllers
// must perform action-specific target/state preflight before calling it; this
// store checks the invariant that a request can never name an unregistered
// action or exceed the static one-target contract.
func CreateSupplyActionPreview(db *gorm.DB, input CreateSupplyActionPreviewInput) (models.MediaSupplyActionPreview, error) {
	if db == nil || strings.TrimSpace(input.TenantID) == "" || input.TargetID == uuid.Nil || strings.TrimSpace(input.CreatedBy) == "" || strings.TrimSpace(input.CreatedAccessVersion) == "" || !isDigest(input.EvidenceDigest) || !isDigest(input.PolicyDigest) {
		return models.MediaSupplyActionPreview{}, fmt.Errorf("media supply action preview identity or evidence is invalid")
	}
	descriptor, err := RequireSupplyActionDescriptor(input.ActionKey, input.TargetType)
	if err != nil || descriptor.TargetCap != 1 {
		return models.MediaSupplyActionPreview{}, fmt.Errorf("media supply action preview is not admitted")
	}
	allowed, controlKey, err := MayExecuteSupplyAction(db, input.TenantID, descriptor.Key)
	if err != nil {
		return models.MediaSupplyActionPreview{}, fmt.Errorf("media supply action control could not be checked: %w", err)
	}
	if !allowed {
		return models.MediaSupplyActionPreview{}, fmt.Errorf("media supply action is disabled by %s", controlKey)
	}
	now := time.Now().UTC()
	expires := input.ExpiresAt.UTC()
	if expires.IsZero() {
		expires = now.Add(supplyActionPreviewTTL)
	}
	if !expires.After(now) || expires.After(now.Add(supplyActionPreviewTTL)) {
		return models.MediaSupplyActionPreview{}, fmt.Errorf("media supply action preview expiry is outside its bounded window")
	}
	preview := models.MediaSupplyActionPreview{
		PublicID: uuid.New(), TenantID: strings.TrimSpace(input.TenantID), ActionKey: descriptor.Key, ActionVersion: "v1",
		TargetType: descriptor.TargetType, TargetID: input.TargetID, EvidenceDigest: strings.ToLower(input.EvidenceDigest),
		PolicyDigest: strings.ToLower(input.PolicyDigest), PreflightEvidence: actionJSON(input.PreflightEvidence, `{}`),
		PlannedEffects: actionJSON(input.PlannedEffects, `{}`), AffectedSubjects: actionJSON(input.AffectedSubjects, `[]`),
		DeepLinks: actionJSON(input.DeepLinks, `[]`), State: models.MediaSupplyActionPreviewActive, ExpiresAt: expires,
		CreatedBy: strings.TrimSpace(input.CreatedBy), CreatedAccessVersion: strings.TrimSpace(input.CreatedAccessVersion),
		ExecutionMode: SupplyExecutionApprovalRequired,
	}
	if err := db.Create(&preview).Error; err != nil {
		return models.MediaSupplyActionPreview{}, err
	}
	return preview, nil
}

// ApproveSupplyActionPreview atomically binds one actor confirmation to the
// immutable preview and queues one durable request. Repeated approval returns
// the already-created request; it never allocates a second attempt.
func ApproveSupplyActionPreview(db *gorm.DB, tenantID, previewID, actorID, approvalAccessVersion, confirmationEvidenceDigest string) (models.MediaSupplyActionRequest, bool, error) {
	if db == nil || strings.TrimSpace(tenantID) == "" || strings.TrimSpace(actorID) == "" || strings.TrimSpace(approvalAccessVersion) == "" || !isDigest(confirmationEvidenceDigest) {
		return models.MediaSupplyActionRequest{}, false, fmt.Errorf("media supply action approval identity or evidence is invalid")
	}
	previewPublicID, err := uuid.Parse(strings.TrimSpace(previewID))
	if err != nil {
		return models.MediaSupplyActionRequest{}, false, fmt.Errorf("media supply action preview ID is invalid")
	}
	var request models.MediaSupplyActionRequest
	created := false
	err = db.Transaction(func(tx *gorm.DB) error {
		var preview models.MediaSupplyActionPreview
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id = ? AND tenant_id = ?", previewPublicID, tenantID).First(&preview).Error; err != nil {
			return err
		}
		if requestErr := tx.Where("preview_id = ? AND tenant_id = ?", preview.PublicID, tenantID).First(&request).Error; requestErr == nil {
			return nil
		} else if requestErr != gorm.ErrRecordNotFound {
			return requestErr
		}
		now := time.Now().UTC()
		if preview.State != models.MediaSupplyActionPreviewActive || !preview.ExpiresAt.After(now) || preview.CreatedBy != strings.TrimSpace(actorID) || preview.CreatedAccessVersion != strings.TrimSpace(approvalAccessVersion) || preview.EvidenceDigest != strings.ToLower(strings.TrimSpace(confirmationEvidenceDigest)) {
			return fmt.Errorf("media supply action approval preconditions changed")
		}
		descriptor, err := RequireSupplyActionDescriptor(preview.ActionKey, preview.TargetType)
		if err != nil {
			return err
		}
		allowed, controlKey, err := MayExecuteSupplyAction(tx, tenantID, descriptor.Key)
		if err != nil {
			return fmt.Errorf("media supply action control could not be checked: %w", err)
		}
		if !allowed {
			return fmt.Errorf("media supply action is disabled by %s", controlKey)
		}
		if !SupplyActionOwnerReady(descriptor.ExecutionOwner, now) {
			return fmt.Errorf("the %s Supply action owner is not ready to accept a new action", descriptor.ExecutionOwner)
		}
		idempotency := supplyActionIdempotency(preview)
		request = models.MediaSupplyActionRequest{
			PublicID: uuid.New(), TenantID: tenantID, PreviewID: preview.PublicID, ActionKey: descriptor.Key, ActionVersion: preview.ActionVersion,
			TargetType: preview.TargetType, TargetID: preview.TargetID, ExecutionOwner: descriptor.ExecutionOwner, IdempotencyKey: idempotency,
			ExecutionMode: SupplyExecutionApprovalRequired,
			State:         models.MediaSupplyActionRequestQueued, ApprovedBy: strings.TrimSpace(actorID), ApprovalEvidenceDigest: preview.EvidenceDigest, ApprovalAccessVersion: strings.TrimSpace(approvalAccessVersion),
			ApprovedAt: &now, PlannedEffects: preview.PlannedEffects, AffectedSubjects: preview.AffectedSubjects, DeepLinks: preview.DeepLinks,
		}
		if err := tx.Create(&request).Error; err != nil {
			return err
		}
		if result := tx.Model(&models.MediaSupplyActionPreview{}).Where("public_id = ? AND tenant_id = ? AND state = ?", preview.PublicID, tenantID, models.MediaSupplyActionPreviewActive).Update("state", models.MediaSupplyActionPreviewConsumed); result.Error != nil || result.RowsAffected != 1 {
			if result.Error != nil {
				return result.Error
			}
			return fmt.Errorf("media supply action preview changed while approving")
		}
		created = true
		return appendSupplyActionEvent(tx, request, nil, "approved_queued", map[string]any{"preview_id": preview.PublicID.String(), "action_key": descriptor.Key})
	})
	return request, created, err
}

func qualifiedSupplyActionIdempotency(preview models.MediaSupplyActionPreview, promotion models.MediaSupplyActionPromotion) string {
	digest := sha256.Sum256([]byte(strings.Join([]string{ContractVersion, "safe_auto", preview.TenantID, preview.ActionKey, preview.TargetID.String(), preview.EvidenceDigest, preview.PolicyDigest, promotion.PublicID.String()}, "\n")))
	return "media-supply-safe-auto:" + hex.EncodeToString(digest[:])
}

// QueueQualifiedSupplyAction is the only CMS-internal autonomous admission
// path. It accepts the same server-derived evidence as a manual preview but no
// execution mode or promotion identity from a client. CMS resolves and binds
// the current tenant/action promotion atomically; without it no row is made.
func QueueQualifiedSupplyAction(db *gorm.DB, input CreateSupplyActionPreviewInput) (models.MediaSupplyActionRequest, bool, error) {
	if db == nil || strings.TrimSpace(input.TenantID) == "" || input.TargetID == uuid.Nil || !isDigest(input.EvidenceDigest) || !isDigest(input.PolicyDigest) {
		return models.MediaSupplyActionRequest{}, false, fmt.Errorf("qualified Supply action identity or evidence is invalid")
	}
	descriptor, err := RequireSupplyActionDescriptor(input.ActionKey, input.TargetType)
	if err != nil || descriptor.TargetCap != 1 {
		return models.MediaSupplyActionRequest{}, false, fmt.Errorf("qualified Supply action is not admitted")
	}
	versions, ok := QualificationVersions(descriptor.Key)
	if !ok {
		return models.MediaSupplyActionRequest{}, false, fmt.Errorf("qualified Supply action versions are unavailable")
	}
	promotion, err := RequireActiveSupplyPromotion(db, input.TenantID, descriptor.Key, versions.ActionVersion, versions.AdapterVersion, versions.VerifierVersion, versions.SchemaVersion, versions.PolicyVersion)
	if err != nil {
		return models.MediaSupplyActionRequest{}, false, err
	}
	// Autonomous identity is never accepted from a caller. It is the human
	// promoter bound to the sealed report, and IAM must still report the exact
	// access version immediately before CMS admits work.
	access, err := CurrentSupplyActionAccess(context.Background(), promotion.PromotedBy, input.TenantID)
	if err != nil || access.AccessVersion != promotion.PromotedAccessVersion {
		_ = DemoteSupplyActionPromotion(context.Background(), db, input.TenantID, descriptor.Key, "cms:qualification-iam-guard", "promoter_authorization_changed", true)
		return models.MediaSupplyActionRequest{}, false, fmt.Errorf("qualified Supply action promoter authority changed")
	}
	now := time.Now().UTC()
	if !SupplyActionOwnerReady(descriptor.ExecutionOwner, now) {
		return models.MediaSupplyActionRequest{}, false, fmt.Errorf("qualified Supply action owner is not ready")
	}
	expires := input.ExpiresAt.UTC()
	if expires.IsZero() {
		expires = now.Add(supplyActionPreviewTTL)
	}
	if !expires.After(now) || expires.After(now.Add(supplyActionPreviewTTL)) {
		return models.MediaSupplyActionRequest{}, false, fmt.Errorf("qualified Supply evidence expiry is outside its bounded window")
	}
	preview := models.MediaSupplyActionPreview{
		PublicID: uuid.New(), TenantID: strings.TrimSpace(input.TenantID), ActionKey: descriptor.Key, ActionVersion: versions.ActionVersion,
		TargetType: descriptor.TargetType, TargetID: input.TargetID, EvidenceDigest: strings.ToLower(input.EvidenceDigest), PolicyDigest: strings.ToLower(input.PolicyDigest),
		PreflightEvidence: actionJSON(input.PreflightEvidence, `{}`), PlannedEffects: actionJSON(input.PlannedEffects, `{}`), AffectedSubjects: actionJSON(input.AffectedSubjects, `[]`), DeepLinks: actionJSON(input.DeepLinks, `[]`),
		State: models.MediaSupplyActionPreviewConsumed, ExpiresAt: expires, CreatedBy: promotion.PromotedBy, CreatedAccessVersion: promotion.PromotedAccessVersion,
		ExecutionMode: SupplyExecutionSafeAuto, PromotionID: &promotion.PublicID,
	}
	idempotency := qualifiedSupplyActionIdempotency(preview, promotion)
	request := models.MediaSupplyActionRequest{}
	created := false
	err = db.Transaction(func(tx *gorm.DB) error {
		// Re-resolve immediately inside the admission transaction so a demotion
		// cannot race the row creation.
		current, err := RequireActiveSupplyPromotion(tx, input.TenantID, descriptor.Key, versions.ActionVersion, versions.AdapterVersion, versions.VerifierVersion, versions.SchemaVersion, versions.PolicyVersion)
		if err != nil || current.PublicID != promotion.PublicID {
			return fmt.Errorf("qualified Supply action promotion changed during admission")
		}
		if err := tx.Where("tenant_id=? AND idempotency_key=?", preview.TenantID, idempotency).First(&request).Error; err == nil {
			return nil
		} else if err != gorm.ErrRecordNotFound {
			return err
		}
		if err := tx.Create(&preview).Error; err != nil {
			return err
		}
		request = models.MediaSupplyActionRequest{
			PublicID: uuid.New(), TenantID: preview.TenantID, PreviewID: preview.PublicID, ActionKey: descriptor.Key, ActionVersion: versions.ActionVersion,
			TargetType: preview.TargetType, TargetID: preview.TargetID, ExecutionOwner: descriptor.ExecutionOwner,
			ExecutionMode: SupplyExecutionSafeAuto, PromotionID: &current.PublicID, QualificationReportID: &current.ReportID,
			IdempotencyKey: idempotency, State: models.MediaSupplyActionRequestQueued, ApprovedBy: preview.CreatedBy,
			ApprovalEvidenceDigest: preview.EvidenceDigest, ApprovalAccessVersion: preview.CreatedAccessVersion, ApprovedAt: &now,
			PlannedEffects: preview.PlannedEffects, AffectedSubjects: preview.AffectedSubjects, DeepLinks: preview.DeepLinks,
		}
		result := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "tenant_id"}, {Name: "idempotency_key"}}, DoNothing: true}).Create(&request)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			created = false
			return tx.Where("tenant_id=? AND idempotency_key=?", preview.TenantID, idempotency).First(&request).Error
		}
		created = true
		return appendSupplyActionEvent(tx, request, nil, "qualified_queued", map[string]any{"promotion_id": current.PublicID.String(), "report_id": current.ReportID})
	})
	return request, created, err
}

func actionJSON(value datatypes.JSON, fallback string) datatypes.JSON {
	if len(value) == 0 || !json.Valid(value) {
		return datatypes.JSON([]byte(fallback))
	}
	return append(datatypes.JSON(nil), value...)
}

// VerifiedSupplyActionEffects is the only terminal refresh authority exposed
// to clients. It binds the verifier proof to the CMS-owned domains, subjects,
// and validated links frozen on the signed request.
func VerifiedSupplyActionEffects(request models.MediaSupplyActionRequest, proof datatypes.JSON) datatypes.JSON {
	var proofValue any = map[string]any{}
	if len(proof) > 0 {
		_ = json.Unmarshal(proof, &proofValue)
	}
	var subjects any = []any{}
	_ = json.Unmarshal(request.AffectedSubjects, &subjects)
	var links any = []string{}
	_ = json.Unmarshal(request.DeepLinks, &links)
	domains := request.AffectedDomains
	if len(domains) == 0 {
		if descriptor, ok := SupplyAction(request.ActionKey); ok {
			domains = append([]string(nil), descriptor.AffectedDomains...)
		}
	}
	bytes, _ := json.Marshal(map[string]any{
		"schema_version":    "media-supply-verified-effects/v1",
		"proof":             proofValue,
		"affected_domains":  domains,
		"affected_subjects": subjects,
		"deep_links":        links,
	})
	return datatypes.JSON(bytes)
}

func supplyActionIdempotency(preview models.MediaSupplyActionPreview) string {
	digest := sha256.Sum256([]byte(strings.Join([]string{ContractVersion, preview.TenantID, preview.PublicID.String(), preview.ActionKey, preview.TargetID.String(), preview.EvidenceDigest, preview.PolicyDigest}, "\n")))
	return "media-supply-action:" + hex.EncodeToString(digest[:])
}

func appendSupplyActionEvent(tx *gorm.DB, request models.MediaSupplyActionRequest, attempt *models.MediaSupplyActionAttempt, eventType string, payload map[string]any) error {
	if tx == nil || request.PublicID == uuid.Nil || strings.TrimSpace(eventType) == "" {
		return fmt.Errorf("media supply action event identity is incomplete")
	}
	bytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	var maxSequence int64
	if err := tx.Model(&models.MediaSupplyActionEvent{}).Where("tenant_id = ? AND action_request_id = ?", request.TenantID, request.PublicID).Select("COALESCE(MAX(sequence), 0)").Scan(&maxSequence).Error; err != nil {
		return err
	}
	event := models.MediaSupplyActionEvent{PublicID: uuid.New(), TenantID: request.TenantID, ActionRequestID: request.PublicID, Sequence: maxSequence + 1, EventType: eventType, Payload: datatypes.JSON(bytes), OccurredAt: time.Now().UTC()}
	if attempt != nil {
		event.AttemptID = &attempt.PublicID
	}
	digest := sha256.Sum256([]byte(strings.Join([]string{request.PublicID.String(), fmt.Sprintf("%d", event.Sequence), eventType, string(bytes)}, "\n")))
	event.EventKey = "media-supply-action:" + hex.EncodeToString(digest[:])
	return tx.Create(&event).Error
}

// SupplyActionLease fences one named worker from an immutable action request.
// Claiming does not start an owner effect; the adapter must call
// BeginSupplyActionEffect immediately before its deterministic native action.
type SupplyActionLease struct {
	Request    models.MediaSupplyActionRequest
	Attempt    models.MediaSupplyActionAttempt
	ClaimToken uuid.UUID
}

func ClaimNextSupplyAction(db *gorm.DB, owner string, leaseFor time.Duration) (SupplyActionLease, bool, error) {
	if db == nil || strings.TrimSpace(owner) == "" || leaseFor <= 0 || leaseFor > 5*time.Minute {
		return SupplyActionLease{}, false, fmt.Errorf("media supply action claim input is invalid")
	}
	now := time.Now().UTC()
	var lease SupplyActionLease
	err := db.Transaction(func(tx *gorm.DB) error {
		var request models.MediaSupplyActionRequest
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).Where("state = ?", models.MediaSupplyActionRequestQueued).Order("created_at ASC").First(&request).Error; err != nil {
			if err == gorm.ErrRecordNotFound {
				return nil
			}
			return err
		}
		descriptor, err := RequireSupplyActionDescriptor(request.ActionKey, request.TargetType)
		if err != nil || descriptor.ExecutionOwner != request.ExecutionOwner {
			return fmt.Errorf("media supply action request is no longer admitted")
		}
		if err := RecheckSupplyActionExecutionAuthority(tx, request); err != nil {
			return err
		}
		var attemptCount int64
		if err := tx.Model(&models.MediaSupplyActionAttempt{}).Where("tenant_id = ? AND action_request_id = ?", request.TenantID, request.PublicID).Count(&attemptCount).Error; err != nil {
			return err
		}
		// A completed/cancelled pre-effect claim is retained as audit evidence;
		// no request can receive an unbounded series of implicit retries.
		if attemptCount >= 2 {
			proof := datatypes.JSON([]byte(`{"schema_version":"media-supply-action-proof/v1","verified":"absent","reason":"attempt_budget_exhausted"}`))
			if err := tx.Model(&request).Updates(map[string]any{"state": string(SupplyActionFailed), "failure_class": "attempt_budget_exhausted", "verified_effects": proof, "finished_at": now, "claim_expires_at": nil}).Error; err != nil {
				return err
			}
			if err := appendSupplyActionEvent(tx, request, nil, "failed", map[string]any{"failure_class": "attempt_budget_exhausted"}); err != nil {
				return err
			}
			return nil
		}
		token, expires := uuid.New(), now.Add(leaseFor)
		attempt := models.MediaSupplyActionAttempt{PublicID: uuid.New(), TenantID: request.TenantID, ActionRequestID: request.PublicID, AttemptNumber: int(attemptCount) + 1, State: string(SupplyActionClaimed), FenceToken: uuid.New(), OwnerProtocol: descriptor.ExecutionOwner}
		if err := tx.Create(&attempt).Error; err != nil {
			return err
		}
		if err := tx.Model(&request).Updates(map[string]any{"state": string(SupplyActionClaimed), "claim_owner": strings.TrimSpace(owner), "claim_token": token, "claim_epoch": gorm.Expr("claim_epoch + 1"), "claim_expires_at": expires}).Error; err != nil {
			return err
		}
		request.State, request.ClaimOwner, request.ClaimToken, request.ClaimEpoch, request.ClaimExpiresAt = string(SupplyActionClaimed), strings.TrimSpace(owner), &token, request.ClaimEpoch+1, &expires
		lease = SupplyActionLease{Request: request, Attempt: attempt, ClaimToken: token}
		return appendSupplyActionEvent(tx, request, &attempt, "claimed", map[string]any{"owner": strings.TrimSpace(owner), "attempt": attempt.AttemptNumber})
	})
	if err != nil || lease.Request.PublicID == uuid.Nil {
		return lease, false, err
	}
	if accessErr := RecheckSupplyActionAccess(context.Background(), lease.Request); accessErr != nil {
		if transitionErr := TransitionSupplyActionForAccessFailure(db, lease.Request.TenantID, lease.Request.PublicID.String(), lease.ClaimToken.String(), accessErr); transitionErr != nil {
			return SupplyActionLease{}, false, transitionErr
		}
		return SupplyActionLease{}, false, accessErr
	}
	return lease, true, nil
}

// BeginSupplyActionEffect is the one-way effect boundary. Stale tokens and
// disabled/invalid request states fail closed before any native adapter work.
func BeginSupplyActionEffect(db *gorm.DB, tenantID, requestID, owner, claimToken string) (models.MediaSupplyActionAttempt, error) {
	return updateSupplyActionClaim(db, tenantID, requestID, owner, claimToken, true, 0)
}

func RenewSupplyActionClaim(db *gorm.DB, tenantID, requestID, owner, claimToken string, leaseFor time.Duration) (models.MediaSupplyActionAttempt, error) {
	return updateSupplyActionClaim(db, tenantID, requestID, owner, claimToken, false, leaseFor)
}

func updateSupplyActionClaim(db *gorm.DB, tenantID, requestID, owner, claimToken string, begin bool, leaseFor time.Duration) (models.MediaSupplyActionAttempt, error) {
	if db == nil || strings.TrimSpace(tenantID) == "" || strings.TrimSpace(owner) == "" || (!begin && (leaseFor <= 0 || leaseFor > 5*time.Minute)) {
		return models.MediaSupplyActionAttempt{}, fmt.Errorf("media supply action lease input is invalid")
	}
	publicID, err := uuid.Parse(strings.TrimSpace(requestID))
	if err != nil {
		return models.MediaSupplyActionAttempt{}, fmt.Errorf("media supply action request ID is invalid")
	}
	token, err := uuid.Parse(strings.TrimSpace(claimToken))
	if err != nil {
		return models.MediaSupplyActionAttempt{}, fmt.Errorf("media supply action claim token is invalid")
	}
	// The effect owner must obtain a current access snapshot immediately before
	// every meaningful step. If a previous effect may already be visible, the
	// only safe destination is verifier-led terminalization.
	if _, accessErr := recheckSupplyActionAccessByID(db, tenantID, requestID); accessErr != nil {
		if transitionErr := TransitionSupplyActionForAccessFailure(db, tenantID, requestID, token.String(), accessErr); transitionErr != nil {
			return models.MediaSupplyActionAttempt{}, transitionErr
		}
		return models.MediaSupplyActionAttempt{}, accessErr
	}
	now := time.Now().UTC()
	var attempt models.MediaSupplyActionAttempt
	err = db.Transaction(func(tx *gorm.DB) error {
		var request models.MediaSupplyActionRequest
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id = ? AND tenant_id = ?", publicID, tenantID).First(&request).Error; err != nil {
			return err
		}
		if request.ClaimOwner != owner || request.ClaimToken == nil || *request.ClaimToken != token || request.ClaimExpiresAt == nil || !request.ClaimExpiresAt.After(now) {
			return fmt.Errorf("media supply action claim is stale")
		}
		if err := tx.Where("tenant_id = ? AND action_request_id = ?", tenantID, request.PublicID).Order("attempt_number DESC").First(&attempt).Error; err != nil {
			return err
		}
		if begin {
			descriptor, err := RequireSupplyActionDescriptor(request.ActionKey, request.TargetType)
			if err != nil {
				return err
			}
			allowed, controlKey, err := MayExecuteSupplyAction(tx, tenantID, descriptor.Key)
			if err != nil {
				return fmt.Errorf("media supply action control could not be checked: %w", err)
			}
			if !allowed {
				return fmt.Errorf("media supply action is disabled by %s", controlKey)
			}
			if err := RecheckSupplyActionExecutionAuthority(tx, request); err != nil {
				return err
			}
			if request.State != string(SupplyActionClaimed) || attempt.State != string(SupplyActionClaimed) || request.CancellationRequestedAt != nil {
				return fmt.Errorf("media supply action cannot cross its effect boundary")
			}
			if err := tx.Model(&request).Updates(map[string]any{"state": string(SupplyActionRunning)}).Error; err != nil {
				return err
			}
			if err := tx.Model(&attempt).Updates(map[string]any{"state": string(SupplyActionRunning), "started_at": now, "effect_started_at": now}).Error; err != nil {
				return err
			}
			attempt.State, attempt.StartedAt, attempt.EffectStartedAt = string(SupplyActionRunning), &now, &now
			request.State = string(SupplyActionRunning)
			return appendSupplyActionEvent(tx, request, &attempt, "effect_started", map[string]any{"attempt": attempt.AttemptNumber})
		}
		expires := now.Add(leaseFor)
		if err := tx.Model(&request).Update("claim_expires_at", expires).Error; err != nil {
			return err
		}
		return nil
	})
	return attempt, err
}

// TransitionSupplyActionForAccessFailure keeps authorization loss and IAM
// outages fail-closed. Pre-effect work is terminally denied with proof that
// CMS did not start an effect; post-effect work moves to verification and is
// never replayed merely because access could not be rechecked.
func TransitionSupplyActionForAccessFailure(db *gorm.DB, tenantID, requestID, claimToken string, accessErr error) error {
	if db == nil || strings.TrimSpace(tenantID) == "" {
		return fmt.Errorf("media supply action authorization transition is invalid")
	}
	publicID, err := uuid.Parse(strings.TrimSpace(requestID))
	if err != nil {
		return fmt.Errorf("media supply action request ID is invalid")
	}
	token, err := uuid.Parse(strings.TrimSpace(claimToken))
	if err != nil {
		return fmt.Errorf("media supply action claim token is invalid")
	}
	return db.Transaction(func(tx *gorm.DB) error {
		var request models.MediaSupplyActionRequest
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id = ? AND tenant_id = ?", publicID, tenantID).First(&request).Error; err != nil {
			return err
		}
		if request.ClaimToken == nil || *request.ClaimToken != token || request.State == string(SupplyActionCancelled) || IsTerminalSupplyAction(SupplyActionRequestState(request.State)) {
			return nil
		}
		var attempt models.MediaSupplyActionAttempt
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id = ? AND action_request_id = ?", tenantID, request.PublicID).Order("attempt_number DESC").First(&attempt).Error; err != nil {
			return err
		}
		now := time.Now().UTC()
		if attempt.EffectStartedAt == nil {
			proof := datatypes.JSON([]byte(`{"schema_version":"media-supply-action-proof/v1","effect_started":false,"verified":"absent","reason":"authorization_recheck_failed"}`))
			if err := tx.Model(&request).Updates(map[string]any{"state": string(SupplyActionFailed), "failure_class": "authorization_recheck_failed", "verified_effects": VerifiedSupplyActionEffects(request, proof), "finished_at": now, "claim_expires_at": nil}).Error; err != nil {
				return err
			}
			if err := tx.Model(&attempt).Updates(map[string]any{"state": string(SupplyActionFailed), "finished_at": now}).Error; err != nil {
				return err
			}
			request.State, request.FailureClass, request.FinishedAt = string(SupplyActionFailed), "authorization_recheck_failed", &now
			attempt.State, attempt.FinishedAt = string(SupplyActionFailed), &now
			if err := DemoteBoundSupplyPromotion(tx, request, "authorization_uncertain"); err != nil {
				return err
			}
			RecordSupplyVerifierQualificationCaseBestEffort(tx, request, false, VerdictAbsent, false)
			return appendSupplyActionEvent(tx, request, &attempt, "authorization_recheck_failed_before_effect", map[string]any{"access_available": !isAccessRecheckFailure(accessErr)})
		}
		if err := tx.Model(&request).Updates(map[string]any{"state": string(SupplyActionVerifying), "failure_class": "authorization_recheck_failed_after_effect", "claim_expires_at": nil}).Error; err != nil {
			return err
		}
		if err := tx.Model(&attempt).Update("state", string(SupplyActionVerifying)).Error; err != nil {
			return err
		}
		request.State, request.FailureClass = string(SupplyActionVerifying), "authorization_recheck_failed_after_effect"
		attempt.State = string(SupplyActionVerifying)
		if err := DemoteBoundSupplyPromotion(tx, request, "authorization_uncertain_after_effect"); err != nil {
			return err
		}
		return appendSupplyActionEvent(tx, request, &attempt, "authorization_recheck_requires_verification", map[string]any{"access_available": !isAccessRecheckFailure(accessErr)})
	})
}

// CancelSupplyAction cancels work that is definitely pre-effect. Once an
// attempt crossed BeginSupplyActionEffect it can only enter verification; a
// cancellation request must never authorize replay or erase its evidence.
func CancelSupplyAction(db *gorm.DB, tenantID, requestID, actor string) (models.MediaSupplyActionRequest, error) {
	if db == nil || strings.TrimSpace(tenantID) == "" || strings.TrimSpace(actor) == "" {
		return models.MediaSupplyActionRequest{}, fmt.Errorf("media supply action cancellation identity is invalid")
	}
	publicID, err := uuid.Parse(strings.TrimSpace(requestID))
	if err != nil {
		return models.MediaSupplyActionRequest{}, fmt.Errorf("media supply action request ID is invalid")
	}
	var request models.MediaSupplyActionRequest
	err = db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id = ? AND tenant_id = ?", publicID, tenantID).First(&request).Error; err != nil {
			return err
		}
		if IsTerminalSupplyAction(SupplyActionRequestState(request.State)) {
			return nil
		}
		now := time.Now().UTC()
		var attempt models.MediaSupplyActionAttempt
		attemptErr := tx.Where("tenant_id = ? AND action_request_id = ?", tenantID, request.PublicID).Order("attempt_number DESC").First(&attempt).Error
		if attemptErr != nil && attemptErr != gorm.ErrRecordNotFound {
			return attemptErr
		}
		if attemptErr == gorm.ErrRecordNotFound || attempt.EffectStartedAt == nil {
			if err := tx.Model(&request).Updates(map[string]any{"state": string(SupplyActionCancelled), "cancellation_requested_at": now, "finished_at": now}).Error; err != nil {
				return err
			}
			request.State, request.CancellationRequestedAt, request.FinishedAt = string(SupplyActionCancelled), &now, &now
			if attemptErr == nil {
				if err := tx.Model(&attempt).Updates(map[string]any{"state": string(SupplyActionCancelled), "finished_at": now}).Error; err != nil {
					return err
				}
			}
			return appendSupplyActionEvent(tx, request, nil, "cancelled_before_effect", map[string]any{"actor": strings.TrimSpace(actor)})
		}
		if err := tx.Model(&request).Updates(map[string]any{"state": string(SupplyActionVerifying), "cancellation_requested_at": now}).Error; err != nil {
			return err
		}
		if err := tx.Model(&attempt).Update("state", string(SupplyActionVerifying)).Error; err != nil {
			return err
		}
		request.State, request.CancellationRequestedAt = string(SupplyActionVerifying), &now
		attempt.State = string(SupplyActionVerifying)
		return appendSupplyActionEvent(tx, request, &attempt, "cancellation_requires_verification", map[string]any{"actor": strings.TrimSpace(actor)})
	})
	return request, err
}

// RecoverExpiredSupplyActionClaims is a bounded, CMS-only convergence pass.
// It only requeues a claim proved not to have crossed an effect boundary. All
// started work becomes verifying/uncertain and awaits the named adapter's
// independent readback.
func RecoverExpiredSupplyActionClaims(db *gorm.DB, limit int) (int, error) {
	if db == nil || limit < 1 || limit > 1000 {
		return 0, fmt.Errorf("media supply action recovery requires a bounded batch")
	}
	now := time.Now().UTC()
	recovered := 0
	err := db.Transaction(func(tx *gorm.DB) error {
		var requests []models.MediaSupplyActionRequest
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).Where("state IN ? AND claim_expires_at <= ?", []string{string(SupplyActionClaimed), string(SupplyActionRunning)}, now).Order("claim_expires_at ASC").Limit(limit).Find(&requests).Error; err != nil {
			return err
		}
		for _, request := range requests {
			var attempt models.MediaSupplyActionAttempt
			if err := tx.Where("tenant_id = ? AND action_request_id = ?", request.TenantID, request.PublicID).Order("attempt_number DESC").First(&attempt).Error; err != nil {
				return err
			}
			if attempt.EffectStartedAt == nil {
				if err := tx.Model(&request).Updates(map[string]any{"state": string(SupplyActionQueued), "claim_owner": "", "claim_token": nil, "claim_expires_at": nil}).Error; err != nil {
					return err
				}
				if err := tx.Model(&attempt).Updates(map[string]any{"state": string(SupplyActionCancelled), "finished_at": now}).Error; err != nil {
					return err
				}
				request.State = string(SupplyActionQueued)
				attempt.State = string(SupplyActionCancelled)
				if err := appendSupplyActionEvent(tx, request, &attempt, "claim_recovered_before_effect", map[string]any{}); err != nil {
					return err
				}
			} else {
				if err := tx.Model(&request).Updates(map[string]any{"state": string(SupplyActionVerifying), "failure_class": "expired_claim_after_effect"}).Error; err != nil {
					return err
				}
				if err := tx.Model(&attempt).Update("state", string(SupplyActionVerifying)).Error; err != nil {
					return err
				}
				request.State = string(SupplyActionVerifying)
				attempt.State = string(SupplyActionVerifying)
				if err := appendSupplyActionEvent(tx, request, &attempt, "expired_claim_requires_verification", map[string]any{}); err != nil {
					return err
				}
			}
			recovered++
		}
		return nil
	})
	return recovered, err
}
