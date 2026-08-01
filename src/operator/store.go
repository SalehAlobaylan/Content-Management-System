package operator

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"content-management-system/src/models"

	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type PlanStore struct {
	db         *gorm.DB
	signingKey []byte
	now        func() time.Time
}

func NewPlanStore(db *gorm.DB, signingKey []byte) *PlanStore {
	return &PlanStore{db: db, signingKey: append([]byte(nil), signingKey...), now: func() time.Time { return time.Now().UTC() }}
}

func (store *PlanStore) CreatePlan(ctx context.Context, plan CanonicalPlan, expiresAt time.Time) (models.OperatorActionPlan, error) {
	if expiresAt.Before(store.now()) {
		return models.OperatorActionPlan{}, fmt.Errorf("%w: plan expiry must be in the future", ErrInvalidContract)
	}
	if err := plan.Validate(); err != nil {
		return models.OperatorActionPlan{}, err
	}
	digest, signature, err := SignCanonicalPlan(store.signingKey, plan)
	if err != nil {
		return models.OperatorActionPlan{}, err
	}
	payload, err := json.Marshal(plan)
	if err != nil {
		return models.OperatorActionPlan{}, fmt.Errorf("marshal plan payload: %w", err)
	}
	stored := models.OperatorActionPlan{
		TenantID: plan.TenantID, ActorID: plan.ActorID, ToolKey: plan.ToolKey, ToolVersion: plan.ToolVersion,
		State: "awaiting_approval", RiskTier: string(plan.RiskTier), CanonicalPlan: datatypes.JSON(payload),
		EvidenceFingerprint: plan.EvidenceFingerprint, AccessVersion: plan.AccessVersion, Digest: digest, Signature: signature,
		IdempotencyKey: plan.PlanID, ExpiresAt: expiresAt,
	}
	err = store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&stored).Error; err != nil {
			return err
		}
		targets, err := json.Marshal(plan.TargetIDs)
		if err != nil {
			return fmt.Errorf("marshal action targets: %w", err)
		}
		arguments, err := json.Marshal(plan.NormalizedArguments)
		if err != nil {
			return fmt.Errorf("marshal action arguments: %w", err)
		}
		branch, err := json.Marshal(map[string]any{"cancellation": plan.Cancellation, "rollback": plan.Rollback, "contingencies": plan.Contingencies})
		if err != nil {
			return fmt.Errorf("marshal action lifecycle contract: %w", err)
		}
		step := models.OperatorActionStep{PlanID: stored.ID, TenantID: stored.TenantID, Ordinal: 1, State: "pending", ToolKey: stored.ToolKey, Targets: datatypes.JSON(targets), Arguments: datatypes.JSON(arguments), Branch: datatypes.JSON(branch)}
		if err := tx.Create(&step).Error; err != nil {
			return err
		}
		return appendPlanEvent(tx, stored.ID, nil, stored.TenantID, 1, "plan_created", "admin", stored.ActorID, map[string]any{"tool_key": stored.ToolKey, "digest": stored.Digest})
	})
	return stored, err
}

// DecodeStoredPlan verifies the signed CMS plan before any approval, claim, or
// executor may rely on it. It makes database contents fail closed: a row that
// is malformed, forged, or no longer agrees with its registered descriptor is
// not an executable plan.
func (store *PlanStore) DecodeStoredPlan(stored models.OperatorActionPlan) (CanonicalPlan, ToolDescriptor, error) {
	var plan CanonicalPlan
	if err := json.Unmarshal(stored.CanonicalPlan, &plan); err != nil {
		return CanonicalPlan{}, ToolDescriptor{}, fmt.Errorf("%w: stored plan cannot be decoded", ErrInvalidContract)
	}
	if err := VerifyCanonicalPlanSignature(store.signingKey, plan, stored.Digest, stored.Signature); err != nil {
		return CanonicalPlan{}, ToolDescriptor{}, err
	}
	descriptor, ok := DefaultToolCatalog().Lookup(plan.ToolKey)
	if !ok || descriptor.Validate() != nil || plan.ToolVersion != descriptor.Version || plan.Cancellation != descriptor.Cancellation || plan.Rollback != descriptor.Rollback || !sameStrings(plan.Contingencies, descriptor.Contingencies) || stored.ToolKey != plan.ToolKey || stored.ToolVersion != plan.ToolVersion || stored.TenantID != plan.TenantID || stored.ActorID != plan.ActorID || stored.EvidenceFingerprint != plan.EvidenceFingerprint || stored.AccessVersion != plan.AccessVersion || stored.IdempotencyKey != plan.PlanID || stored.RiskTier != string(plan.RiskTier) {
		return CanonicalPlan{}, ToolDescriptor{}, fmt.Errorf("%w: stored plan metadata does not match its signed envelope", ErrInvalidContract)
	}
	return plan, descriptor, nil
}

// ApprovePlan performs the identity/access/digest recheck in the same
// transaction as the append-only approval event and state transition.
func (store *PlanStore) ApprovePlan(ctx context.Context, planID uint, snapshot AccessSnapshot, actorID, confirmationProofHash string) (models.OperatorActionPlan, error) {
	if err := snapshot.ValidateFor(actorID, snapshot.TenantID); err != nil {
		return models.OperatorActionPlan{}, err
	}
	if confirmationProofHash == "" {
		return models.OperatorActionPlan{}, fmt.Errorf("%w: confirmation proof is required", ErrInvalidContract)
	}
	var plan models.OperatorActionPlan
	now := store.now()
	err := store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&plan, planID).Error; err != nil {
			return err
		}
		canonical, descriptor, err := store.DecodeStoredPlan(plan)
		if err != nil {
			return err
		}
		if err := snapshot.ValidateFor(actorID, plan.TenantID); err != nil || !snapshot.HasPermission(descriptor.RequiredPermission) {
			return fmt.Errorf("%w: current approval authority is insufficient", ErrAccessUnavailable)
		}
		if err := EnsureToolCapabilityEnabled(tx, plan.TenantID, descriptor.Key, now); err != nil {
			return err
		}
		if plan.State != "awaiting_approval" || !now.Before(plan.ExpiresAt) || plan.ActorID != actorID || plan.TenantID != snapshot.TenantID || plan.AccessVersion != snapshot.AccessVersion || canonical.ActorID != actorID {
			return fmt.Errorf("%w: plan approval preconditions changed", ErrInvalidContract)
		}
		tier := plan.RiskTier
		if tier != string(RiskRoutine) && tier != string(RiskHigh) {
			return fmt.Errorf("%w: plan has invalid approval tier", ErrInvalidContract)
		}
		approval := models.OperatorPlanApproval{PlanID: plan.ID, TenantID: plan.TenantID, ActorID: actorID, AccessVersion: snapshot.AccessVersion, PlanDigest: plan.Digest, ConfirmationTier: tier, ConfirmationProofHash: confirmationProofHash, ExpiresAt: plan.ExpiresAt}
		if err := tx.Create(&approval).Error; err != nil {
			return err
		}
		job := models.OperatorActionJob{PlanID: plan.ID, TenantID: plan.TenantID, State: "queued", AvailableAt: now}
		if err := tx.Create(&job).Error; err != nil {
			return err
		}
		if err := tx.Model(&plan).Updates(map[string]any{"state": "queued", "approved_at": now}).Error; err != nil {
			return err
		}
		plan.State, plan.ApprovedAt = "queued", &now
		if err := appendNextPlanEvent(tx, plan.ID, nil, plan.TenantID, "plan_approved", "admin", actorID, map[string]any{"approval_id": approval.PublicID.String(), "tier": tier}); err != nil {
			return err
		}
		return appendNextPlanEvent(tx, plan.ID, nil, plan.TenantID, "plan_queued", "system", "", map[string]any{"job_id": job.PublicID.String()})
	})
	return plan, err
}

func sameStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

// LoadPlanForActor exposes only an owned plan and its verified canonical
// preview. It deliberately excludes the HMAC signature and confirmation proof.
func (store *PlanStore) LoadPlanForActor(ctx context.Context, publicID string, tenantID string, actorID string) (models.OperatorActionPlan, CanonicalPlan, error) {
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(actorID) == "" || strings.TrimSpace(publicID) == "" {
		return models.OperatorActionPlan{}, CanonicalPlan{}, fmt.Errorf("%w: plan identity is required", ErrInvalidContract)
	}
	var stored models.OperatorActionPlan
	if err := store.db.WithContext(ctx).Where("public_id=? AND tenant_id=? AND actor_id=?", publicID, tenantID, actorID).First(&stored).Error; err != nil {
		return models.OperatorActionPlan{}, CanonicalPlan{}, err
	}
	plan, _, err := store.DecodeStoredPlan(stored)
	return stored, plan, err
}

func appendNextPlanEvent(tx *gorm.DB, planID uint, stepID *uint, tenantID, eventType, actorType, actorID string, payload map[string]any) error {
	var sequence int64
	if err := tx.Model(&models.OperatorPlanEvent{}).Where("plan_id=?", planID).Select("COALESCE(MAX(sequence), 0)").Scan(&sequence).Error; err != nil {
		return err
	}
	return appendPlanEvent(tx, planID, stepID, tenantID, sequence+1, eventType, actorType, actorID, payload)
}

func appendPlanEvent(tx *gorm.DB, planID uint, stepID *uint, tenantID string, sequence int64, eventType, actorType, actorID string, payload map[string]any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	event := models.OperatorPlanEvent{PlanID: planID, StepID: stepID, TenantID: tenantID, Sequence: sequence, EventType: eventType, ActorType: actorType, ActorID: actorID, Payload: datatypes.JSON(raw)}
	return tx.Create(&event).Error
}
