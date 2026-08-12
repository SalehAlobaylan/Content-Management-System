package supply

import (
	"fmt"
	"strings"
	"time"

	"content-management-system/src/models"

	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	verificationOwner                    = "cms-source-run-verifier"
	verificationVersion                  = "source-run-verifier/v1"
	podsDeliveryObservationRetry         = 5 * time.Minute
	podsDeliveryObservationAttemptBudget = 24
	podsDeliveryObservationItemLimit     = 512
	podsDeliveryObservationUnitLimit     = 512
)

// ChildUnitInput only permits the two dynamic expansion edges in the static
// source-run graph: coordinator -> fetch_page -> normalize_batch. It never
// accepts a queue name, URL, arbitrary method, or caller-provided job ID.
type ChildUnitInput struct {
	TenantID, RequestID, AttemptID, ParentUnitID string
	UnitType, UnitKey, PageID, BatchID           string
}

func (input ChildUnitInput) validate() error {
	for _, value := range []string{input.TenantID, input.RequestID, input.AttemptID, input.ParentUnitID, input.UnitType, input.UnitKey} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("source-run child unit is missing a required identity")
		}
	}
	if len(input.UnitKey) > 255 || len(input.PageID) > 128 || len(input.BatchID) > 128 {
		return fmt.Errorf("source-run child unit identity exceeds its bounded contract")
	}
	switch input.UnitType {
	case "fetch_page":
		if strings.TrimSpace(input.PageID) == "" || strings.TrimSpace(input.BatchID) != "" {
			return fmt.Errorf("fetch-page unit requires only a page ID")
		}
	case "normalize_batch":
		if strings.TrimSpace(input.PageID) == "" || strings.TrimSpace(input.BatchID) == "" {
			return fmt.Errorf("normalize-batch unit requires page and batch IDs")
		}
	default:
		return fmt.Errorf("source-run child unit type is not admitted")
	}
	return nil
}

// AuthorizeChildUnit atomically extends an open manifest and returns the
// existing unit on duplicate admission. It is the only dynamic child-unit
// authorizer; callers cannot construct provider-effecting work identities.
func AuthorizeChildUnit(db *gorm.DB, input ChildUnitInput) (models.SourceRunExecutionUnit, bool, error) {
	if db == nil {
		return models.SourceRunExecutionUnit{}, false, fmt.Errorf("source-run store requires a database")
	}
	if err := input.validate(); err != nil {
		return models.SourceRunExecutionUnit{}, false, err
	}
	requestID, err := uuid.Parse(strings.TrimSpace(input.RequestID))
	if err != nil {
		return models.SourceRunExecutionUnit{}, false, fmt.Errorf("source-run request ID is invalid")
	}
	attemptID, err := uuid.Parse(strings.TrimSpace(input.AttemptID))
	if err != nil {
		return models.SourceRunExecutionUnit{}, false, fmt.Errorf("source-run attempt ID is invalid")
	}
	parentID, err := uuid.Parse(strings.TrimSpace(input.ParentUnitID))
	if err != nil {
		return models.SourceRunExecutionUnit{}, false, fmt.Errorf("source-run parent execution-unit ID is invalid")
	}
	var unit models.SourceRunExecutionUnit
	created := false
	err = db.Transaction(func(tx *gorm.DB) error {
		var request models.SourceRunRequest
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id = ? AND tenant_id = ?", requestID, input.TenantID).First(&request).Error; err != nil {
			return err
		}
		if !CanAuthorizeManifestChild(ManifestState(request.ManifestState)) || IsTerminalRequest(RequestState(request.State)) {
			return fmt.Errorf("source-run manifest is not open for child authorization")
		}
		var attempt models.SourceRunAttempt
		if err := tx.Where("public_id = ? AND tenant_id = ? AND source_run_request_id = ?", attemptID, input.TenantID, requestID).First(&attempt).Error; err != nil {
			return err
		}
		if IsTerminalAttempt(AttemptState(attempt.State)) {
			return fmt.Errorf("terminal source-run attempt cannot receive a child unit")
		}
		var parent models.SourceRunExecutionUnit
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id = ? AND tenant_id = ? AND source_run_request_id = ? AND source_run_attempt_id = ?", parentID, input.TenantID, requestID, attemptID).First(&parent).Error; err != nil {
			return err
		}
		if !admittedChildEdge(parent.UnitType, input.UnitType) {
			return fmt.Errorf("source-run manifest edge %s -> %s is not admitted", parent.UnitType, input.UnitType)
		}
		if err := tx.Where("tenant_id = ? AND source_run_attempt_id = ? AND unit_key = ?", input.TenantID, attemptID, input.UnitKey).First(&unit).Error; err == nil {
			if unit.ParentUnitID == nil || *unit.ParentUnitID != parentID || unit.UnitType != input.UnitType || unit.PageID != input.PageID || unit.BatchID != input.BatchID {
				return fmt.Errorf("source-run child unit idempotency key conflicts with its immutable identity")
			}
			return nil
		} else if err != gorm.ErrRecordNotFound {
			return err
		}
		unitID := uuid.New()
		jobID, err := DeterministicUnitJobID(input.TenantID, requestID.String(), attemptID.String(), unitID.String(), attempt.FenceToken.String())
		if err != nil {
			return err
		}
		unit = models.SourceRunExecutionUnit{
			PublicID: unitID, TenantID: input.TenantID, SourceRunRequestID: requestID, SourceRunAttemptID: attemptID,
			ContentSourceID: request.ContentSourceID, ParentUnitID: &parentID, UnitType: input.UnitType, UnitKey: input.UnitKey,
			PageID: input.PageID, BatchID: input.BatchID, JobID: jobID, AttemptFenceToken: attempt.FenceToken, State: string(UnitAuthorized),
		}
		if err := tx.Create(&unit).Error; err != nil {
			return err
		}
		updates := map[string]any{"expected_unit_count": gorm.Expr("expected_unit_count + 1"), "manifest_version": gorm.Expr("manifest_version + 1")}
		if input.UnitType == "fetch_page" {
			updates["expected_page_count"] = gorm.Expr("expected_page_count + 1")
		}
		if input.UnitType == "normalize_batch" {
			updates["expected_batch_count"] = gorm.Expr("expected_batch_count + 1")
		}
		if err := tx.Model(&request).Updates(updates).Error; err != nil {
			return err
		}
		created = true
		return nil
	})
	return unit, created, err
}

func admittedChildEdge(parentType, childType string) bool {
	return (parentType == "coordinator" && childType == "fetch_page") || (parentType == "fetch_page" && childType == "normalize_batch")
}

// FreezeFetchPageChildren records the producer's finite page boundary before
// the request can seal. The digest is recomputed from CMS-authorized children
// during sealing, so a producer cannot claim work it did not receive.
func FreezeFetchPageChildren(db *gorm.DB, tenantID, pageUnitID string, declaredCount int, declaredDigest string) error {
	if db == nil || strings.TrimSpace(tenantID) == "" || declaredCount < 0 || !isDigest(declaredDigest) {
		return fmt.Errorf("valid tenant, page declaration, and child digest are required")
	}
	pageID, err := uuid.Parse(strings.TrimSpace(pageUnitID))
	if err != nil {
		return fmt.Errorf("source-run page unit ID is invalid")
	}
	return db.Transaction(func(tx *gorm.DB) error {
		var page models.SourceRunExecutionUnit
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id = ? AND tenant_id = ?", pageID, tenantID).First(&page).Error; err != nil {
			return err
		}
		if page.UnitType != "fetch_page" {
			return fmt.Errorf("only fetch-page units can freeze dynamic children")
		}
		var request models.SourceRunRequest
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id = ? AND tenant_id = ?", page.SourceRunRequestID, tenantID).First(&request).Error; err != nil {
			return err
		}
		if request.ManifestState != string(ManifestOpen) {
			return fmt.Errorf("source-run manifest is not open for page freezing")
		}
		if page.DeclaredChildDigest != "" && (page.DeclaredChildCount != declaredCount || page.DeclaredChildDigest != strings.ToLower(declaredDigest)) {
			return fmt.Errorf("source-run page children were already frozen with another declaration")
		}
		return tx.Model(&page).Updates(map[string]any{"declared_child_count": declaredCount, "declared_child_digest": strings.ToLower(declaredDigest)}).Error
	})
}

// SealManifest prevents all future dynamic authorizations after every page
// declaration agrees with the exact set of CMS-authorized normalize units.
func SealManifest(db *gorm.DB, tenantID, requestID string) (models.SourceRunRequest, error) {
	if db == nil || strings.TrimSpace(tenantID) == "" {
		return models.SourceRunRequest{}, fmt.Errorf("explicit tenant and database are required")
	}
	publicID, err := uuid.Parse(strings.TrimSpace(requestID))
	if err != nil {
		return models.SourceRunRequest{}, fmt.Errorf("source-run request ID is invalid")
	}
	var request models.SourceRunRequest
	err = db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id = ? AND tenant_id = ?", publicID, tenantID).First(&request).Error; err != nil {
			return err
		}
		if request.ManifestState == string(ManifestSealed) {
			return nil
		}
		if request.ManifestState != string(ManifestOpen) {
			return fmt.Errorf("source-run manifest is already sealing")
		}
		var pages []models.SourceRunExecutionUnit
		if err := tx.Where("tenant_id = ? AND source_run_request_id = ? AND unit_type = ?", tenantID, publicID, "fetch_page").Order("public_id ASC").Find(&pages).Error; err != nil {
			return err
		}
		for _, page := range pages {
			if !IsTerminalUnit(ExecutionUnitState(page.State)) {
				return fmt.Errorf("fetch-page %s has not reached a terminal state", page.PublicID)
			}
			if !isDigest(page.DeclaredChildDigest) {
				return fmt.Errorf("fetch-page %s has no sealed child declaration", page.PublicID)
			}
			var children []models.SourceRunExecutionUnit
			if err := tx.Select("unit_key").Where("tenant_id = ? AND parent_unit_id = ? AND unit_type = ?", tenantID, page.PublicID, "normalize_batch").Order("unit_key ASC").Find(&children).Error; err != nil {
				return err
			}
			keys := make([]string, 0, len(children))
			for _, child := range children {
				keys = append(keys, child.UnitKey)
			}
			digest, err := ManifestChildDigest(keys)
			if err != nil {
				return err
			}
			if page.DeclaredChildCount != len(keys) || page.DeclaredChildDigest != digest {
				return fmt.Errorf("fetch-page %s declaration does not match CMS-authorized children", page.PublicID)
			}
		}
		now := time.Now().UTC()
		result := tx.Model(&models.SourceRunRequest{}).Where("id = ? AND tenant_id = ? AND manifest_state = ? AND manifest_version = ?", request.ID, tenantID, string(ManifestOpen), request.ManifestVersion).Updates(map[string]any{
			"manifest_state": string(ManifestSealing), "manifest_version": gorm.Expr("manifest_version + 1"),
		})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return fmt.Errorf("source-run manifest changed while sealing")
		}
		result = tx.Model(&models.SourceRunRequest{}).Where("id = ? AND tenant_id = ? AND manifest_state = ? AND manifest_version = ?", request.ID, tenantID, string(ManifestSealing), request.ManifestVersion+1).Updates(map[string]any{
			"manifest_state": string(ManifestSealed), "manifest_version": gorm.Expr("manifest_version + 1"), "manifest_sealed_at": now,
		})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return fmt.Errorf("source-run manifest changed while finalizing its seal")
		}
		request.ManifestState, request.ManifestSealedAt, request.ManifestVersion = string(ManifestSealed), &now, request.ManifestVersion+2
		return nil
	})
	return request, err
}

type UnitLeaseInput struct {
	TenantID, UnitID, Owner, LeaseToken string
}

func (input UnitLeaseInput) validate(requireOwner bool) (uuid.UUID, uuid.UUID, error) {
	if strings.TrimSpace(input.TenantID) == "" || strings.TrimSpace(input.UnitID) == "" || strings.TrimSpace(input.LeaseToken) == "" || (requireOwner && strings.TrimSpace(input.Owner) == "") {
		return uuid.Nil, uuid.Nil, fmt.Errorf("source-run unit lease identity is incomplete")
	}
	unitID, err := uuid.Parse(input.UnitID)
	if err != nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("source-run execution-unit ID is invalid")
	}
	token, err := uuid.Parse(input.LeaseToken)
	if err != nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("source-run execution lease token is invalid")
	}
	return unitID, token, nil
}

// BeginUnitEffect is the second, fenced CAS immediately before an executor
// performs a side effect. A lease acquisition alone is never evidence that an
// effect began.
func BeginUnitEffect(db *gorm.DB, input UnitLeaseInput) (models.SourceRunExecutionUnit, error) {
	unitID, token, err := input.validate(true)
	if db == nil || err != nil {
		if err == nil {
			err = fmt.Errorf("source-run store requires a database")
		}
		return models.SourceRunExecutionUnit{}, err
	}
	now := time.Now().UTC()
	var unit models.SourceRunExecutionUnit
	err = db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id = ? AND tenant_id = ?", unitID, input.TenantID).First(&unit).Error; err != nil {
			return err
		}
		if err := validateCurrentUnitLease(unit, input.Owner, token, now); err != nil {
			return err
		}
		if unit.State != string(UnitAccepted) {
			return fmt.Errorf("source-run execution unit cannot begin an effect from %s", unit.State)
		}
		if err := tx.Model(&unit).Updates(map[string]any{"state": string(UnitRunning), "effect_started_at": now, "started_at": now, "heartbeat_at": now}).Error; err != nil {
			return err
		}
		if err := tx.Model(&models.SourceRunAttempt{}).Where("public_id = ? AND tenant_id = ? AND state IN ?", unit.SourceRunAttemptID, input.TenantID, []string{string(AttemptAuthorized), string(AttemptClaimed)}).Updates(map[string]any{"state": string(AttemptRunning), "started_at": now}).Error; err != nil {
			return err
		}
		return tx.Model(&models.SourceRunRequest{}).Where("public_id = ? AND tenant_id = ? AND state IN ?", unit.SourceRunRequestID, input.TenantID, []string{string(RequestRequested), string(RequestAccepted)}).Updates(map[string]any{"state": string(RequestRunning), "started_at": now}).Error
	})
	unit.State, unit.EffectStartedAt, unit.StartedAt, unit.HeartbeatAt = string(UnitRunning), &now, &now, &now
	return unit, err
}

func RenewUnitLease(db *gorm.DB, input UnitLeaseInput, leaseFor time.Duration) error {
	unitID, token, err := input.validate(true)
	if db == nil || err != nil || leaseFor <= 0 {
		if err == nil {
			err = fmt.Errorf("source-run lease duration must be positive")
		}
		return err
	}
	now, expires := time.Now().UTC(), time.Now().UTC().Add(leaseFor)
	result := db.Model(&models.SourceRunExecutionUnit{}).Where("public_id = ? AND tenant_id = ? AND execution_owner = ? AND execution_lease_token = ? AND execution_lease_expires_at > ? AND state IN ?", unitID, input.TenantID, input.Owner, token, now, []string{string(UnitAccepted), string(UnitRunning)}).Updates(map[string]any{"execution_lease_expires_at": expires, "heartbeat_at": now})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return fmt.Errorf("source-run execution-unit lease is no longer current")
	}
	return nil
}

// RenewAttemptClaim keeps dispatcher ownership bounded. It intentionally does
// not grant a new fence or revive a verification-required attempt.
func RenewAttemptClaim(db *gorm.DB, tenantID, attemptID, owner, dispatcherToken string, leaseFor time.Duration) error {
	if db == nil || strings.TrimSpace(tenantID) == "" || strings.TrimSpace(owner) == "" || leaseFor <= 0 {
		return fmt.Errorf("tenant, dispatcher owner, database, and positive lease are required")
	}
	publicID, err := uuid.Parse(strings.TrimSpace(attemptID))
	if err != nil {
		return fmt.Errorf("source-run attempt ID is invalid")
	}
	token, err := uuid.Parse(strings.TrimSpace(dispatcherToken))
	if err != nil {
		return fmt.Errorf("source-run dispatcher token is invalid")
	}
	now := time.Now().UTC()
	result := db.Model(&models.SourceRunAttempt{}).Where("public_id = ? AND tenant_id = ? AND dispatcher_owner = ? AND dispatcher_token = ? AND dispatcher_lease_expires_at > ? AND state IN ?", publicID, tenantID, owner, token, now, []string{string(AttemptClaimed), string(AttemptRunning)}).Updates(map[string]any{"dispatcher_lease_expires_at": now.Add(leaseFor), "heartbeat_at": now})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return fmt.Errorf("source-run dispatcher claim is no longer current")
	}
	return nil
}

// RequestUnitCancellation prevents a not-yet-started effect. Once an effect
// may have started, it deliberately moves to verification instead of claiming
// cancellation succeeded.
func RequestUnitCancellation(db *gorm.DB, tenantID, unitID string) (models.SourceRunExecutionUnit, error) {
	if db == nil || strings.TrimSpace(tenantID) == "" {
		return models.SourceRunExecutionUnit{}, fmt.Errorf("explicit tenant and database are required")
	}
	publicID, err := uuid.Parse(strings.TrimSpace(unitID))
	if err != nil {
		return models.SourceRunExecutionUnit{}, fmt.Errorf("source-run execution-unit ID is invalid")
	}
	now := time.Now().UTC()
	var unit models.SourceRunExecutionUnit
	err = db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id = ? AND tenant_id = ?", publicID, tenantID).First(&unit).Error; err != nil {
			return err
		}
		if IsTerminalUnit(ExecutionUnitState(unit.State)) {
			return nil
		}
		updates := map[string]any{"cancellation_requested_at": now}
		if unit.EffectStartedAt == nil {
			updates["state"], updates["terminal_outcome"], updates["finished_at"] = string(UnitCancelled), string(OutcomeCancelled), now
		} else {
			updates["state"], updates["verification_required"] = string(UnitVerificationRequired), true
		}
		if err := tx.Model(&unit).Updates(updates).Error; err != nil {
			return err
		}
		unit.CancellationRequestedAt = &now
		if unit.EffectStartedAt != nil {
			unit.State, unit.VerificationRequired = string(UnitVerificationRequired), true
			if _, err := ensureVerificationTask(tx, unit, "cancellation"); err != nil {
				return err
			}
		} else {
			unit.State, unit.TerminalOutcome, unit.FinishedAt = string(UnitCancelled), string(OutcomeCancelled), &now
		}
		return reconcileAttemptAndRequest(tx, tenantID, unit.SourceRunAttemptID, unit.SourceRunRequestID)
	})
	return unit, err
}

// ReapExpiredUnitLeases recovers expired ownership without replaying an
// uncertain effect. Units that crossed the effect boundary become durable
// verification tasks; pre-effect units expire safely.
func ReapExpiredUnitLeases(db *gorm.DB, tenantID string, limit int) ([]uuid.UUID, error) {
	if db == nil || strings.TrimSpace(tenantID) == "" || limit < 1 || limit > 1000 {
		return nil, fmt.Errorf("tenant and a bounded reap limit are required")
	}
	now := time.Now().UTC()
	var recovered []uuid.UUID
	err := db.Transaction(func(tx *gorm.DB) error {
		var units []models.SourceRunExecutionUnit
		// Coordinators never perform provider or ingest effects; they remain
		// supervisory until CMS seals their manifest. Reaping one as a normal
		// execution unit would manufacture a false partial result after healthy
		// child units complete.
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).Where("tenant_id = ? AND unit_type <> ? AND state IN ? AND execution_lease_expires_at <= ?", tenantID, "coordinator", []string{string(UnitAccepted), string(UnitRunning)}, now).Order("execution_lease_expires_at ASC").Limit(limit).Find(&units).Error; err != nil {
			return err
		}
		for _, unit := range units {
			if unit.EffectStartedAt == nil {
				if err := tx.Model(&unit).Updates(map[string]any{"state": string(UnitExpired), "terminal_outcome": string(OutcomeUnknown), "finished_at": now}).Error; err != nil {
					return err
				}
			} else {
				if err := tx.Model(&unit).Updates(map[string]any{"state": string(UnitVerificationRequired), "verification_required": true}).Error; err != nil {
					return err
				}
				if _, err := ensureVerificationTask(tx, unit, "expired_execution_lease"); err != nil {
					return err
				}
			}
			if err := reconcileAttemptAndRequest(tx, tenantID, unit.SourceRunAttemptID, unit.SourceRunRequestID); err != nil {
				return err
			}
			recovered = append(recovered, unit.PublicID)
		}
		return nil
	})
	return recovered, err
}

type VerificationLease struct {
	Task       models.SourceRunVerificationTask
	ClaimToken uuid.UUID
}

// ClaimVerificationTask grants a fenced verifier lease. Expired verification
// claims are safe to reclaim because verification observes, never repeats,
// the provider effect.
func ClaimVerificationTask(db *gorm.DB, tenantID, taskID, owner string, leaseFor time.Duration) (VerificationLease, error) {
	if db == nil || strings.TrimSpace(tenantID) == "" || strings.TrimSpace(owner) == "" || leaseFor <= 0 {
		return VerificationLease{}, fmt.Errorf("tenant, verifier owner, database, and positive lease are required")
	}
	publicID, err := uuid.Parse(strings.TrimSpace(taskID))
	if err != nil {
		return VerificationLease{}, fmt.Errorf("verification task ID is invalid")
	}
	now := time.Now().UTC()
	var lease VerificationLease
	err = db.Transaction(func(tx *gorm.DB) error {
		var task models.SourceRunVerificationTask
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id = ? AND tenant_id = ?", publicID, tenantID).First(&task).Error; err != nil {
			return err
		}
		if task.State == models.SourceRunVerificationTaskTerminal {
			return fmt.Errorf("verification task is terminal")
		}
		if task.NotBeforeAt != nil && task.NotBeforeAt.After(now) {
			return fmt.Errorf("verification task is not due")
		}
		if task.ClaimExpiresAt != nil && task.ClaimExpiresAt.After(now) && task.ClaimOwner != owner {
			return fmt.Errorf("verification task is already claimed")
		}
		token, expires := uuid.New(), now.Add(leaseFor)
		if err := tx.Model(&task).Updates(map[string]any{"state": models.SourceRunVerificationTaskClaimed, "claim_owner": owner, "claim_token": token, "claim_epoch": gorm.Expr("claim_epoch + 1"), "claim_expires_at": expires, "heartbeat_at": now, "attempt_count": gorm.Expr("attempt_count + 1")}).Error; err != nil {
			return err
		}
		task.State, task.ClaimOwner, task.ClaimToken, task.ClaimEpoch, task.ClaimExpiresAt, task.HeartbeatAt, task.AttemptCount = models.SourceRunVerificationTaskClaimed, owner, &token, task.ClaimEpoch+1, &expires, &now, task.AttemptCount+1
		lease = VerificationLease{Task: task, ClaimToken: token}
		return nil
	})
	return lease, err
}

func BeginVerification(db *gorm.DB, tenantID, taskID, owner, claimToken string) error {
	return updateVerificationLease(db, tenantID, taskID, owner, claimToken, 0, true)
}

func RenewVerificationLease(db *gorm.DB, tenantID, taskID, owner, claimToken string, leaseFor time.Duration) error {
	return updateVerificationLease(db, tenantID, taskID, owner, claimToken, leaseFor, false)
}

func updateVerificationLease(db *gorm.DB, tenantID, taskID, owner, claimToken string, leaseFor time.Duration, begin bool) error {
	if db == nil || strings.TrimSpace(tenantID) == "" || strings.TrimSpace(owner) == "" || (leaseFor <= 0 && !begin) {
		return fmt.Errorf("verification lease input is incomplete")
	}
	publicID, err := uuid.Parse(strings.TrimSpace(taskID))
	if err != nil {
		return fmt.Errorf("verification task ID is invalid")
	}
	token, err := uuid.Parse(strings.TrimSpace(claimToken))
	if err != nil {
		return fmt.Errorf("verification claim token is invalid")
	}
	now := time.Now().UTC()
	updates := map[string]any{"heartbeat_at": now}
	states := []string{models.SourceRunVerificationTaskClaimed, models.SourceRunVerificationTaskRunning}
	if begin {
		updates["state"] = models.SourceRunVerificationTaskRunning
		states = []string{models.SourceRunVerificationTaskClaimed}
	} else {
		updates["claim_expires_at"] = now.Add(leaseFor)
	}
	result := db.Model(&models.SourceRunVerificationTask{}).Where("public_id = ? AND tenant_id = ? AND claim_owner = ? AND claim_token = ? AND claim_expires_at > ? AND state IN ?", publicID, tenantID, owner, token, now, states).Updates(updates)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return fmt.Errorf("verification lease is no longer current")
	}
	return nil
}

type VerificationResultInput struct {
	TenantID, TaskID, Owner, ClaimToken, EventKey, EvidenceSnapshot, ProvenanceDigest string
	Verdict                                                                           VerificationVerdict
	// VerifiedOutcome is set only by a CMS-owned observer. It lets a positive
	// observation distinguish a verified no-change run from one that delivered
	// new CMS content; callers of the raw completion capability cannot select it.
	VerifiedOutcome SourceRunOutcome
	Payload         datatypes.JSON
}

// CompleteVerification appends immutable evidence and then terminalizes the
// task. An unknown result remains visible as verification-required; it is
// never converted into an absence or a retry permission.
func CompleteVerification(db *gorm.DB, input VerificationResultInput) (models.SourceRunReconciliationEvent, bool, error) {
	if db == nil || strings.TrimSpace(input.TenantID) == "" || strings.TrimSpace(input.Owner) == "" || strings.TrimSpace(input.EventKey) == "" || strings.TrimSpace(input.EvidenceSnapshot) == "" || !isDigest(input.ProvenanceDigest) || !validVerdict(input.Verdict) {
		return models.SourceRunReconciliationEvent{}, false, fmt.Errorf("verification result is missing required evidence")
	}
	taskID, err := uuid.Parse(strings.TrimSpace(input.TaskID))
	if err != nil {
		return models.SourceRunReconciliationEvent{}, false, fmt.Errorf("verification task ID is invalid")
	}
	token, err := uuid.Parse(strings.TrimSpace(input.ClaimToken))
	if err != nil {
		return models.SourceRunReconciliationEvent{}, false, fmt.Errorf("verification claim token is invalid")
	}
	if len(input.Payload) == 0 {
		input.Payload = datatypes.JSON([]byte(`{}`))
	}
	now := time.Now().UTC()
	var event models.SourceRunReconciliationEvent
	created := false
	err = db.Transaction(func(tx *gorm.DB) error {
		var task models.SourceRunVerificationTask
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id = ? AND tenant_id = ?", taskID, input.TenantID).First(&task).Error; err != nil {
			return err
		}
		if task.State == models.SourceRunVerificationTaskTerminal {
			if task.TerminalEventID != nil {
				return tx.Where("public_id = ? AND tenant_id = ?", *task.TerminalEventID, input.TenantID).First(&event).Error
			}
			return fmt.Errorf("terminal verification task has no immutable event")
		}
		if task.ClaimOwner != input.Owner || task.ClaimToken == nil || *task.ClaimToken != token || task.ClaimExpiresAt == nil || !task.ClaimExpiresAt.After(now) || (task.State != models.SourceRunVerificationTaskClaimed && task.State != models.SourceRunVerificationTaskRunning) {
			return fmt.Errorf("verification lease is stale")
		}
		var attempt models.SourceRunAttempt
		if err := tx.Where("public_id = ? AND tenant_id = ?", nullableUUID(task.SourceRunAttemptID), input.TenantID).First(&attempt).Error; err != nil {
			return err
		}
		if task.ExecutionUnitID == nil {
			return fmt.Errorf("verification task is missing an execution unit")
		}
		var unit models.SourceRunExecutionUnit
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id = ? AND tenant_id = ?", *task.ExecutionUnitID, input.TenantID).First(&unit).Error; err != nil {
			return err
		}
		event = models.SourceRunReconciliationEvent{PublicID: uuid.New(), TenantID: input.TenantID, EventKey: input.EventKey, SourceRunRequestID: task.SourceRunRequestID, SourceRunAttemptID: task.SourceRunAttemptID, ExecutionUnitID: task.ExecutionUnitID, ContentSourceID: task.ContentSourceID, AttemptFenceToken: &attempt.FenceToken, EffectIdentity: task.EffectIdentity, ScopeType: task.ScopeType, ScopeID: task.ScopeID, Stage: task.Stage, Verdict: string(input.Verdict), EvidenceSnapshot: input.EvidenceSnapshot, VerifierSchemaVersion: task.VerifierSchemaVersion, VerificationTaskID: task.PublicID, VerifierLeaseToken: token, CausationID: task.CausationID, ProvenanceDigest: strings.ToLower(input.ProvenanceDigest), Payload: input.Payload, ObservedAt: now}
		result := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "tenant_id"}, {Name: "event_key"}}, DoNothing: true}).Create(&event)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			if err := tx.Where("tenant_id = ? AND event_key = ?", input.TenantID, input.EventKey).First(&event).Error; err != nil {
				return err
			}
		} else {
			created = true
		}
		if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "tenant_id"}, {Name: "evidence_kind"}, {Name: "evidence_id"}, {Name: "reducer_version"}}, DoNothing: true}).Create(&models.SourceRunProjectionWork{PublicID: uuid.New(), TenantID: input.TenantID, EvidenceKind: "reconciliation_event", EvidenceID: event.PublicID, ReducerVersion: projectionReducerVersion, State: "queued"}).Error; err != nil {
			return err
		}
		if isPodsDeliveryTask(task) {
			return completePodsDeliveryTask(tx, task, event, input.Verdict, now)
		}
		if err := tx.Model(&task).Updates(map[string]any{"state": models.SourceRunVerificationTaskTerminal, "terminal_verdict": string(input.Verdict), "terminal_event_id": event.PublicID, "heartbeat_at": now}).Error; err != nil {
			return err
		}
		if input.Verdict == VerdictUnknown {
			if err := tx.Model(&unit).Updates(map[string]any{"state": string(UnitVerificationRequired), "verification_required": true}).Error; err != nil {
				return err
			}
		} else {
			state, outcome := UnitSucceeded, OutcomeNewItems
			if input.Verdict == VerdictAbsent {
				state, outcome = UnitFailed, OutcomeProviderFailed
			} else if input.VerifiedOutcome == OutcomeNoChange || input.VerifiedOutcome == OutcomeNewItems {
				outcome = input.VerifiedOutcome
			}
			if err := tx.Model(&unit).Updates(map[string]any{"state": string(state), "terminal_outcome": string(outcome), "finished_at": now, "verification_required": false}).Error; err != nil {
				return err
			}
		}
		if task.CausationID == "consumer_delivery" && input.Verdict == VerdictPresent && unit.UnitType == "coordinator" {
			if err := ensurePodsDeliveryVerificationTask(tx, unit, event); err != nil {
				return err
			}
		}
		return reconcileAttemptAndRequest(tx, input.TenantID, unit.SourceRunAttemptID, unit.SourceRunRequestID)
	})
	return event, created, err
}

func isPodsDeliveryTask(task models.SourceRunVerificationTask) bool {
	return strings.HasPrefix(task.CausationID, "consumer_pods_delivery:")
}

// completePodsDeliveryTask records a read-only public-serving observation. It
// never changes the completed source-run unit, repeats an effect, or grants a
// retry to an executor. Unknown delivery evidence is retried only within the
// bounded observer budget, then retained as a terminal degraded verdict.
func completePodsDeliveryTask(tx *gorm.DB, task models.SourceRunVerificationTask, event models.SourceRunReconciliationEvent, verdict VerificationVerdict, now time.Time) error {
	canRetry := verdict == VerdictUnknown && task.AttemptCount < podsDeliveryObservationAttemptBudget && (task.DeadlineAt == nil || task.DeadlineAt.After(now))
	if canRetry {
		return tx.Model(&task).Updates(map[string]any{
			"state":             models.SourceRunVerificationTaskQueued,
			"claim_owner":       "",
			"claim_token":       nil,
			"claim_expires_at":  nil,
			"heartbeat_at":      now,
			"not_before_at":     now.Add(podsDeliveryObservationRetry),
			"terminal_verdict":  "",
			"terminal_event_id": nil,
		}).Error
	}
	if err := tx.Model(&task).Updates(map[string]any{
		"state":             models.SourceRunVerificationTaskTerminal,
		"claim_owner":       "",
		"claim_token":       nil,
		"claim_expires_at":  nil,
		"heartbeat_at":      now,
		"not_before_at":     nil,
		"terminal_verdict":  string(verdict),
		"terminal_event_id": event.PublicID,
	}).Error; err != nil {
		return err
	}
	if verdict == VerdictPresent {
		return tx.Model(&models.ContentSource{}).Where("public_id = ? AND tenant_id = ?", task.ContentSourceID, task.TenantID).Update("last_delivery_verified_at", now).Error
	}
	return nil
}

func ensurePodsDeliveryVerificationTask(tx *gorm.DB, unit models.SourceRunExecutionUnit, ingestEvent models.SourceRunReconciliationEvent) error {
	var request models.SourceRunRequest
	if err := tx.Select("lane").Where("public_id = ? AND tenant_id = ?", unit.SourceRunRequestID, unit.TenantID).First(&request).Error; err != nil {
		return err
	}
	if request.Lane != models.SourceCategoryMedia {
		return nil
	}
	key := "source-run-pods-delivery:" + unit.PublicID.String() + ":" + ingestEvent.PublicID.String()
	task := models.SourceRunVerificationTask{
		PublicID: uuid.New(), TenantID: unit.TenantID, TaskKey: key,
		SourceRunRequestID: unit.SourceRunRequestID, SourceRunAttemptID: &unit.SourceRunAttemptID, ExecutionUnitID: &unit.PublicID,
		ContentSourceID: unit.ContentSourceID, EffectIdentity: "source-unit:" + unit.JobID,
		ScopeType: "pods_delivery", ScopeID: unit.PublicID.String(), Stage: string(ReceiptStageDelivery),
		EvidenceBoundary: "cms_pods_serving_predicate_and_generation", CausationID: "consumer_pods_delivery:" + ingestEvent.PublicID.String(),
		VerifierName: verificationOwner, VerifierSchemaVersion: verificationVersion, State: models.SourceRunVerificationTaskQueued,
	}
	return tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "tenant_id"}, {Name: "task_key"}}, DoNothing: true}).Create(&task).Error
}

func ensureVerificationTask(tx *gorm.DB, unit models.SourceRunExecutionUnit, reason string) (models.SourceRunVerificationTask, error) {
	key := "source-run-verify:" + unit.PublicID.String() + ":" + unit.AttemptFenceToken.String()
	evidenceBoundary := "provider_receipt_and_cms_ingest"
	if unit.UnitType == "coordinator" && reason == "consumer_delivery" {
		// The coordinator is supervisory. Its task is a bounded aggregate
		// readback of the already completed normalization children, not a
		// provider effect and never a new execution permission.
		evidenceBoundary = "cms_downstream_content_readback"
	}
	task := models.SourceRunVerificationTask{PublicID: uuid.New(), TenantID: unit.TenantID, TaskKey: key, SourceRunRequestID: unit.SourceRunRequestID, SourceRunAttemptID: &unit.SourceRunAttemptID, ExecutionUnitID: &unit.PublicID, ContentSourceID: unit.ContentSourceID, EffectIdentity: "source-unit:" + unit.JobID, ScopeType: "execution_unit", ScopeID: unit.PublicID.String(), Stage: unit.UnitType, EvidenceBoundary: evidenceBoundary, CausationID: reason, VerifierName: verificationOwner, VerifierSchemaVersion: verificationVersion, State: models.SourceRunVerificationTaskQueued}
	result := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "tenant_id"}, {Name: "task_key"}}, DoNothing: true}).Create(&task)
	if result.Error != nil {
		return models.SourceRunVerificationTask{}, result.Error
	}
	if result.RowsAffected == 0 {
		if err := tx.Where("tenant_id = ? AND task_key = ?", unit.TenantID, key).First(&task).Error; err != nil {
			return models.SourceRunVerificationTask{}, err
		}
	}
	return task, nil
}

// EnsureUnitVerificationTask is the narrow CMS owner handshake for a single
// already-effected unit. It only creates/reuses the verifier task; it grants
// no execution lease and cannot replay the unit or provider work.
func EnsureUnitVerificationTask(db *gorm.DB, tenantID, unitID, reason string) (models.SourceRunVerificationTask, error) {
	if db == nil || strings.TrimSpace(tenantID) == "" || strings.TrimSpace(reason) == "" {
		return models.SourceRunVerificationTask{}, fmt.Errorf("verification task requires explicit tenant and reason")
	}
	publicID, err := uuid.Parse(strings.TrimSpace(unitID))
	if err != nil {
		return models.SourceRunVerificationTask{}, fmt.Errorf("source-run execution unit ID is invalid")
	}
	var task models.SourceRunVerificationTask
	err = db.Transaction(func(tx *gorm.DB) error {
		var unit models.SourceRunExecutionUnit
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id = ? AND tenant_id = ? AND state = ? AND effect_started_at IS NOT NULL", publicID, tenantID, string(UnitVerificationRequired)).First(&unit).Error; err != nil {
			return err
		}
		created, err := ensureVerificationTask(tx, unit, reason)
		if err != nil {
			return err
		}
		task = created
		return nil
	})
	return task, err
}

func requireVerificationForAttempt(tx *gorm.DB, tenantID string, attemptID uuid.UUID, reason string) error {
	var units []models.SourceRunExecutionUnit
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id = ? AND source_run_attempt_id = ? AND effect_started_at IS NOT NULL AND state NOT IN ?", tenantID, attemptID, []string{string(UnitSucceeded), string(UnitFailed), string(UnitCancelled), string(UnitExpired)}).Find(&units).Error; err != nil {
		return err
	}
	for _, unit := range units {
		if err := tx.Model(&unit).Updates(map[string]any{"state": string(UnitVerificationRequired), "verification_required": true}).Error; err != nil {
			return err
		}
		if _, err := ensureVerificationTask(tx, unit, reason); err != nil {
			return err
		}
	}
	if len(units) == 0 {
		return nil
	}
	return reconcileAttemptAndRequest(tx, tenantID, attemptID, units[0].SourceRunRequestID)
}

func reconcileAttemptAndRequest(tx *gorm.DB, tenantID string, attemptID, requestID uuid.UUID) error {
	var units []models.SourceRunExecutionUnit
	if err := tx.Where("tenant_id = ? AND source_run_attempt_id = ?", tenantID, attemptID).Find(&units).Error; err != nil {
		return err
	}
	attemptState, requestState, allTerminal := aggregateUnitStates(units)
	if !allTerminal && attemptState != AttemptVerificationRequired {
		return nil
	}
	now := time.Now().UTC()
	attemptUpdates := map[string]any{"state": string(attemptState)}
	if allTerminal {
		attemptUpdates["finished_at"] = now
	}
	if attemptState == AttemptVerificationRequired {
		attemptUpdates["verification_required_at"] = now
	}
	if err := tx.Model(&models.SourceRunAttempt{}).Where("public_id = ? AND tenant_id = ?", attemptID, tenantID).Updates(attemptUpdates).Error; err != nil {
		return err
	}
	var request models.SourceRunRequest
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id = ? AND tenant_id = ?", requestID, tenantID).First(&request).Error; err != nil {
		return err
	}
	if attemptState == AttemptVerificationRequired {
		return tx.Model(&request).Updates(map[string]any{"state": string(RequestVerificationRequired), "evidence_state": "unknown"}).Error
	}
	if !allTerminal || request.ManifestState != string(ManifestSealed) {
		return nil
	}
	settled, err := requestProjectionsSettled(tx, tenantID, requestID)
	if err != nil {
		return err
	}
	if !settled {
		return nil
	}
	requestUpdates := map[string]any{"state": string(requestState), "finished_at": now, "finalized_at": now}
	if requestState == RequestSucceeded {
		requestUpdates["verified_at"] = now
		requestUpdates["evidence_state"] = "verified"
	}
	if err := settleSourceRunBudget(tx, request, now); err != nil {
		return err
	}
	return tx.Model(&request).Updates(requestUpdates).Error
}

// AdvanceSourceRunManifests is CMS-only convergence work. It never contacts a
// provider, dispatches a queue job, or repeats an effect. It seals manifests
// only after receipts have made every fetch page terminal, then promotes the
// coordinator to delivery verification from the already-authorized child
// units. This keeps the coordinator as a durable supervisory unit rather than
// a second provider effect that an executor would need to replay after a
// crash.
func AdvanceSourceRunManifests(db *gorm.DB, limit int) error {
	if db == nil || limit < 1 || limit > 1000 {
		return fmt.Errorf("source-run manifest advancement requires a bounded limit")
	}
	var requests []models.SourceRunRequest
	if err := db.Where("manifest_state IN ? AND state NOT IN ?", []string{string(ManifestOpen), string(ManifestSealed)}, []string{string(RequestSucceeded), string(RequestPartial), string(RequestBlocked), string(RequestFailed), string(RequestCancelled), string(RequestExpired)}).Order("updated_at ASC").Limit(limit).Find(&requests).Error; err != nil {
		return err
	}
	for _, request := range requests {
		if request.ManifestState == string(ManifestOpen) {
			// Not-yet-terminal pages are a normal condition, not an error. A
			// later receipt projection will revisit the request.
			if _, err := SealManifest(db, request.TenantID, request.PublicID.String()); err != nil {
				continue
			}
		}
		if err := finalizeSealedCoordinator(db, request.TenantID, request.PublicID); err != nil {
			return err
		}
	}
	return nil
}

func finalizeSealedCoordinator(db *gorm.DB, tenantID string, requestID uuid.UUID) error {
	return db.Transaction(func(tx *gorm.DB) error {
		var request models.SourceRunRequest
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id = ? AND tenant_id = ?", requestID, tenantID).First(&request).Error; err != nil {
			return err
		}
		if request.ManifestState != string(ManifestSealed) {
			return nil
		}
		var units []models.SourceRunExecutionUnit
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id = ? AND source_run_request_id = ?", tenantID, requestID).Find(&units).Error; err != nil {
			return err
		}
		var coordinator *models.SourceRunExecutionUnit
		children := make([]models.SourceRunExecutionUnit, 0, len(units))
		for i := range units {
			if units[i].UnitType == "coordinator" {
				coordinator = &units[i]
				continue
			}
			children = append(children, units[i])
		}
		if coordinator == nil || IsTerminalUnit(ExecutionUnitState(coordinator.State)) || len(children) == 0 {
			return nil
		}
		for _, unit := range children {
			if !IsTerminalUnit(ExecutionUnitState(unit.State)) {
				return nil
			}
		}
		_, childRequestState, _ := aggregateUnitStates(children)
		state, outcome := UnitSucceeded, OutcomeNoChange
		switch childRequestState {
		case RequestFailed:
			state, outcome = UnitFailed, OutcomeProviderFailed
		case RequestCancelled:
			state, outcome = UnitCancelled, OutcomeCancelled
		case RequestPartial:
			// Unit states deliberately have no synthetic "partial" value. A
			// failed coordinator plus its successful children produces the
			// truthful request-level partial verdict.
			state, outcome = UnitFailed, OutcomePartial
		}
		now := time.Now().UTC()
		if childRequestState == RequestSucceeded {
			// Receipt success proves that the producer finished its work, not
			// that the resulting CMS state exists. The CMS-owned verifier will
			// read the exact child attribution before this coordinator (and hence
			// the request) is allowed to terminally succeed.
			if coordinator.State != string(UnitVerificationRequired) {
				if err := tx.Model(&models.SourceRunExecutionUnit{}).Where("public_id = ? AND tenant_id = ? AND state NOT IN ?", coordinator.PublicID, tenantID, []string{string(UnitSucceeded), string(UnitFailed), string(UnitCancelled), string(UnitExpired)}).Updates(map[string]any{"state": string(UnitVerificationRequired), "verification_required": true, "terminal_outcome": "", "finished_at": nil}).Error; err != nil {
					return err
				}
			}
			if _, err := ensureVerificationTask(tx, *coordinator, "consumer_delivery"); err != nil {
				return err
			}
			if err := rebuildRequestCounters(tx, tenantID, requestID); err != nil {
				return err
			}
			return reconcileAttemptAndRequest(tx, tenantID, coordinator.SourceRunAttemptID, requestID)
		}
		if err := tx.Model(&models.SourceRunExecutionUnit{}).Where("public_id = ? AND tenant_id = ? AND state NOT IN ?", coordinator.PublicID, tenantID, []string{string(UnitSucceeded), string(UnitFailed), string(UnitCancelled), string(UnitExpired)}).Updates(map[string]any{"state": string(state), "terminal_outcome": string(outcome), "finished_at": now, "verification_required": false}).Error; err != nil {
			return err
		}
		if err := rebuildRequestCounters(tx, tenantID, requestID); err != nil {
			return err
		}
		return reconcileAttemptAndRequest(tx, tenantID, coordinator.SourceRunAttemptID, requestID)
	})
}

// requestProjectionsSettled prevents a terminal current-state projection from
// racing ahead of the immutable evidence reducers that justify it.
func requestProjectionsSettled(tx *gorm.DB, tenantID string, requestID uuid.UUID) (bool, error) {
	var pending int64
	err := tx.Raw(`
		SELECT COUNT(*)
		FROM source_run_projection_work work
		LEFT JOIN source_run_receipts receipt
		  ON work.evidence_kind = 'receipt' AND receipt.public_id = work.evidence_id AND receipt.tenant_id = work.tenant_id
		LEFT JOIN source_run_reconciliation_events reconciliation
		  ON work.evidence_kind = 'reconciliation_event' AND reconciliation.public_id = work.evidence_id AND reconciliation.tenant_id = work.tenant_id
		WHERE work.tenant_id = ?
		  AND work.state <> 'succeeded'
		  AND (receipt.source_run_request_id = ? OR reconciliation.source_run_request_id = ?)
	`, tenantID, requestID, requestID).Scan(&pending).Error
	return pending == 0, err
}

func aggregateUnitStates(units []models.SourceRunExecutionUnit) (AttemptState, RequestState, bool) {
	if len(units) == 0 {
		return AttemptVerificationRequired, RequestVerificationRequired, false
	}
	var succeeded, failed, cancelled, pending int
	for _, unit := range units {
		switch ExecutionUnitState(unit.State) {
		case UnitSucceeded:
			succeeded++
		case UnitFailed, UnitExpired:
			failed++
		case UnitCancelled:
			cancelled++
		case UnitVerificationRequired:
			return AttemptVerificationRequired, RequestVerificationRequired, false
		default:
			pending++
		}
	}
	if pending > 0 {
		return AttemptRunning, RequestRunning, false
	}
	if failed > 0 && succeeded > 0 {
		return AttemptPartial, RequestPartial, true
	}
	if failed > 0 {
		return AttemptFailed, RequestFailed, true
	}
	if cancelled > 0 && succeeded == 0 {
		return AttemptCancelled, RequestCancelled, true
	}
	if cancelled > 0 {
		return AttemptPartial, RequestPartial, true
	}
	return AttemptSucceeded, RequestSucceeded, true
}

func validateCurrentUnitLease(unit models.SourceRunExecutionUnit, owner string, token uuid.UUID, now time.Time) error {
	if unit.ExecutionOwner != owner || unit.ExecutionLeaseToken == nil || *unit.ExecutionLeaseToken != token || unit.ExecutionLeaseExpiresAt == nil || !unit.ExecutionLeaseExpiresAt.After(now) {
		return fmt.Errorf("source-run execution-unit lease is stale")
	}
	if unit.CancellationRequestedAt != nil {
		return fmt.Errorf("source-run execution unit has been cancelled")
	}
	return nil
}

func validVerdict(verdict VerificationVerdict) bool {
	return verdict == VerdictPresent || verdict == VerdictAbsent || verdict == VerdictUnknown
}

func isDigest(value string) bool {
	value = strings.TrimSpace(value)
	if len(value) != 64 {
		return false
	}
	for _, c := range value {
		if !(c >= '0' && c <= '9') && !(c >= 'a' && c <= 'f') && !(c >= 'A' && c <= 'F') {
			return false
		}
	}
	return true
}

func nullableUUID(value *uuid.UUID) uuid.UUID {
	if value == nil {
		return uuid.Nil
	}
	return *value
}
