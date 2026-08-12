package controllers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log"
	"sort"
	"strings"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/supply"

	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	mediaSupplyEvaluationInterval    = 5 * time.Minute
	mediaSupplyEvaluationTenantLimit = 100
	mediaSupplyEvaluationLockFamily  = "media_supply_read_evaluation"
)

// StartMediaSupplyEvaluationHeartbeat periodically records only CMS-derived
// Supply Continuity attention evidence. It never creates an Operator plan,
// accesses Redis, selects a queue/provider, or retries uncertain work. The one
// optional mutation is an exact CMS-derived Supply request whose action class
// has a current tenant-bound Safe Auto promotion; without that it is read-only.
//
// Each run enumerates tenants from explicit Media source ownership instead of
// inventing a default tenant. The per-tenant PostgreSQL advisory lock makes the
// evidence write replica-safe while preserving the evaluator's bounded scope.
func StartMediaSupplyEvaluationHeartbeat(db *gorm.DB) {
	go func() {
		runMediaSupplyEvaluationDue(db)
		ticker := time.NewTicker(mediaSupplyEvaluationInterval)
		defer ticker.Stop()
		for range ticker.C {
			runMediaSupplyEvaluationDue(db)
		}
	}()
}

func runMediaSupplyEvaluationDue(db *gorm.DB) {
	defer func() {
		if recovered := recover(); recovered != nil {
			log.Printf("media supply evaluator heartbeat recovered panic: %v", recovered)
		}
	}()
	if db == nil {
		return
	}

	tenantIDs, err := mediaSupplyEvaluationTenants(db)
	if err != nil {
		log.Printf("media supply evaluator tenant discovery failed: %v", err)
		return
	}

	for _, tenantID := range tenantIDs {
		release, acquired := tryAcquireTenantAutopilotLock(db, mediaSupplyEvaluationLockFamily, tenantID)
		if !acquired {
			continue
		}
		func() {
			defer release()
			// Terminalization is a separate safe-recovery path: it must remain
			// available even when the subtractive control blocks new attention
			// records. It uses fresh CMS evidence and never creates source work.
			status := buildMediaSupplyStatus(db, tenantID)
			if err := reconcileMediaSupplyEpisodes(db, tenantID, status); err != nil {
				log.Printf("media supply episode recovery failed for tenant %q: %v", tenantID, err)
			}
			// Re-read after acquiring the tenant fence so a concurrent emergency
			// stop cannot race an observation into an episode write.
			enabled, err := mediaSupplyEvaluationRecordingEnabled(db, tenantID)
			if err != nil {
				recordMediaSupplyEvaluationCheckpointFailure(db, tenantID, models.MediaSupplyEvaluationTriggerScheduled, models.MediaSupplyEvaluationOutcomeControlUnavailable)
				log.Printf("media supply evaluator control lookup failed for tenant %q: %v", tenantID, err)
				return
			}
			if !enabled {
				recordMediaSupplyEvaluationCheckpointFailure(db, tenantID, models.MediaSupplyEvaluationTriggerScheduled, models.MediaSupplyEvaluationOutcomeDisabled)
				return
			}
			// The evaluator, rather than the read-only status endpoint, owns the
			// append-only direct-return observation and its bounded retention.
			if err := supply.RecordPodsExposureReturnProof(db, tenantID, status.Exposure); err != nil {
				log.Printf("media supply return-proof record failed for tenant %q: %v", tenantID, err)
			}
			episode, _, err := recordMediaSupplyEpisode(db, tenantID, status.SupplyEvaluation)
			if err != nil {
				recordMediaSupplyEvaluationCheckpointFailure(db, tenantID, models.MediaSupplyEvaluationTriggerScheduled, models.MediaSupplyEvaluationOutcomeRecordFailed)
				log.Printf("media supply evaluator evidence record failed for tenant %q: %v", tenantID, err)
				return
			}
			if err := recordMediaSupplyQualificationObservations(db, tenantID, episode, status.SupplyEvaluation); err != nil {
				log.Printf("media supply qualification observation failed for tenant %q: %v", tenantID, err)
			}
			if episode != nil {
				if err := queuePromotedMediaSupplyRecoveries(db, tenantID, *episode); err != nil {
					log.Printf("media supply promoted recovery admission failed for tenant %q: %v", tenantID, err)
				}
			}
			if err := recordMediaSupplyEvaluationCheckpoint(db, tenantID, models.MediaSupplyEvaluationTriggerScheduled, status.SupplyEvaluation); err != nil {
				log.Printf("media supply evaluator checkpoint record failed for tenant %q: %v", tenantID, err)
			}
		}()
	}
	supply.MarkMediaSupplyEvaluatorHeartbeat(time.Now().UTC())
}

// queuePromotedMediaSupplyRecoveries is the only scheduled Safe Auto admission
// seam. Eligibility and targets are re-derived from CMS rows, and each native
// owner request is initialized in the same transaction as its action ledger.
// With no exact active promotion this function is a read-only no-op.
func queuePromotedMediaSupplyRecoveries(db *gorm.DB, tenantID string, episode models.MediaSupplyEpisode) error {
	items, err := mediaSupplyEpisodeEligibleActions(db, tenantID, episode.PublicID)
	if err != nil {
		return err
	}
	priority := map[string]int{}
	for index, key := range supply.SupplyActionKeys() {
		priority[key] = index
	}
	sort.SliceStable(items, func(i, j int) bool { return priority[items[i].Key] < priority[items[j].Key] })
	for _, candidate := range items {
		if candidate.ManualOnly || candidate.Disabled {
			continue
		}
		versions, registered := supply.QualificationVersions(candidate.Key)
		if !registered {
			continue
		}
		var active int64
		if err := db.Model(&models.MediaSupplyActionPromotion{}).Where("tenant_id=? AND action_key=? AND state=? AND action_version=? AND adapter_version=? AND verifier_version=? AND schema_version=? AND policy_version=?", tenantID, candidate.Key, "active", versions.ActionVersion, versions.AdapterVersion, versions.VerifierVersion, versions.SchemaVersion, versions.PolicyVersion).Count(&active).Error; err != nil {
			return err
		}
		if active != 1 {
			continue
		}
		targetID, err := mediaSupplyEligibleTargetID(db, tenantID, episode.PublicID, candidate)
		if err != nil {
			continue
		}
		policy := sha256.Sum256([]byte("media-supply-action-policy/v1\n" + candidate.Key + "\n" + candidate.TargetType))
		preflight, _ := json.Marshal(map[string]any{"schema_version": "media-supply-action-preflight/v1", "episode_id": episode.PublicID.String(), "eligibility_id": candidate.ID, "evidence_digest": episode.EvidenceDigest, "initiator": "qualified_safe_auto"})
		planned, _ := json.Marshal(map[string]any{"action_key": candidate.Key, "target_type": candidate.TargetType, "verification": "CMS-derived terminal state"})
		subjects, _ := json.Marshal([]map[string]string{{"type": candidate.TargetType, "id": targetID.String()}})
		err = db.Transaction(func(tx *gorm.DB) error {
			request, created, queueErr := supply.QueueQualifiedSupplyAction(tx, supply.CreateSupplyActionPreviewInput{TenantID: tenantID, ActionKey: candidate.Key, TargetType: candidate.TargetType, TargetID: targetID, EvidenceDigest: episode.EvidenceDigest, PolicyDigest: hex.EncodeToString(policy[:]), PreflightEvidence: datatypes.JSON(preflight), PlannedEffects: datatypes.JSON(planned), AffectedSubjects: datatypes.JSON(subjects), DeepLinks: datatypes.JSON([]byte(`["/platform/media/circulation"]`))})
			if queueErr != nil || !created {
				return queueErr
			}
			return initializeApprovedMediaSupplyAction(tx, request)
		})
		if err != nil {
			return err
		}
		// One episode represents one first failed boundary. Never fan a single
		// observation into several autonomous owner effects even when multiple
		// independently promoted descriptors are technically eligible.
		return nil
	}
	return nil
}

func recordMediaSupplyQualificationObservations(db *gorm.DB, tenantID string, episode *models.MediaSupplyEpisode, evaluation supply.SupplyEvaluation) error {
	digest, err := supply.EvaluationEvidenceDigest(evaluation)
	if err != nil {
		return err
	}
	eligible := map[string]bool{}
	if episode != nil {
		items, eligibilityErr := mediaSupplyEpisodeEligibleActions(db, tenantID, episode.PublicID)
		if eligibilityErr != nil {
			return eligibilityErr
		}
		for _, item := range items {
			if !item.ManualOnly && !item.Disabled {
				eligible[item.Key] = true
			}
		}
	}
	verdict := supply.VerdictPresent
	if evaluation.EvidenceCompleteness != "complete" || evaluation.Verdict == supply.SupplyVerdictEvidenceUnavailable {
		verdict = supply.VerdictUnknown
	} else if supply.IsEpisodeWorthy(evaluation.Verdict) {
		verdict = supply.VerdictAbsent
	}
	observedAt := evaluation.EvaluatedAt.UTC()
	if observedAt.IsZero() {
		observedAt = time.Now().UTC()
	}
	for _, actionKey := range supply.SupplyActionKeys() {
		correlation := supply.DigestForQualificationObservation(digest, actionKey)
		caseKey := "supply-observe:" + actionKey + ":" + observedAt.Format("20060102T150405.000000000")
		if _, err := supply.RecordSupplyObserveQualificationCase(context.Background(), db, tenantID, actionKey, caseKey, correlation, eligible[actionKey], verdict); err != nil {
			return err
		}
	}
	return nil
}

func mediaSupplyEvaluationTenants(db *gorm.DB) ([]string, error) {
	var tenantIDs []string
	err := db.Model(&models.ContentSource{}).
		Distinct("tenant_id").
		Where("category = ? AND tenant_id <> ?", models.SourceCategoryMedia, "").
		Order("tenant_id ASC").
		Limit(mediaSupplyEvaluationTenantLimit).
		Pluck("tenant_id", &tenantIDs).Error
	if err != nil {
		return nil, err
	}

	// Defend the durable evaluator from malformed legacy rows even though the
	// query excludes the common empty value. Never normalize a missing tenant
	// into a global/default tenant.
	result := make([]string, 0, len(tenantIDs))
	seen := make(map[string]struct{}, len(tenantIDs))
	for _, tenantID := range tenantIDs {
		tenantID = strings.TrimSpace(tenantID)
		if tenantID == "" {
			continue
		}
		if _, exists := seen[tenantID]; exists {
			continue
		}
		seen[tenantID] = struct{}{}
		result = append(result, tenantID)
	}
	return result, nil
}

// mediaSupplyEvaluationRecordingEnabled resolves only the static evaluator
// control. Missing rows are the operational default; a matching durable row is
// subtractive and disables just episode recording. Status reads remain
// available, so an emergency stop never hides the current evidence boundary.
func mediaSupplyEvaluationRecordingEnabled(db *gorm.DB, tenantID string) (bool, error) {
	if db == nil || strings.TrimSpace(tenantID) == "" {
		return false, nil
	}
	var control models.MediaSupplyControl
	err := db.Where("tenant_id = ? AND control_key = ? AND scope_type = ? AND scope_id = ?", tenantID,
		models.MediaSupplyControlReadEvaluation, models.MediaSupplyControlScopeTenant, models.MediaSupplyControlScopeAll).
		First(&control).Error
	if err == gorm.ErrRecordNotFound {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return mediaSupplyEvaluationMayRecord(&control), nil
}

// mediaSupplyEvaluationMayRecord makes the control's subtractive semantics
// testable without treating a caller-provided key as a capability selector.
func mediaSupplyEvaluationMayRecord(control *models.MediaSupplyControl) bool {
	return control == nil
}

// recordMediaSupplyEvaluationCheckpoint writes the latest evaluation proof
// after its optional episode update commits. It is safe to overwrite because
// it is only a liveness checkpoint; immutable attention history remains in
// media_supply_episode_events. No caller controls trigger or outcome strings.
func recordMediaSupplyEvaluationCheckpoint(db *gorm.DB, tenantID, trigger string, evaluation supply.SupplyEvaluation) error {
	if db == nil || strings.TrimSpace(tenantID) == "" || !isMediaSupplyEvaluationTrigger(trigger) {
		return gorm.ErrInvalidData
	}
	digest, err := supply.EvaluationEvidenceDigest(evaluation)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	evaluatedAt := evaluation.EvaluatedAt.UTC()
	if evaluatedAt.IsZero() {
		evaluatedAt = now
	}
	checkpoint := models.MediaSupplyEvaluationCheckpoint{
		TenantID: tenantID, LastTrigger: trigger, LastOutcome: models.MediaSupplyEvaluationOutcomeEvaluated,
		LastObservedAt: now, LastEvaluatedAt: &evaluatedAt, EvaluationDigest: &digest,
	}
	return db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "tenant_id"}},
		DoUpdates: clause.Assignments(map[string]interface{}{
			"last_trigger":       checkpoint.LastTrigger,
			"last_outcome":       checkpoint.LastOutcome,
			"last_observed_at":   checkpoint.LastObservedAt,
			"last_evaluated_at":  checkpoint.LastEvaluatedAt,
			"evaluation_digest":  checkpoint.EvaluationDigest,
			"last_failure_class": nil,
			"updated_at":         checkpoint.LastObservedAt,
		}),
	}).Create(&checkpoint).Error
}

func recordMediaSupplyEvaluationCheckpointFailure(db *gorm.DB, tenantID, trigger, outcome string) {
	if db == nil || strings.TrimSpace(tenantID) == "" || !isMediaSupplyEvaluationTrigger(trigger) || !isMediaSupplyEvaluationFailureOutcome(outcome) {
		return
	}
	now := time.Now().UTC()
	checkpoint := models.MediaSupplyEvaluationCheckpoint{
		TenantID: tenantID, LastTrigger: trigger, LastOutcome: outcome,
		LastObservedAt: now, LastFailureClass: &outcome,
	}
	if err := db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "tenant_id"}},
		DoUpdates: clause.Assignments(map[string]interface{}{
			"last_trigger":       checkpoint.LastTrigger,
			"last_outcome":       checkpoint.LastOutcome,
			"last_observed_at":   checkpoint.LastObservedAt,
			"last_evaluated_at":  nil,
			"evaluation_digest":  nil,
			"last_failure_class": checkpoint.LastFailureClass,
			"updated_at":         checkpoint.LastObservedAt,
		}),
	}).Create(&checkpoint).Error; err != nil {
		log.Printf("media supply evaluator checkpoint failure record failed for tenant %q: %v", tenantID, err)
	}
}

func isMediaSupplyEvaluationTrigger(trigger string) bool {
	return trigger == models.MediaSupplyEvaluationTriggerScheduled || trigger == models.MediaSupplyEvaluationTriggerManual
}

func isMediaSupplyEvaluationFailureOutcome(outcome string) bool {
	return outcome == models.MediaSupplyEvaluationOutcomeDisabled ||
		outcome == models.MediaSupplyEvaluationOutcomeControlUnavailable ||
		outcome == models.MediaSupplyEvaluationOutcomeRecordFailed
}
