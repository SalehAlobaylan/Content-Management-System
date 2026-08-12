package controllers

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"content-management-system/src/artifacts"
	"content-management-system/src/atomizationwork"
	"content-management-system/src/models"
	operatorpkg "content-management-system/src/operator"
	"content-management-system/src/pipeline"
	"content-management-system/src/supply"
	"content-management-system/src/utils"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type mediaSupplyEligibleAction struct {
	ID              string   `json:"id"`
	Key             string   `json:"key"`
	TargetType      string   `json:"target_type"`
	Risk            string   `json:"risk"`
	ExecutionOwner  string   `json:"execution_owner"`
	AffectedDomains []string `json:"affected_domains"`
	ManualOnly      bool     `json:"manual_only"`
	Disabled        bool     `json:"disabled"`
	DisabledControl string   `json:"disabled_control,omitempty"`
}

func currentMediaSupplyActionAccess(c *gin.Context, principal utils.AdminPrincipal) (string, bool) {
	snapshot, err := supply.CurrentSupplyActionAccess(c.Request.Context(), principal.UserID, principal.TenantID)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Current Supply action access cannot be verified"})
		return "", false
	}
	return snapshot.AccessVersion, true
}

// ListMediaSupplyEpisodeActions derives exact possible repairs from current
// CMS rows and one episode's immutable evidence. It emits no action when a
// target is unknown, stale, cross-tenant, or lacks an installed adapter.
func ListMediaSupplyEpisodeActions(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	episodeID, err := uuid.Parse(strings.TrimSpace(c.Param("id")))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid Supply episode"})
		return
	}
	items, err := mediaSupplyEpisodeEligibleActions(c.MustGet("db").(*gorm.DB), principal.TenantID, episodeID)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			c.JSON(http.StatusNotFound, gin.H{"message": "Supply episode not found"})
			return
		}
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Supply action eligibility is unavailable"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"schema_version": "media-supply-actions/v1", "episode_id": episodeID, "items": items})
}

func mediaSupplyEpisodeEligibleActions(db *gorm.DB, tenantID string, episodeID uuid.UUID) ([]mediaSupplyEligibleAction, error) {
	var episode models.MediaSupplyEpisode
	if err := db.Where("public_id = ? AND tenant_id = ?", episodeID, tenantID).First(&episode).Error; err != nil {
		return nil, err
	}
	if episode.State == models.MediaSupplyEpisodeResolved || episode.EvidenceCompleteness != "complete" {
		return []mediaSupplyEligibleAction{}, nil
	}
	var subjects []struct {
		Type string `json:"type"`
		ID   string `json:"id"`
	}
	if json.Unmarshal(episode.AffectedSubjects, &subjects) != nil {
		return []mediaSupplyEligibleAction{}, nil
	}
	items := make([]mediaSupplyEligibleAction, 0, 4)
	for _, subject := range subjects {
		id, parseErr := uuid.Parse(subject.ID)
		if parseErr != nil {
			continue
		}
		switch subject.Type {
		case "content_item":
			// A Pipeline repair is eligible only when the episode already carries
			// an exact item subject and CMS can re-derive one non-ambiguous stage.
			if descriptor, found := supply.SupplyAction(supply.SupplyActionPipelineResumeExactStage); found {
				if candidate, err := pipeline.CandidateForItem(db, tenantID, id); err == nil && pipeline.CheckCandidate(db, candidate) == nil {
					items = append(items, makeMediaSupplyEligibleAction(episode, descriptor, id))
				}
			}
			var item models.ContentItem
			if db.Where("public_id = ? AND tenant_id = ?", id, tenantID).First(&item).Error == nil && item.Status != models.ContentStatusArchived {
				if _, membershipErr := supply.CandidateForFeedMembership(db, tenantID, id); membershipErr == nil {
					if descriptor, found := supply.SupplyAction(supply.SupplyActionFeedGenerationAttachVerifiedMember); found {
						items = append(items, makeMediaSupplyEligibleAction(episode, descriptor, id))
					}
				}
				if _, _, _, _, atomizationErr := atomizationwork.Candidate(db, tenantID, id); atomizationErr == nil {
					if descriptor, found := supply.SupplyAction(supply.SupplyActionAtomizationExecuteExactParent); found {
						items = append(items, makeMediaSupplyEligibleAction(episode, descriptor, id))
					}
				}
				for _, pair := range []struct{ Artifact, Key string }{{artifacts.ArtifactTranscript, supply.SupplyActionArtifactRequestTranscript}, {artifacts.ArtifactImageEmbedding, supply.SupplyActionArtifactRequestImageEmbedding}, {artifacts.ArtifactTextEmbedding, supply.SupplyActionArtifactRequestTextEmbedding}, {artifacts.ArtifactLLMMetadata, supply.SupplyActionArtifactRequestLLMMetadata}} {
					missing, evidenceErr := artifacts.Missing(item, pair.Artifact)
					if evidenceErr == nil && missing {
						if descriptor, found := supply.SupplyAction(pair.Key); found {
							items = append(items, makeMediaSupplyEligibleAction(episode, descriptor, id))
						}
					}
				}
			}
		case "content_source":
			if episode.Verdict == "source_due_not_admitted" {
				if descriptor, found := supply.SupplyAction(supply.SupplyActionRepairMissedAdmission); found {
					items = append(items, makeMediaSupplyEligibleAction(episode, descriptor, id))
				}
			}
		case "source_run_request":
			var attempts []models.SourceRunAttempt
			if db.Where("tenant_id = ? AND source_run_request_id = ?", tenantID, id).Find(&attempts).Error != nil {
				continue
			}
			for _, attempt := range attempts {
				if attempt.State == string(supply.AttemptClaimed) && attempt.DispatcherLeaseExpiresAt != nil && !attempt.DispatcherLeaseExpiresAt.After(time.Now().UTC()) {
					if descriptor, found := supply.SupplyAction(supply.SupplyActionReclaimDispatchClaim); found {
						items = append(items, makeMediaSupplyEligibleAction(episode, descriptor, attempt.PublicID))
					}
				}
			}
			var units []models.SourceRunExecutionUnit
			if db.Where("tenant_id = ? AND source_run_request_id = ?", tenantID, id).Find(&units).Error != nil {
				continue
			}
			for _, unit := range units {
				if unit.UnitType == "coordinator" && unit.State == string(supply.UnitAuthorized) && unit.EffectStartedAt == nil {
					if descriptor, found := supply.SupplyAction(supply.SupplyActionAdoptUnitJob); found {
						items = append(items, makeMediaSupplyEligibleAction(episode, descriptor, unit.PublicID))
					}
				}
				if unit.State == string(supply.UnitAuthorized) && unit.EffectStartedAt == nil {
					if descriptor, found := supply.SupplyAction(supply.SupplyActionCancelUnstarted); found {
						items = append(items, makeMediaSupplyEligibleAction(episode, descriptor, unit.PublicID))
					}
				}
				if unit.State == string(supply.UnitAccepted) && unit.EffectStartedAt == nil && unit.ExecutionLeaseExpiresAt != nil && !unit.ExecutionLeaseExpiresAt.After(time.Now().UTC()) {
					if descriptor, found := supply.SupplyAction(supply.SupplyActionTransferUnitLease); found {
						items = append(items, makeMediaSupplyEligibleAction(episode, descriptor, unit.PublicID))
					}
				}
				if unit.State == string(supply.UnitVerificationRequired) {
					if descriptor, found := supply.SupplyAction(supply.SupplyActionVerifyEffect); found {
						items = append(items, makeMediaSupplyEligibleAction(episode, descriptor, unit.PublicID))
					}
				}
				if unit.State == string(supply.UnitSucceeded) && unit.TerminalOutcome == string(supply.OutcomeNoChange) {
					var proofCount int64
					if db.Model(&models.SourceRunVerificationTask{}).Where("tenant_id = ? AND execution_unit_id = ? AND state = ? AND terminal_verdict = ?", tenantID, unit.PublicID, models.SourceRunVerificationTaskTerminal, string(supply.VerdictPresent)).Count(&proofCount).Error == nil && proofCount > 0 {
						if descriptor, found := supply.SupplyAction(supply.SupplyActionFinalizeVerifiedNoChange); found {
							items = append(items, makeMediaSupplyEligibleAction(episode, descriptor, unit.PublicID))
						}
					}
				}
			}
			var retained []models.SourceRunRetainedReceipt
			if db.Where("tenant_id = ? AND source_run_request_id = ? AND state = ?", tenantID, id, "retained").Order("created_at ASC").Find(&retained).Error == nil {
				for _, receipt := range retained {
					if descriptor, found := supply.SupplyAction(supply.SupplyActionRedeliverReceipt); found {
						items = append(items, makeMediaSupplyEligibleAction(episode, descriptor, receipt.PublicID))
					}
				}
			}
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	for index := range items {
		allowed, controlKey, err := supply.MayExecuteSupplyAction(db, tenantID, items[index].Key)
		if err != nil {
			return nil, err
		}
		items[index].Disabled = !allowed
		items[index].DisabledControl = controlKey
	}
	return items, nil
}

func makeMediaSupplyEligibleAction(episode models.MediaSupplyEpisode, descriptor supply.SupplyActionDescriptor, targetID uuid.UUID) mediaSupplyEligibleAction {
	_, artifactInstalled := artifacts.DescriptorForAction(descriptor.Key)
	installed := descriptor.Key == supply.SupplyActionCancelUnstarted || descriptor.Key == supply.SupplyActionRepairMissedAdmission || descriptor.Key == supply.SupplyActionReclaimDispatchClaim || descriptor.Key == supply.SupplyActionVerifyEffect || descriptor.Key == supply.SupplyActionTransferUnitLease || descriptor.Key == supply.SupplyActionFinalizeVerifiedNoChange || descriptor.Key == supply.SupplyActionAdoptUnitJob || descriptor.Key == supply.SupplyActionRedeliverReceipt || descriptor.Key == supply.SupplyActionPipelineResumeExactStage || descriptor.Key == supply.SupplyActionAtomizationExecuteExactParent || descriptor.Key == supply.SupplyActionFeedGenerationAttachVerifiedMember || artifactInstalled
	return mediaSupplyEligibleAction{ID: mediaSupplyEligibilityID(episode.PublicID, descriptor.Key, targetID, episode.EvidenceDigest), Key: descriptor.Key, TargetType: descriptor.TargetType, Risk: descriptor.Risk, ExecutionOwner: descriptor.ExecutionOwner, AffectedDomains: descriptor.AffectedDomains, ManualOnly: !installed}
}

func mediaSupplyEligibilityID(episodeID uuid.UUID, key string, targetID uuid.UUID, evidenceDigest string) string {
	digest := sha256.Sum256([]byte(strings.Join([]string{"media-supply-eligibility/v1", episodeID.String(), key, targetID.String(), evidenceDigest}, "\n")))
	return hex.EncodeToString(digest[:])
}

// revalidateMediaSupplyActionPreview makes approval a second evidence
// boundary. A preview is not an authority to reuse a target after its episode,
// controls, policy, or exact CMS-derived target have changed.
func revalidateMediaSupplyActionPreview(db *gorm.DB, tenantID string, preview models.MediaSupplyActionPreview) error {
	if db == nil || preview.TenantID != tenantID || preview.PublicID == uuid.Nil {
		return fmt.Errorf("media supply action preview identity is invalid")
	}
	var preflight struct {
		EpisodeID     string `json:"episode_id"`
		EligibilityID string `json:"eligibility_id"`
	}
	if err := json.Unmarshal(preview.PreflightEvidence, &preflight); err != nil {
		return fmt.Errorf("media supply action preview preflight is malformed")
	}
	episodeID, err := uuid.Parse(strings.TrimSpace(preflight.EpisodeID))
	if err != nil {
		return fmt.Errorf("media supply action preview episode is invalid")
	}
	var episode models.MediaSupplyEpisode
	if err := db.Where("public_id = ? AND tenant_id = ?", episodeID, tenantID).First(&episode).Error; err != nil {
		return fmt.Errorf("media supply action episode is unavailable: %w", err)
	}
	if episode.EvidenceDigest != preview.EvidenceDigest {
		return fmt.Errorf("media supply action evidence changed")
	}
	items, err := mediaSupplyEpisodeEligibleActions(db, tenantID, episodeID)
	if err != nil {
		return err
	}
	var candidate *mediaSupplyEligibleAction
	for index := range items {
		item := &items[index]
		if item.Key == preview.ActionKey && item.ID == preflight.EligibilityID && !item.ManualOnly && !item.Disabled {
			candidate = item
			break
		}
	}
	if candidate == nil {
		return fmt.Errorf("media supply action is no longer eligible")
	}
	targetID, err := mediaSupplyEligibleTargetID(db, tenantID, episodeID, *candidate)
	if err != nil || targetID != preview.TargetID || candidate.TargetType != preview.TargetType {
		return fmt.Errorf("media supply action target changed")
	}
	policy := sha256.Sum256([]byte("media-supply-action-policy/v1\n" + candidate.Key + "\n" + candidate.TargetType))
	if preview.PolicyDigest != hex.EncodeToString(policy[:]) {
		return fmt.Errorf("media supply action policy changed")
	}
	return nil
}

// CreateMediaSupplyActionPreview accepts only an opaque CMS eligibility ID.
// The server reconstructs the candidate from fresh episode/source-run state;
// no target ID, arguments, provider URL, job, queue, or receipt bytes cross
// this browser boundary.
func CreateMediaSupplyActionPreview(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	accessVersion, ok := currentMediaSupplyActionAccess(c, principal)
	if !ok {
		return
	}
	var body struct {
		EpisodeID     string `json:"episode_id"`
		EligibilityID string `json:"eligibility_id"`
	}
	if err := decodeStrictJSON(c, &body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid Supply action preview"})
		return
	}
	episodeID, err := uuid.Parse(strings.TrimSpace(body.EpisodeID))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid Supply episode"})
		return
	}
	items, err := mediaSupplyEpisodeEligibleActions(c.MustGet("db").(*gorm.DB), principal.TenantID, episodeID)
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"message": "Supply action preconditions changed"})
		return
	}
	key := strings.TrimSpace(c.Param("key"))
	var candidate *mediaSupplyEligibleAction
	for i := range items {
		if items[i].Key == key && items[i].ID == strings.TrimSpace(body.EligibilityID) {
			candidate = &items[i]
			break
		}
	}
	if candidate == nil || candidate.ManualOnly {
		c.JSON(http.StatusConflict, gin.H{"message": "Supply action is not currently eligible"})
		return
	}
	var episode models.MediaSupplyEpisode
	db := c.MustGet("db").(*gorm.DB)
	if err := db.Where("public_id = ? AND tenant_id = ?", episodeID, principal.TenantID).First(&episode).Error; err != nil {
		c.JSON(http.StatusConflict, gin.H{"message": "Supply episode changed"})
		return
	}
	// The opaque eligibility ID binds this reconstructed target. It is not read
	// from the browser or a model response.
	var targetID uuid.UUID
	// Reconstruct the one candidate target by recomputing IDs against source rows.
	targetID, err = mediaSupplyEligibleTargetID(db, principal.TenantID, episodeID, *candidate)
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"message": "Supply action target changed"})
		return
	}
	policy := sha256.Sum256([]byte("media-supply-action-policy/v1\n" + candidate.Key + "\n" + candidate.TargetType))
	preflight, _ := json.Marshal(map[string]any{"schema_version": "media-supply-action-preflight/v1", "episode_id": episodeID.String(), "eligibility_id": candidate.ID, "evidence_digest": episode.EvidenceDigest})
	planned, _ := json.Marshal(map[string]any{"action_key": candidate.Key, "target_type": candidate.TargetType, "verification": "CMS-derived terminal state"})
	subjects, _ := json.Marshal([]map[string]string{{"type": candidate.TargetType, "id": targetID.String()}})
	preview, err := supply.CreateSupplyActionPreview(db, supply.CreateSupplyActionPreviewInput{TenantID: principal.TenantID, ActionKey: candidate.Key, TargetType: candidate.TargetType, TargetID: targetID, EvidenceDigest: episode.EvidenceDigest, PolicyDigest: hex.EncodeToString(policy[:]), CreatedBy: principal.UserID, CreatedAccessVersion: accessVersion, PreflightEvidence: datatypes.JSON(preflight), PlannedEffects: datatypes.JSON(planned), AffectedSubjects: datatypes.JSON(subjects), DeepLinks: datatypes.JSON([]byte(`["/platform/media/circulation"]`))})
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"message": "Supply action preview could not be created"})
		return
	}
	c.JSON(http.StatusCreated, preview)
}

// mediaSupplyEligibleTargetID re-derives only the targets whose complete CMS
// adapter is installed. Other descriptors remain visible as manual-only.
func mediaSupplyEligibleTargetID(db *gorm.DB, tenantID string, episodeID uuid.UUID, candidate mediaSupplyEligibleAction) (uuid.UUID, error) {
	var episode models.MediaSupplyEpisode
	if err := db.Where("public_id = ? AND tenant_id = ?", episodeID, tenantID).First(&episode).Error; err != nil {
		return uuid.Nil, err
	}
	var subjects []struct {
		Type string `json:"type"`
		ID   string `json:"id"`
	}
	if json.Unmarshal(episode.AffectedSubjects, &subjects) != nil {
		return uuid.Nil, gorm.ErrRecordNotFound
	}
	for _, subject := range subjects {
		if candidate.Key == supply.SupplyActionFeedGenerationAttachVerifiedMember && subject.Type == "content_item" {
			itemID, err := uuid.Parse(subject.ID)
			if err == nil {
				if _, candidateErr := supply.CandidateForFeedMembership(db, tenantID, itemID); candidateErr == nil && mediaSupplyEligibilityID(episodeID, candidate.Key, itemID, episode.EvidenceDigest) == candidate.ID {
					return itemID, nil
				}
			}
		}
		if candidate.Key == supply.SupplyActionAtomizationExecuteExactParent && subject.Type == "content_item" {
			itemID, err := uuid.Parse(subject.ID)
			if err == nil {
				if _, _, _, _, candidateErr := atomizationwork.Candidate(db, tenantID, itemID); candidateErr == nil && mediaSupplyEligibilityID(episodeID, candidate.Key, itemID, episode.EvidenceDigest) == candidate.ID {
					return itemID, nil
				}
			}
		}
		if descriptor, ok := artifacts.DescriptorForAction(candidate.Key); ok && subject.Type == "content_item" {
			itemID, err := uuid.Parse(subject.ID)
			if err != nil {
				continue
			}
			var item models.ContentItem
			if db.Where("public_id=? AND tenant_id=?", itemID, tenantID).First(&item).Error != nil || item.Status == models.ContentStatusArchived {
				continue
			}
			missing, evidenceErr := artifacts.Missing(item, descriptor.Artifact)
			if evidenceErr == nil && missing && mediaSupplyEligibilityID(episodeID, candidate.Key, itemID, episode.EvidenceDigest) == candidate.ID {
				return itemID, nil
			}
		}
		if candidate.Key == supply.SupplyActionPipelineResumeExactStage && subject.Type == "content_item" {
			itemID, err := uuid.Parse(subject.ID)
			if err != nil {
				continue
			}
			if current, err := pipeline.CandidateForItem(db, tenantID, itemID); err == nil && pipeline.CheckCandidate(db, current) == nil && mediaSupplyEligibilityID(episodeID, candidate.Key, itemID, episode.EvidenceDigest) == candidate.ID {
				return itemID, nil
			}
		}
		if candidate.Key == supply.SupplyActionVerifyEffect && subject.Type == "source_run_request" {
			requestID, err := uuid.Parse(subject.ID)
			if err != nil {
				continue
			}
			var unit models.SourceRunExecutionUnit
			if err := db.Where("tenant_id = ? AND source_run_request_id = ? AND state = ? AND effect_started_at IS NOT NULL", tenantID, requestID, string(supply.UnitVerificationRequired)).Order("created_at ASC").First(&unit).Error; err != nil {
				continue
			}
			if mediaSupplyEligibilityID(episodeID, candidate.Key, unit.PublicID, episode.EvidenceDigest) == candidate.ID {
				return unit.PublicID, nil
			}
		}
		if candidate.Key == supply.SupplyActionTransferUnitLease && subject.Type == "source_run_request" {
			requestID, err := uuid.Parse(subject.ID)
			if err != nil {
				continue
			}
			var unit models.SourceRunExecutionUnit
			if err := db.Where("tenant_id = ? AND source_run_request_id = ? AND state = ? AND effect_started_at IS NULL AND cancellation_requested_at IS NULL AND execution_lease_expires_at <= ?", tenantID, requestID, string(supply.UnitAccepted), time.Now().UTC()).Order("created_at ASC").First(&unit).Error; err != nil {
				continue
			}
			var receipts int64
			receiptQuery := db.Model(&models.SourceRunReceipt{}).Where("tenant_id = ? AND execution_unit_id = ?", tenantID, unit.PublicID).Count(&receipts)
			if receiptQuery.Error != nil || receipts != 0 {
				continue
			}
			if mediaSupplyEligibilityID(episodeID, candidate.Key, unit.PublicID, episode.EvidenceDigest) == candidate.ID {
				return unit.PublicID, nil
			}
		}
		if candidate.Key == supply.SupplyActionFinalizeVerifiedNoChange && subject.Type == "source_run_request" {
			requestID, err := uuid.Parse(subject.ID)
			if err != nil {
				continue
			}
			var unit models.SourceRunExecutionUnit
			if err := db.Where("tenant_id = ? AND source_run_request_id = ? AND state = ? AND terminal_outcome = ?", tenantID, requestID, string(supply.UnitSucceeded), string(supply.OutcomeNoChange)).Order("created_at ASC").First(&unit).Error; err != nil {
				continue
			}
			var proofs int64
			proofQuery := db.Model(&models.SourceRunVerificationTask{}).Where("tenant_id = ? AND execution_unit_id = ? AND state = ? AND terminal_verdict = ?", tenantID, unit.PublicID, models.SourceRunVerificationTaskTerminal, string(supply.VerdictPresent)).Count(&proofs)
			if proofQuery.Error != nil || proofs == 0 {
				continue
			}
			if mediaSupplyEligibilityID(episodeID, candidate.Key, unit.PublicID, episode.EvidenceDigest) == candidate.ID {
				return unit.PublicID, nil
			}
		}
		if candidate.Key == supply.SupplyActionAdoptUnitJob && subject.Type == "source_run_request" {
			requestID, err := uuid.Parse(subject.ID)
			if err != nil {
				continue
			}
			var unit models.SourceRunExecutionUnit
			if err := db.Where("tenant_id = ? AND source_run_request_id = ? AND unit_type = ? AND state = ? AND effect_started_at IS NULL", tenantID, requestID, "coordinator", string(supply.UnitAuthorized)).Order("created_at ASC").First(&unit).Error; err != nil {
				continue
			}
			var attempt models.SourceRunAttempt
			if err := db.Where("tenant_id = ? AND public_id = ? AND state = ? AND root_execution_unit_id = ?", tenantID, unit.SourceRunAttemptID, string(supply.AttemptAuthorized), unit.PublicID).First(&attempt).Error; err != nil {
				continue
			}
			if mediaSupplyEligibilityID(episodeID, candidate.Key, unit.PublicID, episode.EvidenceDigest) == candidate.ID {
				return unit.PublicID, nil
			}
		}
		if candidate.Key == supply.SupplyActionRedeliverReceipt && subject.Type == "source_run_request" {
			requestID, err := uuid.Parse(subject.ID)
			if err != nil {
				continue
			}
			var retained []models.SourceRunRetainedReceipt
			if err := db.Where("tenant_id = ? AND source_run_request_id = ? AND state = ?", tenantID, requestID, "retained").Order("created_at ASC").Find(&retained).Error; err != nil {
				continue
			}
			for _, receipt := range retained {
				if mediaSupplyEligibilityID(episodeID, candidate.Key, receipt.PublicID, episode.EvidenceDigest) == candidate.ID {
					return receipt.PublicID, nil
				}
			}
		}
		if candidate.Key == supply.SupplyActionReclaimDispatchClaim && subject.Type == "source_run_request" {
			requestID, err := uuid.Parse(subject.ID)
			if err != nil {
				continue
			}
			var attempt models.SourceRunAttempt
			if err := db.Where("tenant_id = ? AND source_run_request_id = ? AND state = ? AND dispatcher_lease_expires_at <= ?", tenantID, requestID, string(supply.AttemptClaimed), time.Now().UTC()).Order("created_at ASC").First(&attempt).Error; err != nil {
				continue
			}
			var started int64
			if err := db.Model(&models.SourceRunExecutionUnit{}).Where("tenant_id = ? AND source_run_attempt_id = ? AND effect_started_at IS NOT NULL", tenantID, attempt.PublicID).Count(&started).Error; err != nil || started != 0 {
				continue
			}
			if mediaSupplyEligibilityID(episodeID, candidate.Key, attempt.PublicID, episode.EvidenceDigest) == candidate.ID {
				return attempt.PublicID, nil
			}
		}
		if candidate.Key == supply.SupplyActionRepairMissedAdmission && subject.Type == "content_source" {
			sourceID, err := uuid.Parse(subject.ID)
			if err != nil {
				continue
			}
			var source models.ContentSource
			if err := db.Where("public_id = ? AND tenant_id = ? AND is_active = TRUE AND next_due_at IS NOT NULL AND next_due_at <= ? AND (intake_circuit_until IS NULL OR intake_circuit_until <= ?)", sourceID, tenantID, time.Now().UTC(), time.Now().UTC()).First(&source).Error; err != nil {
				continue
			}
			var active int64
			if err := db.Model(&models.SourceRunRequest{}).Where("tenant_id = ? AND content_source_id = ? AND state IN ?", tenantID, source.PublicID, []string{string(supply.RequestRequested), string(supply.RequestAccepted), string(supply.RequestRunning), string(supply.RequestVerificationRequired)}).Count(&active).Error; err != nil || active != 0 {
				continue
			}
			if mediaSupplyEligibilityID(episodeID, candidate.Key, source.PublicID, episode.EvidenceDigest) == candidate.ID {
				return source.PublicID, nil
			}
		}
		if candidate.Key != supply.SupplyActionCancelUnstarted {
			continue
		}
		if subject.Type == "source_run_request" {
			requestID, err := uuid.Parse(subject.ID)
			if err != nil {
				continue
			}
			var unit models.SourceRunExecutionUnit
			if err := db.Where("tenant_id = ? AND source_run_request_id = ? AND state = ? AND effect_started_at IS NULL", tenantID, requestID, string(supply.UnitAuthorized)).Order("created_at ASC").First(&unit).Error; err == nil && mediaSupplyEligibilityID(episodeID, candidate.Key, unit.PublicID, episode.EvidenceDigest) == candidate.ID {
				return unit.PublicID, nil
			}
		}
	}
	return uuid.Nil, gorm.ErrRecordNotFound
}

func operatorSupplyEligibleCandidate(db *gorm.DB, tenantID string, episodeID uuid.UUID, actionKey string) (mediaSupplyEligibleAction, uuid.UUID, error) {
	items, err := mediaSupplyEpisodeEligibleActions(db, tenantID, episodeID)
	if err != nil {
		return mediaSupplyEligibleAction{}, uuid.Nil, err
	}
	matches := make([]mediaSupplyEligibleAction, 0, 1)
	for _, candidate := range items {
		if candidate.Key == actionKey && !candidate.ManualOnly && !candidate.Disabled {
			matches = append(matches, candidate)
		}
	}
	if len(matches) != 1 {
		return mediaSupplyEligibleAction{}, uuid.Nil, fmt.Errorf("Operator Supply recovery requires one unambiguous current candidate")
	}
	targetID, err := mediaSupplyEligibleTargetID(db, tenantID, episodeID, matches[0])
	return matches[0], targetID, err
}

// queueOperatorMediaSupplyRecovery converts one already-approved Operator plan
// into the same native Supply preview/approval ledger used by the cockpit. The
// episode is the only plan target; CMS re-derives the native item/run target.
func queueOperatorMediaSupplyRecovery(db *gorm.DB, tenantID, actorID string, access operatorpkg.AccessSnapshot, actionKey string, episodeID uuid.UUID) (models.MediaSupplyActionRequest, error) {
	if err := supply.ValidateSupplyActionApprovalSnapshot(access, actorID, tenantID); err != nil {
		return models.MediaSupplyActionRequest{}, err
	}
	accessVersion := access.AccessVersion
	candidate, targetID, err := operatorSupplyEligibleCandidate(db, tenantID, episodeID, actionKey)
	if err != nil {
		return models.MediaSupplyActionRequest{}, err
	}
	var episode models.MediaSupplyEpisode
	if err := db.Where("public_id=? AND tenant_id=?", episodeID, tenantID).First(&episode).Error; err != nil {
		return models.MediaSupplyActionRequest{}, err
	}
	policy := sha256.Sum256([]byte("media-supply-action-policy/v1\n" + candidate.Key + "\n" + candidate.TargetType))
	preflight, _ := json.Marshal(map[string]any{"schema_version": "media-supply-action-preflight/v1", "episode_id": episodeID.String(), "eligibility_id": candidate.ID, "evidence_digest": episode.EvidenceDigest, "initiator": "operator_signed_plan"})
	planned, _ := json.Marshal(map[string]any{"action_key": candidate.Key, "target_type": candidate.TargetType, "verification": "CMS-derived terminal state"})
	subjects, _ := json.Marshal([]map[string]string{{"type": candidate.TargetType, "id": targetID.String()}})
	preview, err := supply.CreateSupplyActionPreview(db, supply.CreateSupplyActionPreviewInput{TenantID: tenantID, ActionKey: candidate.Key, TargetType: candidate.TargetType, TargetID: targetID, EvidenceDigest: episode.EvidenceDigest, PolicyDigest: hex.EncodeToString(policy[:]), CreatedBy: actorID, CreatedAccessVersion: accessVersion, PreflightEvidence: datatypes.JSON(preflight), PlannedEffects: datatypes.JSON(planned), AffectedSubjects: datatypes.JSON(subjects), DeepLinks: datatypes.JSON([]byte(`["/platform/media/circulation"]`))})
	if err != nil {
		return models.MediaSupplyActionRequest{}, err
	}
	var request models.MediaSupplyActionRequest
	err = db.Transaction(func(tx *gorm.DB) error {
		var lockedPreview models.MediaSupplyActionPreview
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id = ? AND tenant_id = ?", preview.PublicID, tenantID).First(&lockedPreview).Error; err != nil {
			return err
		}
		if err := revalidateMediaSupplyActionPreview(tx, tenantID, lockedPreview); err != nil {
			return err
		}
		var created bool
		var approveErr error
		request, created, approveErr = supply.ApproveSupplyActionPreview(tx, tenantID, preview.PublicID.String(), actorID, accessVersion, preview.EvidenceDigest)
		if approveErr != nil || !created {
			return approveErr
		}
		return initializeApprovedMediaSupplyAction(tx, request)
	})
	if err != nil {
		return models.MediaSupplyActionRequest{}, err
	}
	decorateMediaSupplyAction(&request)
	return request, nil
}

// initializeApprovedMediaSupplyAction is part of the approval transaction.
// A queued action never becomes visible to an owner before its exact native
// request/repair exists; rollback removes both records on any stale preflight.
func initializeApprovedMediaSupplyAction(db *gorm.DB, request models.MediaSupplyActionRequest) error {
	switch request.ActionKey {
	case supply.SupplyActionPipelineResumeExactStage:
		_, err := pipeline.CreateApprovedRepair(db, request)
		return err
	case supply.SupplyActionAtomizationExecuteExactParent:
		_, err := atomizationwork.CreateApproved(db, request)
		return err
	case supply.SupplyActionFeedGenerationAttachVerifiedMember:
		_, err := supply.CreateApprovedFeedMembershipRepair(db, request)
		return err
	default:
		if _, ok := artifacts.DescriptorForAction(request.ActionKey); ok {
			_, err := artifacts.CreateApproved(db, request)
			return err
		}
		return nil
	}
}

func ConfirmMediaSupplyActionPreview(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	accessVersion, ok := currentMediaSupplyActionAccess(c, principal)
	if !ok {
		return
	}
	var body struct {
		Confirmation string `json:"confirmation"`
	}
	if err := decodeStrictJSON(c, &body); err != nil || strings.TrimSpace(body.Confirmation) != "CONFIRM" {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Explicit CONFIRM confirmation is required"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	previewID := c.Param("id")
	var preview models.MediaSupplyActionPreview
	if err := db.Where("public_id = ? AND tenant_id = ?", previewID, principal.TenantID).First(&preview).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"message": "Supply action preview not found"})
		return
	}
	var request models.MediaSupplyActionRequest
	var created bool
	err := db.Transaction(func(tx *gorm.DB) error {
		var lockedPreview models.MediaSupplyActionPreview
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id = ? AND tenant_id = ?", previewID, principal.TenantID).First(&lockedPreview).Error; err != nil {
			return err
		}
		if err := revalidateMediaSupplyActionPreview(tx, principal.TenantID, lockedPreview); err != nil {
			return err
		}
		var approveErr error
		request, created, approveErr = supply.ApproveSupplyActionPreview(tx, principal.TenantID, previewID, principal.UserID, accessVersion, preview.EvidenceDigest)
		if approveErr != nil || !created {
			return approveErr
		}
		return initializeApprovedMediaSupplyAction(tx, request)
	})
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"message": "Supply action approval preconditions changed"})
		return
	}
	decorateMediaSupplyAction(&request)
	c.JSON(http.StatusAccepted, request)
}

func GetMediaSupplyAction(c *gin.Context)    { mediaSupplyActionResponse(c, false) }
func CancelMediaSupplyAction(c *gin.Context) { mediaSupplyActionResponse(c, true) }

func decorateMediaSupplyAction(request *models.MediaSupplyActionRequest) {
	if request == nil {
		return
	}
	if descriptor, ok := supply.SupplyAction(request.ActionKey); ok {
		request.AffectedDomains = append([]string(nil), descriptor.AffectedDomains...)
	}
}

func mediaSupplyActionResponse(c *gin.Context, cancel bool) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var request models.MediaSupplyActionRequest
	if err := db.Where("public_id = ? AND tenant_id = ?", c.Param("id"), principal.TenantID).First(&request).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"message": "Supply action not found"})
		return
	}
	if cancel {
		var updated models.MediaSupplyActionRequest
		err := db.Transaction(func(tx *gorm.DB) error {
			if request.ActionKey == supply.SupplyActionPipelineResumeExactStage {
				if err := pipeline.CancelByAction(tx, principal.TenantID, request.PublicID, principal.UserID); err != nil {
					return err
				}
			}
			if _, artifactAction := artifacts.DescriptorForAction(request.ActionKey); artifactAction {
				if err := artifacts.CancelByAction(tx, principal.TenantID, request.PublicID, principal.UserID); err != nil {
					return err
				}
			}
			if request.ActionKey == supply.SupplyActionAtomizationExecuteExactParent {
				if err := atomizationwork.CancelByAction(tx, principal.TenantID, request.PublicID, principal.UserID); err != nil {
					return err
				}
			}
			if request.ActionKey == supply.SupplyActionFeedGenerationAttachVerifiedMember {
				if err := supply.CancelFeedMembershipRepairByAction(tx, principal.TenantID, request.PublicID); err != nil {
					return err
				}
			}
			var err error
			updated, err = supply.CancelSupplyAction(tx, principal.TenantID, request.PublicID.String(), principal.UserID)
			return err
		})
		if err != nil {
			c.JSON(http.StatusConflict, gin.H{"message": "Supply action cannot be cancelled"})
			return
		}
		request = updated
	}
	decorateMediaSupplyAction(&request)
	c.JSON(http.StatusOK, request)
}

func ListMediaSupplyActionEvents(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var request models.MediaSupplyActionRequest
	if err := db.Where("public_id = ? AND tenant_id = ?", c.Param("id"), principal.TenantID).First(&request).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"message": "Supply action not found"})
		return
	}
	after := int64(0)
	if raw := strings.TrimSpace(c.Query("after")); raw != "" {
		if _, err := fmt.Sscan(raw, &after); err != nil || after < 0 {
			c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid action event cursor"})
			return
		}
	}
	var events []models.MediaSupplyActionEvent
	if err := db.Where("tenant_id = ? AND action_request_id = ? AND sequence > ?", principal.TenantID, request.PublicID, after).Order("sequence ASC").Limit(100).Find(&events).Error; err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Supply action events are unavailable"})
		return
	}
	next := after
	if len(events) > 0 {
		next = events[len(events)-1].Sequence
	}
	c.JSON(http.StatusOK, gin.H{"id": request.PublicID, "state": request.State, "events": events, "next_sequence": next})
}
