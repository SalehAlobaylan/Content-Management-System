package controllers

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"content-management-system/src/contentreset"
	"content-management-system/src/lifecycle"
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Qualification is a reviewed release property, never an operator override.
// An owner must have both an installed adapter and a durable qualification row
// whose version matches this build. The registry ships empty, so all effects
// stay disabled until the qualification suite is run and recorded.
var contentResetOwners = map[contentreset.Contract]contentreset.Owner{
	(contentResetPrepareOwner{}).Contract():             contentResetPrepareOwner{},
	(contentResetPodsCandidateOwner{}).Contract():       contentResetPodsCandidateOwner{},
	(contentResetNewsCandidateOwner{}).Contract():       contentResetNewsCandidateOwner{},
	(contentResetPodsRetireOwner{}).Contract():          contentResetPodsRetireOwner{},
	(contentResetNewsRetireOwner{}).Contract():          contentResetNewsRetireOwner{},
	(contentResetServingOwner{lane: "news"}).Contract(): contentResetServingOwner{lane: "news"},
	(contentResetServingOwner{lane: "pods"}).Contract(): contentResetServingOwner{lane: "pods"},
	(contentResetReplayBranchOwner{}).Contract():        contentResetReplayBranchOwner{},
	(contentResetReplayPageOwner{}).Contract():          contentResetReplayPageOwner{},
	(contentResetProviderReleaseOwner{}).Contract():     contentResetProviderReleaseOwner{},
	(contentResetHandoffOwner{}).Contract():             contentResetHandoffOwner{},
	(contentResetPauseOwner{}).Contract():               contentResetPauseOwner{},
	(contentResetPublishOwner{}).Contract():             contentResetPublishOwner{},
	(contentResetRollbackOwner{}).Contract():            contentResetRollbackOwner{},
	(contentResetCleanupOwner{}).Contract():             contentResetCleanupOwner{},
	(contentResetReadinessOwner{}).Contract():           contentResetReadinessOwner{},
	(contentResetCompleteOwner{}).Contract():            contentResetCompleteOwner{},
}

type contentResetExecutionContract struct {
	Environment    contentreset.Environment `json:"environment"`
	Version        string                   `json:"version"`
	Owners         []contentreset.Contract  `json:"owners"`
	Qualifications []string                 `json:"qualifications"`
}

func requiredContentResetContracts(req contentResetPlanRequest) []contentreset.Contract {
	contracts := []contentreset.Contract{
		{Owner: "cms/content-reset", Effect: "prepare", TargetType: "campaign", Version: "v1"},
		{Owner: "cms/content-reset", Effect: "verify_complete", TargetType: "campaign", Version: "v1"},
		{Owner: "cms/storage", Effect: "cleanup_exact", TargetType: "manifest_batch", Version: "v1"},
	}
	lanes := []string{req.Lane}
	if req.Lane == "both" {
		lanes = []string{"news", "pods"}
	}
	for _, lane := range lanes {
		contracts = append(contracts, contentreset.Contract{Owner: "cms/" + lane, Effect: "retire_exact", TargetType: "manifest_batch", Version: "v1"}, contentreset.Contract{Owner: "cms/" + lane, Effect: "verify_serving", TargetType: "campaign", Version: "v1"})
		if req.Operation == "fresh_start" {
			contracts = append(contracts, contentreset.Contract{Owner: "cms/" + lane, Effect: "build_candidate", TargetType: "campaign", Version: "v1"})
		}
	}
	if req.Operation == "empty" {
		contracts = append(contracts, contentreset.Contract{Owner: "cms/source-run", Effect: "acquire_intake_pause", TargetType: "campaign", Version: "v1"})
	}
	if req.Operation == "fresh_start" {
		contracts = append(contracts, contentreset.Contract{Owner: "cms/feedstate", Effect: "publish", TargetType: "campaign", Version: "v1"})
		contracts = append(contracts, contentreset.Contract{Owner: "cms/content-reset", Effect: "verify_readiness", TargetType: "campaign", Version: "v1"})
		contracts = append(contracts, contentreset.Contract{Owner: "cms/source-run", Effect: "prepare_replay_branch", TargetType: "source", Version: "v1"})
		contracts = append(contracts, contentreset.Contract{Owner: "cms/source-run", Effect: "replay_page", TargetType: "source_branch", Version: "v1"})
		contracts = append(contracts, contentreset.Contract{Owner: "cms/source-run", Effect: "release_provider_slot", TargetType: "source_branch", Version: "v1"})
		contracts = append(contracts, contentreset.Contract{Owner: "cms/source-run", Effect: "replay_and_handoff", TargetType: "source_branch", Version: "v1"})
		if req.CapacityStrategy == "build_first" {
			contracts = append(contracts, contentreset.Contract{Owner: "cms/feedstate", Effect: "rollback", TargetType: "campaign", Version: "v1"})
		}
	}
	if req.InteractionPolicy == "preserve_history" {
		contracts = append(contracts, contentreset.Contract{Owner: "cms/history", Effect: "retire_references", TargetType: "manifest_batch", Version: "v1"})
	}
	if req.NewsAvailabilityExceptionRequested {
		contracts = append(contracts, contentreset.Contract{Owner: "cms/news", Effect: "availability_exception", TargetType: "campaign", Version: "v1"})
	}
	return contracts
}

func contentResetContractFor(req contentResetPlanRequest) (contentResetExecutionContract, []contentResetBlocker) {
	contract := contentResetExecutionContract{Version: "content-reset/v1", Owners: requiredContentResetContracts(req), Qualifications: []string{}}
	blockers := []contentResetBlocker{}
	if req.Operation == "fresh_start" && req.Replay.Mode != "bounded_recent" && req.Replay.Mode != "available_history" {
		blockers = append(blockers, contentResetBlocker{Code: "replay_boundary_contract_missing", Owner: "cms/source-run", Reason: "This replay mode needs an observed start boundary or exact refetch contract that is not installed", NextAction: "Implement and qualify the selected replay mode before approving its preview"})
	}
	if req.Operation == "fresh_start" && (req.Lane == "pods" || req.Lane == "both") && !contentResetStagedDeliveryPrivate() {
		blockers = append(blockers, contentResetBlocker{
			Code: "staged_delivery_unqualified", Owner: "cms/storage",
			Reason:     "Candidate Pods media is uploaded to the ordinary public delivery path; no private or signed candidate-delivery boundary (including thumbnails and HLS segments) is installed.",
			NextAction: "Qualify a private/signed staged-delivery boundary and its publication/abandonment transitions before approving a Pods replacement.",
		})
	}
	for _, owner := range contract.Owners {
		adapter, installed := contentResetOwners[owner]
		qualification := contentResetQualificationValue(owner)
		if !installed || adapter == nil || adapter.Contract() != owner {
			blockers = append(blockers, contentResetBlocker{Code: "owner_contract_missing", Owner: owner.Owner, Reason: owner.Effect + " has no installed " + owner.Version + " campaign contract", NextAction: "Implement and qualify this owner contract before creating a new approval preview"})
		} else if qualification == "" {
			blockers = append(blockers, contentResetBlocker{Code: "owner_contract_unqualified", Owner: owner.Owner, Reason: owner.Effect + " is installed but lacks release qualification", NextAction: "Qualify this owner contract before creating a new approval preview"})
		}
		contract.Qualifications = append(contract.Qualifications, qualification)
	}
	return contract, blockers
}

type contentResetControlRequest struct {
	Revision        int    `json:"revision"`
	ExpectedVersion int64  `json:"expected_version"`
	ManifestHash    string `json:"manifest_hash"`
	Confirmation    string `json:"confirmation,omitempty"`
	ReauthProof     string `json:"reauth_proof,omitempty"`
	Reason          string `json:"reason"`
	StepID          string `json:"step_id,omitempty"`
}

type contentResetControlResponse struct {
	CampaignID uuid.UUID                    `json:"campaign_id"`
	State      string                       `json:"state"`
	Execution  models.ContentResetExecution `json:"execution"`
}

type contentResetControlError struct {
	Code, Message string
	Status        int
	Blockers      []contentResetBlocker
}

func (e *contentResetControlError) Error() string { return e.Message }
func contentResetConflict(code, message string) error {
	return &contentResetControlError{Code: code, Message: message, Status: http.StatusConflict}
}

func contentResetConfirmation(campaign models.ContentResetCampaign, revision models.ContentResetRevision) string {
	if revision.ManifestHash == nil || len(*revision.ManifestHash) < 12 {
		return ""
	}
	return strings.ToUpper(campaign.Operation) + " " + strings.ToUpper(campaign.Lane) + " " + strconv.FormatInt(revision.TargetCount, 10) + " ITEMS " + strings.ToUpper((*revision.ManifestHash)[:12])
}

func contentResetProof(principal utils.AdminPrincipal, campaign models.ContentResetCampaign, req contentResetControlRequest, action string) (string, error) {
	secret, err := utils.GetJWTSecret()
	if err != nil {
		return "", err
	}
	claims, err := utils.ParseContentResetReauthProof(req.ReauthProof, secret)
	if err != nil || claims.UserID != principal.UserID || claims.TenantID != principal.TenantID || claims.PlanID != campaign.PublicID.String() || claims.ManifestHash != req.ManifestHash || claims.Action != action {
		return "", &contentResetControlError{Code: "reauth_required", Message: "Fresh reauthentication for this campaign, manifest, and action is required", Status: http.StatusForbidden}
	}
	return contentreset.Hash([]string{claims.Issuer, claims.ID}), nil
}

func writeContentResetControlError(c *gin.Context, err error) {
	var controlErr *contentResetControlError
	switch {
	case errors.As(err, &controlErr):
		c.JSON(controlErr.Status, gin.H{"error": controlErr.Message, "code": controlErr.Code, "blockers": controlErr.Blockers})
	case errors.Is(err, gorm.ErrRecordNotFound):
		c.JSON(http.StatusNotFound, gin.H{"error": "campaign or execution not found"})
	case lifecycle.IsConflict(err) || lifecycle.IsIntakePaused(err):
		c.JSON(http.StatusConflict, gin.H{"error": "Another lifecycle owner holds this scope", "code": "operation_conflict"})
	default:
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Content Reset control could not be persisted", "code": "coordinator_unavailable"})
	}
}

func ApproveContentResetCampaign(c *gin.Context)      { mutateContentResetControl(c, "approve") }
func StartContentResetCampaign(c *gin.Context)        { mutateContentResetControl(c, "start") }
func PauseContentResetCampaign(c *gin.Context)        { mutateContentResetControl(c, "pause") }
func ResumeContentResetCampaign(c *gin.Context)       { mutateContentResetControl(c, "resume") }
func RevokeContentResetApproval(c *gin.Context)       { mutateContentResetControl(c, "revoke_approval") }
func RetryContentResetStep(c *gin.Context)            { mutateContentResetControl(c, "retry") }
func PublishContentResetCampaign(c *gin.Context)      { mutateContentResetControl(c, "publish") }
func RollbackContentResetCampaign(c *gin.Context)     { mutateContentResetControl(c, "rollback") }
func AuthorizeContentResetCleanup(c *gin.Context)     { mutateContentResetControl(c, "authorize-cleanup") }
func ClosePartialContentResetCampaign(c *gin.Context) { mutateContentResetControl(c, "close-partial") }
func ResumeContentResetIntake(c *gin.Context)         { mutateContentResetControl(c, "resume-intake") }

func mutateContentResetControl(c *gin.Context, action string) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "campaign not found"})
		return
	}
	key := strings.TrimSpace(c.GetHeader("Idempotency-Key"))
	var req contentResetControlRequest
	if len(key) < 8 || len(key) > 128 || decodeContentResetJSON(c, &req) != nil || req.Revision < 1 || req.ExpectedVersion < 0 || len(req.ManifestHash) != 64 || len(strings.TrimSpace(req.Reason)) < 3 || len(req.Reason) > 1000 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "revision, expected_version, manifest_hash, reason and an 8–128 character Idempotency-Key are required"})
		return
	}
	var retryStepID *uuid.UUID
	if action == "retry" {
		stepID, parseErr := uuid.Parse(req.StepID)
		if parseErr != nil || stepID == uuid.Nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "retry requires an exact step_id"})
			return
		}
		retryStepID = &stepID
	} else if req.StepID != "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "step_id is only valid for retry"})
		return
	}
	// Proof bytes are secrets and are never persisted. Intent includes the
	// authenticated actor; another admin cannot replay someone else's decision.
	hashed := req
	hashed.ReauthProof = ""
	requestHash := contentreset.Hash(struct {
		Action, Actor string
		Request       contentResetControlRequest
	}{action, principal.UserID, hashed})
	db := c.MustGet("db").(*gorm.DB)
	refreshContentResetQualifications(db)
	var response datatypes.JSON
	err = db.Transaction(func(tx *gorm.DB) error {
		decisionID := uuid.New()
		var campaign models.ContentResetCampaign
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=?", principal.TenantID, id).First(&campaign).Error; err != nil {
			return err
		}
		var previous models.ContentResetDecision
		if err := tx.Where("tenant_id=? AND campaign_id=? AND idempotency_key=?", principal.TenantID, campaign.ID, key).First(&previous).Error; err == nil {
			if previous.RequestHash != requestHash || previous.Action != action || previous.ActorID != principal.UserID {
				return contentResetConflict("idempotency_conflict", "This key was already used for a different decision")
			}
			response = previous.Response
			return nil
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		var revision models.ContentResetRevision
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND campaign_id=? AND revision=?", principal.TenantID, campaign.ID, campaign.CurrentRevision).First(&revision).Error; err != nil {
			return err
		}
		if req.Revision != campaign.CurrentRevision || revision.ManifestHash == nil || *revision.ManifestHash != req.ManifestHash {
			return contentResetConflict("plan_drift", "Campaign revision or manifest changed")
		}
		var intent contentResetPlanRequest
		if json.Unmarshal(revision.Request, &intent) != nil {
			return contentResetConflict("plan_drift", "Frozen campaign request is invalid")
		}
		var run models.ContentResetExecution
		runErr := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND campaign_id=?", principal.TenantID, campaign.ID).First(&run).Error
		if runErr != nil && !errors.Is(runErr, gorm.ErrRecordNotFound) {
			return runErr
		}
		if action == "approve" {
			if runErr == nil || req.ExpectedVersion != 0 {
				return contentResetConflict("control_version_conflict", "This campaign already has an approval; refresh its execution")
			}
		} else if runErr != nil {
			return contentResetConflict("approval_required", "Approve the current preview before controlling a run")
		} else if run.Version != req.ExpectedVersion || run.RevisionID != revision.ID || run.ManifestHash != req.ManifestHash {
			return contentResetConflict("control_version_conflict", "The execution changed; refresh before deciding")
		}
		proofAction := "control"
		switch action {
		case "approve", "start":
			proofAction = "start"
		case "publish":
			proofAction = "publish"
		case "rollback":
			proofAction = "rollback"
		case "authorize-cleanup":
			proofAction = "cleanup"
		case "resume-intake":
			proofAction = "resume_intake"
		}
		var proofHash *string
		// Pausing only stops admission. It must remain available during an IAM
		// outage; resuming or granting new authority requires fresh proof.
		if action != "pause" {
			hash, err := contentResetProof(principal, campaign, req, proofAction)
			if err != nil {
				return err
			}
			proofHash = &hash
			var used int64
			if err := tx.Model(&models.ContentResetDecision{}).Where("proof_hash=?", hash).Count(&used).Error; err != nil {
				return err
			}
			if used != 0 {
				return contentResetConflict("reauth_already_used", "This reauthentication proof already authorized a decision")
			}
		}
		now := time.Now().UTC()
		switch action {
		case "approve", "start":
			contract, blockers := contentResetContractFor(intent)
			if len(blockers) > 0 {
				return &contentResetControlError{Code: "reconstruction_unqualified", Message: "Required owner contracts are not qualified for this operation", Status: http.StatusConflict, Blockers: blockers}
			}
			contract.Environment, err = contentreset.ReadEnvironment(tx)
			if err != nil {
				return contentResetConflict("environment_unverified", "The canonical database identity, schema or writer epoch is not verified")
			}
			if action == "approve" && (campaign.State != "previewed" || revision.State != "previewed") {
				return contentResetConflict("preview_blocked", "Only an unblocked completed preview may be approved")
			}
			if action == "start" && (campaign.State != "approved" || run.StartedAt != nil || !run.ApprovalExpiresAt.After(now)) {
				return contentResetConflict("approval_expired", "A current unstarted approval is required")
			}
			validation, err := validateContentResetPreview(tx, principal.TenantID, campaign.PublicID)
			if err != nil {
				return err
			}
			if !validation.ManifestIntegrityValid || !validation.PreviewCurrent || len(validation.Issues) > 0 || len(validation.Blockers) > 0 {
				return &contentResetControlError{Code: "plan_drift", Message: "Preview evidence, protections or scope changed; create a fresh preview", Status: http.StatusConflict, Blockers: validation.Blockers}
			}
			if req.Confirmation != contentResetConfirmation(campaign, revision) {
				return contentResetConflict("confirmation_mismatch", "Typed confirmation does not match the frozen scope")
			}
			// Large manifest validation can outlive the short approval/proof
			// window. Recheck time at the actual authority boundary.
			now = time.Now().UTC()
			if revision.ExpiresAt == nil || !revision.ExpiresAt.After(now) {
				return contentResetConflict("preview_expired", "The preview expired during validation")
			}
			if _, err := contentResetProof(principal, campaign, req, proofAction); err != nil {
				return err
			}
			if action == "approve" {
				data, err := json.Marshal(contract)
				if err != nil {
					return err
				}
				run = models.ContentResetExecution{TenantID: campaign.TenantID, CampaignID: campaign.ID, RevisionID: revision.ID, ManifestHash: req.ManifestHash, Version: 1, Phase: "approved", ApprovedBy: principal.UserID, ApprovedAt: now, ApprovalExpiresAt: *revision.ExpiresAt, ContractHash: contentreset.Hash(contract), Contract: data}
				if err := tx.Create(&run).Error; err != nil {
					return err
				}
				campaign.State = "approved"
			} else {
				if run.ContractHash != contentreset.Hash(contract) {
					return contentResetConflict("owner_contract_changed", "Owner versions or qualification changed since approval")
				}
				// Reserve lane publication ownership without blocking normal
				// intake during build-first. Domain owners acquire their exact
				// write fences only at the approved destructive boundary.
				lanes := []string{campaign.Lane}
				if campaign.Lane == "both" {
					lanes = []string{"news", "pods"}
				}
				resources := make([]lifecycle.Resource, 0, len(lanes))
				for _, lane := range lanes {
					resources = append(resources, lifecycle.Resource{Type: lifecycle.ResourceLane, Key: lane})
				}
				if _, err := lifecycle.Acquire(tx, campaign.TenantID, campaign.ID, "cms/content-reset", lifecycle.PhaseFeedRecovery, resources); err != nil {
					return err
				}
				campaign.State = "executing"
				if err := tx.Model(&campaign).Update("state", campaign.State).Error; err != nil {
					return err
				}
				owners := make([]contentreset.Owner, 0, len(contentResetOwners))
				for _, owner := range contentResetOwners {
					owners = append(owners, owner)
				}
				engine, err := contentreset.NewEngine(owners...)
				if err != nil {
					return err
				}
				if err := engine.Enqueue(tx, campaign, revision, []contentreset.PlannedStep{{Key: "prepare", Contract: contract.Owners[0], TargetID: campaign.PublicID.String(), Parameters: json.RawMessage(`{}`)}}); err != nil {
					return err
				}
				run.StartedAt = &now
				run.Phase = "preparing"
				run.Version++
			}
		case "pause":
			if run.StartedAt == nil || campaign.State == "complete" || campaign.State == "cancelled" {
				return contentResetConflict("invalid_transition", "Only an active execution can be paused")
			}
			run.PauseRequested = true
			run.Version++
		case "resume":
			if run.StartedAt == nil || !run.PauseRequested || campaign.State == "complete" || campaign.State == "cancelled" {
				return contentResetConflict("invalid_transition", "Only a paused active execution can be resumed")
			}
			if run.Phase == "replay_budget_exhausted" {
				return contentResetConflict("replay_budget_exhausted", "The approved replay budget was exhausted before provider coverage was complete; a changed scope requires a new preview and approval")
			}
			contract, blockers := contentResetContractFor(intent)
			contract.Environment, err = contentreset.ReadEnvironment(tx)
			if err != nil {
				return contentResetConflict("environment_unverified", "The approved database environment is unavailable")
			}
			if len(blockers) > 0 || contentreset.Hash(contract) != run.ContractHash {
				return contentResetConflict("owner_contract_changed", "Current owner contracts do not match this approval")
			}
			var unsettled int64
			if err := tx.Model(&models.ContentResetStep{}).Where("tenant_id=? AND campaign_id=? AND state IN ?", campaign.TenantID, campaign.ID, []string{"claimed", "outcome_unknown", "failed", "blocked"}).Count(&unsettled).Error; err != nil {
				return err
			}
			if unsettled != 0 {
				return contentResetConflict("owner_outcome_unknown", "Settle in-flight and blocked owner actions before resuming")
			}
			run.PauseRequested = false
			run.Version++
			run.Phase = "running"
			if campaign.State == "partial" {
				campaign.State = "executing"
			}
		case "revoke_approval":
			if campaign.State != "approved" || run.StartedAt != nil {
				return contentResetConflict("invalid_transition", "Only an unstarted approval can be revoked")
			}
			var steps int64
			if err := tx.Model(&models.ContentResetStep{}).Where("tenant_id=? AND campaign_id=?", campaign.TenantID, campaign.ID).Count(&steps).Error; err != nil {
				return err
			}
			if steps != 0 {
				return contentResetConflict("owner_outcome_unknown", "An approval with owner steps cannot be revoked")
			}
			campaign.State = "cancelled"
			campaign.CancelledAt = &now
			run.Phase = "approval_revoked"
			run.Version++
			if err := tx.Model(&revision).Update("state", "superseded").Error; err != nil {
				return err
			}
		case "retry":
			if run.StartedAt == nil || !run.PauseRequested || campaign.State != "partial" {
				return contentResetConflict("invalid_transition", "Pause and reconcile the execution before retrying a failed step")
			}
			contract, blockers := contentResetContractFor(intent)
			contract.Environment, err = contentreset.ReadEnvironment(tx)
			if err != nil {
				return contentResetConflict("environment_unverified", "The approved database environment is unavailable")
			}
			if len(blockers) > 0 || contentreset.Hash(contract) != run.ContractHash {
				return contentResetConflict("owner_contract_changed", "Current owner contracts do not match this approval")
			}
			var step models.ContentResetStep
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND campaign_id=? AND revision_id=? AND public_id=?", campaign.TenantID, campaign.ID, revision.ID, *retryStepID).First(&step).Error; err != nil {
				return err
			}
			if !contentreset.Retryable(step) {
				return contentResetConflict("retry_not_proven_safe", "The failed owner receipt must prove that no effect was committed; admitted or unknown effects must be reconciled")
			}
			var command contentreset.Command
			if json.Unmarshal(step.Command, &command) != nil || command.CampaignID != campaign.PublicID || command.RevisionID != revision.PublicID || command.ManifestHash != run.ManifestHash {
				return contentResetConflict("retry_not_proven_safe", "The owner receipt does not match this campaign revision")
			}
			result := tx.Model(&step).Where("state='failed' AND lease_token IS NULL").Updates(map[string]any{"state": "pending", "retry_decision_id": decisionID, "lease_until": nil, "last_error": "operator_retry_authorized"})
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != 1 {
				return contentResetConflict("control_version_conflict", "The failed step changed before retry admission")
			}
			run.Version++
			run.Phase = "paused"
		case "publish":
			if campaign.Operation != "fresh_start" || campaign.State != "executing" || run.StartedAt == nil || run.PublishedAt != nil || run.RolledBackAt != nil || run.PauseRequested {
				return contentResetConflict("invalid_transition", "Only an active replacement awaiting publication can be published")
			}
			readiness, readinessErr := verifyContentResetReplacementReadiness(tx, campaign, revision, contentreset.Command{
				CommandID: decisionID, CampaignID: campaign.PublicID, RevisionID: revision.PublicID,
				TenantID: campaign.TenantID, ManifestHash: run.ManifestHash,
			})
			if readinessErr != nil {
				return readinessErr
			}
			if readiness.State != "succeeded" {
				return &contentResetControlError{Code: "replacement_incomplete", Message: "The replacement is not ready for publication", Status: http.StatusConflict, Blockers: []contentResetBlocker{{Code: readiness.ReasonCode, Owner: "cms/content-reset", Reason: string(readiness.Evidence), NextAction: "Resolve the replacement blocker before publication"}}}
			}
			if req.Confirmation != contentResetMilestoneConfirmation("publish", campaign, revision) {
				return contentResetConflict("confirmation_mismatch", "Typed publication confirmation does not match the frozen scope")
			}
			fences, fenceErr := captureContentResetPublicationFences(tx, principal.TenantID, campaign)
			if fenceErr != nil {
				return &contentResetControlError{Code: "replacement_incomplete", Message: "Publication fences are not stable: " + fenceErr.Error(), Status: http.StatusConflict}
			}
			retainOldView := intent.CapacityStrategy != "clear_first"
			nowPublication := time.Now().UTC()
			recoveryDeadline := nowPublication.Add(contentResetRecoveryWindow)
			if !retainOldView {
				recoveryDeadline = nowPublication
			}
			if _, err := writeContentResetMilestone(tx, principal.TenantID, campaign, revision, "publication", contentResetPublicationPayload{
				Fences: fences, RecoveryDeadline: recoveryDeadline,
				CleanupAuthorizedUntil: nowPublication.Add(contentResetCleanupAuthorizationWindow),
				RetainOldView:          retainOldView, ReadinessHash: contentResetReadinessHash(readiness),
			}, principal.Email); err != nil {
				return err
			}
			run.Version++
			run.Phase = "awaiting_publication"
		case "rollback":
			if campaign.Operation != "fresh_start" || intent.CapacityStrategy != "build_first" || run.PublishedAt == nil || run.RolledBackAt != nil || run.CleanupNotBefore == nil || !run.CleanupNotBefore.After(time.Now().UTC()) {
				return contentResetConflict("invalid_transition", "Rollback requires a published build-first replacement inside its recovery window")
			}
			if req.Confirmation != contentResetMilestoneConfirmation("rollback", campaign, revision) {
				return contentResetConflict("confirmation_mismatch", "Typed rollback confirmation does not match the frozen scope")
			}
			fences, fenceErr := captureContentResetRollbackFences(tx, principal.TenantID, campaign)
			if fenceErr != nil {
				return &contentResetControlError{Code: "rollback_unavailable", Message: "Rollback is unavailable: " + fenceErr.Error(), Status: http.StatusConflict}
			}
			if _, err := writeContentResetMilestone(tx, principal.TenantID, campaign, revision, "rollback", contentResetRollbackPayload{
				Fences: fences, RollbackDeadline: *run.CleanupNotBefore,
			}, principal.Email); err != nil {
				return err
			}
			engine, engineErr := contentResetEngine()
			if engineErr != nil {
				return engineErr
			}
			if err := engine.Enqueue(tx, campaign, revision, []contentreset.PlannedStep{{
				Key: "rollback", Contract: (contentResetRollbackOwner{}).Contract(), TargetID: campaign.PublicID.String(),
				Parameters: json.RawMessage(`{}`), Requires: []string{"verify/readiness"},
			}}); err != nil {
				return err
			}
			run.Version++
			run.Phase = "rolling_back"
		case "authorize-cleanup":
			if run.PublishedAt == nil || run.RolledBackAt != nil || run.StartedAt == nil {
				return contentResetConflict("invalid_transition", "Cleanup authorization requires a published, not-rolled-back replacement")
			}
			if campaign.State == "complete" || campaign.State == "cancelled" {
				return contentResetConflict("invalid_transition", "This campaign has already reached a terminal state")
			}
			deadline := time.Now().UTC().Add(contentResetCleanupAuthorizationWindow)
			run.CleanupAuthorizedUntil = &deadline
			run.PauseRequested = false
			run.Phase = "cleanup_window"
			run.Version++
		case "resume-intake":
			if campaign.Operation != "empty" || campaign.State != "complete" || run.CompletedAt == nil || run.PublishedAt != nil || run.RolledBackAt != nil {
				return contentResetConflict("invalid_transition", "Intake resume requires a completed Empty campaign")
			}
			var activePauses int64
			if err := tx.Model(&models.ContentResetIntakePause{}).Where("tenant_id=? AND campaign_id=? AND state='active'", campaign.TenantID, campaign.ID).Count(&activePauses).Error; err != nil {
				return err
			}
			if activePauses == 0 {
				return contentResetConflict("invalid_transition", "This Empty campaign has no active intake pause to release")
			}
			if req.Confirmation != contentResetMilestoneConfirmation("resume intake", campaign, revision) {
				return contentResetConflict("confirmation_mismatch", "Typed intake resume confirmation does not match the frozen scope")
			}
			if err := releaseContentResetIntakePauses(tx, campaign); err != nil {
				return err
			}
			if err := appendContentResetEvidence(tx, campaign, &revision, "intake/resumed/"+key, "operator_intake_resume", "cms/content-reset", map[string]any{
				"actor_id": principal.UserID, "reason": req.Reason, "restart_policy": "resume_live_checkpoints",
				"released_campaign_pauses": activePauses,
			}); err != nil {
				return err
			}
			run.Phase = "intake_resumed"
			run.Version++
		case "close-partial":
			if run.StartedAt == nil || !run.PauseRequested || campaign.State == "complete" || campaign.State == "cancelled" {
				return contentResetConflict("invalid_transition", "Only a paused active execution can be closed as partial")
			}
			var unsettled int64
			if err := tx.Model(&models.ContentResetStep{}).Where("tenant_id=? AND campaign_id=? AND state IN ?", campaign.TenantID, campaign.ID, []string{"claimed", "outcome_unknown", "waiting", "pending", "deferred"}).Count(&unsettled).Error; err != nil {
				return err
			}
			if unsettled != 0 {
				return contentResetConflict("owner_outcome_unknown", "Settle in-flight owner actions before closing the campaign as partial")
			}
			var residual []models.ContentResetStep
			if err := tx.Where("tenant_id=? AND campaign_id=? AND state IN ?", campaign.TenantID, campaign.ID, []string{"failed", "blocked"}).Order("id").Find(&residual).Error; err != nil {
				return err
			}
			residualKeys := make([]string, 0, len(residual))
			for _, step := range residual {
				residualKeys = append(residualKeys, step.StepKey)
			}
			if err := tx.Model(&models.ContentResetMilestone{}).Where("tenant_id=? AND campaign_id=? AND state='active'", campaign.TenantID, campaign.ID).
				Updates(map[string]any{"state": "revoked", "revoked_at": time.Now().UTC()}).Error; err != nil {
				return err
			}
			if err := releaseContentResetIntakePauses(tx, campaign); err != nil {
				return err
			}
			if err := releaseContentResetLaneClaims(tx, campaign); err != nil {
				return err
			}
			campaign.State = "closed_partial"
			run.Phase = "closed_partial"
			run.Version++
			var residualStaged int64
			if tx.Migrator().HasTable(&models.SourceItemInstance{}) {
				if err := tx.Model(&models.SourceItemInstance{}).Where("tenant_id=? AND campaign_id=? AND state='staged'", campaign.TenantID, campaign.ID).Count(&residualStaged).Error; err != nil {
					return err
				}
			}
			if err := appendContentResetEvidence(tx, campaign, &revision, "close-partial/"+key, "campaign_closed_partial", "cms/content-reset", map[string]any{
				"actor_id": principal.UserID, "reason": req.Reason, "residual_steps": residualKeys,
				"residual_staged_instances":   residualStaged,
				"candidate_delivery_residual": residualStaged > 0,
				"candidate_delivery_policy":   "withheld_urls_no_private_boundary",
			}); err != nil {
				return err
			}
		default:
			return errors.New("unregistered Content Reset control")
		}
		if err := tx.Model(&campaign).Updates(map[string]any{"state": campaign.State, "cancelled_at": campaign.CancelledAt}).Error; err != nil {
			return err
		}
		if action != "approve" {
			if err := tx.Model(&run).Updates(map[string]any{"version": run.Version, "phase": run.Phase, "pause_requested": run.PauseRequested, "started_at": run.StartedAt, "cleanup_authorized_until": run.CleanupAuthorizedUntil}).Error; err != nil {
				return err
			}
		}
		response, err = json.Marshal(contentResetControlResponse{campaign.PublicID, campaign.State, run})
		if err != nil {
			return err
		}
		decision := models.ContentResetDecision{PublicID: decisionID, TenantID: principal.TenantID, CampaignID: campaign.ID, IdempotencyKey: key, Action: action, ActorID: principal.UserID, RequestHash: requestHash, ProofHash: proofHash, Response: response, CommandID: retryStepID}
		if err := tx.Create(&decision).Error; err != nil {
			return err
		}
		return appendContentResetEvidence(tx, campaign, &revision, "decision/"+key, "operator_decision", "cms/content-reset", map[string]any{"action": action, "actor_id": principal.UserID, "version": run.Version, "reason": req.Reason, "manifest_hash": req.ManifestHash, "request_hash": requestHash})
	}, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		writeContentResetControlError(c, err)
		return
	}
	c.Data(http.StatusOK, "application/json", response)
}

func GetContentResetExecution(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		writeContentResetControlError(c, gorm.ErrRecordNotFound)
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var campaign models.ContentResetCampaign
	if err := db.Where("tenant_id=? AND public_id=?", principal.TenantID, id).First(&campaign).Error; err != nil {
		writeContentResetControlError(c, err)
		return
	}
	var revision models.ContentResetRevision
	if err := db.Where("tenant_id=? AND campaign_id=? AND revision=?", principal.TenantID, campaign.ID, campaign.CurrentRevision).First(&revision).Error; err != nil {
		writeContentResetControlError(c, err)
		return
	}
	var intent contentResetPlanRequest
	if json.Unmarshal(revision.Request, &intent) != nil {
		writeContentResetControlError(c, errors.New("invalid intent"))
		return
	}
	_, blockers := contentResetContractFor(intent)
	var previewBlockers []contentResetBlocker
	if err := json.Unmarshal(revision.Blockers, &previewBlockers); err != nil {
		writeContentResetControlError(c, errors.New("invalid frozen preview blockers"))
		return
	}
	blockers = append(blockers, previewBlockers...)
	if campaign.State == "previewed" || campaign.State == "approved" {
		if revision.State != "previewed" || revision.ManifestHash == nil || revision.ExpiresAt == nil || !revision.ExpiresAt.After(time.Now().UTC()) {
			blockers = append(blockers, contentResetBlocker{Code: "preview_expired_or_unfrozen", Owner: "cms/content-reset", Reason: "The preview is no longer a current frozen approval boundary", NextAction: "Create and review a fresh preview"})
		}
	}
	var run *models.ContentResetExecution
	if db.Migrator().HasTable(&models.ContentResetExecution{}) {
		var record models.ContentResetExecution
		if err := db.Where("tenant_id=? AND campaign_id=?", principal.TenantID, campaign.ID).First(&record).Error; err == nil {
			run = &record
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			writeContentResetControlError(c, err)
			return
		}
	} else {
		blockers = append(blockers, contentResetBlocker{Code: "coordinator_schema_unavailable", Owner: "cms/content-reset", Reason: "The execution migration is not applied", NextAction: "Apply the reviewed canonical migration through the database runbook"})
	}
	type stepCount struct {
		State string `json:"state"`
		Count int64  `json:"count"`
	}
	counts := []stepCount{}
	if err := db.Model(&models.ContentResetStep{}).Select("state, COUNT(*) AS count").Where("tenant_id=? AND campaign_id=?", principal.TenantID, campaign.ID).Group("state").Order("state").Scan(&counts).Error; err != nil {
		writeContentResetControlError(c, err)
		return
	}
	milestones := []models.ContentResetMilestone{}
	if db.Migrator().HasTable(&models.ContentResetMilestone{}) {
		if err := db.Where("tenant_id=? AND campaign_id=? AND state='active'", principal.TenantID, campaign.ID).Order("kind").Find(&milestones).Error; err != nil {
			writeContentResetControlError(c, err)
			return
		}
	}
	canPublish, canRollback, canAuthorizeCleanup, canResumeIntake := false, false, false, false
	now := time.Now().UTC()
	if campaign.Operation == "empty" && campaign.State == "complete" && run != nil && run.CompletedAt != nil {
		var activePauses int64
		if err := db.Model(&models.ContentResetIntakePause{}).Where("tenant_id=? AND campaign_id=? AND state='active'", principal.TenantID, campaign.ID).Count(&activePauses).Error; err == nil && activePauses > 0 {
			canResumeIntake = true
		}
	}
	if run != nil && len(blockers) == 0 {
		canPublish = campaign.Operation == "fresh_start" && campaign.State == "executing" && run.StartedAt != nil &&
			run.PublishedAt == nil && run.RolledBackAt == nil && !run.PauseRequested && run.Phase == "awaiting_publication"
		canRollback = campaign.Operation == "fresh_start" && intent.CapacityStrategy == "build_first" &&
			run.PublishedAt != nil && run.RolledBackAt == nil && run.CleanupNotBefore != nil && run.CleanupNotBefore.After(now)
		canAuthorizeCleanup = run.StartedAt != nil && run.PublishedAt != nil && run.RolledBackAt == nil &&
			campaign.State != "complete" && campaign.State != "cancelled"
	}
	c.JSON(http.StatusOK, gin.H{
		"campaign_id": campaign.PublicID, "state": campaign.State, "execution": run,
		"confirmation": contentResetConfirmation(campaign, revision),
		"blockers":     blockers, "steps": counts, "milestones": milestones,
		"publication_confirmation":   contentResetMilestoneConfirmation("publish", campaign, revision),
		"rollback_confirmation":      contentResetMilestoneConfirmation("rollback", campaign, revision),
		"resume_intake_confirmation": contentResetMilestoneConfirmation("resume intake", campaign, revision),
		"can_publish":                canPublish, "can_rollback": canRollback,
		"can_authorize_cleanup": canAuthorizeCleanup, "can_resume_intake": canResumeIntake,
	})
}

// ListContentResetStagedItems exposes isolated replacement content to an
// authenticated Content Reset administrator. Public feeds, detail, playback and
// retrieval keep resolving only the active serving view, so staged media never
// becomes addressable through an ordinary consumer path.
func ListContentResetStagedItems(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		writeContentResetControlError(c, gorm.ErrRecordNotFound)
		return
	}
	lane := strings.ToLower(strings.TrimSpace(c.Query("lane")))
	if lane == "" {
		lane = "pods"
	}
	if lane != "news" && lane != "pods" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "lane must be news or pods"})
		return
	}
	cursor, err := uuid.Parse(c.DefaultQuery("cursor", uuid.Nil.String()))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid cursor"})
		return
	}
	limit, err := strconv.Atoi(c.DefaultQuery("limit", "100"))
	if err != nil || limit < 1 || limit > 200 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "limit must be between 1 and 200"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var campaign models.ContentResetCampaign
	if err := db.Where("tenant_id=? AND public_id=?", principal.TenantID, id).First(&campaign).Error; err != nil {
		writeContentResetControlError(c, err)
		return
	}
	generationLane := mapContentResetGenerationLane(lane)
	var generation models.FeedGeneration
	if err := db.Where("tenant_id=? AND lane=? AND purpose='content_reset' AND content_reset_campaign_id=?", principal.TenantID, generationLane, campaign.ID).
		Order("created_at DESC").First(&generation).Error; err != nil {
		writeContentResetControlError(c, err)
		return
	}
	type stagedItem struct {
		ID               uuid.UUID `json:"id"`
		GenerationID     uuid.UUID `json:"generation_id"`
		Lane             string    `json:"lane"`
		Type             string    `json:"type"`
		Status           string    `json:"status"`
		Title            *string   `json:"title,omitempty"`
		DurationSec      *int      `json:"duration_sec,omitempty"`
		PlaybackURL      *string   `json:"playback_url,omitempty"`
		PlaybackType     *string   `json:"playback_type,omitempty"`
		FallbackPlayback *string   `json:"fallback_playback_url,omitempty"`
		Staged           bool      `json:"staged"`
	}
	deliveryProtected := contentResetStagedDeliveryPrivate()
	deliveryNote := "Candidate origin objects are on the public media path. Playback URLs are withheld until a private or signed staged-delivery boundary is qualified."
	if deliveryProtected {
		deliveryNote = "Candidate playback is served only through the qualified private delivery boundary."
	}
	var memberships []models.FeedGenerationMembership
	if err := db.Where("generation_id=? AND member_type=? AND member_id>?", generation.PublicID, stagedMemberType(generationLane), cursor).
		Order("member_id ASC").Limit(limit + 1).Find(&memberships).Error; err != nil {
		writeContentResetControlError(c, err)
		return
	}
	more := len(memberships) > limit
	if more {
		memberships = memberships[:limit]
	}
	ids := make([]uuid.UUID, 0, len(memberships))
	next := cursor
	for _, membership := range memberships {
		ids = append(ids, membership.MemberID)
		next = membership.MemberID
	}
	result := []stagedItem{}
	if len(ids) > 0 {
		var items []models.ContentItem
		if err := db.Where("tenant_id=? AND public_id IN ?", principal.TenantID, ids).Find(&items).Error; err != nil {
			writeContentResetControlError(c, err)
			return
		}
		byID := make(map[uuid.UUID]models.ContentItem, len(items))
		for _, item := range items {
			byID[item.PublicID] = item
		}
		for _, membership := range memberships {
			item, found := byID[membership.MemberID]
			if !found {
				continue
			}
			entry := stagedItem{
				ID: item.PublicID, GenerationID: generation.PublicID, Lane: contentResetLaneForType(item.Type),
				Type: string(item.Type), Status: string(item.Status),
				Title: item.Title, DurationSec: item.DurationSec, Staged: true,
			}
			// Origin objects currently live on the public media path. Until a
			// private/signed delivery boundary is qualified, never hand out a
			// candidate URL: authenticated CMS listing is not object
			// authorization.
			if deliveryProtected {
				entry.PlaybackURL = item.PlaybackURL
				entry.PlaybackType = item.PlaybackType
				entry.FallbackPlayback = item.FallbackPlaybackURL
			}
			result = append(result, entry)
		}
	}
	c.JSON(http.StatusOK, gin.H{
		"data": result, "has_more": more, "next_cursor": next, "lane": lane,
		"generation_id": generation.PublicID, "staged": true,
		"delivery_protected": deliveryProtected,
		"delivery_note":      deliveryNote,
	})
}

func stagedMemberType(generationLane string) string {
	if generationLane == "media" {
		return "feed_unit"
	}
	return "news_item"
}

func ListContentResetSteps(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		writeContentResetControlError(c, gorm.ErrRecordNotFound)
		return
	}
	cursor, err := strconv.ParseUint(c.DefaultQuery("cursor", "0"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid cursor"})
		return
	}
	limit, err := strconv.Atoi(c.DefaultQuery("limit", "100"))
	if err != nil || limit < 1 || limit > 500 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "limit must be between 1 and 500"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var campaign models.ContentResetCampaign
	if err := db.Where("tenant_id=? AND public_id=?", principal.TenantID, id).First(&campaign).Error; err != nil {
		writeContentResetControlError(c, err)
		return
	}
	var steps []models.ContentResetStep
	if err := db.Where("tenant_id=? AND campaign_id=? AND id>?", principal.TenantID, campaign.ID, cursor).Order("id ASC").Limit(limit + 1).Find(&steps).Error; err != nil {
		writeContentResetControlError(c, err)
		return
	}
	more := len(steps) > limit
	if more {
		steps = steps[:limit]
	}
	data := make([]gin.H, 0, len(steps))
	next := cursor
	for _, step := range steps {
		// Owner command parameters, raw receipts and fencing tokens stay internal.
		data = append(data, gin.H{"id": step.PublicID, "step_key": step.StepKey, "owner": step.Owner, "effect": step.EffectType, "state": step.State, "attempts": step.AttemptCount, "reason_code": step.LastError, "updated_at": step.UpdatedAt, "retry_safe": contentreset.Retryable(step)})
		next = uint64(step.ID)
	}
	c.JSON(http.StatusOK, gin.H{"data": data, "has_more": more, "next_cursor": next, "limit": limit})
}
