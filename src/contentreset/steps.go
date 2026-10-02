// Package contentreset owns campaign command delivery. Domain effects remain
// with their existing owners; an HTTP caller cannot submit executable commands.
package contentreset

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"content-management-system/src/models"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const StepLease = 90 * time.Second

var (
	ErrFenced      = errors.New("content reset command lease lost")
	ErrUnavailable = errors.New("content reset owner contract unavailable")
	ErrPaused      = errors.New("content reset execution is paused")
)

// Contract is compiled into an owner adapter. Changing its version invalidates
// approvals made against the old owner behavior.
type Contract struct {
	Owner      string `json:"owner"`
	Effect     string `json:"effect"`
	TargetType string `json:"target_type"`
	Version    string `json:"version"`
}

type Command struct {
	CommandID    uuid.UUID       `json:"command_id"`
	CampaignID   uuid.UUID       `json:"campaign_id"`
	RevisionID   uuid.UUID       `json:"revision_id"`
	TenantID     string          `json:"tenant_id"`
	ManifestHash string          `json:"manifest_hash"`
	Contract     Contract        `json:"contract"`
	TargetID     string          `json:"target_id"`
	Parameters   json.RawMessage `json:"parameters"`
}

// Observation is an owner readback, not an HTTP/queue acknowledgement. Only a
// verified result may settle a step. Unknown results are never re-executed.
type Observation struct {
	CommandID      uuid.UUID       `json:"command_id"`
	CommandHash    string          `json:"command_hash"`
	State          string          `json:"state"` // succeeded, waiting, deferred, blocked, failed, outcome_unknown
	ReasonCode     string          `json:"reason_code"`
	Evidence       json.RawMessage `json:"evidence"`
	NoEffectProven bool            `json:"no_effect_proven,omitempty"`
}

type Owner interface {
	Contract() Contract
	Execute(context.Context, *gorm.DB, Command, uuid.UUID) (Observation, error)
	Reconcile(context.Context, *gorm.DB, Command) (Observation, error)
}

type AdmissionPolicy func(Contract, []byte) bool

type Engine struct {
	owners map[Contract]Owner
	admit  AdmissionPolicy
}

func NewEngine(owners ...Owner) (*Engine, error) {
	return NewEngineWithAdmission(nil, owners...)
}

// A release may revoke effect admission while retaining the same installed
// readback adapter. Reconciliation must remain available for admitted work.
func NewEngineWithAdmission(policy AdmissionPolicy, owners ...Owner) (*Engine, error) {
	e := &Engine{owners: make(map[Contract]Owner)}
	e.admit = policy
	for _, owner := range owners {
		if owner == nil {
			return nil, ErrUnavailable
		}
		c := owner.Contract()
		if strings.TrimSpace(c.Owner) == "" || strings.TrimSpace(c.Effect) == "" || strings.TrimSpace(c.TargetType) == "" || strings.TrimSpace(c.Version) == "" {
			return nil, ErrUnavailable
		}
		if _, exists := e.owners[c]; exists {
			return nil, errors.New("duplicate Content Reset owner contract")
		}
		e.owners[c] = owner
	}
	return e, nil
}

func Hash(v any) string {
	raw, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var canonical any
	if decoder.Decode(&canonical) != nil {
		return ""
	}
	raw, err = json.Marshal(canonical)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// PlannedStep is an internal owner-planner product, never a request DTO.
// Prerequisites must already exist, so insertion order proves acyclicity.
type PlannedStep struct {
	Key        string
	Contract   Contract
	TargetID   string
	Parameters json.RawMessage
	Requires   []string
}

// Enqueue persists a bounded page of outbox commands in the caller's transaction.
// The caller must hold the campaign row lock. Commands and dependency edges are
// immutable; repeated planner pages must match the complete previous graph.
func (e *Engine) Enqueue(tx *gorm.DB, campaign models.ContentResetCampaign, revision models.ContentResetRevision, page []PlannedStep) error {
	if tx == nil || !planningState(campaign.State) || revision.CampaignID != campaign.ID || revision.TenantID != campaign.TenantID || revision.ManifestHash == nil || len(page) == 0 || len(page) > 30 {
		return errors.New("invalid bounded Content Reset command page")
	}
	var current models.ContentResetCampaign
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND id=?", campaign.TenantID, campaign.ID).First(&current).Error; err != nil {
		return err
	}
	if !planningState(current.State) || current.CurrentRevision != revision.Revision || current.PublicID != campaign.PublicID {
		return ErrFenced
	}
	for _, planned := range page {
		if _, ok := e.owners[planned.Contract]; !ok {
			return ErrUnavailable
		}
		if len(planned.Key) == 0 || len(planned.Key) > 255 || len(planned.TargetID) == 0 || len(planned.TargetID) > 255 || !json.Valid(planned.Parameters) || len(planned.Requires) > 30 {
			return errors.New("invalid Content Reset command")
		}
		var required []uint
		seen := map[string]bool{}
		for _, key := range planned.Requires {
			if key == planned.Key || seen[key] {
				return errors.New("invalid Content Reset dependency")
			}
			seen[key] = true
			var dep models.ContentResetStep
			if err := tx.Where("tenant_id=? AND campaign_id=? AND revision_id=? AND step_key=?", campaign.TenantID, campaign.ID, revision.ID, key).First(&dep).Error; err != nil {
				return fmt.Errorf("Content Reset dependency must exist first: %w", err)
			}
			required = append(required, dep.ID)
		}
		var step models.ContentResetStep
		err := tx.Where("tenant_id=? AND campaign_id=? AND step_key=?", campaign.TenantID, campaign.ID, planned.Key).First(&step).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		id := step.PublicID
		if errors.Is(err, gorm.ErrRecordNotFound) {
			id = uuid.New()
		}
		command := Command{id, campaign.PublicID, revision.PublicID, campaign.TenantID, *revision.ManifestHash, planned.Contract, planned.TargetID, planned.Parameters}
		raw, err := json.Marshal(command)
		if err != nil {
			return err
		}
		if step.ID != 0 {
			var stored Command
			if json.Unmarshal(step.Command, &stored) != nil || Hash(stored) != Hash(command) || step.RevisionID != revision.ID {
				return errors.New("Content Reset command identity collision")
			}
			var edges []models.ContentResetStepDependency
			if err := tx.Where("tenant_id=? AND campaign_id=? AND revision_id=? AND step_id=?", campaign.TenantID, campaign.ID, revision.ID, step.ID).Find(&edges).Error; err != nil {
				return err
			}
			if len(edges) != len(required) {
				return errors.New("Content Reset dependency graph changed")
			}
			for _, edge := range edges {
				found := false
				for _, id := range required {
					found = found || id == edge.RequiresID
				}
				if !found {
					return errors.New("Content Reset dependency graph changed")
				}
			}
			continue
		}
		step = models.ContentResetStep{PublicID: id, TenantID: campaign.TenantID, CampaignID: campaign.ID, RevisionID: revision.ID, StepKey: planned.Key, Owner: planned.Contract.Owner, TargetType: planned.Contract.TargetType, TargetID: planned.TargetID, EffectType: planned.Contract.Effect, State: "pending", Command: datatypes.JSON(raw)}
		if err := tx.Create(&step).Error; err != nil {
			return err
		}
		for _, id := range required {
			if err := tx.Create(&models.ContentResetStepDependency{TenantID: campaign.TenantID, CampaignID: campaign.ID, RevisionID: revision.ID, StepID: step.ID, RequiresID: id}).Error; err != nil {
				return err
			}
		}
	}
	return nil
}

type claim struct {
	Step      models.ContentResetStep
	Command   Command
	Reconcile bool
}

func lockRun(tx *gorm.DB, tenant string, campaignID uint) (models.ContentResetCampaign, models.ContentResetExecution, error) {
	var campaign models.ContentResetCampaign
	var run models.ContentResetExecution
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND id=?", tenant, campaignID).First(&campaign).Error; err != nil {
		return campaign, run, err
	}
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND campaign_id=?", tenant, campaign.ID).First(&run).Error
	return campaign, run, err
}

func (e *Engine) acquire(db *gorm.DB, tenant string, campaignID uint) (*claim, error) {
	var result *claim
	err := db.Transaction(func(tx *gorm.DB) error {
		campaign, run, err := lockRun(tx, tenant, campaignID)
		if err != nil {
			return err
		}
		if run.ContractHash != Hash(json.RawMessage(run.Contract)) {
			return errors.New("Content Reset approval contract integrity invalid")
		}
		var approved struct {
			Owners      []Contract  `json:"owners"`
			Environment Environment `json:"environment"`
		}
		if json.Unmarshal(run.Contract, &approved) != nil {
			return ErrUnavailable
		}
		// Terminal campaigns may only reconcile an already-admitted command whose
		// receipt was lost; they can never admit a new effect. This also lets the
		// worker recover a final completion/rollback receipt after the campaign
		// committed terminally but the finish transaction was lost.
		terminalCampaign := campaign.State == "complete" || campaign.State == "closed_partial"
		if !terminalCampaign && campaign.State != "executing" && campaign.State != "partial" && campaign.State != "published" && campaign.State != "cleanup_pending" {
			return nil
		}
		now := time.Now().UTC()
		// Expiry is uncertainty. This transition is durable even while paused.
		if err := tx.Model(&models.ContentResetStep{}).Where("tenant_id=? AND campaign_id=? AND revision_id=? AND state='claimed' AND lease_until<=?", tenant, campaignID, run.RevisionID, now).Updates(map[string]any{"state": "outcome_unknown", "lease_token": nil, "lease_until": nil, "last_error": "owner_lease_expired"}).Error; err != nil {
			return err
		}
		query := tx.Model(&models.ContentResetStep{}).Where("tenant_id=? AND campaign_id=? AND revision_id=?", tenant, campaignID, run.RevisionID).
			Where("state IN ('pending','deferred','waiting','outcome_unknown') AND (lease_until IS NULL OR lease_until<=?)", now).
			Where(`NOT EXISTS (SELECT 1 FROM content_reset_step_dependencies d JOIN content_reset_steps predecessor ON predecessor.id=d.requires_id AND predecessor.tenant_id=d.tenant_id AND predecessor.campaign_id=d.campaign_id AND predecessor.revision_id=d.revision_id WHERE d.step_id=content_reset_steps.id AND d.tenant_id=content_reset_steps.tenant_id AND predecessor.state<>'succeeded')`)
		// Filter effects before selecting a row: an unqualified pending command
		// must not starve readback of a later admitted/unknown command.
		allowed := []string{}
		arguments := []any{}
		for _, contract := range approved.Owners {
			if _, installed := e.owners[contract]; !installed {
				continue
			}
			if e.admit != nil && !e.admit(contract, run.Contract) {
				continue
			}
			allowed = append(allowed, "(owner=? AND effect_type=? AND target_type=? AND command->'contract'->>'version'=?)")
			arguments = append(arguments, contract.Owner, contract.Effect, contract.TargetType, contract.Version)
		}
		if len(allowed) == 0 {
			query = query.Where("state IN ('waiting','outcome_unknown')")
		} else {
			query = query.Where("(state IN ('waiting','outcome_unknown') OR ("+strings.Join(allowed, " OR ")+"))", arguments...)
		}
		// Pausing prevents new effects but permits readback to settle effects
		// which might have completed before the pause or a worker crash.
		if run.PauseRequested {
			query = query.Where("state IN ('waiting','outcome_unknown')")
		}
		if terminalCampaign {
			query = query.Where("state IN ('waiting','outcome_unknown')")
		}
		var step models.ContentResetStep
		if err := query.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).Order("id ASC").First(&step).Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		} else if err != nil {
			return err
		}
		var command Command
		if json.Unmarshal(step.Command, &command) != nil || command.CommandID != step.PublicID || command.CampaignID != campaign.PublicID || command.TenantID != tenant || command.ManifestHash != run.ManifestHash || command.Contract.Owner != step.Owner || command.Contract.Effect != step.EffectType || command.Contract.TargetType != step.TargetType || command.TargetID != step.TargetID {
			return errors.New("Content Reset outbox identity invalid")
		}
		var revision models.ContentResetRevision
		if err := tx.Where("tenant_id=? AND campaign_id=? AND id=?", tenant, campaignID, run.RevisionID).First(&revision).Error; err != nil {
			return err
		}
		if command.RevisionID != revision.PublicID {
			return errors.New("Content Reset outbox revision invalid")
		}
		// Environment binding refuses new effects. Terminal readback may run
		// after the environment changed because it never writes content.
		if !terminalCampaign {
			environment, err := ReadEnvironment(tx)
			if err != nil {
				return err
			}
			if environment != approved.Environment {
				return errors.New("Content Reset approved environment changed")
			}
		}
		contractApproved := false
		for _, owner := range approved.Owners {
			contractApproved = contractApproved || owner == command.Contract
		}
		if !contractApproved {
			return ErrUnavailable
		}
		if _, ok := e.owners[command.Contract]; !ok {
			return ErrUnavailable
		}
		reconcile := step.State == "outcome_unknown" || step.State == "waiting"
		if terminalCampaign && !reconcile {
			return nil
		}
		if !reconcile && e.admit != nil && !e.admit(command.Contract, run.Contract) {
			return ErrUnavailable
		}
		state := "claimed"
		if reconcile {
			state = step.State
		}
		token, until := uuid.New(), now.Add(StepLease)
		if err := tx.Model(&step).Updates(map[string]any{"state": state, "lease_token": token, "lease_until": until, "attempt_count": gorm.Expr("attempt_count+1")}).Error; err != nil {
			return err
		}
		step.LeaseToken = &token
		step.LeaseUntil = &until
		step.State = state
		result = &claim{step, command, reconcile}
		return nil
	})
	return result, err
}

func validObservation(command Command, observation Observation) bool {
	if observation.CommandID != command.CommandID || observation.CommandHash != Hash(command) || !json.Valid(observation.Evidence) || len(observation.Evidence) > 1<<20 || len(observation.ReasonCode) > 128 {
		return false
	}
	var evidence map[string]json.RawMessage
	if json.Unmarshal(observation.Evidence, &evidence) != nil || evidence == nil {
		return false
	}
	if observation.State == "deferred" && !observation.NoEffectProven {
		return false
	}
	if observation.NoEffectProven && observation.State != "deferred" && observation.State != "failed" {
		return false
	}
	switch observation.State {
	case "succeeded", "waiting", "deferred", "blocked", "failed", "outcome_unknown":
		return true
	default:
		return false
	}
}

// Retryable requires the exact failed owner receipt. Uncertainty, partially
// admitted work, and an unbound receipt can never authorize another effect.
func Retryable(step models.ContentResetStep) bool {
	if step.State != "failed" || step.LeaseToken != nil {
		return false
	}
	var command Command
	var observation Observation
	return json.Unmarshal(step.Command, &command) == nil && command.CommandID == step.PublicID &&
		command.TenantID == step.TenantID && command.Contract.Owner == step.Owner &&
		command.Contract.Effect == step.EffectType && command.Contract.TargetType == step.TargetType && command.TargetID == step.TargetID &&
		json.Unmarshal(step.Receipt, &observation) == nil && validObservation(command, observation) &&
		observation.State == "failed" && observation.NoEffectProven
}

func (e *Engine) finish(db *gorm.DB, work claim, observation Observation) error {
	return db.Transaction(func(tx *gorm.DB) error {
		campaign, run, err := lockRun(tx, work.Command.TenantID, work.Step.CampaignID)
		if err != nil {
			return err
		}
		if run.RevisionID != work.Step.RevisionID {
			return ErrFenced
		}
		var step models.ContentResetStep
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND campaign_id=? AND id=?", campaign.TenantID, campaign.ID, work.Step.ID).First(&step).Error; err != nil {
			return err
		}
		now := time.Now().UTC()
		if step.LeaseToken == nil || work.Step.LeaseToken == nil || *step.LeaseToken != *work.Step.LeaseToken || step.LeaseUntil == nil || !step.LeaseUntil.After(now) || (step.State != "claimed" && step.State != "outcome_unknown" && step.State != "waiting") {
			return ErrFenced
		}
		if !validObservation(work.Command, observation) {
			return errors.New("invalid Content Reset owner observation")
		}
		// Deferred is a proof that no effect was admitted. An uncertain or
		// admitted command cannot become eligible for execution again.
		if work.Reconcile && observation.State == "deferred" {
			return errors.New("Content Reset readback cannot authorize redispatch")
		}
		raw, err := json.Marshal(observation)
		if err != nil {
			return err
		}
		updates := map[string]any{"state": observation.State, "receipt": datatypes.JSON(raw), "lease_token": nil, "lease_until": nil, "last_error": observation.ReasonCode}
		// Unknown readbacks back off without re-dispatching the effect.
		if observation.State == "outcome_unknown" || observation.State == "waiting" || observation.State == "deferred" {
			updates["lease_until"] = now.Add(time.Minute)
		}
		if err := tx.Model(&step).Updates(updates).Error; err != nil {
			return err
		}
		id := work.Step.RevisionID
		entry := models.ContentResetEvidence{CampaignID: campaign.ID, RevisionID: &id, TenantID: campaign.TenantID, EvidenceKey: "step/" + step.PublicID.String() + "/" + work.Step.LeaseToken.String(), EvidenceType: "owner_observation", Owner: step.Owner, Payload: datatypes.JSON(raw), PayloadHash: Hash(observation), ObservedAt: now}
		if err := tx.Create(&entry).Error; err != nil {
			return err
		}
		// Terminal campaigns only persist the recovered receipt. They must not
		// be reopened, paused, or moved back to partial by a readback.
		terminalCampaign := campaign.State == "complete" || campaign.State == "closed_partial"
		if !terminalCampaign && (observation.State == "outcome_unknown" || observation.State == "blocked" || observation.State == "failed") {
			if err := tx.Model(&run).Updates(map[string]any{"phase": "owner_attention", "pause_requested": true, "version": gorm.Expr("version+1")}).Error; err != nil {
				return err
			}
			if campaign.State == "executing" {
				return tx.Model(&campaign).Update("state", "partial").Error
			}
		}
		return nil
	})
}

func planningState(state string) bool {
	return state == "executing" || state == "published" || state == "cleanup_pending"
}

// Advance performs at most one owner operation. A request context cancellation,
// transport error, malformed receipt, or panic leaves uncertainty, never success.
// A later call reconciles with the same command ID. The owner must independently
// deduplicate that ID at its actual effect boundary.
func (e *Engine) Advance(ctx context.Context, db *gorm.DB, tenant string, campaignID uint) (bool, error) {
	if db == nil || strings.TrimSpace(tenant) == "" || campaignID == 0 {
		return false, errors.New("Content Reset execution identity required")
	}
	work, err := e.acquire(db.WithContext(ctx), tenant, campaignID)
	if err != nil || work == nil {
		return false, err
	}
	owner := e.owners[work.Command.Contract]
	callCtx, cancel := context.WithTimeout(ctx, StepLease/2)
	defer cancel()
	observation, ownerErr := invokeOwner(callCtx, db.WithContext(callCtx), owner, *work)
	observation = normalizeOwnerResult(*work, observation, ownerErr)
	if work.Reconcile && observation.State == "deferred" {
		observation = Observation{CommandID: work.Command.CommandID, CommandHash: Hash(work.Command), State: "outcome_unknown", ReasonCode: "owner_readback_cannot_redispatch", Evidence: json.RawMessage(`{"verified":false}`)}
	}
	// Persist uncertainty even if the browser or scheduler canceled its context.
	settleCtx, settleCancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer settleCancel()
	return true, e.finish(db.WithContext(settleCtx), *work, observation)
}

// normalizeOwnerResult converts an owner failure into a durable observation.
// A pause fence is proof that no effect was admitted, so the command returns to
// the deferred pool for exactly-once re-execution after resume. Every other
// failure stays uncertain and is only reconciled.
func normalizeOwnerResult(work claim, observation Observation, ownerErr error) Observation {
	if errors.Is(ownerErr, ErrPaused) {
		return Observation{CommandID: work.Command.CommandID, CommandHash: Hash(work.Command), State: "deferred", ReasonCode: "execution_paused", Evidence: json.RawMessage(`{"paused":true}`), NoEffectProven: true}
	}
	if ownerErr != nil || !validObservation(work.Command, observation) {
		return Observation{CommandID: work.Command.CommandID, CommandHash: Hash(work.Command), State: "outcome_unknown", ReasonCode: "owner_readback_required", Evidence: json.RawMessage(`{"verified":false}`)}
	}
	return observation
}

func invokeOwner(ctx context.Context, db *gorm.DB, owner Owner, work claim) (observation Observation, err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("Content Reset owner panicked")
		}
	}()
	if work.Reconcile {
		return owner.Reconcile(ctx, db, work.Command)
	}
	return owner.Execute(ctx, db, work.Command, *work.Step.LeaseToken)
}

// LockOwnerCommand is the local owner's admission fence. Call it inside the
// transaction performing an effect, before locking domain rows. A late worker
// cannot mutate merely because its immutable command ID is still recognizable.
func LockOwnerCommand(tx *gorm.DB, command Command, token uuid.UUID) (models.ContentResetCampaign, models.ContentResetRevision, error) {
	var campaign models.ContentResetCampaign
	var revision models.ContentResetRevision
	if token == uuid.Nil || tx == nil {
		return campaign, revision, ErrFenced
	}
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=?", command.TenantID, command.CampaignID).First(&campaign).Error; err != nil {
		return campaign, revision, err
	}
	_, run, err := lockRun(tx, command.TenantID, campaign.ID)
	if err != nil {
		return campaign, revision, err
	}
	if campaign.State != "executing" && campaign.State != "published" && campaign.State != "cleanup_pending" && campaign.State != "partial" {
		return campaign, revision, ErrFenced
	}
	if run.ManifestHash != command.ManifestHash || run.StartedAt == nil {
		return campaign, revision, ErrFenced
	}
	// A campaign pause is an admission fence, not just a display flag. A
	// command claimed before the pause must not begin a fresh destructive
	// transaction after it was acknowledged. Readback settles admitted work;
	// this only blocks new effects.
	if run.PauseRequested {
		return campaign, revision, ErrPaused
	}
	if err := tx.Where("tenant_id=? AND campaign_id=? AND id=? AND public_id=? AND revision=?", command.TenantID, campaign.ID, run.RevisionID, command.RevisionID, campaign.CurrentRevision).First(&revision).Error; err != nil {
		return campaign, revision, err
	}
	var step models.ContentResetStep
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND campaign_id=? AND revision_id=? AND public_id=?", command.TenantID, campaign.ID, revision.ID, command.CommandID).First(&step).Error; err != nil {
		return campaign, revision, err
	}
	if step.State != "claimed" || step.LeaseToken == nil || *step.LeaseToken != token || step.LeaseUntil == nil || !step.LeaseUntil.After(time.Now().UTC()) || Hash(json.RawMessage(step.Command)) != Hash(command) {
		return campaign, revision, ErrFenced
	}
	if err := ValidateExecutionEnvironment(tx, run); err != nil {
		return campaign, revision, err
	}
	return campaign, revision, nil
}
