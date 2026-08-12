package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

const (
	MediaSupplyEpisodeOpen       = "open"
	MediaSupplyEpisodeRecovering = "recovering"
	MediaSupplyEpisodeResolved   = "resolved"

	// MediaSupplyControlReadEvaluation is a code-owned control identity. It is
	// never supplied by a browser or a model: a row for this identity removes
	// only the scheduled evidence-recording authority for its tenant.
	MediaSupplyControlReadEvaluation = "supply_read_evaluation"
	// MediaSupplyControlNormalIntakeScheduling removes only new routine source
	// admissions for the tenant/lane. It cannot stop receipts, verification,
	// cancellation, or recovery of work that may already have crossed an
	// effect boundary.
	MediaSupplyControlNormalIntakeScheduling = "normal_intake_scheduling"
	MediaSupplyControlExceptionalRecovery    = "exceptional_recovery_execution"
	MediaSupplyControlIntakeCircuit          = "intake_admission_circuit"
	MediaSupplyControlScopeTenant            = "tenant"
	MediaSupplyControlScopeAll               = "all"

	MediaSupplyEvaluationOutcomeEvaluated          = "evaluated"
	MediaSupplyEvaluationOutcomeDisabled           = "disabled"
	MediaSupplyEvaluationOutcomeControlUnavailable = "control_unavailable"
	MediaSupplyEvaluationOutcomeRecordFailed       = "record_failed"
	MediaSupplyEvaluationTriggerScheduled          = "scheduled"
	MediaSupplyEvaluationTriggerManual             = "manual"

	MediaSupplyActionPreviewActive           = "active"
	MediaSupplyActionPreviewConsumed         = "consumed"
	MediaSupplyActionPreviewInvalidated      = "invalidated"
	MediaSupplyActionRequestAwaitingApproval = "awaiting_approval"
	MediaSupplyActionRequestQueued           = "queued"
	MediaSupplyActionRequestClaimed          = "claimed"
	MediaSupplyActionRequestRunning          = "running"
	MediaSupplyActionRequestVerifying        = "verifying"
	MediaSupplyActionRequestSucceeded        = "succeeded"
	MediaSupplyActionRequestFailed           = "failed"
	MediaSupplyActionRequestCancelled        = "cancelled"
	MediaSupplyActionRequestUncertain        = "uncertain"
)

// MediaSupplyControl is a durable, subtractive tenant control for the Supply
// Continuity evaluator. Absence means the evaluator retains its code-default
// read-only authority; a row means that one named evaluator is disabled. It is
// intentionally not a policy switch for source admission, Aggregation, a
// provider, or Pods visibility.
//
// There is deliberately no generic control endpoint. Any future write must be
// a separately registered, signed CMS action with a fixed control identity.
type MediaSupplyControl struct {
	ID         uint      `gorm:"primaryKey" json:"-"`
	TenantID   string    `gorm:"type:varchar(64);not null;uniqueIndex:uq_media_supply_controls_scope" json:"tenant_id"`
	ControlKey string    `gorm:"type:varchar(64);not null;uniqueIndex:uq_media_supply_controls_scope" json:"control_key"`
	ScopeType  string    `gorm:"type:varchar(24);not null;uniqueIndex:uq_media_supply_controls_scope" json:"scope_type"`
	ScopeID    string    `gorm:"type:varchar(64);not null;uniqueIndex:uq_media_supply_controls_scope" json:"scope_id"`
	DisabledAt time.Time `gorm:"type:timestamptz;not null" json:"disabled_at"`
	DisabledBy string    `gorm:"type:varchar(128);not null" json:"disabled_by"`
	Reason     string    `gorm:"type:text;not null" json:"reason"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// IsKnownMediaSupplyControlKey is deliberately code-owned. Callers never
// choose a control identity from a request body or model output.
func IsKnownMediaSupplyControlKey(key string) bool {
	switch key {
	case MediaSupplyControlReadEvaluation, MediaSupplyControlNormalIntakeScheduling,
		MediaSupplyControlExceptionalRecovery, MediaSupplyControlIntakeCircuit:
		return true
	case "supply_action:source_run.repair_missed_admission",
		"supply_action:source_run.reclaim_dispatch_claim",
		"supply_action:source_run.transfer_execution_unit_lease",
		"supply_action:source_run.adopt_unit_job",
		"supply_action:source_run.redeliver_receipt",
		"supply_action:source_run.verify_effect",
		"supply_action:source_run.finalize_verified_no_change",
		"supply_action:source_run.cancel_unstarted",
		"supply_action:pipeline.resume_exact_stage":
		return true
	case "supply_action:artifact.request_transcript",
		"supply_action:artifact.request_image_embedding",
		"supply_action:artifact.request_text_embedding",
		"supply_action:artifact.request_llm_metadata":
		// Supply-action controls are intentionally enumerated here instead of
		// accepting a caller-provided prefix. The models package cannot import
		// the action registry, so this static mirror keeps the database control
		// boundary closed without introducing an import cycle.
		return true
	case "supply_action:atomization.execute_exact_parent",
		"supply_action:studio.clear_exact_children",
		"supply_action:feed_generation.attach_verified_member":
		return true
	default:
		return false
	}
}

func (MediaSupplyControl) TableName() string { return "media_supply_controls" }

// MediaSupplyEvaluationCheckpoint is the latest durable liveness and outcome
// proof for one tenant's bounded evidence evaluator. It is an upserted
// checkpoint rather than an unbounded run log: historical attention belongs
// in immutable episode events, while this row answers whether the evaluator is
// currently observing at all. It carries no source command or provider state.
type MediaSupplyEvaluationCheckpoint struct {
	ID               uint       `gorm:"primaryKey" json:"-"`
	TenantID         string     `gorm:"type:varchar(64);not null;uniqueIndex" json:"tenant_id"`
	LastTrigger      string     `gorm:"type:varchar(16);not null" json:"last_trigger"`
	LastOutcome      string     `gorm:"type:varchar(32);not null" json:"last_outcome"`
	LastObservedAt   time.Time  `gorm:"type:timestamptz;not null" json:"last_observed_at"`
	LastEvaluatedAt  *time.Time `gorm:"type:timestamptz" json:"last_evaluated_at,omitempty"`
	EvaluationDigest *string    `gorm:"type:varchar(128)" json:"evaluation_digest,omitempty"`
	LastFailureClass *string    `gorm:"type:varchar(64)" json:"last_failure_class,omitempty"`
	CreatedAt        time.Time  `json:"created_at"`
	UpdatedAt        time.Time  `json:"updated_at"`
}

func (MediaSupplyEvaluationCheckpoint) TableName() string {
	return "media_supply_evaluation_checkpoints"
}

// MediaSupplyEpisode is the durable, tenant-scoped attention record for one
// deterministic Supply Continuity fingerprint. It is not a recommendation,
// retry, or queue command. A later native action may reference it but cannot
// reuse the episode as execution authority.
type MediaSupplyEpisode struct {
	ID                   uint           `gorm:"primaryKey" json:"-"`
	PublicID             uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID             string         `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	Fingerprint          string         `gorm:"type:varchar(128);not null;index" json:"fingerprint"`
	FirstFailedBoundary  string         `gorm:"type:varchar(64);not null" json:"first_failed_boundary"`
	Verdict              string         `gorm:"type:varchar(64);not null" json:"verdict"`
	Severity             string         `gorm:"type:varchar(16);not null" json:"severity"`
	Owner                string         `gorm:"type:varchar(128);not null" json:"owner"`
	State                string         `gorm:"type:varchar(24);not null;index" json:"state"`
	Summary              string         `gorm:"type:text;not null" json:"summary"`
	AffectedSubjects     datatypes.JSON `gorm:"type:jsonb;not null" json:"affected_subjects"`
	EvidenceDigest       string         `gorm:"type:varchar(128);not null" json:"evidence_digest"`
	EvidenceCompleteness string         `gorm:"type:varchar(24);not null" json:"evidence_completeness"`
	Evidence             datatypes.JSON `gorm:"type:jsonb;not null" json:"evidence"`
	FirstSeenAt          time.Time      `gorm:"type:timestamptz;not null" json:"first_seen_at"`
	LastSeenAt           time.Time      `gorm:"type:timestamptz;not null;index" json:"last_seen_at"`
	SLODeadlineAt        *time.Time     `gorm:"type:timestamptz" json:"slo_deadline_at,omitempty"`
	ResolvedAt           *time.Time     `gorm:"type:timestamptz" json:"resolved_at,omitempty"`
	ResolutionProof      datatypes.JSON `gorm:"type:jsonb" json:"resolution_proof,omitempty"`
	CreatedAt            time.Time      `json:"created_at"`
	UpdatedAt            time.Time      `json:"updated_at"`
}

func (MediaSupplyEpisode) TableName() string { return "media_supply_episodes" }

// MediaSupplyEpisodeEvent is immutable evidence of an episode open/update.
// The migration rejects mutation so an event can never be silently rewritten
// after an admin has relied on its recorded evaluation.
type MediaSupplyEpisodeEvent struct {
	ID             uint           `gorm:"primaryKey" json:"-"`
	PublicID       uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID       string         `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	EpisodeID      uuid.UUID      `gorm:"type:uuid;not null;index" json:"episode_id"`
	EventKey       string         `gorm:"type:varchar(255);not null" json:"event_key"`
	EventType      string         `gorm:"type:varchar(32);not null" json:"event_type"`
	EvidenceDigest string         `gorm:"type:varchar(128);not null" json:"evidence_digest"`
	Evaluation     datatypes.JSON `gorm:"type:jsonb;not null" json:"evaluation"`
	OccurredAt     time.Time      `gorm:"type:timestamptz;not null;index" json:"occurred_at"`
	CreatedAt      time.Time      `json:"created_at"`
}

func (MediaSupplyEpisodeEvent) TableName() string { return "media_supply_episode_events" }

// MediaSupplyActionPreview freezes a single, server-derived repair proposal.
// It is not executable authority: approval consumes this exact evidence-bound
// preview into a separately durable action request.
type MediaSupplyActionPreview struct {
	ID                   uint           `gorm:"primaryKey" json:"-"`
	PublicID             uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID             string         `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	ActionKey            string         `gorm:"type:varchar(128);not null;index" json:"action_key"`
	ActionVersion        string         `gorm:"type:varchar(32);not null" json:"action_version"`
	TargetType           string         `gorm:"type:varchar(64);not null" json:"target_type"`
	TargetID             uuid.UUID      `gorm:"type:uuid;not null;index" json:"target_id"`
	EvidenceDigest       string         `gorm:"type:varchar(128);not null" json:"evidence_digest"`
	PolicyDigest         string         `gorm:"type:varchar(128);not null" json:"policy_digest"`
	PreflightEvidence    datatypes.JSON `gorm:"type:jsonb;not null" json:"preflight_evidence"`
	PlannedEffects       datatypes.JSON `gorm:"type:jsonb;not null" json:"planned_effects"`
	AffectedSubjects     datatypes.JSON `gorm:"type:jsonb;not null" json:"affected_subjects"`
	DeepLinks            datatypes.JSON `gorm:"type:jsonb;not null" json:"deep_links"`
	State                string         `gorm:"type:varchar(24);not null;index" json:"state"`
	ExpiresAt            time.Time      `gorm:"type:timestamptz;not null;index" json:"expires_at"`
	CreatedBy            string         `gorm:"type:varchar(128);not null" json:"created_by"`
	CreatedAccessVersion string         `gorm:"type:varchar(128)" json:"-"`
	ExecutionMode        string         `gorm:"type:varchar(24);not null;default:'approval_required'" json:"execution_mode"`
	PromotionID          *uuid.UUID     `gorm:"type:uuid" json:"promotion_id,omitempty"`
	CreatedAt            time.Time      `json:"created_at"`
	UpdatedAt            time.Time      `json:"updated_at"`
}

func (MediaSupplyActionPreview) TableName() string { return "media_supply_action_previews" }

// MediaSupplyActionRequest is the immutable approval-to-worker handoff. A
// worker claim is fenced and a crash after an owner effect must terminalize as
// verifying/uncertain, never queue an implicit retry.
type MediaSupplyActionRequest struct {
	ID                      uint           `gorm:"primaryKey" json:"-"`
	PublicID                uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID                string         `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	PreviewID               uuid.UUID      `gorm:"type:uuid;not null;index" json:"preview_id"`
	EpisodeID               *uuid.UUID     `gorm:"type:uuid;index" json:"episode_id,omitempty"`
	ActionKey               string         `gorm:"type:varchar(128);not null;index" json:"action_key"`
	ActionVersion           string         `gorm:"type:varchar(32);not null" json:"action_version"`
	TargetType              string         `gorm:"type:varchar(64);not null" json:"target_type"`
	TargetID                uuid.UUID      `gorm:"type:uuid;not null;index" json:"target_id"`
	ExecutionOwner          string         `gorm:"type:varchar(64);not null" json:"execution_owner"`
	ExecutionMode           string         `gorm:"type:varchar(24);not null;default:'approval_required'" json:"execution_mode"`
	PromotionID             *uuid.UUID     `gorm:"type:uuid" json:"promotion_id,omitempty"`
	QualificationReportID   *uint          `json:"qualification_report_id,omitempty"`
	IdempotencyKey          string         `gorm:"type:varchar(255);not null" json:"idempotency_key"`
	State                   string         `gorm:"type:varchar(32);not null;index" json:"state"`
	ApprovedBy              string         `gorm:"type:varchar(128)" json:"approved_by,omitempty"`
	ApprovalEvidenceDigest  string         `gorm:"type:varchar(128)" json:"approval_evidence_digest,omitempty"`
	ApprovalAccessVersion   string         `gorm:"type:varchar(128)" json:"-"`
	ApprovedAt              *time.Time     `gorm:"type:timestamptz" json:"approved_at,omitempty"`
	ClaimOwner              string         `gorm:"type:varchar(128)" json:"claim_owner,omitempty"`
	ClaimToken              *uuid.UUID     `gorm:"type:uuid" json:"-"`
	ClaimEpoch              int64          `gorm:"not null;default:0" json:"claim_epoch"`
	ClaimExpiresAt          *time.Time     `gorm:"type:timestamptz;index" json:"claim_expires_at,omitempty"`
	CancellationRequestedAt *time.Time     `gorm:"type:timestamptz" json:"cancellation_requested_at,omitempty"`
	BeforeEffects           datatypes.JSON `gorm:"type:jsonb" json:"before_effects,omitempty"`
	PlannedEffects          datatypes.JSON `gorm:"type:jsonb;not null" json:"planned_effects"`
	AfterEffects            datatypes.JSON `gorm:"type:jsonb" json:"after_effects,omitempty"`
	VerifiedEffects         datatypes.JSON `gorm:"type:jsonb" json:"verified_effects,omitempty"`
	AffectedSubjects        datatypes.JSON `gorm:"type:jsonb;not null" json:"affected_subjects"`
	AffectedDomains         []string       `gorm:"-" json:"affected_domains,omitempty"`
	DeepLinks               datatypes.JSON `gorm:"type:jsonb;not null" json:"deep_links"`
	FailureClass            string         `gorm:"type:varchar(64)" json:"failure_class,omitempty"`
	FinishedAt              *time.Time     `gorm:"type:timestamptz" json:"finished_at,omitempty"`
	CreatedAt               time.Time      `json:"created_at"`
	UpdatedAt               time.Time      `json:"updated_at"`
}

func (MediaSupplyActionRequest) TableName() string { return "media_supply_action_requests" }

type MediaSupplyActionAttempt struct {
	ID              uint       `gorm:"primaryKey" json:"-"`
	PublicID        uuid.UUID  `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID        string     `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	ActionRequestID uuid.UUID  `gorm:"type:uuid;not null;index" json:"action_request_id"`
	AttemptNumber   int        `gorm:"not null" json:"attempt_number"`
	State           string     `gorm:"type:varchar(32);not null" json:"state"`
	FenceToken      uuid.UUID  `gorm:"type:uuid;not null" json:"fence_token"`
	OwnerProtocol   string     `gorm:"type:varchar(64);not null" json:"owner_protocol"`
	StartedAt       *time.Time `gorm:"type:timestamptz" json:"started_at,omitempty"`
	EffectStartedAt *time.Time `gorm:"type:timestamptz" json:"effect_started_at,omitempty"`
	FinishedAt      *time.Time `gorm:"type:timestamptz" json:"finished_at,omitempty"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
}

func (MediaSupplyActionAttempt) TableName() string { return "media_supply_action_attempts" }

// MediaSupplyActionEvent is append-only audit/progress evidence. It never
// contains credentials, arbitrary commands, raw provider payloads, or a
// browser-provided target.
type MediaSupplyActionEvent struct {
	ID              uint           `gorm:"primaryKey" json:"-"`
	PublicID        uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID        string         `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	ActionRequestID uuid.UUID      `gorm:"type:uuid;not null;index" json:"action_request_id"`
	AttemptID       *uuid.UUID     `gorm:"type:uuid;index" json:"attempt_id,omitempty"`
	Sequence        int64          `gorm:"not null" json:"sequence"`
	EventKey        string         `gorm:"type:varchar(255);not null" json:"event_key"`
	EventType       string         `gorm:"type:varchar(48);not null" json:"event_type"`
	Payload         datatypes.JSON `gorm:"type:jsonb;not null" json:"payload"`
	OccurredAt      time.Time      `gorm:"type:timestamptz;not null" json:"occurred_at"`
	CreatedAt       time.Time      `json:"created_at"`
}

func (MediaSupplyActionEvent) TableName() string { return "media_supply_action_events" }
