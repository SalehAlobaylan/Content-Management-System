package models

import (
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"time"
)

const (
	RedundancyVerdictClear          = "not_duplicate"
	RedundancyVerdictProbable       = "probable_duplicate"
	RedundancyVerdictHighConfidence = "high_confidence_duplicate"
	RedundancyVerdictConfirmed      = "confirmed_duplicate"
	RedundancyVerdictRejected       = "rejected_human"
)

type RedundancyPolicy struct {
	ID                   uint           `gorm:"primaryKey" json:"id"`
	TenantID             string         `gorm:"uniqueIndex" json:"tenant_id"`
	Enabled              bool           `json:"enabled"`
	CollapseEnabled      bool           `json:"collapse_enabled"`
	SweepIntervalMinutes int            `json:"sweep_interval_minutes"`
	MaxFrontierItems     int            `json:"max_frontier_items"`
	MaxPairsScored       int            `json:"max_pairs_scored"`
	ProposalFloor        float64        `json:"proposal_floor"`
	EmitCirculationRecs  bool           `json:"emit_circulation_recs"`
	LaneSettings         datatypes.JSON `gorm:"type:jsonb;default:'{}'" json:"lane_settings"`
	ScanState            datatypes.JSON `gorm:"type:jsonb;default:'{}'" json:"scan_state"`
	ConfirmRules         datatypes.JSON `gorm:"type:jsonb;default:'{}'" json:"confirm_rules"`
	PausedUntil          *time.Time     `json:"paused_until"`
	LastSweptAt          *time.Time     `json:"last_swept_at"`
	CreatedAt            time.Time      `json:"created_at"`
	UpdatedAt            time.Time      `json:"updated_at"`
}

func (RedundancyPolicy) TableName() string { return "redundancy_policies" }
func DefaultRedundancyPolicy(t string) RedundancyPolicy {
	return RedundancyPolicy{TenantID: t, CollapseEnabled: true, SweepIntervalMinutes: 360, MaxFrontierItems: 500, MaxPairsScored: 2000, ProposalFloor: .75}
}

type RedundancyRun struct {
	ID         uint           `gorm:"primaryKey" json:"-"`
	PublicID   uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID   string         `json:"tenant_id"`
	Trigger    string         `json:"trigger"`
	Status     string         `json:"status"`
	Summary    string         `json:"summary"`
	Counts     datatypes.JSON `json:"counts"`
	Error      string         `json:"error"`
	ErrorClass string         `json:"error_class"`
	StartedAt  time.Time      `json:"started_at"`
	FinishedAt *time.Time     `json:"finished_at"`
	CreatedAt  time.Time      `json:"created_at"`
	UpdatedAt  time.Time      `json:"updated_at"`
}

func (RedundancyRun) TableName() string { return "redundancy_runs" }

type RedundancyPair struct {
	ID                 uint       `gorm:"primaryKey" json:"-"`
	PublicID           uuid.UUID  `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID           string     `json:"tenant_id"`
	ItemAID            uuid.UUID  `json:"item_a_id"`
	ItemBID            uuid.UUID  `json:"item_b_id"`
	FamilyID           *uint      `json:"family_id"`
	LatestEvaluationID *uint      `json:"latest_evaluation_id"`
	Confidence         float64    `json:"confidence"`
	Verdict            string     `json:"verdict"`
	Tombstoned         bool       `json:"tombstoned"`
	ReviewedBy         string     `json:"reviewed_by"`
	ReviewedAt         *time.Time `json:"reviewed_at"`
	RejectReason       string     `json:"reject_reason"`
	CreatedAt          time.Time  `json:"created_at"`
	UpdatedAt          time.Time  `json:"updated_at"`
}

func (RedundancyPair) TableName() string { return "redundancy_pairs" }

type RedundancyPairEvaluation struct {
	ID               uint           `gorm:"primaryKey" json:"id"`
	PairID           uint           `json:"pair_id"`
	RunID            *uint          `json:"run_id"`
	InputFingerprint string         `json:"input_fingerprint"`
	EvaluatorVersion string         `json:"evaluator_version"`
	LaneScores       datatypes.JSON `json:"lane_scores"`
	Confidence       float64        `json:"confidence"`
	MachineVerdict   string         `json:"machine_verdict"`
	CreatedAt        time.Time      `json:"created_at"`
}

func (RedundancyPairEvaluation) TableName() string { return "redundancy_pair_evaluations" }

type RedundancyFamily struct {
	ID                     uint           `gorm:"primaryKey" json:"-"`
	PublicID               uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID               string         `json:"tenant_id"`
	Status                 string         `json:"status"`
	CanonicalContentItemID uuid.UUID      `json:"canonical_content_item_id"`
	CanonicalLockedBy      string         `json:"canonical_locked_by"`
	CanonicalReasons       datatypes.JSON `json:"canonical_reasons"`
	FirstConfirmedAt       time.Time      `json:"first_confirmed_at"`
	LastConfirmedAt        time.Time      `json:"last_confirmed_at"`
	DissolvedAt            *time.Time     `json:"dissolved_at"`
	DissolvedBy            string         `json:"dissolved_by"`
	DissolveReason         string         `json:"dissolve_reason"`
	CreatedAt              time.Time      `json:"created_at"`
	UpdatedAt              time.Time      `json:"updated_at"`
}

func (RedundancyFamily) TableName() string { return "redundancy_families" }

type RedundancyFamilyMember struct {
	ID            uint       `gorm:"primaryKey" json:"id"`
	FamilyID      uint       `json:"family_id"`
	TenantID      string     `json:"tenant_id"`
	ContentItemID uuid.UUID  `json:"content_item_id"`
	Role          string     `json:"role"`
	Since         time.Time  `json:"since"`
	EndedAt       *time.Time `json:"ended_at"`
}

func (RedundancyFamilyMember) TableName() string { return "redundancy_family_members" }

type RedundancyAction struct {
	ID             uint           `gorm:"primaryKey" json:"-"`
	PublicID       uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID       string         `json:"tenant_id"`
	RunID          *uint          `json:"run_id"`
	PairID         *uint          `json:"pair_id"`
	FamilyID       *uint          `json:"family_id"`
	ActionKind     string         `json:"action_kind"`
	Actor          string         `json:"actor"`
	Outcome        string         `json:"outcome"`
	Reason         string         `json:"reason"`
	Metadata       datatypes.JSON `json:"metadata"`
	IdempotencyKey *string        `json:"idempotency_key"`
	CreatedAt      time.Time      `json:"created_at"`
}

func (RedundancyAction) TableName() string { return "redundancy_actions" }

type RedundancyFingerprint struct {
	ID                 uint           `gorm:"primaryKey" json:"id"`
	TenantID           string         `json:"tenant_id"`
	ContentItemID      uuid.UUID      `json:"content_item_id"`
	TranscriptChecksum string         `json:"transcript_checksum"`
	BodyHash           string         `json:"body_hash"`
	Signature          datatypes.JSON `json:"signature"`
	ShingleCount       int            `json:"shingle_count"`
	CreatedAt          time.Time      `json:"created_at"`
	UpdatedAt          time.Time      `json:"updated_at"`
}

func (RedundancyFingerprint) TableName() string { return "redundancy_fingerprints" }
