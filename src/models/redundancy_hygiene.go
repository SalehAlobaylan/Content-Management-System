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
	ID                   uint   `gorm:"primaryKey"`
	TenantID             string `gorm:"uniqueIndex"`
	Enabled              bool
	CollapseEnabled      bool
	SweepIntervalMinutes int
	MaxFrontierItems     int
	MaxPairsScored       int
	ProposalFloor        float64
	EmitCirculationRecs  bool
	ConfirmRules         datatypes.JSON
	PausedUntil          *time.Time
	LastSweptAt          *time.Time
	CreatedAt            time.Time
	UpdatedAt            time.Time
}

func (RedundancyPolicy) TableName() string { return "redundancy_policies" }
func DefaultRedundancyPolicy(t string) RedundancyPolicy {
	return RedundancyPolicy{TenantID: t, CollapseEnabled: true, SweepIntervalMinutes: 360, MaxFrontierItems: 500, MaxPairsScored: 2000, ProposalFloor: .75}
}

type RedundancyRun struct {
	ID         uint      `gorm:"primaryKey"`
	PublicID   uuid.UUID `json:"id"`
	TenantID   string
	Trigger    string
	Status     string
	Summary    string
	Counts     datatypes.JSON
	Error      string
	ErrorClass string
	StartedAt  time.Time
	FinishedAt *time.Time
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

func (RedundancyRun) TableName() string { return "redundancy_runs" }

type RedundancyPair struct {
	ID                 uint      `gorm:"primaryKey"`
	PublicID           uuid.UUID `json:"id"`
	TenantID           string
	ItemAID            uuid.UUID
	ItemBID            uuid.UUID
	FamilyID           *uint
	LatestEvaluationID *uint
	Confidence         float64
	Verdict            string
	Tombstoned         bool
	ReviewedBy         string
	ReviewedAt         *time.Time
	RejectReason       string
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

func (RedundancyPair) TableName() string { return "redundancy_pairs" }

type RedundancyPairEvaluation struct {
	ID               uint `gorm:"primaryKey"`
	PairID           uint
	RunID            *uint
	InputFingerprint string
	EvaluatorVersion string
	LaneScores       datatypes.JSON
	Confidence       float64
	MachineVerdict   string
	CreatedAt        time.Time
}

func (RedundancyPairEvaluation) TableName() string { return "redundancy_pair_evaluations" }

type RedundancyFamily struct {
	ID                     uint      `gorm:"primaryKey"`
	PublicID               uuid.UUID `json:"id"`
	TenantID               string
	Status                 string
	CanonicalContentItemID uuid.UUID
	CanonicalLockedBy      string
	CanonicalReasons       datatypes.JSON
	FirstConfirmedAt       time.Time
	LastConfirmedAt        time.Time
	DissolvedAt            *time.Time
	DissolvedBy            string
	DissolveReason         string
	CreatedAt              time.Time
	UpdatedAt              time.Time
}

func (RedundancyFamily) TableName() string { return "redundancy_families" }

type RedundancyFamilyMember struct {
	ID            uint `gorm:"primaryKey"`
	FamilyID      uint
	TenantID      string
	ContentItemID uuid.UUID
	Role          string
	Since         time.Time
	EndedAt       *time.Time
}

func (RedundancyFamilyMember) TableName() string { return "redundancy_family_members" }

type RedundancyAction struct {
	ID             uint      `gorm:"primaryKey"`
	PublicID       uuid.UUID `json:"id"`
	TenantID       string
	RunID          *uint
	PairID         *uint
	FamilyID       *uint
	ActionKind     string
	Actor          string
	Outcome        string
	Reason         string
	Metadata       datatypes.JSON
	IdempotencyKey string
	CreatedAt      time.Time
}

func (RedundancyAction) TableName() string { return "redundancy_actions" }

type RedundancyFingerprint struct {
	ID                 uint `gorm:"primaryKey"`
	TenantID           string
	ContentItemID      uuid.UUID
	TranscriptChecksum string
	BodyHash           string
	ShingleCount       int
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

func (RedundancyFingerprint) TableName() string { return "redundancy_fingerprints" }
