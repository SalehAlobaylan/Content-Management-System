package models

import (
	"time"

	"gorm.io/datatypes"
)

// WorkloadGovernorPolicy is deployment-wide capacity policy. Tenant financial
// authority remains with the financial and transcription owners.
type WorkloadGovernorPolicy struct {
	Scope                 string         `gorm:"primaryKey;type:varchar(32)" json:"scope"`
	Mode                  string         `gorm:"type:varchar(16);not null;default:off" json:"mode"`
	Revision              int64          `gorm:"not null;default:1" json:"revision"`
	EnforcedClasses       datatypes.JSON `gorm:"type:jsonb;not null" json:"enforced_classes"`
	ResourceLimits        datatypes.JSON `gorm:"type:jsonb;not null" json:"resource_limits"`
	SnapshotMaxAgeSeconds int            `gorm:"not null;default:90" json:"snapshot_max_age_seconds"`
	RecoverySeconds       int            `gorm:"not null;default:120" json:"recovery_seconds"`
	LeaseSeconds          int            `gorm:"not null;default:600" json:"lease_seconds"`
	StarvationMinutes     int            `gorm:"not null;default:120" json:"starvation_minutes"`
	ObservationHours      int            `gorm:"not null;default:24" json:"observation_hours"`
	MinClassRequests      int            `gorm:"not null;default:20" json:"min_class_requests"`
	UpdatedBy             string         `gorm:"type:varchar(120)" json:"updated_by"`
	PausedUntil           *time.Time     `json:"paused_until,omitempty"`
	UpdatedAt             time.Time      `json:"updated_at"`
}

func (WorkloadGovernorPolicy) TableName() string { return "workload_governor_policies" }

type WorkloadPermit struct {
	ID              string         `gorm:"primaryKey;type:uuid" json:"id"`
	TenantID        string         `gorm:"type:varchar(64);not null;uniqueIndex:idx_workload_request,priority:1;index" json:"tenant_id"`
	RequestKey      string         `gorm:"type:varchar(128);not null;uniqueIndex:idx_workload_request,priority:2" json:"request_key"`
	System          string         `gorm:"type:varchar(80);not null;index" json:"system"`
	Class           string         `gorm:"type:varchar(48);not null;index" json:"class"`
	RunID           string         `gorm:"type:varchar(128);not null" json:"run_id"`
	Origin          string         `gorm:"type:varchar(16);not null" json:"origin"`
	OwnerKind       string         `gorm:"type:varchar(64);not null;index:idx_workload_owner,priority:2" json:"owner_kind"`
	OwnerID         string         `gorm:"type:varchar(128);not null;index:idx_workload_owner,priority:3" json:"owner_id"`
	RequestedUnits  int            `gorm:"not null" json:"requested_units"`
	GrantedUnits    int            `gorm:"not null" json:"granted_units"`
	ActualUnits     *int           `json:"actual_units,omitempty"`
	State           string         `gorm:"type:varchar(24);not null;index" json:"state"`
	Mode            string         `gorm:"type:varchar(16);not null" json:"mode"`
	Verdict         string         `gorm:"type:varchar(24);not null" json:"verdict"`
	WouldVerdict    string         `gorm:"type:varchar(24);not null" json:"would_verdict"`
	Reasons         datatypes.JSON `gorm:"type:jsonb;not null" json:"reasons"`
	Reservations    datatypes.JSON `gorm:"type:jsonb;not null" json:"reservations"`
	Bypass          bool           `gorm:"not null;default:false" json:"bypass"`
	PolicyRevision  int64          `gorm:"not null" json:"policy_revision"`
	CreatedAt       time.Time      `gorm:"index" json:"created_at"`
	UpdatedAt       time.Time      `json:"updated_at"`
	ExpiresAt       time.Time      `gorm:"index" json:"expires_at"`
	BeganAt         *time.Time     `json:"began_at,omitempty"`
	ReleasedAt      *time.Time     `json:"released_at,omitempty"`
	SettlementProof string         `gorm:"type:varchar(512)" json:"settlement_proof,omitempty"`
}

func (WorkloadPermit) TableName() string { return "workload_permits" }

type WorkloadDecision struct {
	ID             uint64         `gorm:"primaryKey" json:"id"`
	PermitID       string         `gorm:"type:varchar(36);index" json:"permit_id,omitempty"`
	TenantID       string         `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	System         string         `gorm:"type:varchar(80);not null;index" json:"system"`
	Class          string         `gorm:"type:varchar(48);not null;index" json:"class"`
	Event          string         `gorm:"type:varchar(32);not null" json:"event"`
	Verdict        string         `gorm:"type:varchar(24);not null" json:"verdict"`
	WouldVerdict   string         `gorm:"type:varchar(24);not null" json:"would_verdict"`
	RequestedUnits int            `gorm:"not null;default:0" json:"requested_units"`
	GrantedUnits   int            `gorm:"not null;default:0" json:"granted_units"`
	ActualUnits    *int           `json:"actual_units,omitempty"`
	Reasons        datatypes.JSON `gorm:"type:jsonb;not null" json:"reasons"`
	Evidence       datatypes.JSON `gorm:"type:jsonb;not null" json:"evidence"`
	Mode           string         `gorm:"type:varchar(16);not null" json:"mode"`
	PolicyRevision int64          `gorm:"not null" json:"policy_revision"`
	Actor          string         `gorm:"type:varchar(120)" json:"actor,omitempty"`
	CreatedAt      time.Time      `gorm:"index" json:"created_at"`
}

func (WorkloadDecision) TableName() string { return "workload_decisions" }

// Snapshots are an append-only bounded observation history, not a lease ledger.
type WorkloadCapacitySnapshot struct {
	ID            uint64         `gorm:"primaryKey" json:"id"`
	Resource      string         `gorm:"type:varchar(64);not null;index:idx_workload_snapshot,priority:1" json:"resource"`
	State         string         `gorm:"type:varchar(24);not null" json:"state"`
	Available     bool           `gorm:"not null" json:"available"`
	Healthy       bool           `gorm:"not null" json:"healthy"`
	Pressure      float64        `gorm:"not null;default:0" json:"pressure"`
	Reason        string         `gorm:"type:varchar(256)" json:"reason"`
	ObservedAt    time.Time      `gorm:"index:idx_workload_snapshot,priority:2,sort:desc" json:"observed_at"`
	CreatedAt     time.Time      `gorm:"index" json:"created_at"`
	RecoverySince *time.Time     `json:"recovery_since,omitempty"`
	Evidence      datatypes.JSON `gorm:"type:jsonb;not null" json:"evidence"`
}

func (WorkloadCapacitySnapshot) TableName() string { return "workload_capacity_snapshots" }
