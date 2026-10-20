// Package workload coordinates short-horizon capacity without replacing native
// execution, finance, health containment or tenant admission owners.
package workload

import (
	"context"
	"time"

	"content-management-system/src/models"
	"gorm.io/gorm"
)

const (
	ModeOff          = "off"
	ModeObserve      = "observe"
	ModeEnforce      = "enforce"
	OriginAutomation = "automation"
	OriginCampaign   = "campaign"
	OriginHuman      = "human"
	StateAcquired    = "acquired"
	StateRunning     = "running"
	StateHandedOff   = "handed_off"
	StateUncertain   = "uncertain"
)

type Request struct {
	TenantID   string `json:"tenant_id"`
	System     string `json:"system"`
	Class      string `json:"class"`
	Units      int    `json:"units"`
	RunID      string `json:"run_id"`
	Origin     string `json:"origin"`
	OwnerKind  string `json:"owner_kind"`
	OwnerID    string `json:"owner_id"`
	RequestKey string `json:"request_key,omitempty"`
}

type Decision struct {
	PermitID     string     `json:"permit_id,omitempty"`
	GrantedUnits int        `json:"granted_units"`
	Verdict      string     `json:"verdict"`
	WouldVerdict string     `json:"would_verdict"`
	Reasons      []string   `json:"reasons"`
	Mode         string     `json:"mode"`
	Bypass       bool       `json:"bypass"`
	Unavailable  bool       `json:"unavailable"`
	ExpiresAt    *time.Time `json:"expires_at,omitempty"`
}

func (d Decision) Allowed() bool {
	return d.GrantedUnits > 0 && (d.Verdict == "admit" || d.Verdict == "admit_reduced")
}

// Pressure is an observed native pressure signal, never added to reservations.
// Reservations instead consume separately registered CMS initiation slots.
type Sample struct {
	Available  bool           `json:"available"`
	Healthy    bool           `json:"healthy"`
	Pressure   float64        `json:"pressure"`
	ObservedAt time.Time      `json:"observed_at"`
	Reason     string         `json:"reason"`
	Incident   bool           `json:"incident"`
	Evidence   map[string]any `json:"evidence,omitempty"`
}

type SpendSignal struct {
	Available  bool      `json:"available"`
	State      string    `json:"state"` // normal | warning | capped | unknown
	Reason     string    `json:"reason"`
	ObservedAt time.Time `json:"observed_at"`
}

type SampleProvider func(context.Context, []string) (map[string]Sample, error)
type SpendProvider func(context.Context, *gorm.DB, Request) (SpendSignal, error)

type Engine struct {
	DB             *gorm.DB
	Clock          func() time.Time
	SampleProvider SampleProvider
	SpendProvider  SpendProvider
	// Failure diagnostics are injectable for isolated qualification. Runtime
	// responses/logs continue to expose only sanitized failure classes.
	failureObserver func(error)
	offCache        *disabledPolicyCache
}

type ResourceLimit struct {
	Soft     float64 `json:"soft"`
	Hard     float64 `json:"hard"`
	Capacity int     `json:"capacity"`
}

type Policy = models.WorkloadGovernorPolicy

type PolicyUpdate struct {
	Revision              int64                    `json:"revision"`
	Mode                  string                   `json:"mode"`
	EnforcedClasses       []string                 `json:"enforced_classes"`
	ResourceLimits        map[string]ResourceLimit `json:"resource_limits"`
	SnapshotMaxAgeSeconds int                      `json:"snapshot_max_age_seconds"`
	RecoverySeconds       int                      `json:"recovery_seconds"`
	LeaseSeconds          int                      `json:"lease_seconds"`
	StarvationMinutes     int                      `json:"starvation_minutes"`
	QualificationDigest   string                   `json:"qualification_digest"`
	ReviewNote            string                   `json:"review_note"`
	Confirmation          string                   `json:"confirmation"`
}

type ResourceStatus struct {
	Key        string        `json:"key"`
	Label      string        `json:"label"`
	Kind       string        `json:"kind"`
	State      string        `json:"state"`
	Fresh      bool          `json:"fresh"`
	Available  bool          `json:"available"`
	Healthy    bool          `json:"healthy"`
	ObservedAt *time.Time    `json:"observed_at,omitempty"`
	Pressure   float64       `json:"pressure"`
	Reserved   int           `json:"reserved"`
	Limit      ResourceLimit `json:"limit"`
	Reason     string        `json:"reason"`
}

type Attention struct {
	TenantID string    `json:"tenant_id"`
	System   string    `json:"system"`
	Class    string    `json:"class"`
	Reason   string    `json:"reason"`
	Since    time.Time `json:"since"`
	Count    int64     `json:"count"`
}

type Summary struct {
	ActivePermits    int64 `json:"active_permits"`
	UncertainPermits int64 `json:"uncertain_permits"`
	OverduePermits   int64 `json:"overdue_permits"`
	Decisions24h     int64 `json:"decisions_24h"`
	WouldDefer24h    int64 `json:"would_defer_24h"`
	Unavailable24h   int64 `json:"unavailable_24h"`
}

type Status struct {
	Policy              Policy           `json:"policy"`
	Mode                string           `json:"mode"`
	EffectiveMode       string           `json:"effective_mode"`
	Available           bool             `json:"available"`
	HeartbeatAt         *time.Time       `json:"heartbeat_at,omitempty"`
	Resources           []ResourceStatus `json:"resources"`
	Summary             Summary          `json:"summary"`
	Attention           []Attention      `json:"attention"`
	Coverage            []CoverageEntry  `json:"coverage"`
	Reasons             []string         `json:"reasons"`
	UnavailableCount    uint64           `json:"unavailable_count"`
	DisabledJournalLoss uint64           `json:"disabled_journal_loss"`
}

type ClassQualification struct {
	Class                 string           `json:"class"`
	Eligible              bool             `json:"eligible"`
	Requests              int64            `json:"requests"`
	Settled               int64            `json:"settled"`
	WouldDefer            int64            `json:"would_defer"`
	Unavailable           int64            `json:"unavailable"`
	Unresolved            int64            `json:"unresolved"`
	FirstObservedAt       *time.Time       `json:"first_observed_at,omitempty"`
	Missing               []string         `json:"missing"`
	Callers               []CallerEvidence `json:"callers"`
	MeanCompletionSeconds float64          `json:"mean_completion_seconds"`
}

type CallerEvidence struct {
	System   string `json:"system"`
	Requests int64  `json:"requests"`
	Observed bool   `json:"observed"`
}

type ResourceEvidence struct {
	Resource     string  `json:"resource"`
	Samples      int64   `json:"samples"`
	Unknown      int64   `json:"unknown"`
	Saturated    int64   `json:"saturated"`
	GrowthAlarms int64   `json:"growth_alarms"`
	MaxPressure  float64 `json:"max_pressure"`
}

type Qualification struct {
	Digest           string               `json:"digest"`
	WindowStart      time.Time            `json:"window_start"`
	WindowEnd        time.Time            `json:"window_end"`
	RequiredHours    int                  `json:"required_hours"`
	RequiredRequests int                  `json:"required_requests"`
	Classes          []ClassQualification `json:"classes"`
	FreshResources   int                  `json:"fresh_resources"`
	TotalResources   int                  `json:"total_resources"`
	Evidence         []string             `json:"evidence"`
	Resources        []ResourceEvidence   `json:"resources"`
}

type ListFilter struct {
	TenantID       string
	System         string
	Class          string
	State          string
	Verdict        string
	Limit          int
	BeforeID       uint64
	BeforeTime     *time.Time
	BeforePermitID string
}
