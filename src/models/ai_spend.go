package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

const (
	AISpendVerdictWithin      = "within"
	AISpendVerdictWarning     = "warning"
	AISpendVerdictOverPace    = "over_pace"
	AISpendVerdictBoundedStop = "bounded_stop"
	AISpendVerdictPaused      = "paused"
)

type AISpendEvent struct {
	OverrideID           *uuid.UUID     `gorm:"type:uuid" json:"override_id,omitempty"`
	HumanActorID         string         `json:"human_actor_id,omitempty"`
	ID                   uint           `gorm:"primaryKey" json:"id"`
	EventCount           int64          `gorm:"not null;default:1" json:"event_count"`
	FoldedAt             *time.Time     `json:"folded_at,omitempty"`
	UsageUnavailable     bool           `json:"usage_unavailable"`
	EventID              uuid.UUID      `gorm:"type:uuid;uniqueIndex;not null" json:"event_id"`
	OccurredAt           time.Time      `gorm:"not null;index" json:"occurred_at"`
	SpendClass           string         `gorm:"type:varchar(32);not null;index" json:"spend_class"`
	Operation            string         `gorm:"type:varchar(64);not null" json:"operation"`
	Provider             string         `gorm:"type:varchar(64)" json:"provider"`
	Model                string         `gorm:"type:varchar(160)" json:"model"`
	Units                datatypes.JSON `gorm:"type:jsonb;not null" json:"units"`
	CostUSD              float64        `json:"cost_usd"`
	AvoidedCostUSD       float64        `json:"avoided_cost_usd"`
	Cached               bool           `json:"cached"`
	Estimated            bool           `json:"estimated"`
	AvoidedCostEstimated bool           `json:"avoided_cost_estimated"`
	Unpriced             bool           `json:"unpriced"`
	Backfilled           bool           `json:"backfilled"`
	PriceRowID           *uint          `json:"price_row_id,omitempty"`
	TriggerSource        string         `gorm:"type:varchar(64);not null;default:'unknown';index" json:"trigger_source"`
	SystemRunID          string         `gorm:"type:varchar(96);index" json:"system_run_id,omitempty"`
	TenantID             string         `gorm:"type:varchar(64);not null;default:'default'" json:"tenant_id"`
	SourceService        string         `gorm:"type:varchar(64);not null" json:"source_service"`
	OverCapHuman         bool           `json:"over_cap_human"`
	CreatedAt            time.Time      `json:"created_at"`
}

func (AISpendEvent) TableName() string { return "ai_spend_events" }

type AIPriceBook struct {
	ID             uint      `gorm:"primaryKey" json:"id"`
	SpendClass     string    `gorm:"type:varchar(32);not null" json:"spend_class"`
	Provider       string    `gorm:"type:varchar(64);not null" json:"provider"`
	ModelPattern   string    `gorm:"type:varchar(160);not null" json:"model_pattern"`
	InputUSDPer1M  float64   `gorm:"column:input_usd_per_1m" json:"input_usd_per_1m"`
	OutputUSDPer1M float64   `gorm:"column:output_usd_per_1m" json:"output_usd_per_1m"`
	UnitUSD        float64   `gorm:"column:unit_usd" json:"unit_usd"`
	EffectiveFrom  time.Time `gorm:"not null" json:"effective_from"`
	Note           string    `json:"note,omitempty"`
	CreatedBy      string    `json:"created_by,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
}

func (AIPriceBook) TableName() string { return "ai_price_book" }

type AISpendRollup struct {
	HumanActorID           string         `json:"human_actor_id"`
	OverCapHumanEvents     int64          `json:"over_cap_human_events"`
	TenantID               string         `json:"tenant_id"`
	EstimatedEvents        int64          `json:"estimated_events"`
	UnpricedEvents         int64          `json:"unpriced_events"`
	UnknownEvents          int64          `json:"unknown_events"`
	UsageUnavailableEvents int64          `json:"usage_unavailable_events"`
	AvoidedEstimatedUSD    float64        `json:"avoided_estimated_usd"`
	ID                     uint           `gorm:"primaryKey" json:"id"`
	Day                    time.Time      `gorm:"type:date;not null" json:"day"`
	SpendClass             string         `json:"spend_class"`
	Operation              string         `json:"operation"`
	Provider               string         `json:"provider"`
	Model                  string         `json:"model"`
	TriggerSource          string         `json:"trigger_source"`
	SystemRunID            string         `json:"system_run_id,omitempty"`
	Events                 int64          `json:"events"`
	Units                  datatypes.JSON `gorm:"type:jsonb" json:"units"`
	CostUSD                float64        `json:"cost_usd"`
	AvoidedCostUSD         float64        `json:"avoided_cost_usd"`
	CacheHits              int64          `json:"cache_hits"`
	Backfilled             bool           `json:"backfilled"`
	UpdatedAt              time.Time      `json:"updated_at"`
}

func (AISpendRollup) TableName() string { return "ai_spend_rollups" }

type AISpendPolicy struct {
	ComputeWatchLimits         datatypes.JSON `gorm:"type:jsonb;default:'{}'" json:"compute_watch_limits"`
	ID                         uint           `gorm:"primaryKey" json:"id"`
	TenantID                   string         `gorm:"type:varchar(64);not null;uniqueIndex" json:"tenant_id"`
	Enabled                    bool           `json:"enabled"`
	AggregationIntervalMinutes int            `json:"aggregation_interval_minutes"`
	ForecastHorizonDays        int            `json:"forecast_horizon_days"`
	RetentionDays              int            `json:"retention_days"`
	SpikeMultiplier            float64        `json:"spike_multiplier"`
	PausedUntil                *time.Time     `json:"paused_until,omitempty"`
	LastRunAt                  *time.Time     `json:"last_run_at,omitempty"`
	CreatedAt                  time.Time      `json:"created_at"`
	UpdatedAt                  time.Time      `json:"updated_at"`
}

func (AISpendPolicy) TableName() string { return "ai_spend_policies" }

type AISpendBudget struct {
	ID              uint       `gorm:"primaryKey" json:"id"`
	TenantID        string     `json:"tenant_id"`
	Scope           string     `json:"scope"`
	CapUSD          *float64   `json:"cap_usd"`
	WarnPct         float64    `json:"warn_pct"`
	HardPct         float64    `json:"hard_pct"`
	SpendUSD        float64    `json:"spend_usd"`
	ReservedUSD     float64    `json:"reserved_usd"`
	WindowStartedAt time.Time  `json:"window_started_at"`
	PausedUntil     *time.Time `json:"paused_until,omitempty"`
	UpdatedBy       string     `json:"updated_by,omitempty"`
	UpdatedAt       time.Time  `json:"updated_at"`
}

func (AISpendBudget) TableName() string { return "ai_spend_budgets" }

type AISpendRun struct {
	ID                 uint           `gorm:"primaryKey" json:"id"`
	TenantID           string         `json:"tenant_id"`
	Trigger            string         `json:"trigger"`
	Status             string         `json:"status"`
	Headline           string         `json:"headline"`
	WatermarksAdvanced datatypes.JSON `json:"watermarks_advanced,omitempty"`
	BudgetVerdicts     datatypes.JSON `json:"budget_verdicts,omitempty"`
	HygieneCounters    datatypes.JSON `json:"hygiene_counters,omitempty"`
	EventsFolded       int64          `json:"events_folded"`
	StartedAt          time.Time      `json:"started_at"`
	CompletedAt        *time.Time     `json:"completed_at,omitempty"`
	DurationMS         int64          `json:"duration_ms"`
	Error              string         `json:"error,omitempty"`
	ErrorClass         string         `json:"error_class,omitempty"`
}

func (AISpendRun) TableName() string { return "ai_spend_runs" }

type AISpendEpisode struct {
	ReopenTimes   datatypes.JSON `gorm:"type:jsonb;default:'[]'" json:"reopen_times,omitempty"`
	ClosedBy      string         `json:"closed_by,omitempty"`
	ClosedAt      *time.Time     `json:"closed_at,omitempty"`
	Confirmations int            `json:"confirmations"`
	HealthyRuns   int            `json:"healthy_runs"`
	Reopens       int            `json:"reopens"`
	ID            uint           `gorm:"primaryKey" json:"id"`
	TenantID      string         `json:"tenant_id"`
	Kind          string         `json:"kind"`
	Scope         string         `json:"scope,omitempty"`
	Status        string         `json:"status"`
	FirstSeenAt   time.Time      `json:"first_seen_at"`
	LastSeenAt    time.Time      `json:"last_seen_at"`
	Evidence      datatypes.JSON `json:"evidence,omitempty"`
	Attribution   datatypes.JSON `json:"attribution,omitempty"`
	CloseReason   string         `json:"close_reason,omitempty"`
	FalsePositive bool           `json:"false_positive"`
	CreatedAt     time.Time      `json:"created_at"`
	UpdatedAt     time.Time      `json:"updated_at"`
}

func (AISpendEpisode) TableName() string { return "ai_spend_episodes" }

// Acknowledgements outlive raw retention, so redelivery cannot bill twice.
type AISpendReceipt struct {
	EventID   uuid.UUID `gorm:"type:uuid;primaryKey" json:"event_id"`
	CreatedAt time.Time `json:"created_at"`
}

func (AISpendReceipt) TableName() string { return "ai_spend_receipts" }

type AISpendEmitter struct {
	Closed                 bool       `json:"closed"`
	GuardUnavailable       int64      `json:"guard_unavailable"`
	LastLossAt             *time.Time `json:"last_loss_at,omitempty"`
	LastGuardUnavailableAt *time.Time `json:"last_guard_unavailable_at,omitempty"`
	ProcessID              string     `gorm:"primaryKey" json:"process_id"`
	TenantID               string     `gorm:"primaryKey" json:"tenant_id"`
	SourceService          string     `json:"source_service"`
	Sequence               int64      `json:"sequence"`
	Emitted                int64      `json:"emitted"`
	Accepted               int64      `json:"accepted"`
	Dropped                int64      `json:"dropped"`
	Unobservable           int64      `json:"unobservable"`
	StartedAt              time.Time  `json:"started_at"`
	UpdatedAt              time.Time  `json:"updated_at"`
}

func (AISpendEmitter) TableName() string { return "ai_spend_emitters" }

type AISpendOverride struct {
	AdmissionSnapshot datatypes.JSON `gorm:"type:jsonb;default:'{}'" json:"admission_snapshot"`
	ID                uuid.UUID      `gorm:"type:uuid;primaryKey" json:"id"`
	TenantID          string         `json:"tenant_id"`
	TriggerSource     string         `json:"trigger_source"`
	CreatedBy         string         `json:"created_by"`
	ExpiresAt         time.Time      `json:"expires_at"`
	CreatedAt         time.Time      `json:"created_at"`
}

func (AISpendOverride) TableName() string { return "ai_spend_overrides" }

type AISpendInvoice struct {
	ID        uint      `gorm:"primaryKey" json:"id"`
	TenantID  string    `json:"tenant_id"`
	Provider  string    `json:"provider"`
	Month     time.Time `gorm:"type:date" json:"month"`
	AmountUSD float64   `json:"amount_usd"`
	Note      string    `json:"note"`
	CreatedBy string    `json:"created_by"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (AISpendInvoice) TableName() string { return "ai_spend_invoices" }

type AISpendSTTSettlement struct {
	JobID          uuid.UUID `gorm:"type:uuid;primaryKey" json:"job_id"`
	TenantID       string    `json:"tenant_id"`
	CostUSD        float64   `json:"cost_usd"`
	OwnerUpdatedAt time.Time `json:"owner_updated_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

func (AISpendSTTSettlement) TableName() string { return "ai_spend_stt_settlements" }

// Permanent provenance survives raw retention alongside the event receipt.
type AISpendOverrideUse struct {
	EventID       uuid.UUID `gorm:"type:uuid;primaryKey" json:"event_id"`
	OverrideID    uuid.UUID `gorm:"type:uuid" json:"override_id"`
	TenantID      string    `json:"tenant_id"`
	HumanActorID  string    `json:"human_actor_id"`
	TriggerSource string    `json:"trigger_source"`
	SystemRunID   string    `json:"system_run_id"`
	OccurredAt    time.Time `json:"occurred_at"`
}

func (AISpendOverrideUse) TableName() string { return "ai_spend_override_uses" }
