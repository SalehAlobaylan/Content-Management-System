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
	ID                                                            uint           `gorm:"primaryKey" json:"id"`
	EventID                                                       uuid.UUID      `gorm:"type:uuid;uniqueIndex;not null" json:"event_id"`
	OccurredAt                                                    time.Time      `gorm:"not null;index" json:"occurred_at"`
	SpendClass                                                    string         `gorm:"type:varchar(32);not null;index" json:"spend_class"`
	Operation                                                     string         `gorm:"type:varchar(64);not null" json:"operation"`
	Provider                                                      string         `gorm:"type:varchar(64)" json:"provider"`
	Model                                                         string         `gorm:"type:varchar(160)" json:"model"`
	Units                                                         datatypes.JSON `gorm:"type:jsonb;not null" json:"units"`
	CostUSD, AvoidedCostUSD                                       float64
	Cached, Estimated, AvoidedCostEstimated, Unpriced, Backfilled bool
	PriceRowID                                                    *uint     `json:"price_row_id,omitempty"`
	TriggerSource                                                 string    `gorm:"type:varchar(64);not null;default:'unknown';index" json:"trigger_source"`
	SystemRunID                                                   string    `gorm:"type:varchar(96);index" json:"system_run_id,omitempty"`
	TenantID                                                      string    `gorm:"type:varchar(64);not null;default:'default'" json:"tenant_id"`
	SourceService                                                 string    `gorm:"type:varchar(64);not null" json:"source_service"`
	OverCapHuman                                                  bool      `json:"over_cap_human"`
	CreatedAt                                                     time.Time `json:"created_at"`
}

func (AISpendEvent) TableName() string { return "ai_spend_events" }

type AIPriceBook struct {
	ID                                     uint   `gorm:"primaryKey" json:"id"`
	SpendClass                             string `gorm:"type:varchar(32);not null" json:"spend_class"`
	Provider                               string `gorm:"type:varchar(64);not null" json:"provider"`
	ModelPattern                           string `gorm:"type:varchar(160);not null" json:"model_pattern"`
	InputUSDPer1M, OutputUSDPer1M, UnitUSD float64
	EffectiveFrom                          time.Time `gorm:"not null" json:"effective_from"`
	Note, CreatedBy                        string
	CreatedAt                              time.Time
}

func (AIPriceBook) TableName() string { return "ai_price_book" }

type AISpendRollup struct {
	ID                                                                 uint      `gorm:"primaryKey"`
	Day                                                                time.Time `gorm:"type:date;not null"`
	SpendClass, Operation, Provider, Model, TriggerSource, SystemRunID string
	Events                                                             int64
	Units                                                              datatypes.JSON `gorm:"type:jsonb"`
	CostUSD, AvoidedCostUSD                                            float64
	CacheHits                                                          int64
	Backfilled                                                         bool
	UpdatedAt                                                          time.Time
}

func (AISpendRollup) TableName() string { return "ai_spend_rollups" }

type AISpendPolicy struct {
	ID                                                             uint   `gorm:"primaryKey"`
	TenantID                                                       string `gorm:"type:varchar(64);not null;uniqueIndex"`
	Enabled                                                        bool
	AggregationIntervalMinutes, ForecastHorizonDays, RetentionDays int
	SpikeMultiplier                                                float64
	PausedUntil, LastRunAt                                         *time.Time
	CreatedAt, UpdatedAt                                           time.Time
}

func (AISpendPolicy) TableName() string { return "ai_spend_policies" }

type AISpendBudget struct {
	ID                                      uint `gorm:"primaryKey"`
	TenantID, Scope                         string
	CapUSD                                  *float64
	WarnPct, HardPct, SpendUSD, ReservedUSD float64
	WindowStartedAt                         time.Time
	PausedUntil                             *time.Time
	UpdatedBy                               string
	UpdatedAt                               time.Time
}

func (AISpendBudget) TableName() string { return "ai_spend_budgets" }

type AISpendRun struct {
	ID                                                  uint `gorm:"primaryKey"`
	TenantID, Trigger, Status, Headline                 string
	WatermarksAdvanced, BudgetVerdicts, HygieneCounters datatypes.JSON
	EventsFolded                                        int64
	StartedAt                                           time.Time
	CompletedAt                                         *time.Time
	DurationMS                                          int64
	Error, ErrorClass                                   string
}

func (AISpendRun) TableName() string { return "ai_spend_runs" }

type AISpendEpisode struct {
	ID                            uint `gorm:"primaryKey"`
	TenantID, Kind, Scope, Status string
	FirstSeenAt, LastSeenAt       time.Time
	Evidence, Attribution         datatypes.JSON
	CloseReason                   string
	FalsePositive                 bool
	CreatedAt, UpdatedAt          time.Time
}

func (AISpendEpisode) TableName() string { return "ai_spend_episodes" }
