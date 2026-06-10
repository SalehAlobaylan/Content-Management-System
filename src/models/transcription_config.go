package models

import "time"

// TranscriptionConfig is the admin-tunable, single-row-per-tenant config that
// governs on-demand / automatic STT enrichment of media items. Mirrors the
// RankingConfig pattern. Surfaced in the Platform-Console "Media" tab.
//
// Why a config table (not env vars): per the repo Config Discipline, runtime
// tuning knobs (the auto-STT toggle, the spend cap) belong in an admin-editable
// CMS table, not the operator env contract. The engine *selector* (which
// provider boots) stays an env var on Media-Service — this only governs WHEN
// CMS asks for STT and the budget guard.
type TranscriptionConfig struct {
	ID       uint   `gorm:"primaryKey" json:"-"`
	TenantID string `gorm:"type:varchar(64);not null;uniqueIndex:idx_transcription_config_tenant" json:"tenant_id"`

	// AutoSttEnabled is the toggle: when ON, CMS auto-runs STT on items whose
	// only caption is YouTube auto-generated (caption_state=youtube_auto) and on
	// caption-less media. When OFF, STT runs only on explicit manual trigger.
	AutoSttEnabled bool `gorm:"default:false" json:"auto_stt_enabled"`

	// Provider is informational for the UI badge ("STT (Deepgram)"). The actual
	// engine is selected by Media-Service's STT_PROVIDER env at boot.
	Provider string `gorm:"type:varchar(32);default:'deepgram'" json:"provider"`

	// MonthlyBudgetCapUsd caps auto+manual STT spend per rolling 30-day window.
	// 0 = no cap. Guard lives in CMS (not the provider dashboard) so we can't
	// forget to set it provider-side.
	MonthlyBudgetCapUsd float64 `gorm:"type:double precision;default:0" json:"monthly_budget_cap_usd"`

	// MonthlySpendUsd accumulates estimated STT cost in the current window.
	MonthlySpendUsd float64 `gorm:"type:double precision;default:0" json:"monthly_spend_usd"`

	// MonthlyReservedUsd tracks accepted/in-flight STT work. Terminal jobs move
	// from reserved to actual spend, or release the reservation on failure/skip.
	MonthlyReservedUsd float64 `gorm:"type:double precision;default:0" json:"monthly_reserved_usd"`

	// AutoRepairEnabled lets CMS automatically upgrade weak transcripts inside
	// the budget cap when quality scoring flags them as auto_repair.
	AutoRepairEnabled bool `gorm:"default:true" json:"auto_repair_enabled"`

	QualityReviewThreshold     float64 `gorm:"type:double precision;default:0.75" json:"quality_review_threshold"`
	QualityAutoRepairThreshold float64 `gorm:"type:double precision;default:0.45" json:"quality_auto_repair_threshold"`

	// MonthlyWindowStart marks when the current spend window opened; spend resets
	// when now > start + 30d.
	MonthlyWindowStart time.Time `gorm:"type:timestamp" json:"monthly_window_start"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (TranscriptionConfig) TableName() string {
	return "transcription_configs"
}

// DefaultTranscriptionConfig returns a config with safe defaults for a tenant:
// auto-STT OFF (manual only), no budget cap, fresh spend window.
func DefaultTranscriptionConfig(tenantID string) TranscriptionConfig {
	return TranscriptionConfig{
		TenantID:                   tenantID,
		AutoSttEnabled:             false,
		Provider:                   "deepgram",
		MonthlyBudgetCapUsd:        0,
		MonthlySpendUsd:            0,
		MonthlyReservedUsd:         0,
		AutoRepairEnabled:          true,
		QualityReviewThreshold:     0.75,
		QualityAutoRepairThreshold: 0.45,
		MonthlyWindowStart:         time.Now(),
	}
}
