package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

// Media Circulation Engine — stage 2 (advisory verdict/recommendation layer).
//
// Unlike News Circulation (freshness-driven repeated polling), Media Circulation
// is the admission controller of a bounded, quality-ranked library whose bound is
// economic (S3/R2 bill), not physical. It is a THIN AGGREGATOR: it re-derives
// nothing, it reads the already-built pillars (Storage+Quality, Ranking,
// Atomization) and composes them into verdicts. See docs/media-circulation-engine.md.
//
// This file holds the two persistence foundations for the layer: the tenant
// policy (circulation-specific tuning knobs only — cost/protection knobs stay in
// storage_policies and are reused) and the recommendation ledger the layer emits
// and, later, hands to the stage-5 Autopilot as its track record.

const (
	MediaCirculationPresetConservative = "conservative"
	MediaCirculationPresetBalanced     = "balanced"
	MediaCirculationPresetIntakeHungry = "intake_hungry"

	// Recommendation decision units (D4): admit reasons over a source pull
	// opportunity, evict reasons over a media item/family.
	MediaCirculationUnitSource     = "source"
	MediaCirculationUnitItemFamily = "item_family"

	// Recommendation lifecycle.
	MediaCirculationRecStatusPending    = "pending"
	MediaCirculationRecStatusApplied    = "applied"
	MediaCirculationRecStatusDismissed  = "dismissed"
	MediaCirculationRecStatusSuperseded = "superseded"
)

// MediaCirculationPolicy stores the tenant-level circulation knobs. It holds ONLY
// circulation-specific tuning (D12); storage cost/protection/op-budget knobs are
// NOT duplicated here — they stay in storage_policies and are read through the
// storage-health aggregation (D10). Disabled by default so turning the engine on
// is always a deliberate, reversible choice.
type MediaCirculationPolicy struct {
	ID       uint   `gorm:"primaryKey" json:"-"`
	TenantID string `gorm:"type:varchar(64);not null;uniqueIndex:idx_media_circulation_policy_tenant" json:"tenant_id"`

	Enabled bool   `gorm:"not null;default:false" json:"enabled"`
	Preset  string `gorm:"type:varchar(32);not null;default:'balanced'" json:"preset"`

	// Quality gate (D8): below the storage cost target the absolute value floor
	// applies; at/above target the marginal margin decides whether incoming beats
	// the eviction candidate.
	ValueFloor     float64 `gorm:"type:double precision;not null;default:0.15" json:"value_floor"`
	MarginalMargin float64 `gorm:"type:double precision;not null;default:0.10" json:"marginal_margin"`

	// Intake budget (D13).
	MaxIntakePerSourcePerCycle int `gorm:"type:integer;not null;default:5" json:"max_intake_per_source_per_cycle"`
	MaxIntakePerCycle          int `gorm:"type:integer;not null;default:25" json:"max_intake_per_cycle"`

	// Source cadence bounds — media polls far slower than news.
	SourceMinIntervalMinutes int `gorm:"type:integer;not null;default:60" json:"source_min_interval_minutes"`
	SourceMaxIntervalMinutes int `gorm:"type:integer;not null;default:10080" json:"source_max_interval_minutes"`

	// Freshness is a demand signal, never a gate override (D2a).
	FreshnessDemandWeight float64 `gorm:"type:double precision;not null;default:0.20" json:"freshness_demand_weight"`

	LastEvaluatedAt *time.Time `gorm:"type:timestamp" json:"last_evaluated_at,omitempty"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (MediaCirculationPolicy) TableName() string {
	return "media_circulation_policies"
}

func DefaultMediaCirculationPolicy(tenantID string) MediaCirculationPolicy {
	return MediaCirculationPolicy{
		TenantID:                   tenantID,
		Enabled:                    false,
		Preset:                     MediaCirculationPresetBalanced,
		ValueFloor:                 0.15,
		MarginalMargin:             0.10,
		MaxIntakePerSourcePerCycle: 5,
		MaxIntakePerCycle:          25,
		SourceMinIntervalMinutes:   60,
		SourceMaxIntervalMinutes:   10080,
		FreshnessDemandWeight:      0.20,
	}
}

// MediaCirculationRecommendation is the persisted, reviewable output of the layer
// (D11). Rows are created in later slices; the table and its autopilot-ready shape
// (stable public_id, outcome, reason snapshot) exist from Slice 1 so the stage-5
// Autopilot can consume this history as its track record without a reshape.
type MediaCirculationRecommendation struct {
	ID       uint      `gorm:"primaryKey" json:"-"`
	PublicID uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex:idx_media_circ_recs_public_id" json:"id"`
	TenantID string    `gorm:"type:varchar(64);not null;index:idx_media_circ_recs_tenant_unit_status,priority:1;index:idx_media_circ_recs_tenant_subject,priority:1" json:"tenant_id"`

	// Decision unit (D4): "source" (admit) or "item_family" (evict).
	UnitType string `gorm:"type:varchar(24);not null;index:idx_media_circ_recs_tenant_unit_status,priority:2" json:"unit_type"`

	// The concrete subject: a content_source (admit) or a content_item/family
	// (evict). SubjectKind disambiguates for the reader.
	SubjectID   uuid.UUID `gorm:"type:uuid;not null;index:idx_media_circ_recs_tenant_subject,priority:2" json:"subject_id"`
	SubjectKind string    `gorm:"type:varchar(24)" json:"subject_kind,omitempty"`

	Verdict string  `gorm:"type:varchar(32);not null" json:"verdict"`
	Action  string  `gorm:"type:varchar(32);not null" json:"action"`
	Score   float64 `gorm:"type:double precision;not null;default:0" json:"score"`

	// Human-readable proof (D9) and the input snapshot behind the verdict.
	Reasons datatypes.JSON `gorm:"type:jsonb" json:"reasons,omitempty"`
	Metrics datatypes.JSON `gorm:"type:jsonb" json:"metrics,omitempty"`

	Status  string `gorm:"type:varchar(24);not null;default:'pending';index:idx_media_circ_recs_tenant_unit_status,priority:3" json:"status"`
	Outcome string `gorm:"type:varchar(32)" json:"outcome,omitempty"`

	Applied   bool       `gorm:"not null;default:false" json:"applied"`
	AppliedAt *time.Time `gorm:"type:timestamp" json:"applied_at,omitempty"`
	AppliedBy string     `gorm:"type:varchar(255)" json:"applied_by,omitempty"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (MediaCirculationRecommendation) TableName() string {
	return "media_circulation_recommendations"
}
