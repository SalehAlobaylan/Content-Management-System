package controllers

import (
	"content-management-system/src/feedcontract"
	"content-management-system/src/models"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

const requiredPodsProducingSources = 10

type sourceDiversityRow struct {
	SourceID                 string `json:"source_id"`
	SourceName               string `json:"source_name"`
	Active                   bool   `json:"active"`
	ExplicitlyScheduled      bool   `json:"explicitly_scheduled"`
	OutsideIntakeCircuit     bool   `json:"outside_intake_circuit"`
	ProviderSuccessful7d     bool   `json:"provider_successful_within_7d"`
	FetchedCandidates30d     int64  `json:"fetched_candidates_30d"`
	LegalCandidates30d       int64  `json:"legal_candidates_30d"`
	FilteredCandidates30d    int64  `json:"filtered_candidates_30d"`
	MaterializedItems30d     int64  `json:"materialized_items_30d"`
	VerifiedMedia30d         int64  `json:"verified_media_30d"`
	ReadyVisibleUnits30d     int64  `json:"ready_visible_units_30d"`
	PublicReturns30d         int64  `json:"public_returns_30d"`
	FirstPageReturns30d      int64  `json:"first_page_returns_30d"`
	Producing                bool   `json:"producing"`
	FailingBoundary          string `json:"failing_boundary"`
	GapReason                string `json:"gap_reason,omitempty"`
	SchedulingDiagnosticsURL string `json:"scheduling_diagnostics_url"`
	DiscoveryURL             string `json:"discovery_url"`
	PendingSuggestionsURL    string `json:"pending_suggestions_url"`
}

func AdminGetSourceDiversity(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	now := time.Now().UTC()
	sevenDaysAgo, thirtyDaysAgo := now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour)
	var sources []models.ContentSource
	if err := db.Where("tenant_id=? AND category=?", principal.TenantID, models.SourceCategoryMedia).
		Order("name ASC, public_id ASC").Find(&sources).Error; err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "source diversity diagnostics unavailable"})
		return
	}
	rows := make([]sourceDiversityRow, 0, len(sources))
	producing := 0
	hasYieldDaily := db.Migrator().HasTable(&models.MediaSourceYieldDaily{})
	hasTelemetry := db.Migrator().HasTable(&models.SourceRunTelemetry{})
	hasBoundaryObservations := db.Migrator().HasTable(&models.PodsBoundaryObservation{})
	hasAtomizedPodsSchema := feedcontract.SupportsAtomizedPodsSchema(db)
	hasPlaybackURL := db.Migrator().HasColumn(&models.ContentItem{}, "playback_url")
	for _, source := range sources {
		row := sourceDiversityRow{
			SourceID: source.PublicID.String(), SourceName: source.Name, Active: source.IsActive,
			ExplicitlyScheduled:      source.NextDueAt != nil,
			OutsideIntakeCircuit:     source.IntakeCircuitUntil == nil || !source.IntakeCircuitUntil.After(now),
			ProviderSuccessful7d:     source.LastProviderSuccessAt != nil && source.LastProviderSuccessAt.After(sevenDaysAgo),
			SchedulingDiagnosticsURL: "/admin/media/circulation",
			DiscoveryURL:             "/admin/discovery/media-sources/context?source_id=" + source.PublicID.String(),
			PendingSuggestionsURL:    "/admin/discovery/suggestions?source_id=" + source.PublicID.String(),
		}
		base := db.Model(&models.ContentItem{}).Where("tenant_id=? AND content_source_id=? AND created_at>=?", principal.TenantID, source.PublicID, thirtyDaysAgo)
		base.Where("duration_sec IS NOT NULL AND duration_sec BETWEEN ? AND ?", feedcontract.PodsMinDurationSec, feedcontract.PodsHardMaxDuration).Count(&row.LegalCandidates30d)
		base.Count(&row.MaterializedItems30d)
		if hasPlaybackURL {
			base.Where("playback_url IS NOT NULL AND playback_url<>''").Count(&row.VerifiedMedia30d)
		} else {
			base.Where("media_url IS NOT NULL AND media_url<>''").Count(&row.VerifiedMedia30d)
		}
		if hasAtomizedPodsSchema {
			base.Where("status=? AND is_feed_unit=true AND feed_visibility=?", models.ContentStatusReady, feedcontract.FeedVisibilityShown).Count(&row.ReadyVisibleUnits30d)
		} else {
			base.Where("status=? AND media_url IS NOT NULL AND media_url<>''", models.ContentStatusReady).Count(&row.ReadyVisibleUnits30d)
		}
		if hasTelemetry {
			var telemetry struct {
				Fetched  int64
				Filtered int64
			}
			db.Model(&models.SourceRunTelemetry{}).
				Select("COALESCE(SUM(fetched), 0) AS fetched, COALESCE(SUM(filtered), 0) AS filtered").
				Where("tenant_id=? AND source_id=? AND ((finished_at IS NOT NULL AND finished_at>=?) OR (finished_at IS NULL AND created_at>=?))", principal.TenantID, source.PublicID, thirtyDaysAgo, thirtyDaysAgo).
				Scan(&telemetry)
			row.FetchedCandidates30d = telemetry.Fetched
			row.FilteredCandidates30d = telemetry.Filtered
		}
		if hasYieldDaily {
			var daily struct {
				Fetched       int64
				Legal         int64
				Filtered      int64
				Materialized  int64
				Verified      int64
				ReadyVisible  int64
				PublicReturns int64
				FirstPage     int64
			}
			db.Model(&models.MediaSourceYieldDaily{}).
				Select("COALESCE(SUM(fetched_candidates),0) AS fetched, COALESCE(SUM(legal_duration_candidates),0) AS legal, COALESCE(SUM(filtered_candidates),0) AS filtered, COALESCE(SUM(materialized_items),0) AS materialized, COALESCE(SUM(verified_media),0) AS verified, COALESCE(SUM(ready_visible_units),0) AS ready_visible, COALESCE(SUM(public_returns),0) AS public_returns, COALESCE(SUM(first_page_returns),0) AS first_page").
				Where("tenant_id=? AND content_source_id=? AND yield_date>=?", principal.TenantID, source.PublicID, thirtyDaysAgo.Format("2006-01-02")).
				Scan(&daily)
			if daily.Fetched > row.FetchedCandidates30d {
				row.FetchedCandidates30d = daily.Fetched
			}
			if daily.Legal > row.LegalCandidates30d {
				row.LegalCandidates30d = daily.Legal
			}
			if daily.Filtered > row.FilteredCandidates30d {
				row.FilteredCandidates30d = daily.Filtered
			}
			if daily.Materialized > row.MaterializedItems30d {
				row.MaterializedItems30d = daily.Materialized
			}
			if daily.Verified > row.VerifiedMedia30d {
				row.VerifiedMedia30d = daily.Verified
			}
			if daily.ReadyVisible > row.ReadyVisibleUnits30d {
				row.ReadyVisibleUnits30d = daily.ReadyVisible
			}
			if daily.PublicReturns > row.PublicReturns30d {
				row.PublicReturns30d = daily.PublicReturns
			}
			if daily.FirstPage > row.FirstPageReturns30d {
				row.FirstPageReturns30d = daily.FirstPage
			}
		}
		if hasBoundaryObservations {
			itemIDs := db.Model(&models.ContentItem{}).
				Select("public_id").
				Where("tenant_id=? AND content_source_id=? AND created_at>=?", principal.TenantID, source.PublicID, thirtyDaysAgo)
			var publicReturns int64
			db.Model(&models.PodsBoundaryObservation{}).
				Where("tenant_id=? AND boundary=? AND verdict=? AND observed_at>=? AND content_item_id IN (?)", principal.TenantID, "feed_return", "present", thirtyDaysAgo, itemIDs).
				Count(&publicReturns)
			if publicReturns > row.PublicReturns30d {
				row.PublicReturns30d = publicReturns
			}
			var firstPageReturns int64
			db.Model(&models.PodsBoundaryObservation{}).
				Where("tenant_id=? AND boundary=? AND verdict=? AND observed_at>=? AND content_item_id IN (?)", principal.TenantID, "page_render", "present", thirtyDaysAgo, itemIDs).
				Count(&firstPageReturns)
			if firstPageReturns > row.FirstPageReturns30d {
				row.FirstPageReturns30d = firstPageReturns
			}
		}
		row.Producing = row.Active && row.ExplicitlyScheduled && row.OutsideIntakeCircuit && row.ProviderSuccessful7d && row.LegalCandidates30d > 0 && row.VerifiedMedia30d > 0 && row.ReadyVisibleUnits30d > 0
		if row.Producing {
			producing++
			row.FailingBoundary = "none"
		} else {
			row.FailingBoundary, row.GapReason = sourceDiversityFailure(row)
		}
		rows = append(rows, row)
	}
	c.JSON(http.StatusOK, gin.H{
		"tenant_id":                principal.TenantID,
		"target_producing_sources": requiredPodsProducingSources,
		"producing_source_count":   producing,
		"gap":                      maxInt(requiredPodsProducingSources-producing, 0),
		"qualified":                producing >= requiredPodsProducingSources,
		"generated_at":             now,
		"sources":                  rows,
		"approval_policy":          "source approval remains human-controlled; this endpoint is diagnostic only",
	})
}

func sourceDiversityFailure(row sourceDiversityRow) (string, string) {
	if !row.Active {
		return "inactive", "Activate and approve this Media source."
	}
	if !row.ExplicitlyScheduled {
		return "schedule_missing", "Add an explicit CMS schedule before expecting a run."
	}
	if !row.OutsideIntakeCircuit {
		return "intake_circuit_open", "Wait for or resolve the intake circuit; no provider failure is inferred."
	}
	if !row.ProviderSuccessful7d {
		return "provider_success_missing_7d", "No provider-success evidence exists in the last seven days."
	}
	if row.LegalCandidates30d == 0 {
		return "no_legal_candidate_30d", "The source has not produced a legal-duration candidate in 30 days."
	}
	if row.MaterializedItems30d == 0 {
		return "not_materialized_30d", "Candidates did not reach CMS materialization."
	}
	if row.VerifiedMedia30d == 0 {
		return "media_not_verified_30d", "Materialized media has no verified playback artifact."
	}
	if row.ReadyVisibleUnits30d == 0 {
		return "not_ready_visible_30d", "Verified media has not entered the visible Pods feed."
	}
	return "unknown", "The source is missing one or more producing-source proofs."
}
