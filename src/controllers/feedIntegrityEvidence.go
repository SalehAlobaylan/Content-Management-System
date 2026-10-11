package controllers

import (
	"content-management-system/src/models"
	"encoding/json"
	"fmt"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"time"
)

func integrityEpisodeEvidence(f models.FeedIntegrityFinding) datatypes.JSON {
	var data map[string]interface{}
	_ = json.Unmarshal(f.Evidence, &data)
	if data == nil {
		data = map[string]interface{}{}
	}
	data["observation_version"] = 1
	data["affected_count"] = f.AffectedCount
	data["last_observed_run_id"] = f.RunID
	return integrityJSON(data)
}
func integrityEpisodeVersioned(ep models.FeedIntegrityEpisode) bool {
	var v struct {
		Version int `json:"observation_version"`
	}
	_ = json.Unmarshal(ep.Evidence, &v)
	return v.Version == 1
}

type integrityTrendPoint struct {
	At       time.Time `json:"at"`
	Affected int       `json:"affected"`
}

func integrityTrend(ep models.FeedIntegrityEpisode, now time.Time, affected int) datatypes.JSON {
	var points []integrityTrendPoint
	_ = json.Unmarshal(ep.AffectedTrend, &points)
	points = append(points, integrityTrendPoint{now, affected})
	if len(points) > 20 {
		points = points[len(points)-20:]
	}
	return integrityJSON(points)
}
func integrityEpisodeSuppressed(db *gorm.DB, tenant string, ep models.FeedIntegrityEpisode, now time.Time) bool {
	var rows []models.FeedIntegritySuppression
	if db.Where("tenant_id=? AND check_key=?", tenant, ep.CheckKey).Find(&rows).Error != nil {
		return true
	}
	return integritySuppressed(rows, models.FeedIntegrityFinding{CheckKey: ep.CheckKey, Feed: ep.Feed, Variant: ep.Variant}, ep.Scope, now)
}
func integrityConfirmationBoundary(db *gorm.DB, tenant string, f models.FeedIntegrityFinding) (time.Time, error) {
	boundary := time.Time{}
	var latest models.FeedIntegrityEpisode
	err := db.Where("tenant_id=? AND check_key=? AND feed=? AND variant=? AND scope=? AND status='closed_by_human'", tenant, f.CheckKey, f.Feed, f.Variant, feedIntegrityScope(f)).Order("updated_at DESC").First(&latest).Error
	if err != nil && err != gorm.ErrRecordNotFound {
		return boundary, err
	}
	if err == nil {
		boundary = latest.UpdatedAt
	}
	var suppressions []models.FeedIntegritySuppression
	if err := db.Where("tenant_id=? AND check_key=?", tenant, f.CheckKey).Find(&suppressions).Error; err != nil {
		return boundary, err
	}
	for _, s := range suppressions {
		if (s.Feed != "" && s.Feed != f.Feed) || (s.Variant != "" && s.Variant != f.Variant) || (s.Scope != "" && s.Scope != feedIntegrityScope(f)) {
			continue
		}
		end := s.ExpiresAt
		if s.RevokedAt != nil {
			end = *s.RevokedAt
		}
		if end.After(boundary) {
			boundary = end
		}
	}
	return boundary, nil
}
func integrityAttribution(db *gorm.DB, tenant string, f models.FeedIntegrityFinding, onset time.Time) datatypes.JSON {
	spec := feedIntegrityActionSpecFor(f)
	type candidate struct {
		System     string    `json:"system"`
		ID         string    `json:"id"`
		FinishedAt time.Time `json:"finished_at"`
		Confidence string    `json:"confidence"`
	}
	rows := []candidate{}
	unavailable := []string{}
	tables := []struct{ Table, Timestamp, Owner string }{
		{"media_atomization_runs", "completed_at", "media-studio"}, {"media_circulation_runs", "finished_at", "media-circulation"}, {"media_studio_runs", "finished_at", "media-studio"},
		{"enrichment_autopilot_runs", "finished_at", "enrichment"}, {"news_autopilot_runs", "finished_at", "news-circulation"}, {"preference_autopilot_runs", "finished_at", "cms-feed"}, {"pipeline_autopilot_runs", "finished_at", "cms-feed"},
	}
	for _, t := range tables {
		if t.Owner != spec.Owner {
			continue
		}
		if !db.Migrator().HasTable(t.Table) || !db.Migrator().HasColumn(t.Table, t.Timestamp) {
			unavailable = append(unavailable, t.Table)
			continue
		}
		var matches []struct {
			ID         string
			FinishedAt time.Time
		}
		// A savepoint prevents an optional owner-read failure aborting finalization.
		name := "fi_attr_" + t.Table
		_ = db.SavePoint(name).Error
		err := db.Table(t.Table).Select("public_id::text AS id, COALESCE("+t.Timestamp+",started_at) AS finished_at").Where("tenant_id=? AND status NOT IN ? AND COALESCE("+t.Timestamp+",started_at) BETWEEN ? AND ?", tenant, []string{"running", "pending", "queued"}, onset.Add(-time.Hour), onset).Order(t.Timestamp + " DESC").Limit(10 - len(rows)).Scan(&matches).Error
		if err != nil {
			_ = db.RollbackTo(name).Error
			unavailable = append(unavailable, t.Table)
			continue
		}
		for _, m := range matches {
			rows = append(rows, candidate{t.Table, m.ID, m.FinishedAt, "suspected"})
		}
		if len(rows) >= 10 {
			break
		}
	}
	if len(rows) < 10 {
		var audit []models.AuditLog
		err := db.Where("tenant_id=? AND status='success' AND created_at BETWEEN ? AND ? AND (action LIKE 'ranking.%' OR action LIKE 'circulation.%' OR action LIKE 'storage.%' OR action LIKE 'media_studio.%' OR action LIKE 'atomization.%' OR action LIKE 'enrichment.%' OR action LIKE 'feed.%')", tenant, onset.Add(-time.Hour), onset).Order("created_at DESC").Limit(10 - len(rows)).Find(&audit).Error
		if err == nil {
			for _, a := range audit {
				rows = append(rows, candidate{"audit", fmt.Sprint(a.ID), a.CreatedAt, "suspected"})
			}
		}
	}
	return integrityJSON(map[string]interface{}{"owner": spec.Owner, "deep_link": spec.DeepLink, "candidates": rows, "unavailable": unavailable, "confidence": "suspected"})
}

func integrityEpisodeLastRun(ep models.FeedIntegrityEpisode) uint {
	var data struct {
		Run uint `json:"last_observed_run_id"`
	}
	_ = json.Unmarshal(ep.Evidence, &data)
	return data.Run
}
func integrityEpisodeRunEvidence(ep models.FeedIntegrityEpisode, f models.FeedIntegrityFinding, run uint, now time.Time, flap bool) (datatypes.JSON, int) {
	var previous struct {
		Flaps []time.Time `json:"flap_times"`
	}
	_ = json.Unmarshal(ep.Evidence, &previous)
	times := []time.Time{}
	for _, at := range previous.Flaps {
		if now.Sub(at) <= 24*time.Hour {
			times = append(times, at)
		}
	}
	if flap {
		times = append(times, now)
	}
	if len(times) > 20 {
		times = times[len(times)-20:]
	}
	var data map[string]interface{}
	_ = json.Unmarshal(integrityEpisodeEvidence(f), &data)
	data["last_observed_run_id"] = run
	data["flap_times"] = times
	return integrityJSON(data), len(times)
}
