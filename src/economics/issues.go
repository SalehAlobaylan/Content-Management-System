package economics

import (
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"time"
)

// ObserveIssue advances at most once per closed UTC-day window, not per scheduler tick.
func ObserveIssue(tx *gorm.DB, tenant, kind, scope, headline string, active bool, evidence any, now time.Time) error {
	id := uuid.NewSHA1(uuid.NameSpaceOID, []byte(tenant+":"+kind+":"+scope)).String()
	var old Issue
	e := tx.Where("id=? AND tenant_id=?", id, tenant).Take(&old).Error
	if e != nil && !errors.Is(e, gorm.ErrRecordNotFound) {
		return e
	}
	if old.ID == "" && !active {
		return nil
	}
	if old.ID != "" && old.LastSeenAt.UTC().Truncate(24*time.Hour).Equal(now.UTC().Truncate(24*time.Hour)) {
		return nil
	}
	payload, _ := json.Marshal(evidence)
	if old.ID == "" {
		return tx.Create(&Issue{ID: id, TenantID: tenant, Kind: kind, Scope: scope, Headline: headline, Status: "candidate", Evidence: payload, FirstSeenAt: now, LastSeenAt: now, Confirmations: 1}).Error
	}
	count := old.Confirmations
	status := old.Status
	if active {
		if count < 0 {
			count = 0
		}
		count++
		status = "candidate"
		if count >= 2 {
			status = "open"
		}
	} else {
		if count > 0 {
			count = 0
		}
		count--
		status = "recovering"
		if count <= -2 {
			status = "resolved"
		}
	}
	return tx.Model(&Issue{}).Where("id=? AND tenant_id=?", id, tenant).Updates(map[string]any{"confirmations": count, "status": status, "headline": headline, "evidence": payload, "last_seen_at": now}).Error
}
func EvaluateEfficiencyIssues(tx *gorm.DB, tenant string, now time.Time) error {
	end := now.UTC().Truncate(24 * time.Hour)
	start := end.AddDate(0, 0, -14)
	split := end.AddDate(0, 0, -7)
	var usage struct {
		Current  float64
		Previous float64
	}
	q := `SELECT COALESCE(sum(value) FILTER(WHERE day>=?),0) current,COALESCE(sum(value) FILTER(WHERE day<?),0) previous FROM economics_usage_daily WHERE tenant_id=? AND day>=? AND day<? AND unit='requests' AND subject<>'synthetic'`
	if e := tx.Raw(q, split, split, tenant, start, end).Scan(&usage).Error; e != nil {
		return e
	}
	var cost struct {
		Current  float64
		Previous float64
	}
	if e := tx.Raw(`SELECT COALESCE(sum(direct+shared) FILTER(WHERE day>=?),0) current,COALESCE(sum(direct+shared) FILTER(WHERE day<?),0) previous FROM economics_cost_summaries WHERE tenant_id=? AND day>=? AND day<?`, split, split, tenant, start, end).Scan(&cost).Error; e != nil {
		return e
	}
	active := usage.Previous >= 100 && usage.Current >= 1000 && usage.Current >= usage.Previous*2 && cost.Current-cost.Previous >= 1 && cost.Current >= cost.Previous*2
	if e := ObserveIssue(tx, tenant, "cost_increase", "platform", "Serving demand and supported spending increased", active, map[string]any{"closed_window_end": end, "current_requests": usage.Current, "previous_requests": usage.Previous, "current_supported_usd": cost.Current, "previous_supported_usd": cost.Previous, "basis": "Two closed seven-day windows; requires both usage and material cost increase. Late bill changes alone do not trigger this warning."}, now); e != nil {
		return e
	}
	for _, spec := range []struct {
		Feature, Unit string
		Floor         float64
	}{{"pods", "media_seconds", 3600}, {"news", "news_opens", 30}} {
		var units struct {
			Current     float64
			Previous    float64
			Populations int64
		}
		if e := tx.Raw(`SELECT COALESCE(sum(value) FILTER(WHERE day>=?),0) current,COALESCE(sum(value) FILTER(WHERE day<?),0) previous,count(DISTINCT variant) populations FROM economics_usage_daily WHERE tenant_id=? AND day>=? AND day<? AND unit=? AND subject LIKE 'account:%'`, split, split, tenant, start, end, spec.Unit).Scan(&units).Error; e != nil {
			return e
		}
		var serving struct {
			Current  float64
			Previous float64
		}
		if e := tx.Raw(`SELECT COALESCE(sum(direct+shared) FILTER(WHERE day>=?),0) current,COALESCE(sum(direct+shared) FILTER(WHERE day<?),0) previous FROM economics_cost_summaries WHERE tenant_id=? AND day>=? AND day<? AND feature=? AND method='calibrated_resource_profile' AND subject LIKE 'account:%'`, split, split, tenant, start, end, spec.Feature).Scan(&serving).Error; e != nil {
			return e
		}
		bad := units.Populations == 1 && units.Current >= spec.Floor && units.Previous >= spec.Floor && serving.Previous > 0 && serving.Current-serving.Previous >= 1 && serving.Current/units.Current > 2*serving.Previous/units.Previous
		if e := ObserveIssue(tx, tenant, "efficiency_regression", spec.Feature, "Supported serving cost per observed unit increased", bad, map[string]any{"closed_window_end": end, "feature": spec.Feature, "unit": spec.Unit, "current_units": units.Current, "previous_units": units.Previous, "current_supported_usd": serving.Current, "previous_supported_usd": serving.Previous, "basis": "Serving allocation only; one comparable observation population; shared preparation excluded"}, now); e != nil {
			return e
		}
	}
	return nil
}
