package economics

import (
	"fmt"
	"gorm.io/gorm"
	"time"
)

type EfficiencyMetric struct {
	Feature       string  `json:"feature"`
	Unit          string  `json:"unit"`
	Amount        *Amount `json:"amount"`
	ObservedUnits int64   `json:"observed_units"`
	PeerMedian    *Amount `json:"peer_median"`
	PeerP90       *Amount `json:"peer_p90"`
	PeerCount     int64   `json:"peer_count"`
	Observation   string  `json:"observation"`
	Basis         string  `json:"basis"`
	Reason        string  `json:"reason"`
}

func AccountEfficiency(db *gorm.DB, tenant, subject string, from, to time.Time) ([]EfficiencyMetric, error) {
	out := []EfficiencyMetric{}
	for _, spec := range []struct {
		Feature, Metric, Unit string
		Floor, Factor         int64
	}{{"pods", "media_seconds", "USD per observed media hour", 600, 3600}, {"news", "news_opens", "USD per distinct story opened", 5, 1}} {
		m := EfficiencyMetric{Feature: spec.Feature, Unit: spec.Unit, Basis: "Supported serving costs only; shared preparation excluded", Reason: "Requires matching serving-cost evidence, sufficient usage and a comparable observation population"}
		if spec.Feature == "pods" {
			var ambiguous bool
			if e := db.Raw(`SELECT EXISTS(SELECT 1 FROM economics_content_weights a JOIN economics_content_weights b ON b.tenant_id=a.tenant_id AND b.subject=a.subject AND b.content_id=a.content_id AND b.segment=a.segment AND b.client=a.client AND b.kind=a.kind AND b.month<>a.month WHERE a.tenant_id=? AND a.subject=? AND a.kind='media_seconds' AND a.month>=date_trunc('month',?::timestamptz) AND a.month<?)`, tenant, subject, from, to).Scan(&ambiguous).Error; e != nil {
				return nil, e
			}
			if ambiguous {
				m.Reason = "Cross-month cumulative playback lacks a qualified monthly baseline"
				out = append(out, m)
				continue
			}
		}
		var observations []struct {
			Variant string
			Value   int64
		}
		if e := db.Raw(`SELECT variant,sum(value) value FROM(SELECT * FROM economics_usage_daily UNION ALL SELECT * FROM economics_usage_monthly)u WHERE tenant_id=? AND subject=? AND day>=? AND day<? AND unit=? GROUP BY variant`, tenant, subject, from, to, spec.Metric).Scan(&observations).Error; e != nil {
			return nil, e
		}
		if len(observations) != 1 {
			out = append(out, m)
			continue
		}
		m.ObservedUnits = observations[0].Value
		m.Observation = observations[0].Variant
		if m.ObservedUnits < spec.Floor {
			out = append(out, m)
			continue
		}
		var cost struct {
			Value *string
			Count int64
		}
		if e := db.Raw(`SELECT sum(direct+shared)::text value,count(*) count FROM economics_cost_summaries WHERE tenant_id=? AND subject=? AND day>=? AND day<? AND feature=? AND method IN('calibrated_resource_profile','direct_owner_attribution')`, tenant, subject, from, to, spec.Feature).Scan(&cost).Error; e != nil {
			return nil, e
		}
		if cost.Count == 0 || cost.Value == nil {
			out = append(out, m)
			continue
		}
		total, e := ParseAmount(*cost.Value)
		if e != nil {
			return nil, e
		}
		ratio := Proportion(total, spec.Factor, m.ObservedUnits)
		m.Amount = &ratio
		m.Reason = "Partial, supported serving-cost comparison; high cost alone is not waste"
		lower, upper := spec.Floor, spec.Floor*10
		if m.ObservedUnits >= spec.Floor*100 {
			lower, upper = spec.Floor*100, 0
		} else if m.ObservedUnits >= spec.Floor*10 {
			lower, upper = spec.Floor*10, spec.Floor*100
		}
		m.Basis += fmt.Sprintf("; same feature/client and usage band starting at %d observed units", lower)
		var peers struct {
			Count  int64
			Median *string
			P90    *string
		}
		q := `WITH usage AS(SELECT subject,sum(value) units FROM(SELECT * FROM economics_usage_daily UNION ALL SELECT * FROM economics_usage_monthly)u WHERE tenant_id=? AND day>=? AND day<? AND unit=? AND subject LIKE 'account:%' GROUP BY subject HAVING sum(value)>=? AND (?::bigint=0 OR sum(value)<?::bigint) AND count(DISTINCT variant)=1 AND min(variant)=?),cost AS(SELECT subject,sum(direct+shared) amount FROM economics_cost_summaries WHERE tenant_id=? AND day>=? AND day<? AND feature=? AND method IN('calibrated_resource_profile','direct_owner_attribution') GROUP BY subject),ratios AS(SELECT c.amount*?::numeric/u.units ratio FROM cost c JOIN usage u USING(subject)) SELECT count(*) count,CASE WHEN count(*)>=30 THEN round((percentile_cont(0.5) WITHIN GROUP(ORDER BY ratio))::numeric,12)::text END median,CASE WHEN count(*)>=30 THEN round((percentile_cont(0.9) WITHIN GROUP(ORDER BY ratio))::numeric,12)::text END p90 FROM ratios`
		if e := db.Raw(q, tenant, from, to, spec.Metric, lower, upper, upper, m.Observation, tenant, from, to, spec.Feature, spec.Factor).Scan(&peers).Error; e != nil {
			return nil, e
		}
		m.PeerCount = peers.Count
		if peers.Median != nil {
			a, e := ParseAmount(*peers.Median)
			if e != nil {
				return nil, e
			}
			m.PeerMedian = &a
		}
		if peers.P90 != nil {
			a, e := ParseAmount(*peers.P90)
			if e != nil {
				return nil, e
			}
			m.PeerP90 = &a
		}
		if m.PeerCount < 30 {
			m.Reason = fmt.Sprintf("Supported serving-cost ratio; peer percentiles withheld (%d comparable accounts, minimum 30)", m.PeerCount)
		}
		out = append(out, m)
	}
	return out, nil
}
