package economics

import (
	"fmt"
	"gorm.io/gorm"
	"time"
)

// Legacy owner rollups remain part of platform spending even without a consumer link.
// Pending events and folded rollups are read in one SQL snapshot, avoiding overlap.
func ProjectAIResiduals(tx *gorm.DB, tenant string) error {
	var exists bool
	if e := tx.Raw("SELECT to_regclass('ai_spend_rollups') IS NOT NULL AND to_regclass('ai_spend_stt_settlements') IS NOT NULL").Scan(&exists).Error; e != nil || !exists {
		return e
	}
	cutoff := retainedMonth(time.Now().UTC())
	var expected []struct {
		Day          time.Time
		Provider     string
		Total        string
		UnknownCount int64
	}
	q := `WITH owner AS(SELECT day,provider,cost_usd,unpriced_events+usage_unavailable_events unknown_count FROM ai_spend_rollups WHERE tenant_id=? AND day>=? AND spend_class<>'stt_api' UNION ALL SELECT date_trunc('day',occurred_at AT TIME ZONE 'UTC')::date,provider,cost_usd,CASE WHEN unpriced OR usage_unavailable THEN 1 ELSE 0 END FROM ai_spend_events WHERE tenant_id=? AND occurred_at>=? AND folded_at IS NULL AND spend_class<>'stt_api' UNION ALL SELECT date_trunc('day',j.completed_at AT TIME ZONE 'UTC')::date,j.provider,s.cost_usd,0 FROM ai_spend_stt_settlements s JOIN transcription_jobs j ON j.public_id=s.job_id AND j.tenant_id=s.tenant_id WHERE s.tenant_id=? AND j.completed_at>=?) SELECT day,provider,round(sum(cost_usd)::numeric,12)::text total,sum(unknown_count) unknown_count FROM owner GROUP BY day,provider ORDER BY day DESC LIMIT 5001`
	if e := tx.Raw(q, tenant, cutoff, tenant, cutoff, tenant, cutoff).Scan(&expected).Error; e != nil {
		return e
	}
	if len(expected) > 5000 {
		return fmt.Errorf("legacy cost backfill exceeds bounded release capacity")
	}
	var projected []struct {
		Day             time.Time
		Provider, Total string
	}
	if e := tx.Raw(`SELECT period_start AS day,provider,sum(net)::text total FROM economics_cost_revisions WHERE tenant_id=? AND source_id='ai-owner' AND period_start>=? GROUP BY period_start,provider`, tenant, cutoff).Scan(&projected).Error; e != nil {
		return e
	}
	parts := map[string]Amount{}
	for _, r := range projected {
		v, e := ParseAmount(r.Total)
		if e != nil {
			return e
		}
		parts[r.Provider+":"+r.Day.Format("2006-01-02")] = v
	}
	var previous []Cost
	if e := tx.Raw(`SELECT DISTINCT ON(logical_key) * FROM economics_cost_revisions WHERE tenant_id=? AND source_id='ai-legacy' AND period_start>=? ORDER BY logical_key,revision DESC LIMIT 5001`, tenant, cutoff).Scan(&previous).Error; e != nil {
		return e
	}
	if len(previous) > 5000 {
		return fmt.Errorf("legacy revision slice exceeds release bound")
	}
	previousKeys := make(map[string]bool, len(previous))
	for _, old := range previous {
		previousKeys[old.LogicalKey] = true
	}
	proposals := make([]Cost, 0, len(expected))
	for _, r := range expected {
		total, e := ParseAmount(r.Total)
		if e != nil {
			return e
		}
		residual := Sub(total, nonempty(parts[r.Provider+":"+r.Day.Format("2006-01-02")]))
		c := Cost{TenantID: tenant, SourceID: "ai-legacy", LogicalKey: r.Provider + ":" + r.Day.Format("2006-01-02"), Provider: r.Provider, Resource: "ai", Feature: "unknown", PeriodStart: r.Day, PeriodEnd: r.Day.AddDate(0, 0, 1), Net: residual, BeforeBenefits: &residual, Currency: "USD", Destination: "unallocated", Method: "legacy_priced_owner_rollup", Reason: "Canonical owner total less individually projected receipts; consumer provenance unavailable", Actor: "cms-projection"}
		if r.UnknownCount > 0 {
			c.Method = "incomplete_owner_evidence"
			c.BeforeBenefits = nil
		}
		if residual == Zero && r.UnknownCount == 0 {
			if !previousKeys[c.LogicalKey] {
				continue
			}
		}
		proposals = append(proposals, c)
	}
	return appendKnownFinancialRevisions(tx, proposals, previous)
}
