package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"math"
	"sort"
	"strconv"
	"time"
)

// The reporting margin is not a guaranteed overshoot bound. Concurrent admitted
// work remains unqualified, so neither a dollar nor time tolerance is published.
func aiSpendLagMargin(db *gorm.DB, tenant string, cap float64, p *models.AISpendPolicy, now time.Time, scopes ...string) (float64, error) {
	var recent float64
	q := db.Model(&models.AISpendEvent{}).Where("tenant_id=? AND occurred_at>=? AND occurred_at<?", tenant, now.Add(-time.Hour), now)
	if len(scopes) > 0 {
		q = aiSpendScopeQuery(q, scopes[0])
	} else {
		q = q.Where("spend_class='llm'")
	}
	if err := q.Select("COALESCE(sum(cost_usd),0)").Scan(&recent).Error; err != nil {
		return 0, err
	}
	lag := float64(5 + p.AggregationIntervalMinutes*60 + 60)
	return math.Max(math.Max(.01, cap*.01), math.Max(0, recent)/3600*lag), nil
}

func aiSpendGlobalPriceWriter(p utils.AdminPrincipal) bool {
	if p.Role == "admin" {
		return true
	}
	for _, role := range p.Roles {
		if role == "admin" {
			return true
		}
	}
	return false
}
func aiSpendClosingActor(c *gin.Context) string { p, _ := utils.GetAdminPrincipal(c); return p.UserID }

// A pause/disabled admission returns before budget arithmetic. Preserve the
// reviewed policy and cap anyway, labeling owner-cached spend as such.
func aiSpendAdmissionSnapshot(db *gorm.DB, tenant, trigger string) map[string]any {
	snapshot := CheckTenantSpendAllowance(db, tenant, "llm", 0, trigger, "")
	if policy, err := getTenantAISpendPolicy(db, tenant); err == nil {
		snapshot["policy_enabled"] = policy.Enabled
		snapshot["policy_paused_until"] = policy.PausedUntil
	}
	var budget models.AISpendBudget
	if err := db.Where("tenant_id=? AND scope='class:llm'", tenant).First(&budget).Error; err == nil {
		snapshot["budget_snapshot"] = map[string]any{"scope":budget.Scope,"cap_usd":budget.CapUSD,"warn_pct":budget.WarnPct,"hard_pct":budget.HardPct,"paused_until":budget.PausedUntil,"window_anchor":budget.WindowStartedAt,"cached_spend_usd":budget.SpendUSD,"reserved_usd":budget.ReservedUSD,"budget_updated_at":budget.UpdatedAt}
	} else if err != gorm.ErrRecordNotFound {
		snapshot["budget_snapshot_status"] = "unavailable"
	} else { snapshot["budget_snapshot_status"] = "not_configured" }
	return snapshot
}

func aiSpendBudgetVerdicts(db *gorm.DB, tenant string, p *models.AISpendPolicy, now time.Time) (map[string]any, error) {
	var budgets []models.AISpendBudget
	if err := db.Where("tenant_id=?", tenant).Find(&budgets).Error; err != nil {
		return nil, err
	}
	result := map[string]any{}
	for _, b := range budgets {
		forecast, err := forecastAISpendBudget(db, tenant, b, p, now)
		if err != nil {
			return nil, err
		}
		verdict := "within"
		capSet := b.CapUSD != nil
		projected, known := forecast["projected_window_usd"].(float64)
		if capSet && known && projected > *b.CapUSD {
			verdict = "over_pace"
		}
		if capSet && b.SpendUSD >= *b.CapUSD*b.WarnPct/100 {
			verdict = "warning"
		}
		margin := 0.0
		if capSet {
			margin, err = aiSpendLagMargin(db, tenant, *b.CapUSD, p, now, b.Scope)
			if err != nil {
				return nil, err
			}
		}
		if capSet && b.SpendUSD+b.ReservedUSD+margin >= *b.CapUSD*b.HardPct/100 {
			verdict = "bounded_stop"
		}
		if b.PausedUntil != nil && b.PausedUntil.After(now) {
			verdict = "paused"
		}
		enforcement := "advisory"
		if b.Scope == "class:llm" {
			enforcement = "bounded"
		}
		evidence := map[string]any{"spend_usd": b.SpendUSD, "reserved_usd": b.ReservedUSD, "cap_usd": b.CapUSD, "margin_usd": margin, "forecast": forecast, "window_ends_at": b.WindowStartedAt.Add(aiSpendWindow), "enforcement": enforcement}
		result[b.Scope] = map[string]any{"verdict": verdict, "evidence": evidence}
	}
	return result, nil
}

func aiSpendScopeHeadline(allowance map[string]any, verdicts map[string]any) string {
	headline := aiSpendHeadline(allowance)
	if headline == "observe" || headline == "paused" || headline == "bounded_stop" || allowance["ledger_health"] != "healthy" {
		return headline
	}
	rank := map[string]int{"within": 0, "over_pace": 1, "warning": 2, "paused": 3, "bounded_stop": 4}
	for _, raw := range verdicts {
		v, _ := raw.(map[string]any)
		candidate, _ := v["verdict"].(string)
		if rank[candidate] > rank[headline] {
			headline = candidate
		}
	}
	return headline
}

var aiSpendComputeDefaults = map[string]float64{"embedding": 1000000, "rerank": 1000000, "image_embed": 100000, "stt_local": 1000000}
var aiSpendComputeUnits = map[string]string{"embedding": "items", "rerank": "pairs", "image_embed": "items", "stt_local": "audio_sec"}

func aiSpendComputeLimits(p *models.AISpendPolicy) map[string]float64 {
	result := map[string]float64{}
	for k, v := range aiSpendComputeDefaults {
		result[k] = v
	}
	var configured map[string]float64
	_ = json.Unmarshal(p.ComputeWatchLimits, &configured)
	for k, v := range configured {
		result[k] = v
	}
	return result
}
func validateAISpendComputeLimits(raw []byte) bool {
	if len(raw) == 0 {
		return true
	}
	var configured map[string]float64
	if json.Unmarshal(raw, &configured) != nil {
		return false
	}
	for k, v := range configured {
		if _, ok := aiSpendComputeDefaults[k]; !ok || v <= 0 || math.IsInf(v, 0) || math.IsNaN(v) {
			return false
		}
	}
	return true
}
func aiSpendComputeWatch(db *gorm.DB, tenant string, p *models.AISpendPolicy, now time.Time) ([]map[string]any, error) {
	result := []map[string]any{}
	for class, limit := range aiSpendComputeLimits(p) {
		unit := aiSpendComputeUnits[class]
		var captured struct {
			Count int64
			Used  float64
		}
		if err := db.Model(&models.AISpendRollup{}).Where("tenant_id=? AND spend_class=? AND day=?", tenant, class, now.UTC().Truncate(24*time.Hour)).Select("count(*) count,COALESCE(sum((units->>?)::numeric),0) used", unit).Scan(&captured).Error; err != nil {
			return nil, err
		}
		state := "within"
		if captured.Count == 0 {
			state = "no_evidence"
		} else if captured.Used >= limit {
			state = "watching"
		}
		result = append(result, map[string]any{"spend_class": class, "unit": unit, "used": captured.Used, "daily_threshold": limit, "status": state, "enforcement": "advisory", "day": now.UTC().Format("2006-01-02")})
	}
	return result, nil
}

var aiSpendKnownOperations = []string{"complete", "translate", "summarize", "topic_digest", "topic_label", "chapter_proposal", "chapters_generate", "tag_extract", "classify_source", "operator_reason", "embedding", "rerank", "image_embed", "stt_local", "stt_settlement"}

func aiSpendHygiene(db *gorm.DB, tenant string, now time.Time) (map[string]any, error) {
	var counts struct {
		Unpriced, Unknown, Unavailable int64
		Events                         int64
	}
	if err := db.Model(&models.AISpendRollup{}).Where("tenant_id=? AND day>=?", tenant, now.UTC().Truncate(24*time.Hour).AddDate(0, 0, -1)).Select("COALESCE(sum(unpriced_events),0) unpriced,COALESCE(sum(unknown_events),0) unknown,COALESCE(sum(usage_unavailable_events),0) unavailable,COALESCE(sum(events),0) events").Scan(&counts).Error; err != nil {
		return nil, err
	}
	var emitters struct{ Lost, Unobservable, Unavailable int64 }
	if err := db.Model(&models.AISpendEmitter{}).Where("tenant_id=?", tenant).Select("COALESCE(sum(dropped),0) lost,COALESCE(sum(unobservable),0) unobservable,COALESCE(sum(guard_unavailable),0) unavailable").Scan(&emitters).Error; err != nil {
		return nil, err
	}
	var unknown []string
	if err := db.Model(&models.AISpendRollup{}).Where("tenant_id=? AND operation NOT IN ?", tenant, aiSpendKnownOperations).Distinct("operation").Order("operation").Pluck("operation", &unknown).Error; err != nil {
		return nil, err
	}
	share := 0.0
	if counts.Events > 0 {
		share = float64(counts.Unknown) / float64(counts.Events)
	}
	var uncertain int64
	if err := db.Model(&models.AISpendEmitter{}).Where("tenant_id=? AND NOT closed AND updated_at<? AND updated_at>=?", tenant, now.Add(-2*time.Minute), now.Add(-24*time.Hour)).Count(&uncertain).Error; err != nil {
		return nil, err
	}
	return map[string]any{"unpriced_events": counts.Unpriced, "unknown_events": counts.Unknown, "unknown_share": share, "usage_unavailable_events": counts.Unavailable, "metering_dropped_cumulative": emitters.Lost, "unobservable_cumulative": emitters.Unobservable, "guard_unavailable_cumulative": emitters.Unavailable, "stale_unclosed_emitters": uncertain, "unknown_operations": unknown, "period": "previous_and_current_utc_day", "as_of": now}, nil
}

func aiSpendSpikeSignals(db *gorm.DB, tenant string, p *models.AISpendPolicy, now time.Time) ([]aiSpendSignal, error) {
	end := now.UTC().Truncate(24 * time.Hour)
	var cells []struct {
		SpendClass, TriggerSource string
		Day                       time.Time
		Cost                      float64
	}
	if err := db.Model(&models.AISpendRollup{}).Where("tenant_id=? AND day>=? AND day<=?", tenant, end.AddDate(0, 0, -14), end).Select("spend_class,trigger_source,day,sum(cost_usd) cost").Group("spend_class,trigger_source,day").Scan(&cells).Error; err != nil {
		return nil, err
	}
	grouped := map[string][]float64{}
	first := map[string]time.Time{}
	today := map[string]float64{}
	for _, cell := range cells {
		key := cell.SpendClass + "/" + cell.TriggerSource
		if _, ok := grouped[key]; !ok {
			grouped[key] = make([]float64, 14)
		}
		if first[key].IsZero() || cell.Day.Before(first[key]) {
			first[key] = cell.Day
		}
		if cell.Day.Equal(end) {
			today[key] = cell.Cost
		} else {
			i := int(end.Sub(cell.Day).Hours()/24) - 1
			if i >= 0 && i < 14 {
				grouped[key][i] = cell.Cost
			}
		}
	}
	result := []aiSpendSignal{}
	for key, history := range grouped {
		days := int(end.Sub(first[key]).Hours() / 24)
		// A new cell has unknown history. Do not fabricate a fourteen-day baseline.
		active := false
		evidence := map[string]any{"today_usd": today[key], "history_days": days, "status": "insufficient_history", "attribution": "correlation_only"}
		if days >= 7 {
			if days < 14 {
				history = history[:days]
			}
			sort.Float64s(history)
			middle := len(history) / 2
			median := history[middle]
			if len(history)%2 == 0 {
				median = (history[middle-1] + history[middle]) / 2
			}
			active = today[key] > math.Max(1, median*p.SpikeMultiplier)
			evidence["status"] = "evaluated"
			evidence["median_14d_usd"] = median
			evidence["multiplier"] = p.SpikeMultiplier
		}
		result = append(result, aiSpendSignal{"spike", key, active, evidence})
	}
	// Resolve historical cells that disappeared from the trailing range.
	var previous []models.AISpendEpisode
	if err := db.Where("tenant_id=? AND kind='spike' AND status<>'resolved'", tenant).Find(&previous).Error; err != nil {
		return nil, err
	}
	for _, ep := range previous {
		if _, ok := grouped[ep.Scope]; !ok {
			result = append(result, aiSpendSignal{"spike", ep.Scope, false, map[string]any{"status": "no_recent_spend"}})
		}
	}
	return result, nil
}
func aiSpendSTTDrift(db *gorm.DB, tenant string, cfg models.TranscriptionConfig) (map[string]any, bool, error) {
	var settled, reserved float64
	end := cfg.MonthlyWindowStart.Add(aiSpendWindow)
	if err := db.Model(&models.AISpendEvent{}).Where("tenant_id=? AND spend_class='stt_api' AND occurred_at>=? AND occurred_at<?", tenant, cfg.MonthlyWindowStart, end).Select("COALESCE(sum(cost_usd),0)").Scan(&settled).Error; err != nil {
		return nil, false, err
	}
	if err := db.Model(&models.TranscriptionJob{}).Where("tenant_id=? AND status IN ? AND created_at>=? AND created_at<?", tenant, []string{"queued", "running"}, cfg.MonthlyWindowStart, end).Select("COALESCE(sum(reserved_cost_usd),0)").Scan(&reserved).Error; err != nil {
		return nil, false, err
	}
	evidence := map[string]any{"owner_usd": cfg.MonthlySpendUsd, "ledger_usd": settled, "owner_reserved_usd": cfg.MonthlyReservedUsd, "job_reserved_usd": reserved, "window_started_at": cfg.MonthlyWindowStart, "window_ends_at": end, "owner_url": "/platform/media"}
	return evidence, math.Abs(settled-cfg.MonthlySpendUsd) > .01 || math.Abs(reserved-cfg.MonthlyReservedUsd) > .01, nil
}

func aiSpendRecommendation(kind string) string {
	switch kind {
	case "bounded_stop", "over_pace":
		return "Review the attributed run and scope; adjust the cap or pause after review."
	case "unpriced_events":
		return "Append a price for the unmatched model; historical estimates remain unchanged."
	case "unknown_attribution":
		return "Inspect the initiating workflow and repair missing attribution."
	case "metering_loss", "untrusted_ledger", "restart_uncertainty":
		return "Check emitter delivery and unavailable usage before trusting totals."
	case "stt_drift":
		return "Review settlement and reservations in the transcription owner."
	case "compute_volume":
		return "Review compute demand and the advisory daily threshold."
	default:
		return fmt.Sprintf("Inspect %s evidence and the attributed run before taking action.", kind)
	}
}

// Read-only owner joins expose operational metadata, never prompts or packets.
func aiSpendOwnerReferences(db *gorm.DB, tenant, run string) []map[string]any {
	var triggers []string
	if db.Model(&models.AISpendRollup{}).Where("tenant_id=? AND system_run_id=?", tenant, run).Distinct("trigger_source").Pluck("trigger_source", &triggers).Error != nil {
		return []map[string]any{{"status": "unavailable"}}
	}
	refs := []map[string]any{}
	for _, trigger := range triggers {
		table, fields, url, key := "", "", "", "public_id::text"
		switch trigger {
		case "studio_autopilot":
			table, fields, url = "media_studio_runs", "status,mode,started_at,finished_at,created_by,health_before,health_after", "/platform/media/atomization?tab=autopilot"
		case "preferences_autopilot":
			table, fields, url = "preference_autopilot_runs", "status,mode,started_at,finished_at,created_by,stats_before,stats_after", "/platform/topics?tab=autopilot"
		case "operator":
			table, fields, url = "operator_investigations", "state,actor_id,created_at,updated_at", "/platform/operator"
		case "embedding_campaign":
			table, fields, url, key = "embedding_campaigns", "state,space,created_at,updated_at", "/platform/intelligence/embeddings", "id"
		default:
			continue
		}
		ref := map[string]any{"trigger_source": trigger, "system_run_id": run, "owner_url": url, "status": "unavailable"}
		if key == "id" {
			if _, err := strconv.ParseUint(run, 10, 64); err != nil {
				refs = append(refs, ref)
				continue
			}
		}
		if db.Migrator().HasTable(table) {
			var owner map[string]any
			if db.Table(table).Select(fields).Where("tenant_id=? AND "+key+"=?", tenant, run).Take(&owner).Error == nil {
				for k, v := range owner {
					if s, ok := v.(string); ok && (k == "health_before" || k == "health_after" || k == "stats_before" || k == "stats_after") {
						v = []byte(s)
					}
					if b, ok := v.([]byte); ok && json.Valid(b) {
						var decoded any
						_ = json.Unmarshal(b, &decoded)
						owner[k] = decoded
					}
				}
				ref["status"] = "matched"
				ref["metadata"] = owner
			}
		}
		refs = append(refs, ref)
	}
	return refs
}
