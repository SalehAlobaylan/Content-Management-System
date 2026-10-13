package controllers

import (
	"content-management-system/src/models"
	"gorm.io/gorm"
	"strings"
	"time"
)

// Seven complete UTC days avoids extrapolating the current partial day. The
// earliest tenant evidence bounds coverage; at least three days are required.
func forecastAISpendBudget(db *gorm.DB, tenant string, b models.AISpendBudget, p *models.AISpendPolicy, now time.Time) (map[string]any, error) {
	end := now.UTC().Truncate(24 * time.Hour)
	start := end.AddDate(0, 0, -7)
	q := func() *gorm.DB {
		r := db.Model(&models.AISpendRollup{}).Where("tenant_id=?", tenant)
		if strings.HasPrefix(b.Scope, "class:") {
			r = r.Where("spend_class=?", strings.TrimPrefix(b.Scope, "class:"))
		}
		if strings.HasPrefix(b.Scope, "system:") {
			r = r.Where("trigger_source=?", strings.TrimPrefix(b.Scope, "system:"))
		}
		return r
	}
	var first struct{ At *time.Time }
	if err := q().Select("min(day) at").Scan(&first).Error; err != nil {
		return nil, err
	}
	result := map[string]any{"status": "insufficient_history", "history_days": 0, "horizon_days": p.ForecastHorizonDays, "method": "trailing_7_complete_utc_days", "as_of": now}
	if first.At == nil {
		return result, nil
	}
	if first.At.After(start) {
		start = first.At.UTC().Truncate(24 * time.Hour)
	}
	days := int(end.Sub(start).Hours() / 24)
	result["history_days"] = days
	if days < 3 {
		return result, nil
	}
	var cost float64
	if err := q().Where("day>=? AND day<?", start, end).Select("COALESCE(sum(cost_usd),0)").Scan(&cost).Error; err != nil {
		return nil, err
	}
	daily := cost / float64(days)
	remaining := b.WindowStartedAt.Add(aiSpendWindow).Sub(now).Hours() / 24
	if remaining < 0 {
		remaining = 0
	}
	result["status"] = "estimate"
	result["daily_pace_usd"] = daily
	result["projected_window_usd"] = b.SpendUSD + daily*remaining
	result["horizon_spend_usd"] = daily * float64(p.ForecastHorizonDays)
	if b.CapUSD != nil && daily > 0 {
		available := *b.CapUSD - b.SpendUSD - b.ReservedUSD
		if available < 0 {
			available = 0
		}
		// Very small spend and large caps can exceed time.Duration's range.
		// Keep the date unknown rather than wrap it into a past timestamp.
		capDays := available / daily
		if capDays < float64(1<<63-1)/float64(24*time.Hour) {
			result["cap_reached_at"] = now.Add(time.Duration(capDays * float64(24*time.Hour)))
		}
	}
	return result, nil
}
