package controllers

import (
	"content-management-system/src/models"
	"net/http"
	"sort"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// =============================================================================
// Admin: dashboard payload for the Operations panel
// =============================================================================

type opClassSummary struct {
	Used        int64   `json:"used"`
	Budget      int64   `json:"budget"`
	Remaining   int64   `json:"remaining"`
	Pct         float64 `json:"pct"`
	Status      string  `json:"status"` // ok | warn | cap
	WarnPct     int     `json:"warn_pct"`
	CapPct      int     `json:"cap_pct"`
	ProjectedAt *string `json:"projected_to_exceed_at,omitempty"` // ISO date or nil
}

type opDailyPoint struct {
	Date    string `json:"date"` // YYYY-MM-DD
	ClassA  int64  `json:"class_a"`
	ClassB  int64  `json:"class_b"`
}

type opTypeBreakdownEntry struct {
	OpType  string `json:"op_type"`
	OpClass string `json:"op_class"`
	Count   int64  `json:"count"`
	PctOfClass float64 `json:"pct_of_class"`
}

type opSourceBreakdownEntry struct {
	Source string `json:"source"`
	Count  int64  `json:"count"`
}

type storageOperationsResponse struct {
	Month         string                   `json:"month"` // YYYY-MM
	ClassA        opClassSummary           `json:"class_a"`
	ClassB        opClassSummary           `json:"class_b"`
	Daily         []opDailyPoint           `json:"daily"`
	ByOpType      []opTypeBreakdownEntry   `json:"by_op_type"`
	BySource      []opSourceBreakdownEntry `json:"by_source"`
	GeneratedAt   string                   `json:"generated_at"`
}

// GetStorageOperations handles GET /admin/storage/operations?days=30
//
// Returns:
//   - per-class month-to-date totals + budget bars + status (ok|warn|cap)
//   - linear forecast: at the current burn rate, what date does the budget run out?
//   - daily series for the last N days (default 30) for the trend chart
//   - per-op-type breakdown for the current month (which calls dominate)
//   - per-source breakdown (internal vs cloudflare contribution)
func GetStorageOperations(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	days := atoiDefault(c.Query("days"), 30)
	if days < 1 {
		days = 1
	}
	if days > 365 {
		days = 365
	}

	policy := loadEffectiveStoragePolicy(db, principal.TenantID)
	now := time.Now().UTC()
	monthStart := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	dayCutoff := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -(days - 1))

	// Month-to-date totals per class.
	classA, classB := monthToDateOpCounts(db, principal.TenantID)

	resp := storageOperationsResponse{
		Month:       now.Format("2006-01"),
		GeneratedAt: now.Format(time.RFC3339),
	}
	resp.ClassA = buildClassSummary(classA, policy.ClassAFreeBudget, policy.ClassAWarnPct, policy.ClassACapPct, monthStart, now)
	resp.ClassB = buildClassSummary(classB, policy.ClassBFreeBudget, policy.ClassBWarnPct, policy.ClassBCapPct, monthStart, now)

	// Daily series for the trend chart.
	resp.Daily = dailyOpSeries(db, principal.TenantID, dayCutoff)

	// Per-op-type breakdown for the current month.
	resp.ByOpType = opTypeBreakdown(db, principal.TenantID, monthStart, classA, classB)

	// Per-source breakdown for the current month.
	resp.BySource = sourceBreakdown(db, principal.TenantID, monthStart)

	c.JSON(http.StatusOK, resp)
}

func buildClassSummary(used, budget int64, warnPct, capPct int, monthStart, now time.Time) opClassSummary {
	s := opClassSummary{
		Used:    used,
		Budget:  budget,
		WarnPct: warnPct,
		CapPct:  capPct,
		Status:  classifyBudget(used, budget, warnPct, capPct),
	}
	if budget > 0 {
		s.Pct = float64(used) / float64(budget) * 100.0
		s.Remaining = budget - used
		if s.Remaining < 0 {
			s.Remaining = 0
		}
	}
	// Linear forecast: extrapolate today's burn rate to when the budget hits 100%.
	// Skip for budget=0 (no cap) or used=0 (nothing to extrapolate from).
	if budget > 0 && used > 0 {
		elapsedDays := now.Sub(monthStart).Hours() / 24.0
		if elapsedDays >= 0.5 { // need at least half a day of data to project
			ratePerDay := float64(used) / elapsedDays
			if ratePerDay > 0 {
				daysToBudget := float64(budget) / ratePerDay
				projected := monthStart.Add(time.Duration(daysToBudget * 24 * float64(time.Hour)))
				if projected.Before(now) {
					projected = now // already past — show today
				}
				formatted := projected.Format("2006-01-02")
				s.ProjectedAt = &formatted
			}
		}
	}
	return s
}

func dailyOpSeries(db *gorm.DB, tenantID string, since time.Time) []opDailyPoint {
	type row struct {
		Date    time.Time
		OpClass string
		Total   int64
	}
	var rows []row
	db.Model(&models.StorageOpMetric{}).
		Select("date, op_class, COALESCE(SUM(count), 0) as total").
		Where("tenant_id = ? AND date >= ?", tenantID, since).
		Group("date, op_class").
		Order("date ASC").
		Scan(&rows)

	// Roll up into one entry per date.
	byDate := map[string]*opDailyPoint{}
	for _, r := range rows {
		key := r.Date.Format("2006-01-02")
		entry, ok := byDate[key]
		if !ok {
			entry = &opDailyPoint{Date: key}
			byDate[key] = entry
		}
		switch r.OpClass {
		case "A":
			entry.ClassA = r.Total
		case "B":
			entry.ClassB = r.Total
		}
	}

	keys := make([]string, 0, len(byDate))
	for k := range byDate {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]opDailyPoint, 0, len(keys))
	for _, k := range keys {
		out = append(out, *byDate[k])
	}
	return out
}

func opTypeBreakdown(db *gorm.DB, tenantID string, since time.Time, classATotal, classBTotal int64) []opTypeBreakdownEntry {
	type row struct {
		OpType  string
		OpClass string
		Total   int64
	}
	var rows []row
	db.Model(&models.StorageOpMetric{}).
		Select("op_type, op_class, COALESCE(SUM(count), 0) as total").
		Where("tenant_id = ? AND date >= ?", tenantID, since).
		Group("op_type, op_class").
		Order("total DESC").
		Scan(&rows)

	out := make([]opTypeBreakdownEntry, 0, len(rows))
	for _, r := range rows {
		entry := opTypeBreakdownEntry{
			OpType:  r.OpType,
			OpClass: r.OpClass,
			Count:   r.Total,
		}
		var classTotal int64
		switch r.OpClass {
		case "A":
			classTotal = classATotal
		case "B":
			classTotal = classBTotal
		}
		if classTotal > 0 {
			entry.PctOfClass = float64(r.Total) / float64(classTotal) * 100.0
		}
		out = append(out, entry)
	}
	return out
}

func sourceBreakdown(db *gorm.DB, tenantID string, since time.Time) []opSourceBreakdownEntry {
	type row struct {
		Source string
		Total  int64
	}
	var rows []row
	db.Model(&models.StorageOpMetric{}).
		Select("source, COALESCE(SUM(count), 0) as total").
		Where("tenant_id = ? AND date >= ?", tenantID, since).
		Group("source").
		Order("total DESC").
		Scan(&rows)

	out := make([]opSourceBreakdownEntry, 0, len(rows))
	for _, r := range rows {
		out = append(out, opSourceBreakdownEntry{Source: r.Source, Count: r.Total})
	}
	return out
}
