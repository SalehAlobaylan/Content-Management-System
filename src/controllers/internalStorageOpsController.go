package controllers

import (
	"content-management-system/src/models"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// =============================================================================
// Internal: write op metrics (called hourly by Aggregation flush worker)
// =============================================================================

type opMetricItem struct {
	Tier    string `json:"tier"`     // primary | cold
	OpClass string `json:"op_class"` // A | B
	OpType  string `json:"op_type"`  // PUT | GET | HEAD | DELETE | DELETE_OBJECTS | LIST | COPY | OTHER
	Count   int64  `json:"count"`
}

type writeOpMetricsRequest struct {
	Source   string         `json:"source"`    // internal | cloudflare
	Date     string         `json:"date"`      // YYYY-MM-DD
	TenantID string         `json:"tenant_id"` // optional; defaults to "default"
	Items    []opMetricItem `json:"items"`
}

// InternalWriteOpMetrics handles POST /internal/storage/op-metrics
//
// Behaviour: for each item, UPSERT the row keyed by
// (tenant_id, date, tier, op_class, op_type, source). On conflict, ADD count
// to the existing row's count rather than overwriting. This way Aggregation
// can flush deltas-since-last-tick without us double-counting.
func InternalWriteOpMetrics(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	var req writeOpMetricsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}
	if len(req.Items) == 0 {
		c.JSON(http.StatusOK, gin.H{"success": true, "written": 0})
		return
	}
	source := strings.ToLower(strings.TrimSpace(req.Source))
	if source != "internal" && source != "cloudflare" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "source must be 'internal' or 'cloudflare'"})
		return
	}
	tenantID := strings.TrimSpace(req.TenantID)
	if tenantID == "" {
		tenantID = "default"
	}

	// Parse date — accept YYYY-MM-DD
	date, err := time.Parse("2006-01-02", strings.TrimSpace(req.Date))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "date must be YYYY-MM-DD"})
		return
	}

	written := 0
	for _, it := range req.Items {
		tier := strings.ToLower(strings.TrimSpace(it.Tier))
		if tier == "" {
			tier = "primary"
		}
		opClass := strings.ToUpper(strings.TrimSpace(it.OpClass))
		if opClass != "A" && opClass != "B" {
			continue // skip silently — telemetry shouldn't 4xx for one bad row
		}
		opType := strings.ToUpper(strings.TrimSpace(it.OpType))
		if opType == "" {
			opType = "OTHER"
		}
		if it.Count <= 0 {
			continue
		}

		row := models.StorageOpMetric{
			TenantID: tenantID,
			Date:     date,
			Tier:     tier,
			OpClass:  opClass,
			OpType:   opType,
			Source:   source,
			Count:    it.Count,
		}
		// UPSERT: on conflict (tenant, date, tier, class, type, source), add count.
		err := db.Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "tenant_id"}, {Name: "date"}, {Name: "tier"},
				{Name: "op_class"}, {Name: "op_type"}, {Name: "source"},
			},
			DoUpdates: clause.Assignments(map[string]interface{}{
				"count":      gorm.Expr("storage_op_metrics.count + ?", it.Count),
				"updated_at": time.Now().UTC(),
			}),
		}).Create(&row).Error
		if err == nil {
			written++
		}
	}

	c.JSON(http.StatusOK, gin.H{"success": true, "written": written})
}

// =============================================================================
// Internal: get budget status (called by sweepers before they enqueue work)
// =============================================================================

type opBudgetStatus struct {
	ClassAStatus    string `json:"class_a_status"` // ok | warn | cap
	ClassBStatus    string `json:"class_b_status"`
	ClassAUsed      int64  `json:"class_a_used"`
	ClassBUsed      int64  `json:"class_b_used"`
	ClassARemaining int64  `json:"class_a_remaining"`
	ClassBRemaining int64  `json:"class_b_remaining"`
	ClassABudget    int64  `json:"class_a_budget"`
	ClassBBudget    int64  `json:"class_b_budget"`
}

// InternalGetStorageOpBudget handles GET /internal/storage/op-budget?tenant_id=X
func InternalGetStorageOpBudget(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	tenantID := strings.TrimSpace(c.Query("tenant_id"))
	if tenantID == "" {
		tenantID = "default"
	}

	policy := loadEffectiveStoragePolicy(db, tenantID)
	classA, classB := monthToDateOpCounts(db, tenantID)
	resp := opBudgetStatus{
		ClassAUsed:   classA,
		ClassBUsed:   classB,
		ClassABudget: policy.ClassAFreeBudget,
		ClassBBudget: policy.ClassBFreeBudget,
		ClassAStatus: classifyBudget(classA, policy.ClassAFreeBudget, policy.ClassAWarnPct, policy.ClassACapPct),
		ClassBStatus: classifyBudget(classB, policy.ClassBFreeBudget, policy.ClassBWarnPct, policy.ClassBCapPct),
	}
	if policy.ClassAFreeBudget > 0 {
		resp.ClassARemaining = policy.ClassAFreeBudget - classA
		if resp.ClassARemaining < 0 {
			resp.ClassARemaining = 0
		}
	}
	if policy.ClassBFreeBudget > 0 {
		resp.ClassBRemaining = policy.ClassBFreeBudget - classB
		if resp.ClassBRemaining < 0 {
			resp.ClassBRemaining = 0
		}
	}
	c.JSON(http.StatusOK, resp)
}

// classifyBudget returns one of "ok", "warn", "cap" based on usage vs budget.
// A budget of 0 disables the cap entirely (always returns "ok").
func classifyBudget(used, budget int64, warnPct, capPct int) string {
	if budget <= 0 {
		return "ok"
	}
	pct := float64(used) / float64(budget) * 100.0
	switch {
	case pct >= float64(capPct):
		return "cap"
	case pct >= float64(warnPct):
		return "warn"
	default:
		return "ok"
	}
}

// monthToDateOpCounts sums op counts across all sources for the current
// calendar month, grouped by class. Used by the budget classifier.
func monthToDateOpCounts(db *gorm.DB, tenantID string) (classA, classB int64) {
	now := time.Now().UTC()
	monthStart := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)

	type sumRow struct {
		OpClass string
		Total   int64
	}
	var rows []sumRow
	db.Model(&models.StorageOpMetric{}).
		Select("op_class, COALESCE(SUM(count), 0) as total").
		Where("tenant_id = ? AND date >= ?", tenantID, monthStart).
		Group("op_class").
		Scan(&rows)
	for _, r := range rows {
		switch r.OpClass {
		case "A":
			classA = r.Total
		case "B":
			classB = r.Total
		}
	}
	return
}
