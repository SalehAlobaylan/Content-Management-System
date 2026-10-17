package controllers

import (
	"content-management-system/src/economics"
	"content-management-system/src/models"
	"encoding/json"
	"errors"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"math"
	"net/http"
	"strconv"
	"time"
)

func aiSpendPeriod(c *gin.Context) (time.Time, time.Time, error) {
	end := time.Now().UTC().Truncate(24 * time.Hour).Add(24 * time.Hour)
	start := end.AddDate(0, 0, -30)
	for k, target := range map[string]*time.Time{"from": &start, "to": &end} {
		if s := c.Query(k); s != "" {
			v, err := time.Parse("2006-01-02", s)
			if err != nil {
				return start, end, err
			}
			*target = v
		}
	}
	if !start.Before(end) || end.Sub(start) > 366*24*time.Hour {
		return start, end, errors.New("period must be 1..366 days, with exclusive to date")
	}
	return start, end, nil
}
func aiSpendReadQuery(c *gin.Context, db *gorm.DB, tenant, table string) (*gorm.DB, error) {
	q := db.Table(table).Where("tenant_id=?", tenant)
	from, to, err := aiSpendPeriod(c)
	if err != nil {
		return nil, err
	}
	date := "day"
	if table == "ai_spend_events" {
		date = "occurred_at"
	}
	q = q.Where(date+">=? AND "+date+"<?", from, to)
	for _, f := range []string{"spend_class", "operation", "provider", "model", "trigger_source", "system_run_id", "human_actor_id"} {
		if s := c.Query(f); s != "" {
			q = q.Where(f+"=?", s)
		}
	}
	return q, nil
}
func aiSpendPagination(c *gin.Context) (int, int) {
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "100"))
	offset, _ := strconv.Atoi(c.DefaultQuery("offset", "0"))
	if limit < 1 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	if offset < 0 {
		offset = 0
	}
	return limit, offset
}
func ListAISpendRollups(c *gin.Context) {
	tenant, ok := aiSpendTenantFromAdmin(c)
	if !ok {
		return
	}
	q, err := aiSpendReadQuery(c, c.MustGet("db").(*gorm.DB), tenant, "ai_spend_rollups")
	if err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	var count int64
	if err = q.Count(&count).Error; err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	limit, offset := aiSpendPagination(c)
	var rows []models.AISpendRollup
	if err = q.Order("day DESC,id DESC").Limit(limit).Offset(offset).Find(&rows).Error; err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"rollups": rows, "total": count, "limit": limit, "offset": offset})
}
func ListAISpendEvents(c *gin.Context) {
	tenant, ok := aiSpendTenantFromAdmin(c)
	if !ok {
		return
	}
	q, err := aiSpendReadQuery(c, c.MustGet("db").(*gorm.DB), tenant, "ai_spend_events")
	if err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	var count int64
	if err = q.Count(&count).Error; err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	limit, offset := aiSpendPagination(c)
	var rows []models.AISpendEvent
	if err = q.Order("occurred_at DESC,id DESC").Limit(limit).Offset(offset).Find(&rows).Error; err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"events": rows, "total": count, "limit": limit, "offset": offset})
}
func ListAISpendRuns(c *gin.Context) {
	tenant, ok := aiSpendTenantFromAdmin(c)
	if !ok {
		return
	}
	limit, offset := aiSpendPagination(c)
	q := c.MustGet("db").(*gorm.DB).Model(&models.AISpendRun{}).Where("tenant_id=?", tenant)
	if c.Query("from") != "" || c.Query("to") != "" {
		from, to, err := aiSpendPeriod(c)
		if err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}
		q = q.Where("started_at>=? AND started_at<?", from, to)
	}
	for _, field := range []string{"status", "trigger"} {
		if value := c.Query(field); value != "" {
			q = q.Where(field+"=?", value)
		}
	}
	var count int64
	if err := q.Count(&count).Error; err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	var rows []models.AISpendRun
	if err := q.Order("started_at DESC").Limit(limit).Offset(offset).Find(&rows).Error; err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"runs": rows, "total": count, "limit": limit, "offset": offset})
}

func GetAISpendRun(c *gin.Context) {
	tenant, ok := aiSpendTenantFromAdmin(c)
	if !ok {
		return
	}
	var row models.AISpendRun
	err := c.MustGet("db").(*gorm.DB).Where("tenant_id=? AND id=?", tenant, c.Param("id")).First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		c.JSON(404, gin.H{"error": "run not found"})
		return
	}
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, row)
}
func ListAISpendEpisodes(c *gin.Context) {
	tenant, ok := aiSpendTenantFromAdmin(c)
	if !ok {
		return
	}
	q := c.MustGet("db").(*gorm.DB).Where("tenant_id=?", tenant)
	if s := c.Query("status"); s != "" {
		q = q.Where("status=?", s)
	}
	var rows []models.AISpendEpisode
	limit, offset := aiSpendPagination(c)
	if err := q.Order("updated_at DESC").Limit(limit).Offset(offset).Find(&rows).Error; err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"episodes": rows})
}
func CloseAISpendEpisode(c *gin.Context) {
	tenant, ok := aiSpendTenantFromAdmin(c)
	if !ok {
		return
	}
	var req struct {
		Reason        string `json:"reason"`
		FalsePositive bool   `json:"false_positive"`
	}
	if c.ShouldBindJSON(&req) != nil || req.Reason == "" {
		c.JSON(400, gin.H{"error": "reason required"})
		return
	}
	r := c.MustGet("db").(*gorm.DB).Model(&models.AISpendEpisode{}).Where("id=? AND tenant_id=?", c.Param("id"), tenant).Updates(map[string]any{"status": "resolved", "close_reason": req.Reason, "false_positive": req.FalsePositive, "closed_by": aiSpendClosingActor(c), "reopens": 0, "reopen_times": aiSpendJSON([]time.Time{}), "closed_at": time.Now().UTC(), "updated_at": time.Now().UTC()})
	if r.Error != nil {
		c.JSON(500, gin.H{"error": r.Error.Error()})
		return
	}
	if r.RowsAffected == 0 {
		c.JSON(404, gin.H{"error": "episode not found"})
		return
	}
	c.JSON(200, gin.H{"status": "resolved"})
}

type aiSpendTotals struct {
	CostUSD                float64 `json:"cost_usd"`
	AvoidedCostUSD         float64 `json:"avoided_cost_usd"`
	AvoidedEstimatedUSD    float64 `json:"avoided_estimated_usd"`
	Events                 int64   `json:"events"`
	CacheHits              int64   `json:"cache_hits"`
	EstimatedEvents        int64   `json:"estimated_events"`
	UnpricedEvents         int64   `json:"unpriced_events"`
	UnknownEvents          int64   `json:"unknown_events"`
	UsageUnavailableEvents int64   `json:"usage_unavailable_events"`
}

const aiSpendTotalSQL = "COALESCE(sum(cost_usd),0) cost_usd,COALESCE(sum(avoided_cost_usd),0) avoided_cost_usd,COALESCE(sum(avoided_estimated_usd),0) avoided_estimated_usd,COALESCE(sum(events),0) events,COALESCE(sum(cache_hits),0) cache_hits,COALESCE(sum(estimated_events),0) estimated_events,COALESCE(sum(unpriced_events),0) unpriced_events,COALESCE(sum(unknown_events),0) unknown_events,COALESCE(sum(usage_unavailable_events),0) usage_unavailable_events"

func GetAISpendStatus(c *gin.Context) {
	tenant, ok := aiSpendTenantFromAdmin(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	p, err := getTenantAISpendPolicy(db, tenant)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	q, err := aiSpendReadQuery(c, db, tenant, "ai_spend_rollups")
	if err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	fail := func(err error) bool {
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return true
		}
		return false
	}
	var totals aiSpendTotals
	if fail(q.Select(aiSpendTotalSQL).Scan(&totals).Error) {
		return
	}
	var budgets []models.AISpendBudget
	var episodes []models.AISpendEpisode
	var emitters []models.AISpendEmitter
	if fail(db.Where("tenant_id=?", tenant).Order("scope").Find(&budgets).Error) || fail(db.Where("tenant_id=? AND status IN ?", tenant, []string{"open", "flapping", "recovering"}).Find(&episodes).Error) || fail(db.Where("tenant_id=?", tenant).Order("updated_at DESC").Limit(100).Find(&emitters).Error) {
		return
	}
	evidence := gin.H{}
	for _, table := range []string{"ai_spend_events", "ai_spend_rollups", "ai_spend_runs"} {
		var count int64
		if fail(db.Table(table).Where("tenant_id=?", tenant).Count(&count).Error) {
			return
		}
		key := map[string]string{"ai_spend_events": "event_count", "ai_spend_rollups": "rollup_count", "ai_spend_runs": "run_count"}[table]
		evidence[key] = count
	}
	var lastEvent, lastRollup struct{ Value *time.Time }
	if fail(db.Model(&models.AISpendEvent{}).Where("tenant_id=?", tenant).Select("max(occurred_at) value").Scan(&lastEvent).Error) || fail(db.Model(&models.AISpendRollup{}).Where("tenant_id=?", tenant).Select("max(updated_at) value").Scan(&lastRollup).Error) {
		return
	}
	evidence["last_event_at"] = lastEvent.Value
	evidence["last_rollup_at"] = lastRollup.Value
	evidence["emitters"] = emitters
	health, err := aiSpendLedgerHealth(db, tenant, p, time.Now().UTC())
	if fail(err) {
		return
	}
	evidence["ledger_health"] = health
	var unallocated aiSpendTotals
	from, to, _ := aiSpendPeriod(c)
	if fail(db.Model(&models.AISpendRollup{}).Where("tenant_id='legacy_unallocated' AND day>=? AND day<?", from, to).Select(aiSpendTotalSQL).Scan(&unallocated).Error) {
		return
	}
	// Legacy evidence is globally mixed, so disclose existence without exposing amounts across tenants.
	evidence["legacy_unallocated_present"] = unallocated.Events > 0
	breakdown := gin.H{}
	for _, dimension := range []string{"day", "spend_class", "operation", "provider", "model", "trigger_source"} {
		var rows []struct {
			Key                 string  `json:"key"`
			CostUSD             float64 `json:"cost_usd"`
			Events              int64   `json:"events"`
			AvoidedCostUSD      float64 `json:"avoided_cost_usd"`
			AvoidedEstimatedUSD float64 `json:"avoided_estimated_usd"`
		}
		groupQ, err := aiSpendReadQuery(c, db, tenant, "ai_spend_rollups")
		if fail(err) {
			return
		}
		if fail(groupQ.Select(dimension + "::text key,sum(cost_usd) cost_usd,sum(events) events,sum(avoided_cost_usd) avoided_cost_usd,sum(avoided_estimated_usd) avoided_estimated_usd").Group(dimension).Order(dimension).Scan(&rows).Error) {
			return
		}
		breakdown[dimension] = rows
	}
	var dailyByClass []struct {
		Day        string  `json:"day"`
		SpendClass string  `json:"spend_class"`
		CostUSD    float64 `json:"cost_usd"`
	}
	dailyQ, err := aiSpendReadQuery(c, db, tenant, "ai_spend_rollups")
	if fail(err) {
		return
	}
	if fail(dailyQ.Select("day::text AS \"day\",spend_class,sum(cost_usd) cost_usd").Group("day,spend_class").Order("day,spend_class").Scan(&dailyByClass).Error) {
		return
	}
	var itemEconomics []struct {
		Operation      string  `json:"operation"`
		Items          float64 `json:"items"`
		CostUSD        float64 `json:"cost_usd"`
		CapturedEvents int64   `json:"captured_events"`
		TotalEvents    int64   `json:"total_events"`
	}
	itemQ, err := aiSpendReadQuery(c, db, tenant, "ai_spend_rollups")
	if fail(err) {
		return
	}
	if fail(itemQ.Where("spend_class='llm' AND operation IN ?", []string{"translate", "chapter_proposal", "topic_digest"}).Select("operation,COALESCE(sum((units->>'operation_items')::numeric),0) items,COALESCE(sum(cost_usd) FILTER (WHERE units ? 'operation_items'),0) cost_usd,COALESCE(sum(events) FILTER (WHERE units ? 'operation_items'),0) captured_events,sum(events) total_events").Group("operation").Scan(&itemEconomics).Error) {
		return
	}
	var units []struct {
		SpendClass string  `json:"spend_class"`
		Operation  string  `json:"operation"`
		Key        string  `json:"key"`
		Value      float64 `json:"value"`
	}
	unitQ, err := aiSpendReadQuery(c, db, tenant, "ai_spend_rollups")
	if fail(err) {
		return
	}
	if fail(unitQ.Joins("CROSS JOIN LATERAL jsonb_each_text(ai_spend_rollups.units)").Select("spend_class,operation,key,sum(value::numeric) value").Group("spend_class,operation,key").Scan(&units).Error) {
		return
	}
	var stt models.TranscriptionConfig
	err = db.Where("tenant_id=?", tenant).First(&stt).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		if fail(err) {
			return
		}
	}
	var latest models.AISpendRun
	err = db.Where("tenant_id=?", tenant).Order("started_at DESC").First(&latest).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		if fail(err) {
			return
		}
	}
	forecasts := map[string]any{}
	for _, b := range budgets {
		f, err := forecastAISpendBudget(db, tenant, b, p, time.Now().UTC())
		if fail(err) {
			return
		}
		forecasts[b.Scope] = f
	}
	var pending int64
	if fail(db.Model(&models.AISpendEvent{}).Where("tenant_id=? AND folded_at IS NULL", tenant).Count(&pending).Error) {
		return
	}
	evidence["pending_fold_events"] = pending
	sttDriftStatus := "unavailable"
	var sttDrift map[string]any
	if stt.ID > 0 {
		var active bool
		sttDrift, active, err = aiSpendSTTDrift(db, tenant, stt)
		if fail(err) {
			return
		}
		sttDriftStatus = "within_tolerance"
		if active {
			sttDriftStatus = "drift_detected"
		}
	}
	allowance := CheckTenantSpendAllowance(db, tenant, "llm", 0, "unknown", "")
	verdicts, err := aiSpendBudgetVerdicts(db, tenant, p, time.Now().UTC())
	if fail(err) {
		return
	}
	watches, err := aiSpendComputeWatch(db, tenant, p, time.Now().UTC())
	if fail(err) {
		return
	}
	hygiene, err := aiSpendHygiene(db, tenant, time.Now().UTC())
	if fail(err) {
		return
	}
	evidence["hygiene"] = hygiene
	c.JSON(200, gin.H{"headline": aiSpendScopeHeadline(allowance, verdicts), "budget_verdicts": verdicts, "compute_watch": watches, "item_economics": itemEconomics, "daily_by_class": dailyByClass, "policy": p, "budgets": budgets, "episodes": episodes, "totals": totals, "breakdown": breakdown, "units": units, "forecasts": forecasts, "from": from, "to": to, "evidence": evidence, "latest_run": latest, "allowance": allowance, "stt_owner": gin.H{"configured": stt.ID > 0, "cap_usd": stt.MonthlyBudgetCapUsd, "spend_usd": stt.MonthlySpendUsd, "reserved_usd": stt.MonthlyReservedUsd, "window_started_at": stt.MonthlyWindowStart, "drift_status": sttDriftStatus, "drift": sttDrift, "enforcement": "owner_delegated", "url": "/platform/media"}, "coverage": aiSpendCoverage, "storage": aiSpendStorageView(db, tenant, time.Now().UTC())})
}
func ListAISpendByRun(c *gin.Context) {
	tenant, ok := aiSpendTenantFromAdmin(c)
	if !ok {
		return
	}
	q, err := aiSpendReadQuery(c, c.MustGet("db").(*gorm.DB), tenant, "ai_spend_rollups")
	if err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	var rows []struct {
		TriggerSource string  `json:"trigger_source"`
		SystemRunID   string  `json:"system_run_id"`
		CostUSD       float64 `json:"cost_usd"`
		Events        int64   `json:"events"`
	}
	if err = q.Select("trigger_source,system_run_id,sum(cost_usd) cost_usd,sum(events) events").Group("trigger_source,system_run_id").Order("sum(cost_usd) DESC").Limit(500).Scan(&rows).Error; err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"runs": rows})
}

// Run history has no implicit 30-day cutoff: permanent rollups remain useful
// after raw retention. An explicitly selected period can narrow the projection.
func GetAISpendByRun(c *gin.Context) {
	tenant, ok := aiSpendTenantFromAdmin(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	q := db.Model(&models.AISpendRollup{}).Where("tenant_id=? AND system_run_id=?", tenant, c.Param("id"))
	if c.Query("from") != "" || c.Query("to") != "" {
		from, to, err := aiSpendPeriod(c)
		if err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}
		q = q.Where("day>=? AND day<?", from, to)
	}
	var totals aiSpendTotals
	if err := q.Select(aiSpendTotalSQL).Scan(&totals).Error; err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	var rows []models.AISpendRollup
	limit, offset := aiSpendPagination(c)
	var rollupTotal int64
	if err := q.Select("*").Count(&rollupTotal).Error; err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	if err := q.Select("*").Order("day,operation").Limit(limit).Offset(offset).Find(&rows).Error; err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	var humans []struct {
		models.AISpendOverrideUse
		AdmissionSnapshot json.RawMessage `json:"admission_snapshot"`
	}
	humanQ := db.Model(&models.AISpendOverrideUse{}).Joins("JOIN ai_spend_overrides g ON g.id=ai_spend_override_uses.override_id").Where("ai_spend_override_uses.tenant_id=? AND system_run_id=?", tenant, c.Param("id"))
	var humanTotal int64
	if err := humanQ.Count(&humanTotal).Error; err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	if err := humanQ.Select("ai_spend_override_uses.*,g.admission_snapshot").Order("occurred_at").Limit(limit).Offset(offset).Scan(&humans).Error; err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"system_run_id": c.Param("id"), "totals": totals, "rollups": rows, "rollup_total": rollupTotal, "human_total": humanTotal, "human_confirmations": humans, "owner_references": aiSpendOwnerReferences(db, tenant, c.Param("id")), "limit": limit, "offset": offset})
}
func UpsertAISpendInvoice(c *gin.Context) {
	p, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	var r models.AISpendInvoice
	if c.ShouldBindJSON(&r) != nil || r.Provider == "" || r.Month.IsZero() || math.IsNaN(r.AmountUSD) || math.IsInf(r.AmountUSD, 0) || math.Abs(r.AmountUSD) >= 1e18 {
		c.JSON(400, gin.H{"error": "provider, month and finite USD amount required; negative net credits are supported"})
		return
	}
	r.ID = 0
	r.TenantID = p.TenantID
	r.CreatedBy = p.UserID
	r.Month = time.Date(r.Month.Year(), r.Month.Month(), 1, 0, 0, 0, 0, time.UTC)
	if err := c.MustGet("db").(*gorm.DB).Transaction(func(tx *gorm.DB) error {
		if economics.Available(tx) {
			if e := economics.Lock(tx); e != nil {
				return e
			}
		}
		if e := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "tenant_id"}, {Name: "provider"}, {Name: "month"}}, DoUpdates: clause.AssignmentColumns([]string{"amount_usd", "note", "created_by", "updated_at"})}).Create(&r).Error; e != nil {
			return e
		}
		if economics.Available(tx) {
			return economics.RecordAIInvoice(tx, r)
		}
		return nil
	}); err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, r)
}
func ListAISpendInvoices(c *gin.Context) {
	tenant, ok := aiSpendTenantFromAdmin(c)
	if !ok {
		return
	}
	var rows []struct {
		models.AISpendInvoice
		LedgerUSD float64 `json:"ledger_usd"`
		DeltaUSD  float64 `json:"delta_usd"`
	}
	if err := c.MustGet("db").(*gorm.DB).Raw("SELECT i.*,COALESCE(sum(r.cost_usd),0) ledger_usd,i.amount_usd-COALESCE(sum(r.cost_usd),0) delta_usd FROM ai_spend_invoices i LEFT JOIN ai_spend_rollups r ON r.tenant_id=i.tenant_id AND r.provider=i.provider AND r.day>=i.month AND r.day<i.month+interval '1 month' WHERE i.tenant_id=? GROUP BY i.id ORDER BY i.month DESC LIMIT 100", tenant).Scan(&rows).Error; err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"invoices": rows})
}
