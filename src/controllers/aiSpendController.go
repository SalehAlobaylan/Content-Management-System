package controllers

// AI Spend & Economics Governor: ledger ingest, deterministic rollups, and
// read-only/administrative CMS surface. It deliberately never calls a provider
// or mutates a spender; callers consume the allowance answer separately.

import (
	"encoding/json"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"

	"content-management-system/src/models"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

const aiSpendTenant = "default"

var aiSpendRunMu sync.Mutex

type aiSpendEventInput struct {
	EventID                                                           string         `json:"event_id"`
	OccurredAt                                                        time.Time      `json:"occurred_at"`
	SpendClass                                                        string         `json:"spend_class"`
	Operation                                                         string         `json:"operation"`
	Provider                                                          string         `json:"provider"`
	Model                                                             string         `json:"model"`
	Units                                                             map[string]any `json:"units"`
	Cached, Estimated, AvoidedCostEstimated, Backfilled, OverCapHuman bool
	TriggerSource, SystemRunID, TenantID, SourceService               string
}
type aiSpendIngestRequest struct {
	Events                                    []aiSpendEventInput `json:"events"`
	Emitted, Accepted, Dropped, ProcessStarts int64
}

func aiSpendJSON(v any) datatypes.JSON { b, _ := json.Marshal(v); return datatypes.JSON(b) }
func getAISpendPolicy(db *gorm.DB) (*models.AISpendPolicy, error) {
	var p models.AISpendPolicy
	err := db.Where("tenant_id = ?", aiSpendTenant).First(&p).Error
	if err == gorm.ErrRecordNotFound {
		p = models.AISpendPolicy{TenantID: aiSpendTenant, Enabled: false, AggregationIntervalMinutes: 5, ForecastHorizonDays: 30, SpikeMultiplier: 4, RetentionDays: 90}
		if err := db.Create(&p).Error; err != nil {
			return nil, err
		}
		return &p, nil
	}
	return &p, err
}

func resolveAISpendPrice(db *gorm.DB, in aiSpendEventInput) (*models.AIPriceBook, float64, bool) {
	var rows []models.AIPriceBook
	db.Where("spend_class = ? AND provider = ? AND effective_from <= ?", in.SpendClass, in.Provider, in.OccurredAt).Order("effective_from DESC").Find(&rows)
	for _, row := range rows {
		if row.ModelPattern != "*" && !strings.HasPrefix(in.Model, row.ModelPattern) {
			continue
		}
		units := in.Units
		input, _ := units["input_tokens"].(float64)
		output, _ := units["output_tokens"].(float64)
		audio, _ := units["audio_sec"].(float64)
		items, _ := units["items"].(float64)
		pairs, _ := units["pairs"].(float64)
		cost := input/1_000_000*row.InputUSDPer1M + output/1_000_000*row.OutputUSDPer1M + (audio+items+pairs)*row.UnitUSD
		return &row, cost, false
	}
	return nil, 0, true
}

func InternalIngestAISpendEvents(c *gin.Context) {
	var req aiSpendIngestRequest
	if err := c.ShouldBindJSON(&req); err != nil || len(req.Events) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "events are required"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	accepted := 0
	for _, in := range req.Events {
		id, err := uuid.Parse(in.EventID)
		if err != nil {
			continue
		}
		if in.OccurredAt.IsZero() {
			in.OccurredAt = time.Now().UTC()
		}
		if in.SpendClass == "" {
			in.SpendClass = "unknown"
		}
		if in.Operation == "" {
			in.Operation = "unknown"
		}
		if in.TriggerSource == "" {
			in.TriggerSource = "unknown"
		}
		if in.SourceService == "" {
			in.SourceService = "unknown"
		}
		if in.TenantID == "" {
			in.TenantID = aiSpendTenant
		}
		price, cost, unpriced := resolveAISpendPrice(db, in)
		e := models.AISpendEvent{EventID: id, OccurredAt: in.OccurredAt, SpendClass: in.SpendClass, Operation: in.Operation, Provider: in.Provider, Model: in.Model, Units: aiSpendJSON(in.Units), CostUSD: cost, Cached: in.Cached, Estimated: in.Estimated, AvoidedCostEstimated: in.AvoidedCostEstimated, Unpriced: unpriced, Backfilled: in.Backfilled, TriggerSource: in.TriggerSource, SystemRunID: in.SystemRunID, TenantID: in.TenantID, SourceService: in.SourceService, OverCapHuman: in.OverCapHuman}
		if price != nil {
			e.PriceRowID = &price.ID
		}
		if in.Cached && price != nil {
			e.AvoidedCostUSD = cost
			e.CostUSD = 0
		}
		if result := db.Where("event_id = ?", id).FirstOrCreate(&e); result.Error == nil && result.RowsAffected > 0 {
			accepted++
		}
	}
	c.JSON(http.StatusAccepted, gin.H{"accepted": accepted, "dropped_reported": req.Dropped})
}

// foldAISpendRollups rebuilds every day still represented by the retained raw
// ledger. Rebuilding from source instead of incrementing is idempotent: a
// partially-applied run cannot double-count. Do not use a completed-run
// watermark here: an event can arrive after a runner has selected its days but
// before that runner writes completed_at, which would otherwise strand the
// event behind the watermark forever.
func foldAISpendRollups(db *gorm.DB) (int64, error) {
	var folded int64
	if err := db.Model(&models.AISpendEvent{}).Count(&folded).Error; err != nil {
		return 0, err
	}
	if folded == 0 {
		return 0, nil
	}
	err := db.Exec(`
INSERT INTO ai_spend_rollups
    (day, spend_class, operation, provider, model, trigger_source, system_run_id,
     events, cost_usd, avoided_cost_usd, cache_hits, backfilled, updated_at)
SELECT date(occurred_at), spend_class, operation, COALESCE(provider, ''), COALESCE(model, ''), trigger_source,
       COALESCE(system_run_id, ''),
       count(*), COALESCE(sum(cost_usd), 0), COALESCE(sum(avoided_cost_usd), 0),
       COALESCE(sum(CASE WHEN cached THEN 1 ELSE 0 END), 0), bool_or(backfilled), now()
FROM ai_spend_events
GROUP BY date(occurred_at), spend_class, operation, COALESCE(provider, ''), COALESCE(model, ''), trigger_source, COALESCE(system_run_id, '')
ON CONFLICT (day, spend_class, operation, provider, model, trigger_source, system_run_id)
DO UPDATE SET events = EXCLUDED.events, cost_usd = EXCLUDED.cost_usd,
    avoided_cost_usd = EXCLUDED.avoided_cost_usd, cache_hits = EXCLUDED.cache_hits,
    backfilled = EXCLUDED.backfilled, updated_at = now()
`).Error
	if err != nil {
		return 0, err
	}
	return folded, nil
}

// accumulateAISpendBudgets refreshes each budget scope's window-to-date spend from
// the rollups, so the cockpit and status endpoint show real numbers instead of a
// stale zero. Scope forms: "platform" (all classes), "class:<x>", "system:<x>".
func accumulateAISpendBudgets(db *gorm.DB) error {
	var budgets []models.AISpendBudget
	if err := db.Where("tenant_id = ?", aiSpendTenant).Find(&budgets).Error; err != nil {
		return err
	}
	for _, b := range budgets {
		q := db.Model(&models.AISpendRollup{}).Where("day >= ?", b.WindowStartedAt)
		switch {
		case strings.HasPrefix(b.Scope, "class:"):
			q = q.Where("spend_class = ?", strings.TrimPrefix(b.Scope, "class:"))
		case strings.HasPrefix(b.Scope, "system:"):
			q = q.Where("trigger_source = ?", strings.TrimPrefix(b.Scope, "system:"))
		}
		var spend float64
		q.Select("COALESCE(sum(cost_usd), 0)").Scan(&spend)
		if err := db.Model(&models.AISpendBudget{}).Where("id = ?", b.ID).Update("spend_usd", spend).Error; err != nil {
			return err
		}
	}
	return nil
}

// foldAndAccumulate is the runner's ledger step: recompute rollups, then refresh
// budget window totals. Either half failing surfaces as a run error.
func foldAndAccumulate(db *gorm.DB) (int64, error) {
	folded, err := foldAISpendRollups(db)
	if err != nil {
		return folded, err
	}
	return folded, accumulateAISpendBudgets(db)
}

func RunAISpendGovernorNow(c *gin.Context) {
	runAISpendGovernor(c.MustGet("db").(*gorm.DB), "manual", c)
}
func runAISpendGovernor(db *gorm.DB, trigger string, c *gin.Context) {
	aiSpendRunMu.Lock()
	defer aiSpendRunMu.Unlock()
	started := time.Now()
	run := models.AISpendRun{TenantID: aiSpendTenant, Trigger: trigger, Status: "running", StartedAt: started}
	if err := db.Create(&run).Error; err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	folded, err := foldAndAccumulate(db)
	now := time.Now()
	run.CompletedAt = &now
	run.DurationMS = time.Since(started).Milliseconds()
	if err != nil {
		run.Status = "failed"
		run.Error = err.Error()
		run.ErrorClass = "ledger_fold"
	} else {
		run.Status = "completed"
		run.Headline = "ledger_updated"
		run.EventsFolded = folded
		db.Model(&models.AISpendPolicy{}).Where("tenant_id = ?", aiSpendTenant).Update("last_run_at", now)
	}
	db.Save(&run)
	c.JSON(http.StatusOK, run)
}

func GetAISpendStatus(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	p, err := getAISpendPolicy(db)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	var budgets []models.AISpendBudget
	var episodes []models.AISpendEpisode
	db.Where("tenant_id = ?", aiSpendTenant).Find(&budgets)
	db.Where("tenant_id = ? AND status = 'open'", aiSpendTenant).Find(&episodes)
	c.JSON(200, gin.H{"policy": p, "budgets": budgets, "episodes": episodes})
}
func GetAISpendPolicy(c *gin.Context) {
	p, err := getAISpendPolicy(c.MustGet("db").(*gorm.DB))
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, p)
}
func UpdateAISpendPolicy(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	p, err := getAISpendPolicy(db)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	var in models.AISpendPolicy
	if c.ShouldBindJSON(&in) != nil {
		c.JSON(400, gin.H{"error": "invalid policy"})
		return
	}
	in.ID = p.ID
	in.TenantID = aiSpendTenant
	if in.AggregationIntervalMinutes < 1 {
		in.AggregationIntervalMinutes = 5
	}
	if in.RetentionDays < 30 {
		in.RetentionDays = 90
	}
	db.Save(&in)
	c.JSON(200, in)
}
func ListAISpendRollups(c *gin.Context) {
	var rows []models.AISpendRollup
	c.MustGet("db").(*gorm.DB).Order("day DESC").Limit(500).Find(&rows)
	c.JSON(200, gin.H{"rollups": rows})
}
func ListAISpendRuns(c *gin.Context) {
	var rows []models.AISpendRun
	c.MustGet("db").(*gorm.DB).Order("started_at DESC").Limit(100).Find(&rows)
	c.JSON(200, gin.H{"runs": rows})
}
func ListAISpendEvents(c *gin.Context) {
	var rows []models.AISpendEvent
	c.MustGet("db").(*gorm.DB).Order("occurred_at DESC").Limit(200).Find(&rows)
	c.JSON(200, gin.H{"events": rows})
}
func ListAISpendBudgets(c *gin.Context) {
	var rows []models.AISpendBudget
	c.MustGet("db").(*gorm.DB).Where("tenant_id = ?", aiSpendTenant).Order("scope").Find(&rows)
	c.JSON(200, gin.H{"budgets": rows})
}
func UpsertAISpendBudget(c *gin.Context) {
	var in models.AISpendBudget
	if c.ShouldBindJSON(&in) != nil || strings.TrimSpace(in.Scope) == "" {
		c.JSON(400, gin.H{"error": "scope is required"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var old models.AISpendBudget
	err := db.Where("tenant_id = ? AND scope = ?", aiSpendTenant, in.Scope).First(&old).Error
	in.TenantID = aiSpendTenant
	if in.WarnPct <= 0 || in.WarnPct > 100 {
		in.WarnPct = 70
	}
	if in.HardPct <= 0 || in.HardPct > 100 {
		in.HardPct = 100
	}
	if in.WindowStartedAt.IsZero() {
		in.WindowStartedAt = time.Now().UTC()
	}
	if err == nil {
		in.ID = old.ID
		in.SpendUSD = old.SpendUSD
		in.ReservedUSD = old.ReservedUSD
		db.Save(&in)
	} else if err == gorm.ErrRecordNotFound {
		db.Create(&in)
	} else {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, in)
}

// CheckSpendAllowance is intentionally a bounded operational guard, not a
// financial hard cap: it uses settled ledger state and fails open on absence.
func CheckSpendAllowance(db *gorm.DB, class string, estimatedUSD float64, triggerSource string) map[string]any {
	answer := map[string]any{"allowed": true, "verdict": models.AISpendVerdictWithin, "reason": "within_budget", "remaining_usd": nil, "ledger_health": "healthy", "as_of": time.Now().UTC()}
	p, err := getAISpendPolicy(db)
	if err != nil || !p.Enabled {
		answer["reason"] = "governor_unavailable"
		return answer
	}
	var budget models.AISpendBudget
	if err := db.Where("tenant_id = ? AND scope = ?", aiSpendTenant, "class:"+class).First(&budget).Error; err != nil {
		answer["reason"] = "no_budget"
		return answer
	}
	if budget.PausedUntil != nil && budget.PausedUntil.After(time.Now()) {
		answer["allowed"] = false
		answer["verdict"] = models.AISpendVerdictPaused
		answer["reason"] = "paused"
		return answer
	}
	if budget.CapUSD == nil || *budget.CapUSD <= 0 {
		return answer
	}
	window := budget.WindowStartedAt
	var spend float64
	db.Model(&models.AISpendRollup{}).Where("spend_class = ? AND day >= ?", class, window).Select("COALESCE(sum(cost_usd),0)").Scan(&spend)
	remaining := math.Max(0, *budget.CapUSD-spend-budget.ReservedUSD)
	answer["remaining_usd"] = remaining
	if spend+budget.ReservedUSD+estimatedUSD >= *budget.CapUSD*budget.HardPct/100 {
		answer["allowed"] = false
		answer["verdict"] = models.AISpendVerdictBoundedStop
		answer["reason"] = "budget_bounded_stop"
	} else if spend >= *budget.CapUSD*budget.WarnPct/100 {
		answer["verdict"] = models.AISpendVerdictWarning
		answer["reason"] = "budget_warning"
	}
	return answer
}
func InternalGetAISpendAllowance(c *gin.Context) {
	class := c.DefaultQuery("class", "llm")
	c.JSON(200, CheckSpendAllowance(c.MustGet("db").(*gorm.DB), class, 0, c.DefaultQuery("trigger_source", "unknown")))
}
func ListAIPriceBook(c *gin.Context) {
	var rows []models.AIPriceBook
	c.MustGet("db").(*gorm.DB).Order("effective_from DESC").Find(&rows)
	c.JSON(200, gin.H{"prices": rows})
}
func CreateAIPriceBook(c *gin.Context) {
	var row models.AIPriceBook
	if c.ShouldBindJSON(&row) != nil || row.SpendClass == "" || row.Provider == "" {
		c.JSON(400, gin.H{"error": "spend_class and provider are required"})
		return
	}
	if row.EffectiveFrom.IsZero() {
		row.EffectiveFrom = time.Now().UTC()
	}
	if row.ModelPattern == "" {
		row.ModelPattern = "*"
	}
	if math.IsNaN(row.UnitUSD) {
		c.JSON(400, gin.H{"error": "invalid price"})
		return
	}
	c.MustGet("db").(*gorm.DB).Create(&row)
	c.JSON(201, row)
}

func StartAISpendGovernorHeartbeat(db *gorm.DB) {
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			p, err := getAISpendPolicy(db)
			if err != nil || !p.Enabled || (p.PausedUntil != nil && p.PausedUntil.After(time.Now())) {
				continue
			}
			if p.LastRunAt != nil && time.Since(*p.LastRunAt) < time.Duration(p.AggregationIntervalMinutes)*time.Minute {
				continue
			}
			aiSpendRunMu.Lock()
			folded, err := foldAndAccumulate(db)
			now := time.Now()
			run := models.AISpendRun{TenantID: aiSpendTenant, Trigger: "scheduled", Status: "completed", Headline: "ledger_updated", EventsFolded: folded, StartedAt: now, CompletedAt: &now}
			if err != nil {
				run.Status = "failed"
				run.Error = err.Error()
				run.ErrorClass = "ledger_fold"
			}
			db.Create(&run)
			db.Model(&models.AISpendPolicy{}).Where("tenant_id = ?", aiSpendTenant).Update("last_run_at", now)
			aiSpendRunMu.Unlock()
		}
	}()
}
