package controllers

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"content-management-system/src/economics"
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const aiSpendTenant = "default"
const aiSpendWindow = 30 * 24 * time.Hour

func aiSpendJSON(v any) datatypes.JSON { b, _ := json.Marshal(v); return b }
func aiSpendTenantFromAdmin(c *gin.Context) (string, bool) {
	p, ok := requireAdminPrincipal(c)
	return p.TenantID, ok
}
func getAISpendPolicy(db *gorm.DB) (*models.AISpendPolicy, error) {
	return getTenantAISpendPolicy(db, aiSpendTenant)
}
func getTenantAISpendPolicy(db *gorm.DB, tenant string) (*models.AISpendPolicy, error) {
	p := models.AISpendPolicy{TenantID: tenant, AggregationIntervalMinutes: 5, ForecastHorizonDays: 30, SpikeMultiplier: 4, RetentionDays: 90}
	if err := db.Clauses(clause.OnConflict{DoNothing: true}).Where("tenant_id=?", tenant).FirstOrCreate(&p).Error; err != nil {
		return nil, err
	}
	return &p, nil
}

type aiSpendEventInput struct {
	EconomicsAttributionRef string         `json:"economics_attribution_ref"`
	HumanActorID            string         `json:"human_actor_id"`
	EventID                 string         `json:"event_id"`
	OccurredAt              time.Time      `json:"occurred_at"`
	SpendClass              string         `json:"spend_class"`
	Operation               string         `json:"operation"`
	Provider                string         `json:"provider"`
	Model                   string         `json:"model"`
	Units                   map[string]any `json:"units"`
	EventCount              int64          `json:"event_count"`
	Cached                  bool           `json:"cached"`
	Estimated               bool           `json:"estimated"`
	AvoidedCostEstimated    bool           `json:"avoided_cost_estimated"`
	UsageUnavailable        bool           `json:"usage_unavailable"`
	Backfilled              bool           `json:"backfilled"`
	OverCapHuman            bool           `json:"over_cap_human"`
	OverrideID              string         `json:"override_id"`
	TriggerSource           string         `json:"trigger_source"`
	SystemRunID             string         `json:"system_run_id"`
	TenantID                string         `json:"tenant_id"`
	SourceService           string         `json:"source_service"`
}

func aiSpendNumber(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case int:
		return float64(x)
	case int64:
		return float64(x)
	case json.Number:
		f, _ := x.Float64()
		return f
	}
	return 0
}

// Most-specific literal prefix wins, then the latest version at occurrence.
func resolveAISpendPriceChecked(db *gorm.DB, in aiSpendEventInput) (*models.AIPriceBook, float64, bool, error) {
	var rows []models.AIPriceBook
	if err := db.Where("spend_class=? AND (provider=? OR provider='*') AND effective_from<=?", in.SpendClass, in.Provider, in.OccurredAt).Order("CASE WHEN provider='*' THEN 0 ELSE 1 END DESC, CASE WHEN model_pattern='*' THEN 0 ELSE length(model_pattern) END DESC, effective_from DESC, id DESC").Find(&rows).Error; err != nil {
		return nil, 0, true, err
	}
	for _, r := range rows {
		if r.ModelPattern != "*" && !strings.HasPrefix(in.Model, r.ModelPattern) {
			continue
		}
		// Gemini separates candidate and thought tokens; both are billed output.
		input := aiSpendNumber(in.Units["input_tokens"])
		cached := math.Max(0, math.Min(input, aiSpendNumber(in.Units["input_cache_hit_tokens"])))
		inputCost := input / 1e6 * r.InputUSDPer1M
		if r.CachedInputUSDPer1M != nil {
			inputCost = (input-cached)/1e6*r.InputUSDPer1M + cached/1e6**r.CachedInputUSDPer1M
		}
		cost := inputCost + (aiSpendNumber(in.Units["output_tokens"])+aiSpendNumber(in.Units["thoughts_tokens"]))/1e6*r.OutputUSDPer1M
		cost += (aiSpendNumber(in.Units["audio_sec"]) + aiSpendNumber(in.Units["items"]) + aiSpendNumber(in.Units["pairs"])) * r.UnitUSD
		return &r, cost, r.Provider == "*", nil
	}
	return nil, 0, true, nil
}
func resolveAISpendPrice(db *gorm.DB, in aiSpendEventInput) (*models.AIPriceBook, float64, bool) {
	p, c, u, _ := resolveAISpendPriceChecked(db, in)
	return p, c, u
}

func InternalIngestAISpendEvents(c *gin.Context) {
	var req struct {
		Events []aiSpendEventInput    `json:"events"`
		Health *models.AISpendEmitter `json:"health"`
	}
	if c.ShouldBindJSON(&req) != nil || (len(req.Events) == 0 && req.Health == nil) || len(req.Events) > 100 {
		c.JSON(400, gin.H{"error": "send 1..100 events or a health envelope"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	acks := make([]gin.H, 0, len(req.Events))
	failed := false
	source := ""
	if value, exists := c.Get(utils.InternalPrincipalContextKey); exists {
		source = fmt.Sprint(value)
	}
	for _, in := range req.Events {
		ack := gin.H{"event_id": in.EventID, "status": "invalid"}
		id, err := uuid.Parse(in.EventID)
		if err != nil || in.OccurredAt.IsZero() || in.OccurredAt.After(time.Now().Add(5*time.Minute)) || len(in.TenantID) > 64 || in.EventCount < 0 || in.EventCount > 1000000 {
			acks = append(acks, ack)
			continue
		}
		valid := true
		for _, v := range in.Units {
			n, ok := v.(float64)
			if !ok || n < 0 || math.IsNaN(n) || math.IsInf(n, 0) {
				valid = false
			}
		}
		if !valid || len(in.SpendClass) > 32 || len(in.Operation) > 64 || len(in.Model) > 160 || len(in.TriggerSource) > 64 || len(in.SystemRunID) > 96 || len(in.Provider) > 64 || len(in.SourceService) > 64 || len(in.HumanActorID) > 160 {
			acks = append(acks, ack)
			continue
		}
		if in.TenantID == "" {
			in.TenantID = aiSpendTenant
		}
		if in.TriggerSource == "" {
			in.TriggerSource = "unknown"
		}
		if in.Operation == "" {
			in.Operation = "unknown"
		}
		if in.SpendClass == "" {
			in.SpendClass = "unknown"
		}
		if in.EventCount == 0 {
			in.EventCount = 1
		}
		if source != "" && source != "legacy-shared" {
			in.SourceService = source
		}
		if in.SourceService == "" {
			in.SourceService = "unknown"
		}
		// Paid STT comes exclusively from settled CMS owner jobs.
		if in.SpendClass == "stt_api" {
			ack["reason"] = "owner_settlement_required"
			acks = append(acks, ack)
			continue
		}
		err = db.Transaction(func(tx *gorm.DB) error {
			receipt := models.AISpendReceipt{EventID: id}
			r := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&receipt)
			if r.Error != nil {
				return r.Error
			}
			if r.RowsAffected == 0 {
				ack["status"] = "duplicate"
				return nil
			}
			price, cost, unpriced, e := resolveAISpendPriceChecked(tx, in)
			if e != nil {
				return e
			}
			event := models.AISpendEvent{HumanActorID: in.HumanActorID, EventID: id, OccurredAt: in.OccurredAt.UTC(), SpendClass: in.SpendClass, Operation: in.Operation, Provider: in.Provider, Model: in.Model, Units: aiSpendJSON(in.Units), EventCount: in.EventCount, CostUSD: cost, Cached: in.Cached, Estimated: in.Estimated, UsageUnavailable: in.UsageUnavailable, AvoidedCostEstimated: in.AvoidedCostEstimated, Unpriced: unpriced, Backfilled: in.Backfilled, TriggerSource: in.TriggerSource, SystemRunID: in.SystemRunID, TenantID: in.TenantID, SourceService: in.SourceService}
			if price != nil {
				event.PriceRowID = &price.ID
			}
			if in.Cached {
				event.AvoidedCostUSD = cost
				event.CostUSD = 0
			}
			if _, parseErr := uuid.Parse(in.OverrideID); in.OverrideID != "" && parseErr == nil {
				var grant models.AISpendOverride
				if e := tx.Where("id=? AND tenant_id=? AND trigger_source=? AND created_at<=? AND expires_at>?", in.OverrideID, in.TenantID, in.TriggerSource, in.OccurredAt, in.OccurredAt).First(&grant).Error; e == nil {
					event.OverCapHuman = true
					event.OverrideID = &grant.ID
					event.HumanActorID = grant.CreatedBy
					if err := tx.Create(&models.AISpendOverrideUse{EventID: id, OverrideID: grant.ID, TenantID: in.TenantID, HumanActorID: grant.CreatedBy, TriggerSource: in.TriggerSource, SystemRunID: in.SystemRunID, OccurredAt: in.OccurredAt}).Error; err != nil {
						return err
					}
				} else if !errors.Is(e, gorm.ErrRecordNotFound) {
					return e
				}
			}
			if e := tx.Create(&event).Error; e != nil {
				return e
			}
			if e := economics.LinkReceipt(tx, id, in.TenantID, in.EconomicsAttributionRef); e != nil {
				return e
			}
			ack["status"] = "inserted"
			return nil
		})
		if err != nil {
			ack["status"] = "failed"
			failed = true
		}
		acks = append(acks, ack)
	}
	if req.Health != nil {
		h := req.Health
		if h.TenantID == "" {
			h.TenantID = aiSpendTenant
		}
		if source != "" && source != "legacy-shared" {
			h.SourceService = source
		}
		h.UpdatedAt = time.Now().UTC()
		if h.Dropped+h.Unobservable > 0 {
			h.LastLossAt = &h.UpdatedAt
		}
		if h.GuardUnavailable > 0 {
			h.LastGuardUnavailableAt = &h.UpdatedAt
		}
		if h.ProcessID == "" || len(h.ProcessID) > 96 || h.Sequence < 1 || h.Emitted < 0 || h.Accepted < 0 || h.Dropped < 0 || h.Unobservable < 0 || h.GuardUnavailable < 0 || h.StartedAt.IsZero() {
			c.JSON(400, gin.H{"error": "invalid health envelope", "acknowledgements": acks})
			return
		}
		err := db.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "process_id"}, {Name: "tenant_id"}}, DoUpdates: clause.Assignments(map[string]any{
			"closed":   gorm.Expr("ai_spend_emitters.closed OR EXCLUDED.closed"),
			"sequence": gorm.Expr("EXCLUDED.sequence"), "emitted": gorm.Expr("EXCLUDED.emitted"), "accepted": gorm.Expr("EXCLUDED.accepted"), "dropped": gorm.Expr("EXCLUDED.dropped"), "unobservable": gorm.Expr("EXCLUDED.unobservable"), "guard_unavailable": gorm.Expr("EXCLUDED.guard_unavailable"), "updated_at": h.UpdatedAt,
			"last_loss_at":              gorm.Expr("CASE WHEN EXCLUDED.dropped>ai_spend_emitters.dropped OR EXCLUDED.unobservable>ai_spend_emitters.unobservable THEN EXCLUDED.updated_at ELSE ai_spend_emitters.last_loss_at END"),
			"last_guard_unavailable_at": gorm.Expr("CASE WHEN EXCLUDED.guard_unavailable>ai_spend_emitters.guard_unavailable THEN EXCLUDED.updated_at ELSE ai_spend_emitters.last_guard_unavailable_at END")}), Where: clause.Where{Exprs: []clause.Expression{clause.Expr{SQL: "EXCLUDED.sequence > ai_spend_emitters.sequence AND EXCLUDED.emitted >= ai_spend_emitters.emitted AND EXCLUDED.accepted >= ai_spend_emitters.accepted AND EXCLUDED.dropped >= ai_spend_emitters.dropped AND EXCLUDED.unobservable >= ai_spend_emitters.unobservable AND EXCLUDED.guard_unavailable >= ai_spend_emitters.guard_unavailable"}}}}).Create(h).Error
		if err != nil {
			failed = true
		}
	}
	code := http.StatusAccepted
	if failed {
		code = http.StatusServiceUnavailable
	}
	c.JSON(code, gin.H{"acknowledgements": acks, "health_accepted": req.Health != nil && !failed})
}

// Atomically add only previously-unfolded rows. Fold markers and additions commit
// together; permanent rollups are never replaced with partial retained history.
func foldAISpendRollupsForTenant(tx *gorm.DB, tenant string) (int64, error) {
	var events []models.AISpendEvent
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND folded_at IS NULL", tenant).Order("id").Limit(10000).Find(&events).Error; err != nil {
		return 0, err
	}
	groups := map[string]*models.AISpendRollup{}
	units := map[string]map[string]float64{}
	ids := []uint{}
	var count int64
	for _, e := range events {
		day := e.OccurredAt.UTC().Truncate(24 * time.Hour)
		key := string(aiSpendJSON([]any{day, e.SpendClass, e.Operation, e.Provider, e.Model, e.TriggerSource, e.SystemRunID, e.HumanActorID}))
		r := groups[key]
		if r == nil {
			r = &models.AISpendRollup{TenantID: tenant, Day: day, SpendClass: e.SpendClass, Operation: e.Operation, Provider: e.Provider, Model: e.Model, TriggerSource: e.TriggerSource, SystemRunID: e.SystemRunID, HumanActorID: e.HumanActorID}
			groups[key] = r
			units[key] = map[string]float64{}
		}
		var u map[string]any
		if err := json.Unmarshal(e.Units, &u); err != nil {
			return 0, err
		}
		for k, v := range u {
			units[key][k] += aiSpendNumber(v)
		}
		n := e.EventCount
		if n < 1 {
			n = 1
		}
		count += n
		r.Events += n
		if e.OverCapHuman {
			r.OverCapHumanEvents += n
		}
		r.CostUSD += e.CostUSD
		r.AvoidedCostUSD += e.AvoidedCostUSD
		if e.Cached {
			r.CacheHits += n
		}
		if e.Estimated {
			r.EstimatedEvents += n
		}
		if e.Unpriced {
			r.UnpricedEvents += n
		}
		if e.TriggerSource == "unknown" {
			r.UnknownEvents += n
		}
		if e.UsageUnavailable {
			r.UsageUnavailableEvents += n
		}
		if e.AvoidedCostEstimated {
			r.AvoidedEstimatedUSD += e.AvoidedCostUSD
		}
		r.Backfilled = r.Backfilled || e.Backfilled
		ids = append(ids, e.ID)
	}
	for k, r := range groups {
		r.Units = aiSpendJSON(units[k])
		r.UpdatedAt = time.Now().UTC()
		updates := map[string]any{"updated_at": r.UpdatedAt, "backfilled": gorm.Expr("ai_spend_rollups.backfilled OR EXCLUDED.backfilled"), "units": gorm.Expr(`(SELECT COALESCE(jsonb_object_agg(k,v),'{}'::jsonb) FROM (SELECT key k,sum(value::numeric) v FROM (SELECT * FROM jsonb_each_text(ai_spend_rollups.units) UNION ALL SELECT * FROM jsonb_each_text(EXCLUDED.units)) u GROUP BY key) sums)`)}
		for _, f := range []string{"events", "over_cap_human_events", "cost_usd", "avoided_cost_usd", "cache_hits", "estimated_events", "unpriced_events", "unknown_events", "usage_unavailable_events", "avoided_estimated_usd"} {
			updates[f] = gorm.Expr("ai_spend_rollups." + f + " + EXCLUDED." + f)
		}
		if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "tenant_id"}, {Name: "day"}, {Name: "spend_class"}, {Name: "operation"}, {Name: "provider"}, {Name: "model"}, {Name: "trigger_source"}, {Name: "system_run_id"}, {Name: "human_actor_id"}}, DoUpdates: clause.Assignments(updates)}).Create(r).Error; err != nil {
			return 0, err
		}
	}
	if len(ids) > 0 {
		if err := tx.Model(&models.AISpendEvent{}).Where("id IN ?", ids).Update("folded_at", time.Now().UTC()).Error; err != nil {
			return 0, err
		}
	}
	return count, nil
}
func foldAISpendRollups(db *gorm.DB) (int64, error) {
	var n int64
	err := db.Transaction(func(tx *gorm.DB) error {
		var err error
		n, err = foldAISpendRollupsForTenant(tx, aiSpendTenant)
		return err
	})
	return n, err
}
func aiSpendActiveWindow(start, now time.Time) time.Time {
	if start.IsZero() {
		return now.UTC()
	}
	if !now.Before(start.Add(aiSpendWindow)) {
		start = start.Add(time.Duration(int64(now.Sub(start)/aiSpendWindow)) * aiSpendWindow)
	}
	return start.UTC()
}
func aiSpendScopeQuery(q *gorm.DB, scope string) *gorm.DB {
	if strings.HasPrefix(scope, "class:") {
		return q.Where("spend_class=?", strings.TrimPrefix(scope, "class:"))
	}
	if strings.HasPrefix(scope, "system:") {
		return q.Where("trigger_source=?", strings.TrimPrefix(scope, "system:"))
	}
	return q
}
func accumulateAISpendBudgetsForTenant(tx *gorm.DB, tenant string, now time.Time) error {
	var budgets []models.AISpendBudget
	if err := tx.Where("tenant_id=?", tenant).Find(&budgets).Error; err != nil {
		return err
	}
	for _, b := range budgets {
		start := aiSpendActiveWindow(b.WindowStartedAt, now)
		var spend float64
		q := aiSpendScopeQuery(tx.Model(&models.AISpendEvent{}).Where("tenant_id=? AND occurred_at>=? AND occurred_at<?", tenant, start, start.Add(aiSpendWindow)), b.Scope)
		if err := q.Select("COALESCE(sum(cost_usd),0)").Scan(&spend).Error; err != nil {
			return err
		}
		if err := tx.Model(&b).Updates(map[string]any{"window_started_at": start, "spend_usd": spend}).Error; err != nil {
			return err
		}
	}
	return nil
}
func accumulateAISpendBudgets(db *gorm.DB) error {
	return accumulateAISpendBudgetsForTenant(db, aiSpendTenant, time.Now().UTC())
}
func foldAndAccumulate(db *gorm.DB) (int64, error) {
	n, e := foldAISpendRollups(db)
	if e != nil {
		return n, e
	}
	return n, accumulateAISpendBudgets(db)
}

func CheckTenantSpendAllowance(db *gorm.DB, tenant, class string, estimated float64, trigger, override string, completedAt ...time.Time) map[string]any {
	now := time.Now().UTC()
	a := map[string]any{"allowed": true, "verdict": "within", "reason": "within_budget", "remaining_usd": nil, "ledger_health": "healthy", "as_of": now, "tenant_id": tenant, "cache_ttl_seconds": 60, "enforcement": "bounded", "margin_usd": 0.0}
	unavailable := func() map[string]any {
		a["allowed"] = true
		a["reason"] = "governor_unavailable"
		a["ledger_health"] = "unavailable"
		return a
	}
	p, err := getTenantAISpendPolicy(db, tenant)
	if err != nil {
		return unavailable()
	}
	health, healthErr := aiSpendLedgerHealth(db, tenant, p, now, completedAt...)
	if healthErr != nil {
		return unavailable()
	}
	a["ledger_health"] = health
	a["tolerance_seconds"] = nil
	a["tolerance_usd"] = nil
	a["tolerance_status"] = "unqualified_concurrency"
	a["reporting_lag_seconds"] = 5 + p.AggregationIntervalMinutes*60 + 60
	if !p.Enabled {
		a["reason"] = "governor_disabled"
		return a
	}
	if class != "llm" {
		a["reason"] = "owner_delegated"
		return a
	}
	// Confirmed human work outranks only this governor, including its pauses.
	// The authenticated owner validates actor and independent authority first.
	if override != "" {
		if _, err := uuid.Parse(override); err == nil {
			var grant models.AISpendOverride
			err = db.Where("id=? AND tenant_id=? AND trigger_source=? AND created_at<=? AND expires_at>?", override, tenant, trigger, now, now).First(&grant).Error
			if err == nil {
				a["allowed"] = true
				a["reason"] = "human_confirmed_override"
				a["over_cap_human"] = true
				return a
			}
			if !errors.Is(err, gorm.ErrRecordNotFound) {
				return unavailable()
			}
		}
	}
	if p.PausedUntil != nil && p.PausedUntil.After(now) {
		a["allowed"] = false
		a["reason"] = "policy_paused"
		a["verdict"] = "paused"
		return a
	}
	var b models.AISpendBudget
	err = db.Where("tenant_id=? AND scope=?", tenant, "class:"+class).First(&b).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		a["reason"] = "no_budget"
		return a
	}
	if err != nil {
		return unavailable()
	}
	if b.PausedUntil != nil && b.PausedUntil.After(now) {
		a["allowed"] = false
		a["reason"] = "class_paused"
		a["verdict"] = "paused"
		return a
	}
	start := aiSpendActiveWindow(b.WindowStartedAt, now)
	var spend float64
	if err := db.Model(&models.AISpendEvent{}).Where("tenant_id=? AND spend_class=? AND occurred_at>=? AND occurred_at<?", tenant, class, start, start.Add(aiSpendWindow)).Select("COALESCE(sum(cost_usd),0)").Scan(&spend).Error; err != nil {
		return unavailable()
	}
	a["spend_usd"] = spend
	a["window_started_at"] = start
	a["window_ends_at"] = start.Add(aiSpendWindow)
	if b.CapUSD == nil {
		a["reason"] = "uncapped"
		return a
	}
	margin, err := aiSpendLagMargin(db, tenant, *b.CapUSD, p, now)
	if err != nil {
		return unavailable()
	}
	a["margin_usd"] = margin
	a["remaining_usd"] = math.Max(0, *b.CapUSD-spend-b.ReservedUSD)
	if spend+b.ReservedUSD+math.Max(0, estimated)+margin >= *b.CapUSD*b.HardPct/100 {
		a["allowed"] = false
		a["verdict"] = "bounded_stop"
		a["reason"] = "budget_capped"
	} else if spend >= *b.CapUSD*b.WarnPct/100 {
		a["verdict"] = "warning"
		a["reason"] = "budget_warning"
	}
	return a
}
func CheckSpendAllowance(db *gorm.DB, class string, estimatedUSD float64, triggerSource string) map[string]any {
	return CheckTenantSpendAllowance(db, aiSpendTenant, class, estimatedUSD, triggerSource, "")
}
func InternalGetAISpendAllowance(c *gin.Context) {
	estimate, _ := strconv.ParseFloat(c.Query("estimated_usd"), 64)
	tenant := c.DefaultQuery("tenant_id", aiSpendTenant)
	if len(tenant) > 64 {
		c.JSON(400, gin.H{"error": "invalid tenant"})
		return
	}
	c.JSON(200, CheckTenantSpendAllowance(c.MustGet("db").(*gorm.DB), tenant, c.DefaultQuery("class", "llm"), estimate, c.DefaultQuery("trigger_source", "unknown"), c.Query("override_id")))
}

func GetAISpendPolicy(c *gin.Context) {
	tenant, ok := aiSpendTenantFromAdmin(c)
	if !ok {
		return
	}
	p, err := getTenantAISpendPolicy(c.MustGet("db").(*gorm.DB), tenant)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, p)
}
func UpdateAISpendPolicy(c *gin.Context) {
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
	// Decode over the current row: omitted fields preserve values, explicit false/null apply.
	if c.ShouldBindJSON(p) != nil || p.AggregationIntervalMinutes < 1 || p.AggregationIntervalMinutes > 60 || p.RetentionDays < 90 || p.ForecastHorizonDays < 1 || p.ForecastHorizonDays > 90 || p.SpikeMultiplier < 1 || !validateAISpendComputeLimits(p.ComputeWatchLimits) {
		c.JSON(400, gin.H{"error": "invalid policy: cadence 1..60, retention >=90, horizon 1..90, spike >=1"})
		return
	}
	var original models.AISpendPolicy
	if err = db.Where("tenant_id=?", tenant).First(&original).Error; err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	p.ID = original.ID
	p.TenantID = tenant
	p.LastRunAt = original.LastRunAt
	p.CreatedAt = original.CreatedAt
	if err = db.Save(p).Error; err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, p)
}
func ListAISpendBudgets(c *gin.Context) {
	tenant, ok := aiSpendTenantFromAdmin(c)
	if !ok {
		return
	}
	var rows []models.AISpendBudget
	if err := c.MustGet("db").(*gorm.DB).Where("tenant_id=?", tenant).Order("scope").Find(&rows).Error; err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"budgets": rows})
}
func UpsertAISpendBudget(c *gin.Context) {
	p, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var raw map[string]json.RawMessage
	if c.ShouldBindJSON(&raw) != nil {
		c.JSON(400, gin.H{"error": "invalid budget"})
		return
	}
	var scope string
	_ = json.Unmarshal(raw["scope"], &scope)
	if scope != "platform" && scope != "class:llm" && !(strings.HasPrefix(scope, "system:") && len(scope) > 7 && len(scope) <= 96) {
		c.JSON(400, gin.H{"error": "scope must be platform, class:llm or system:<name>; STT is owner managed"})
		return
	}
	var b models.AISpendBudget
	err := db.Where("tenant_id=? AND scope=?", p.TenantID, scope).First(&b).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		b = models.AISpendBudget{TenantID: p.TenantID, Scope: scope, WarnPct: 70, HardPct: 100, WindowStartedAt: time.Now().UTC()}
	} else if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	for key, target := range map[string]any{"cap_usd": &b.CapUSD, "warn_pct": &b.WarnPct, "hard_pct": &b.HardPct, "paused_until": &b.PausedUntil} {
		if value, exists := raw[key]; exists {
			if json.Unmarshal(value, target) != nil {
				c.JSON(400, gin.H{"error": "invalid " + key})
				return
			}
		}
	}
	if (b.CapUSD != nil && (*b.CapUSD < 0 || math.IsNaN(*b.CapUSD) || math.IsInf(*b.CapUSD, 0))) || b.WarnPct <= 0 || b.HardPct > bHardMax() || b.WarnPct > b.HardPct {
		c.JSON(400, gin.H{"error": "invalid cap or thresholds"})
		return
	}
	b.UpdatedBy = p.UserID
	if err = db.Save(&b).Error; err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, b)
}
func bHardMax() float64 { return 100 }
func ListAIPriceBook(c *gin.Context) {
	var rows []models.AIPriceBook
	if err := c.MustGet("db").(*gorm.DB).Order("effective_from DESC,id DESC").Find(&rows).Error; err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"prices": rows})
}
func CreateAIPriceBook(c *gin.Context) {
	p, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	if !aiSpendGlobalPriceWriter(p) {
		c.JSON(403, gin.H{"error": "shared price book requires administrator authority"})
		return
	}
	var r models.AIPriceBook
	if c.ShouldBindJSON(&r) != nil || r.SpendClass == "" || r.Provider == "" {
		c.JSON(400, gin.H{"error": "spend_class and provider required"})
		return
	}
	rates := []float64{r.InputUSDPer1M, r.OutputUSDPer1M, r.UnitUSD}
	if r.CachedInputUSDPer1M != nil {
		rates = append(rates, *r.CachedInputUSDPer1M)
	}
	for _, n := range rates {
		if n < 0 || math.IsNaN(n) || math.IsInf(n, 0) {
			c.JSON(400, gin.H{"error": "prices must be finite and nonnegative"})
			return
		}
	}
	r.ID = 0
	r.CreatedBy = p.UserID
	r.CreatedAt = time.Time{}
	if r.ModelPattern == "" {
		r.ModelPattern = "*"
	}
	if r.EffectiveFrom.IsZero() {
		r.EffectiveFrom = time.Now().UTC()
	}
	if strings.Contains(r.ModelPattern, "*") && r.ModelPattern != "*" {
		c.JSON(400, gin.H{"error": "use a literal prefix or *"})
		return
	}
	if err := c.MustGet("db").(*gorm.DB).Create(&r).Error; err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(201, r)
}
func PreviewAISpendOverride(c *gin.Context) {
	tenant, ok := aiSpendTenantFromAdmin(c)
	if !ok {
		return
	}
	c.JSON(200, CheckTenantSpendAllowance(c.MustGet("db").(*gorm.DB), tenant, "llm", 0, c.Query("trigger_source"), ""))
}
func ConfirmAISpendOverride(c *gin.Context) {
	p, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	var req struct {
		Confirm bool   `json:"confirm"`
		Trigger string `json:"trigger_source"`
	}
	if c.ShouldBindJSON(&req) != nil || !req.Confirm || req.Trigger == "" || len(req.Trigger) > 64 {
		c.JSON(400, gin.H{"error": "explicit confirmation and trigger_source required"})
		return
	}
	r := models.AISpendOverride{ID: uuid.New(), TenantID: p.TenantID, TriggerSource: req.Trigger, CreatedBy: p.UserID, ExpiresAt: time.Now().UTC().Add(10 * time.Minute), AdmissionSnapshot: aiSpendJSON(aiSpendAdmissionSnapshot(c.MustGet("db").(*gorm.DB), p.TenantID, req.Trigger))}
	if err := c.MustGet("db").(*gorm.DB).Create(&r).Error; err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(201, r)
}

// These errors are kept typed so the HTTP runner can distinguish a busy owner.
var errAISpendBusy = fmt.Errorf("AI spend run already active")

func InternalGetSTTSpendContext(c *gin.Context) {
	var job models.TranscriptionJob
	err := c.MustGet("db").(*gorm.DB).Where("public_id=?", c.Param("id")).First(&job).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		c.JSON(404, gin.H{"error": "transcription job not found"})
		return
	}
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"tenant_id": job.TenantID, "trigger_source": job.TriggerSource, "system_run_id": job.PublicID.String(), "content_item_id": job.ContentItemID, "status": job.Status})
}
