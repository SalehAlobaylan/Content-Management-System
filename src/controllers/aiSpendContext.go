package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"time"
)

// Each owning workflow passes provenance explicitly; request-local values never
// live in mutable package globals. Override IDs are minted by authenticated CMS.
type AISpendContext struct {
	workloadDB              *gorm.DB
	workloadOrigin          string
	workloadLease           *controllerWorkloadLease
	EconomicsAttributionRef string         `json:"economics_attribution_ref,omitempty"`
	HumanActorID            string         `json:"human_actor_id,omitempty"`
	Admission               map[string]any `json:"admission,omitempty"`
	TenantID                string         `json:"tenant_id"`
	TriggerSource           string         `json:"trigger_source"`
	SystemRunID             string         `json:"system_run_id,omitempty"`
	OverrideID              string         `json:"override_id,omitempty"`
	ServingExempt           bool           `json:"serving_exempt,omitempty"`
}

func spendContext(tenant, trigger, run string) AISpendContext {
	if tenant == "" {
		tenant = "default"
	}
	if trigger == "" {
		trigger = "unknown"
	}
	return AISpendContext{TenantID: tenant, TriggerSource: trigger, SystemRunID: run}
}
func firstSpendContext(contexts []AISpendContext) AISpendContext {
	if len(contexts) > 0 {
		return contexts[0]
	}
	return spendContext("default", "unknown", "")
}

// Owners persist this as a skip; metering and the next scheduled attempt continue.
type aiBudgetCappedError struct{ Reason string }

func (e *aiBudgetCappedError) Error() string { return "budget_capped: " + e.Reason }
func admitAISpend(db *gorm.DB, context AISpendContext, estimated float64) error {
	if context.ServingExempt && context.TriggerSource == "serving_news_summary" {
		return nil
	}
	a := CheckTenantSpendAllowance(db, context.TenantID, "llm", estimated, context.TriggerSource, context.OverrideID)
	if a["allowed"] == false {
		return &aiBudgetCappedError{Reason: fmt.Sprint(a["reason"])}
	}
	return nil
}

func aiSpendLedgerHealth(db *gorm.DB, tenant string, p *models.AISpendPolicy, now time.Time, completedAt ...time.Time) (string, error) {
	var incomplete, loss, uncertain int64
	if err := db.Model(&models.AISpendEvent{}).Where("tenant_id=? AND occurred_at>=? AND (unpriced OR usage_unavailable)", tenant, now.Add(-24*time.Hour)).Count(&incomplete).Error; err != nil {
		return "unavailable", err
	}
	if err := db.Model(&models.AISpendEmitter{}).Where("tenant_id=? AND (last_loss_at>=? OR last_guard_unavailable_at>=?)", tenant, now.Add(-24*time.Hour), now.Add(-24*time.Hour)).Count(&loss).Error; err != nil {
		return "unavailable", err
	}
	if err := db.Model(&models.AISpendEmitter{}).Where("tenant_id=? AND NOT closed AND updated_at<? AND updated_at>=?", tenant, now.Add(-2*time.Minute), now.Add(-24*time.Hour)).Count(&uncertain).Error; err != nil {
		return "unavailable", err
	}
	if incomplete+loss+uncertain > 0 {
		return "untrusted", nil
	}
	var pending int64
	if err := db.Model(&models.AISpendEvent{}).Where("tenant_id=? AND folded_at IS NULL AND created_at<?", tenant, now.Add(-time.Duration(p.AggregationIntervalMinutes)*time.Minute)).Count(&pending).Error; err != nil {
		return "unavailable", err
	}
	if pending > 0 {
		return "lagging", nil
	}
	var last models.AISpendRun
	if err := db.Where("tenant_id=?", tenant).Order("started_at DESC").First(&last).Error; err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return "unavailable", err
	}
	if last.Status == "failed" && len(completedAt) == 0 {
		return "degraded", nil
	}
	var latest struct{ At *time.Time }
	if err := db.Model(&models.AISpendRun{}).Where("tenant_id=? AND status='completed'", tenant).Select("max(completed_at) at").Scan(&latest).Error; err != nil {
		return "unavailable", err
	}
	if len(completedAt) > 0 {
		latest.At = &completedAt[0]
	}
	if latest.At == nil {
		return "no_evidence", nil
	}
	if now.Sub(*latest.At) > time.Duration(p.AggregationIntervalMinutes+5)*time.Minute {
		return "stale", nil
	}
	return "healthy", nil
}

// Each row identifies the execution owner and the admission or delegated guard.
var aiSpendCoverage = []map[string]string{
	{"workflow": "Preferences translations", "trigger": "preferences_autopilot", "class": "llm", "owner": "CMS", "guard": "CMS allowance + Enrichment cache", "skip": "preferences actions"},
	{"workflow": "Studio proposals", "trigger": "studio_autopilot", "class": "llm", "owner": "CMS", "guard": "CMS allowance + Enrichment cache", "skip": "Studio actions"},
	{"workflow": "Ingest labels, metadata and chapters", "trigger": "content_pipeline / ingest_ai / media_atomization", "class": "llm", "owner": "Aggregation / CMS", "guard": "Enrichment allowance cache", "skip": "content stage deferral / pending artifacts"},
	{"workflow": "Source classification", "trigger": "source_intelligence", "class": "llm", "owner": "CMS", "guard": "Enrichment allowance cache", "skip": "classification result"},
	{"workflow": "Operator interactive, deep and scheduled", "trigger": "operator", "class": "llm", "owner": "CMS", "guard": "CMS allowance + confirmed human override", "skip": "investigation events"},
	{"workflow": "News serving summary", "trigger": "serving_news_summary", "class": "llm", "owner": "CMS", "guard": "registered serving exemption", "skip": "not applicable"},
	{"workflow": "Text embeddings", "trigger": "embedding_campaign / preferences_autopilot / content_pipeline", "class": "embedding", "owner": "Enrichment", "guard": "compute only", "skip": "not applicable"},
	{"workflow": "Rerank", "trigger": "retrieval / operator", "class": "rerank", "owner": "Enrichment executing role", "guard": "compute only", "skip": "not applicable"},
	{"workflow": "Images", "trigger": "embedding_campaign / ingest_ai", "class": "image_embed", "owner": "Media", "guard": "compute only", "skip": "not applicable"},
	{"workflow": "Transcription", "trigger": "STT owner job", "class": "stt_api / stt_local", "owner": "CMS STT / Media", "guard": "STT owner admission and settlement", "skip": "transcription_jobs"},
}

func manualAISpendContext(c *gin.Context, tenant, trigger, run string) (AISpendContext, bool) {
	ctx := spendContext(tenant, trigger, run)
	ctx = withWorkloadContext(c.MustGet("db").(*gorm.DB), ctx, "human")
	principal, _ := utils.GetAdminPrincipal(c)
	ctx.HumanActorID = principal.UserID
	if id := c.GetHeader("X-AI-Spend-Override"); id != "" {
		var g models.AISpendOverride
		_, parseErr := uuid.Parse(id)
		if parseErr != nil || c.MustGet("db").(*gorm.DB).Where("id=? AND tenant_id=? AND created_by=? AND trigger_source=? AND expires_at>?", id, tenant, principal.UserID, trigger, time.Now()).First(&g).Error != nil {
			c.JSON(403, gin.H{"message": "Spend confirmation is invalid or expired"})
			return ctx, false
		}
		ctx.OverrideID = id
	}
	ctx.Admission = CheckTenantSpendAllowance(c.MustGet("db").(*gorm.DB), tenant, "llm", .01, trigger, ctx.OverrideID)
	return ctx, true
}

func guardedSpendContext(db *gorm.DB, ctx AISpendContext, estimate float64) AISpendContext {
	if ctx.workloadDB == nil {
		ctx = withWorkloadContext(db, ctx, "automation")
	}
	ctx.Admission = CheckTenantSpendAllowance(db, ctx.TenantID, "llm", estimate, ctx.TriggerSource, ctx.OverrideID)
	return ctx
}
func isAISpendCapped(err error) bool { var capped *aiBudgetCappedError; return errors.As(err, &capped) }
func enrichmentBudgetError(status int, body []byte) error {
	if status == 409 {
		var value struct {
			Code string `json:"error_code"`
		}
		if json.Unmarshal(body, &value) == nil && value.Code == "AI_BUDGET_CAPPED" {
			return &aiBudgetCappedError{Reason: "paid dispatch deferred"}
		}
	}
	return nil
}

func aiSpendHeadline(allowance map[string]any) string {
	if allowance["reason"] == "governor_disabled" {
		return "observe"
	}
	if allowance["verdict"] == "paused" {
		return "paused"
	}
	if allowance["allowed"] == false {
		return "bounded_stop"
	}
	if allowance["ledger_health"] != "healthy" {
		return "ledger_" + fmt.Sprint(allowance["ledger_health"])
	}
	return fmt.Sprint(allowance["verdict"])
}
