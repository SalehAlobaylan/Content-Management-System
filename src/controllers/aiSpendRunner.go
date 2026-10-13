package controllers

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"content-management-system/src/models"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func importAISpendSTT(tx *gorm.DB, tenant string) (int64, error) {
	var jobs []models.TranscriptionJob
	// A settlement receipt retains previous actual cost even after raw retention.
	if err := tx.Where("tenant_id=? AND completed_at IS NOT NULL AND provider NOT IN ('local','whisper','faster-whisper') AND (status IN ?)", tenant, []string{"succeeded", "failed", "writeback_failed", "canceled"}).Order("id").Find(&jobs).Error; err != nil {
		return 0, err
	}
	var imported int64
	for _, job := range jobs {
		var old models.AISpendSTTSettlement
		err := tx.Where("job_id=?", job.PublicID).First(&old).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return imported, err
		}
		if err == nil && job.UpdatedAt.Equal(old.OwnerUpdatedAt) {
			continue
		}
		delta := job.ActualCostUsd - old.CostUSD
		if delta != 0 || errors.Is(err, gorm.ErrRecordNotFound) && job.Status == "succeeded" {
			id := uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprintf("wahb/stt/%s/%s/%.12f", job.PublicID, job.UpdatedAt.UTC().Format(time.RFC3339Nano), job.ActualCostUsd)))
			receipt := models.AISpendReceipt{EventID: id}
			r := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&receipt)
			if r.Error != nil {
				return imported, r.Error
			}
			if r.RowsAffected > 0 {
				at := job.CompletedAt.UTC()
				if old.JobID != uuid.Nil {
					at = job.UpdatedAt.UTC()
				}
				units := map[string]any{}
				var metadata map[string]any
				_ = json.Unmarshal(job.Metadata, &metadata)
				if old.JobID == uuid.Nil {
					if duration, ok := metadata["duration_sec"].(float64); ok {
						units["audio_sec"] = duration
					}
				}
				event := models.AISpendEvent{EventID: id, EventCount: 1, OccurredAt: at, TenantID: tenant, SpendClass: "stt_api", Operation: "stt_settlement", Provider: job.Provider, Model: job.Model, Units: aiSpendJSON(units), CostUSD: delta, TriggerSource: job.TriggerSource, SystemRunID: job.PublicID.String(), SourceService: "cms_stt_owner", Backfilled: time.Since(at) > 24*time.Hour}
				if err := tx.Create(&event).Error; err != nil {
					return imported, err
				}
				imported++
			}
		}
		old = models.AISpendSTTSettlement{JobID: job.PublicID, TenantID: tenant, CostUSD: job.ActualCostUsd, OwnerUpdatedAt: job.UpdatedAt, UpdatedAt: time.Now().UTC()}
		if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "job_id"}}, DoUpdates: clause.AssignmentColumns([]string{"cost_usd", "owner_updated_at", "updated_at"})}).Create(&old).Error; err != nil {
			return imported, err
		}
	}
	return imported, nil
}

type aiSpendSignal struct {
	kind, scope string
	active      bool
	evidence    map[string]any
}

func evaluateAISpendEpisodes(tx *gorm.DB, tenant string, p *models.AISpendPolicy, now time.Time) (map[string]any, error) {
	verdicts := map[string]any{}
	if !p.Enabled || p.PausedUntil != nil && p.PausedUntil.After(now) {
		return verdicts, nil
	}
	var err error
	verdicts, err = aiSpendBudgetVerdicts(tx, tenant, p, now)
	if err != nil {
		return nil, err
	}
	signals := []aiSpendSignal{}
	for scope, raw := range verdicts {
		v := raw.(map[string]any)
		evidence := v["evidence"].(map[string]any)
		f := evidence["forecast"].(map[string]any)
		projected, known := f["projected_window_usd"].(float64)
		cap, _ := evidence["cap_usd"].(*float64)
		over := cap != nil && known && projected > *cap
		signals = append(signals, aiSpendSignal{"over_pace", scope, over, evidence}, aiSpendSignal{"bounded_stop", scope, v["verdict"] == "bounded_stop", evidence})
	}
	var totals struct {
		Unpriced, Unknown, Unavailable int64
		Cost                           float64
	}
	if err := tx.Model(&models.AISpendRollup{}).Where("tenant_id=? AND day>=?", tenant, now.UTC().Truncate(24*time.Hour).AddDate(0, 0, -1)).Select("COALESCE(sum(unpriced_events),0) unpriced,COALESCE(sum(unknown_events),0) unknown,COALESCE(sum(usage_unavailable_events),0) unavailable,COALESCE(sum(cost_usd),0) cost").Scan(&totals).Error; err != nil {
		return nil, err
	}
	var loss int64
	if err := tx.Model(&models.AISpendEmitter{}).Where("tenant_id=? AND last_loss_at>=?", tenant, now.Add(-24*time.Hour)).Select("COALESCE(sum(dropped+unobservable),0)").Scan(&loss).Error; err != nil {
		return nil, err
	}
	signals = append(signals, aiSpendSignal{"unpriced_events", "ledger", totals.Unpriced > 0, map[string]any{"events": totals.Unpriced}}, aiSpendSignal{"unknown_attribution", "ledger", totals.Unknown > 0, map[string]any{"events": totals.Unknown}}, aiSpendSignal{"untrusted_ledger", "ledger", loss+totals.Unavailable > 0, map[string]any{"lost_or_unobservable": loss, "usage_unavailable_events": totals.Unavailable}})
	spikes, err := aiSpendSpikeSignals(tx, tenant, p, now)
	if err != nil {
		return nil, err
	}
	signals = append(signals, spikes...)
	watches, err := aiSpendComputeWatch(tx, tenant, p, now)
	if err != nil {
		return nil, err
	}
	for _, watch := range watches {
		signals = append(signals, aiSpendSignal{"compute_volume", fmt.Sprint(watch["spend_class"]), watch["status"] == "watching", watch})
	}
	signals = append(signals, aiSpendSignal{"metering_loss", "ledger", loss > 0, map[string]any{"lost_or_unobservable": loss}})
	var uncertain int64
	if err := tx.Model(&models.AISpendEmitter{}).Where("tenant_id=? AND NOT closed AND updated_at<? AND updated_at>=?", tenant, now.Add(-2*time.Minute), now.Add(-24*time.Hour)).Count(&uncertain).Error; err != nil {
		return nil, err
	}
	signals = append(signals, aiSpendSignal{"restart_uncertainty", "ledger", uncertain > 0, map[string]any{"stale_unclosed_emitters": uncertain, "status": "uncertainty_not_proven_loss"}})
	var cfg models.TranscriptionConfig
	err = tx.Where("tenant_id=?", tenant).First(&cfg).Error
	if err == nil {
		evidence, active, e := aiSpendSTTDrift(tx, tenant, cfg)
		if e != nil {
			return nil, e
		}
		signals = append(signals, aiSpendSignal{"stt_drift", "class:stt_api", active, evidence})
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}
	for _, signal := range signals {
		signal.evidence["recommended_action"] = aiSpendRecommendation(signal.kind)
		var attributed []struct {
			TriggerSource string  `json:"trigger_source"`
			SystemRunID   string  `json:"system_run_id"`
			CostUSD       float64 `json:"cost_usd"`
		}
		attributionQ := tx.Model(&models.AISpendRollup{}).Where("tenant_id=? AND day>=?", tenant, now.UTC().Truncate(24*time.Hour).AddDate(0, 0, -1))
		if signal.kind == "spike" {
			parts := strings.SplitN(signal.scope, "/", 2)
			if len(parts) == 2 {
				attributionQ = attributionQ.Where("spend_class=? AND trigger_source=?", parts[0], parts[1])
			}
		} else {
			attributionQ = aiSpendScopeQuery(attributionQ, signal.scope)
		}
		if err := attributionQ.Select("trigger_source,system_run_id,sum(cost_usd) cost_usd").Group("trigger_source,system_run_id").Order("sum(cost_usd) DESC").Limit(5).Scan(&attributed).Error; err != nil {
			return nil, err
		}

		var ep models.AISpendEpisode
		err := tx.Where("tenant_id=? AND kind=? AND scope=?", tenant, signal.kind, signal.scope).Order("id DESC").First(&ep).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			if !signal.active {
				continue
			}
			ep = models.AISpendEpisode{TenantID: tenant, Kind: signal.kind, Scope: signal.scope, Status: "candidate", FirstSeenAt: now}
		} else if err != nil {
			return nil, err
		}
		wasFlapping := ep.Status == "flapping"
		if signal.active {
			if ep.FalsePositive {
				if ep.HealthyRuns < 3 {
					continue
				}
				// Preserve the reviewed false positive; a new occurrence has its own record.
				ep = models.AISpendEpisode{TenantID: tenant, Kind: signal.kind, Scope: signal.scope, Status: "candidate", FirstSeenAt: now}
			}
			if ep.Status == "resolved" {
				var oldTimes []time.Time
				_ = json.Unmarshal(ep.ReopenTimes, &oldTimes)
				times := []time.Time{}
				for _, at := range oldTimes {
					if !at.Before(now.Add(-24 * time.Hour)) {
						times = append(times, at)
					}
				}
				times = append(times, now)
				ep.ReopenTimes = aiSpendJSON(times)
				ep.Reopens = len(times)
				ep.Status = "candidate"
				ep.Confirmations = 0
				ep.FirstSeenAt = now
			}
			ep.Confirmations++
			ep.HealthyRuns = 0
			ep.LastSeenAt = now
			ep.Evidence = aiSpendJSON(signal.evidence)
			ep.Attribution = aiSpendJSON(map[string]any{"basis": "recent_spend_correlation_only", "runs": attributed})
			if ep.Confirmations >= 2 {
				ep.Status = "open"
			}
			if ep.Reopens >= 3 {
				ep.Status = "flapping"
			}
		} else {
			ep.HealthyRuns++
			ep.Confirmations = 0
			if ep.HealthyRuns >= 3 {
				ep.Status = "resolved"
				if ep.ClosedAt == nil {
					ep.CloseReason = "healthy_three_runs"
				}
			} else if ep.Status != "resolved" {
				ep.Status = "recovering"
			}
		}
		if wasFlapping {
			ep.Status = "flapping"
		}
		if err := tx.Save(&ep).Error; err != nil {
			return nil, err
		}
	}
	return verdicts, nil
}

func executeAISpendRun(db *gorm.DB, tenant, trigger string) (*models.AISpendRun, error) {
	lockTx := db.Begin()
	if lockTx.Error != nil {
		return nil, lockTx.Error
	}
	defer lockTx.Rollback()
	var acquired bool
	if err := lockTx.Raw("SELECT pg_try_advisory_xact_lock(hashtextextended(?,0))", "wahb:ai-spend/xact/v1/"+tenant).Scan(&acquired).Error; err != nil {
		return nil, err
	}
	if !acquired {
		return nil, errAISpendBusy
	}
	started := time.Now().UTC()
	run := models.AISpendRun{TenantID: tenant, Trigger: trigger, Status: "running", StartedAt: started}
	if err := db.Create(&run).Error; err != nil {
		return nil, err
	}
	err := func(tx *gorm.DB) error {
		p, err := getTenantAISpendPolicy(tx, tenant)
		if err != nil {
			return err
		}
		imported, err := importAISpendSTT(tx, tenant)
		if err != nil {
			return fmt.Errorf("stt_settlement: %w", err)
		}
		folded, err := foldAISpendRollupsForTenant(tx, tenant)
		if err != nil {
			return fmt.Errorf("ledger_fold: %w", err)
		}
		run.EventsFolded = folded
		if err := accumulateAISpendBudgetsForTenant(tx, tenant, started); err != nil {
			return fmt.Errorf("budget_window: %w", err)
		}
		verdicts, err := evaluateAISpendEpisodes(tx, tenant, p, started)
		if err != nil {
			return fmt.Errorf("governance: %w", err)
		}
		run.BudgetVerdicts = aiSpendJSON(verdicts)
		cutoff := started.AddDate(0, 0, -p.RetentionDays)
		// Future anchors and expired windows cannot cause active-window raw loss.
		var earliest struct{ Value *time.Time }
		if err := tx.Model(&models.AISpendBudget{}).Where("tenant_id=?", tenant).Select("min(window_started_at) value").Scan(&earliest).Error; err != nil {
			return err
		}
		if earliest.Value != nil && earliest.Value.Before(cutoff) {
			cutoff = *earliest.Value
		}
		var ownerWindow struct{ Value *time.Time }
		if err := tx.Model(&models.TranscriptionConfig{}).Where("tenant_id=?", tenant).Select("min(monthly_window_start) value").Scan(&ownerWindow).Error; err != nil {
			return err
		}
		if ownerWindow.Value != nil && ownerWindow.Value.Before(cutoff) {
			cutoff = *ownerWindow.Value
		}
		sweep := tx.Where("tenant_id=? AND folded_at IS NOT NULL AND occurred_at<?", tenant, cutoff).Delete(&models.AISpendEvent{})
		if sweep.Error != nil {
			return sweep.Error
		}
		hygiene, err := aiSpendHygiene(tx, tenant, started)
		if err != nil {
			return err
		}
		hygiene["raw_events_swept"] = sweep.RowsAffected
		hygiene["stt_settlements"] = imported
		run.HygieneCounters = aiSpendJSON(hygiene)
		var lastID uint
		if err := tx.Model(&models.AISpendEvent{}).Where("tenant_id=? AND folded_at IS NOT NULL", tenant).Select("COALESCE(max(id),0)").Scan(&lastID).Error; err != nil {
			return err
		}
		run.WatermarksAdvanced = aiSpendJSON(map[string]any{"last_folded_event_id": lastID, "snapshot_at": started})
		return tx.Model(p).Update("last_run_at", started).Error
	}(lockTx)
	if err == nil {
		err = lockTx.Commit().Error
	} else {
		_ = lockTx.Rollback().Error
	}
	now := time.Now().UTC()
	run.CompletedAt = &now
	run.DurationMS = now.Sub(started).Milliseconds()
	run.Status = "completed"
	var verdictSnapshot map[string]any
	_ = json.Unmarshal(run.BudgetVerdicts, &verdictSnapshot)
	run.Headline = aiSpendScopeHeadline(CheckTenantSpendAllowance(db, tenant, "llm", 0, "unknown", "", now), verdictSnapshot)
	if err != nil {
		run.Status = "failed"
		run.Headline = "run_failed"
		run.Error = err.Error()
		run.ErrorClass = "ledger_update"
	}
	if saveErr := db.Save(&run).Error; saveErr != nil {
		return &run, saveErr
	}
	return &run, err
}
func RunAISpendGovernorNow(c *gin.Context) {
	tenant, ok := aiSpendTenantFromAdmin(c)
	if !ok {
		return
	}
	run, err := executeAISpendRun(c.MustGet("db").(*gorm.DB), tenant, "manual")
	if errors.Is(err, errAISpendBusy) {
		c.JSON(409, gin.H{"error": err.Error()})
		return
	}
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error(), "run": run})
		return
	}
	c.JSON(200, run)
}
func runAISpendGovernor(db *gorm.DB, trigger string, c *gin.Context) {
	run, err := executeAISpendRun(db, aiSpendTenant, trigger)
	if c != nil {
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error(), "run": run})
		} else {
			c.JSON(200, run)
		}
	}
}
func StartAISpendGovernorHeartbeat(db *gorm.DB) {
	go func() {
		tick := time.NewTicker(time.Minute)
		defer tick.Stop()
		for {
			var tenants []string
			// Tenants with metered activity get observe-mode policies automatically.
			if err := db.Raw("SELECT tenant_id FROM ai_spend_policies UNION SELECT DISTINCT tenant_id FROM ai_spend_events UNION SELECT DISTINCT tenant_id FROM transcription_jobs").Scan(&tenants).Error; err == nil {
				for _, tenant := range tenants {
					p, err := getTenantAISpendPolicy(db, tenant)
					if err == nil && (p.LastRunAt == nil || time.Since(*p.LastRunAt) >= time.Duration(p.AggregationIntervalMinutes)*time.Minute) {
						_, _ = executeAISpendRun(db, tenant, "scheduled")
					}
				}
			}
			<-tick.C
		}
	}()
}
