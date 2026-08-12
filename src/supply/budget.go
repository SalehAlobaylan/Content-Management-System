package supply

import (
	"encoding/json"
	"fmt"
	"time"

	"content-management-system/src/models"

	"gorm.io/datatypes"
	"gorm.io/gorm"
)

const (
	maxTenantReservedProviderCalls       = 64
	maxTenantReservedItems               = 1000
	maxTenantReservedBytes         int64 = 512 << 20
	maxTenantReservedWorkload            = 2048
	defaultRequestProviderCalls          = 20
	defaultRequestBytes            int64 = 64 << 20
)

type sourceRunBudget struct {
	ProviderCalls int
	Items         int
	Bytes         int64
	Workload      int
}

func deriveSourceRunBudget(metadata datatypes.JSON, purpose string) (sourceRunBudget, error) {
	var value struct {
		MaxResults       *int   `json:"max_results"`
		MaxProviderCalls *int   `json:"max_provider_calls"`
		MaxBytes         *int64 `json:"max_bytes"`
	}
	if len(metadata) > 0 && json.Unmarshal(metadata, &value) != nil {
		return sourceRunBudget{}, fmt.Errorf("source-run budget metadata is invalid")
	}
	items := 50
	if value.MaxResults != nil {
		items = *value.MaxResults
	}
	providerCalls := defaultRequestProviderCalls
	if value.MaxProviderCalls != nil {
		providerCalls = *value.MaxProviderCalls
	}
	bytes := defaultRequestBytes
	if value.MaxBytes != nil {
		bytes = *value.MaxBytes
	}
	if purpose == "deferred_drain" {
		if value.MaxResults == nil {
			items = 1
		}
		if value.MaxProviderCalls == nil {
			providerCalls = 12
		}
		if value.MaxBytes == nil {
			bytes = 32 << 20
		}
	}
	if items < 0 || items > 1000 || providerCalls < 1 || providerCalls > 100 || bytes < 1 || bytes > 512<<20 {
		return sourceRunBudget{}, fmt.Errorf("source-run budget is outside its bounded contract")
	}
	return sourceRunBudget{ProviderCalls: providerCalls, Items: items, Bytes: bytes, Workload: items + providerCalls}, nil
}

func reserveSourceRunBudget(tx *gorm.DB, request *models.SourceRunRequest, budget sourceRunBudget) error {
	if tx == nil || request == nil || request.TenantID == "" {
		return fmt.Errorf("source-run budget reservation identity is incomplete")
	}
	if err := tx.Exec("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))", "source-run-budget/v1/"+request.TenantID).Error; err != nil {
		return err
	}
	var totals struct {
		ProviderCalls int
		Items         int
		Bytes         int64
		Workload      int
	}
	if err := tx.Model(&models.SourceRunRequest{}).
		Select("COALESCE(SUM(reserved_provider_calls-consumed_provider_calls-released_provider_calls),0) provider_calls, COALESCE(SUM(reserved_items-consumed_items-released_items),0) items, COALESCE(SUM(reserved_bytes-consumed_bytes-released_bytes),0) bytes, COALESCE(SUM(reserved_workload-consumed_workload-released_workload),0) workload").
		Where("tenant_id=? AND budget_state='reserved'", request.TenantID).Scan(&totals).Error; err != nil {
		return err
	}
	if totals.ProviderCalls+budget.ProviderCalls > maxTenantReservedProviderCalls || totals.Items+budget.Items > maxTenantReservedItems || totals.Bytes+budget.Bytes > maxTenantReservedBytes || totals.Workload+budget.Workload > maxTenantReservedWorkload {
		return fmt.Errorf("tenant source-run rolling reservation budget is exhausted")
	}
	request.ProviderCallCap, request.ItemCap, request.ByteCap, request.WorkloadCap = budget.ProviderCalls, budget.Items, budget.Bytes, budget.Workload
	request.ReservedProviderCalls, request.ReservedItems, request.ReservedBytes, request.ReservedWorkload = budget.ProviderCalls, budget.Items, budget.Bytes, budget.Workload
	request.BudgetState = "reserved"
	return nil
}

func settleSourceRunBudget(tx *gorm.DB, request models.SourceRunRequest, now time.Time) error {
	if request.BudgetState != "reserved" {
		return nil
	}
	var usage struct {
		ProviderCalls int
		Items         int
		Bytes         int64
	}
	if err := tx.Raw(`SELECT
		COUNT(*) FILTER (WHERE event_type='provider_request_started')::integer provider_calls,
		COALESCE(SUM(CASE WHEN event_type='provider_page' AND payload->>'accepted' ~ '^[0-9]+$' THEN (payload->>'accepted')::integer ELSE 0 END),0)::integer items,
		COALESCE(SUM(CASE WHEN event_type='provider_page' AND payload->>'observed_bytes' ~ '^[0-9]+$' THEN (payload->>'observed_bytes')::bigint ELSE 0 END),0)::bigint bytes
		FROM source_run_receipts WHERE tenant_id=? AND source_run_request_id=?`, request.TenantID, request.PublicID).Scan(&usage).Error; err != nil {
		return err
	}
	workload := usage.ProviderCalls + usage.Items
	state := "settled"
	if usage.ProviderCalls > request.ReservedProviderCalls || usage.Items > request.ReservedItems || usage.Bytes > request.ReservedBytes || workload > request.ReservedWorkload {
		state = "exceeded"
	}
	return tx.Model(&models.SourceRunRequest{}).Where("tenant_id=? AND public_id=? AND budget_state='reserved'", request.TenantID, request.PublicID).Updates(map[string]any{
		"consumed_provider_calls": usage.ProviderCalls, "consumed_items": usage.Items, "consumed_bytes": usage.Bytes, "consumed_workload": workload,
		"released_provider_calls": maxInt(request.ReservedProviderCalls-usage.ProviderCalls, 0), "released_items": maxInt(request.ReservedItems-usage.Items, 0),
		"released_bytes": maxInt64(request.ReservedBytes-usage.Bytes, 0), "released_workload": maxInt(request.ReservedWorkload-workload, 0),
		"budget_state": state, "budget_settled_at": now.UTC(),
	}).Error
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
