package controllers

import (
	"content-management-system/src/models"
	"gorm.io/gorm"
	"time"
)

// Storage retains ownership. This projection reads its policy and counters and
// never creates defaults, calls the sweeper, or writes a second storage ledger.
func aiSpendStorageView(db *gorm.DB, tenant string, now time.Time) map[string]any {
	view := map[string]any{"enforcement": "owner_delegated", "status": "unavailable", "bill_usd": nil, "bill_status": "unavailable", "url": "/platform/storage"}
	var policy models.StoragePolicy
	if err := db.Where("tenant_id=? OR tenant_id IS NULL", tenant).Order("tenant_id NULLS LAST").First(&policy).Error; err != nil {
		return view
	}
	month := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	var rows []struct {
		Class string
		Used  int64
		At    *time.Time
	}
	if err := db.Model(&models.StorageOpMetric{}).Where("tenant_id=? AND date>=?", tenant, month).Select("op_class class,sum(count) used,max(updated_at) at").Group("op_class").Scan(&rows).Error; err != nil {
		return view
	}
	view["status"] = "owner_reported"
	view["month"] = month.Format("2006-01")
	view["quota_bytes"] = policy.MaxStorageBytes
	view["class_a"] = buildClassSummary(0, policy.ClassAFreeBudget, policy.ClassAWarnPct, policy.ClassACapPct, month, now)
	view["class_b"] = buildClassSummary(0, policy.ClassBFreeBudget, policy.ClassBWarnPct, policy.ClassBCapPct, month, now)
	for _, r := range rows {
		if r.Class == "A" {
			view["class_a"] = buildClassSummary(r.Used, policy.ClassAFreeBudget, policy.ClassAWarnPct, policy.ClassACapPct, month, now)
		}
		if r.Class == "B" {
			view["class_b"] = buildClassSummary(r.Used, policy.ClassBFreeBudget, policy.ClassBWarnPct, policy.ClassBCapPct, month, now)
		}
		if r.At != nil {
			old, _ := view["as_of"].(*time.Time)
			if old == nil || r.At.After(*old) {
				view["as_of"] = r.At
			}
		}
	}
	if len(rows) == 0 {
		view["status"] = "no_evidence"
	}
	return view
}
