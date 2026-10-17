package economics

import (
	"content-management-system/src/models"
	"fmt"
	"gorm.io/gorm"
	"time"
)

func aiInvoiceCost(i models.AISpendInvoice) (Cost, error) {
	amount, e := ParseAmount(fmt.Sprintf("%.12f", i.AmountUSD))
	if e != nil {
		return Cost{}, e
	}
	reason := i.Note
	if reason == "" {
		reason = "Existing AI invoice entry; provider/account evidence remains operator-owned"
	}
	return Cost{TenantID: i.TenantID, SourceID: "ai-invoice", LogicalKey: i.Provider + ":" + i.Month.Format("2006-01"), Provider: i.Provider, Resource: "ai", Feature: "ai", PeriodStart: i.Month.UTC(), PeriodEnd: i.Month.AddDate(0, 1, 0).UTC(), Net: amount, Currency: "USD", Destination: "reconciliation", Reconciled: true, Reason: reason, Actor: i.CreatedBy, Method: "existing_ai_invoice"}, nil
}
func RecordAIInvoice(db *gorm.DB, i models.AISpendInvoice) error {
	c, e := aiInvoiceCost(i)
	if e != nil {
		return e
	}
	_, e = AppendCost(db, c)
	return e
}
func ProjectAIInvoices(db *gorm.DB, tenant string) error {
	var exists bool
	if e := db.Raw("SELECT to_regclass('ai_spend_invoices') IS NOT NULL").Scan(&exists).Error; e != nil || !exists {
		return e
	}
	var invoices []models.AISpendInvoice
	if e := db.Where("tenant_id=? AND month>=?", tenant, retainedMonth(time.Now().UTC())).Limit(1001).Find(&invoices).Error; e != nil {
		return e
	}
	if len(invoices) > 1000 {
		return fmt.Errorf("invoice slice exceeds release bound")
	}
	var previous []Cost
	if e := db.Raw(`SELECT DISTINCT ON(logical_key) * FROM economics_cost_revisions WHERE tenant_id=? AND source_id='ai-invoice' AND period_start>=? ORDER BY logical_key,revision DESC LIMIT 1001`, tenant, retainedMonth(time.Now().UTC())).Scan(&previous).Error; e != nil {
		return e
	}
	proposals := make([]Cost, 0, len(invoices))
	for _, i := range invoices {
		c, e := aiInvoiceCost(i)
		if e != nil {
			return e
		}
		proposals = append(proposals, c)
	}
	return appendKnownFinancialRevisions(db, proposals, previous)
}
