package economics

import (
	"gorm.io/gorm"
	"time"
)

// Reuse terminal owner attempts. Elapsed time includes IO/waits and must never
// be presented as measured CPU or priced as an extra fee on the hosting bill.
func ProjectJobUsage(tx *gorm.DB, tenant string, now time.Time, since *time.Time) error {
	var exists bool
	if e := tx.Raw("SELECT to_regclass('content_stage_attempts') IS NOT NULL AND to_regclass('content_stage_requests') IS NOT NULL AND to_regclass('content_items') IS NOT NULL").Scan(&exists).Error; e != nil || !exists {
		return e
	}
	from := now.AddDate(0, 0, -90)
	if since != nil && since.After(from) {
		from = *since
	}
	var rows []struct {
		ID, Stage, FailureClass     string
		AuthorID                    *string
		EffectStartedAt, FinishedAt time.Time
	}
	q := `SELECT a.public_id::text id,a.stage,a.failure_class,c.author_id::text author_id,a.effect_started_at,a.finished_at FROM content_stage_attempts a JOIN content_stage_requests r ON r.public_id=a.request_id AND r.tenant_id=a.tenant_id JOIN content_items c ON c.public_id=r.content_item_id AND c.tenant_id=r.tenant_id WHERE a.tenant_id=? AND a.finished_at>=? AND a.finished_at<=? AND a.effect_started_at IS NOT NULL AND a.stage IN('pods_media_artifacts','pods_atomization','news_asset') AND NOT EXISTS(SELECT 1 FROM economics_owner_usage_receipts p WHERE p.tenant_id=a.tenant_id AND p.owner='aggregation_stage' AND p.receipt_key=a.public_id::text) ORDER BY a.id LIMIT 500`
	if e := tx.Raw(q, tenant, from, now).Scan(&rows).Error; e != nil {
		return e
	}
	subjects := []string{}
	for _, r := range rows {
		if r.AuthorID != nil {
			subjects = append(subjects, "account:"+*r.AuthorID)
		}
	}
	barriers, e := loadSubjectBarriers(tx, subjects)
	if e != nil {
		return e
	}
	usage := []Usage{}
	var receipts []struct {
		TenantID, Owner, ReceiptKey string
		ObservedAt                  time.Time
	}
	for _, r := range rows {
		subject, operation := "system", "processing"
		if r.AuthorID != nil {
			subject = "account:" + *r.AuthorID
			operation = "upload"
		}
		subject, _ = barriers.Resolve(subject, r.FinishedAt)
		elapsed := r.FinishedAt.Sub(r.EffectStartedAt).Milliseconds()
		values := map[string]int64{"job_attempts": 1}
		if elapsed >= 0 && elapsed <= 48*time.Hour.Milliseconds() {
			values["job_elapsed_ms"] = elapsed
		}
		if r.FailureClass != "" {
			values["job_failures"] = 1
		}
		for unit, n := range values {
			if n <= 0 {
				continue
			}
			usage = append(usage, Usage{TenantID: tenant, Day: r.FinishedAt.UTC().Truncate(24 * time.Hour), Subject: subject, Operation: operation, Unit: unit, Variant: "elapsed:" + r.Stage, Value: n})
		}
		receipts = append(receipts, struct {
			TenantID, Owner, ReceiptKey string
			ObservedAt                  time.Time
		}{tenant, "aggregation_stage", r.ID, r.FinishedAt})
	}
	if e := addUsage(tx, usage); e != nil {
		return e
	}
	if len(receipts) == 0 {
		return nil
	}
	return tx.Table("economics_owner_usage_receipts").CreateInBatches(receipts, 500).Error
}
