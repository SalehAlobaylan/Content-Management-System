package economics

import (
	"content-management-system/src/models"
	"encoding/json"
	"gorm.io/gorm"
	"math"
	"time"
)

// Units survive zero net charges and local compute. They do not create a second
// charge on top of a provider receipt or hosting bill.
func ProjectOwnerUsage(tx *gorm.DB, tenant string) error {
	var exists bool
	if e := tx.Raw("SELECT to_regclass('ai_spend_events') IS NOT NULL").Scan(&exists).Error; e != nil || !exists {
		return e
	}
	var events []struct {
		models.AISpendEvent
		Subject, OwnerSubject, AttributionFeature, AttributionBenefit string
		ServiceAt                                                     *time.Time
	}
	q := `SELECT e.*,a.subject,a.owner_subject,a.feature attribution_feature,a.benefit attribution_benefit,j.completed_at service_at FROM economics_receipt_links l JOIN ai_spend_events e ON e.event_id=l.event_id AND e.tenant_id=l.tenant_id JOIN economics_attributions a ON a.id=l.attribution_ref AND a.tenant_id=l.tenant_id LEFT JOIN transcription_jobs j ON e.spend_class='stt_api' AND e.source_service='cms_stt_owner' AND j.public_id::text=e.system_run_id AND j.tenant_id=e.tenant_id WHERE l.tenant_id=? AND l.projected_at IS NULL ORDER BY e.id LIMIT 1000`
	if e := tx.Raw(q, tenant).Scan(&events).Error; e != nil {
		return e
	}
	rows := []Usage{}
	ids := []string{}
	for _, event := range events {
		ids = append(ids, event.EventID.String())
		if event.Cached {
			continue
		}
		at := event.OccurredAt.UTC()
		if event.ServiceAt != nil {
			at = event.ServiceAt.UTC()
		}
		op := "processing"
		if event.AttributionFeature == "transcription" {
			op = "transcription_request"
		}
		if event.AttributionFeature == "upload" {
			op = "upload"
		}
		var units map[string]float64
		if json.Unmarshal(event.Units, &units) != nil {
			continue
		}
		values := map[string]float64{"tokens": units["input_tokens"] + units["output_tokens"] + units["thoughts_tokens"], "audio_seconds": units["audio_sec"], "work_ms": units["compute_ms"], "items": units["items"]}
		variant := "owner_receipt"
		if event.AttributionBenefit == "shared_content" {
			variant = "preparation_owner_receipt"
			if event.Subject == "system" && event.OwnerSubject != "" {
				event.Subject = event.OwnerSubject
				variant = "preparation_owned_receipt"
			}
		}
		for unit, n := range values {
			if n <= 0 || n > 1e15 || math.IsNaN(n) || math.IsInf(n, 0) {
				continue
			}
			rows = append(rows, Usage{TenantID: tenant, Day: at.Truncate(24 * time.Hour), Subject: event.Subject, Operation: op, Unit: unit, Variant: variant, Value: int64(n)})
		}
	}
	if e := addUsage(tx, rows); e != nil {
		return e
	}
	if len(ids) == 0 {
		return nil
	}
	return tx.Exec(`UPDATE economics_receipt_links SET projected_at=now() WHERE event_id IN ?`, ids).Error
}
