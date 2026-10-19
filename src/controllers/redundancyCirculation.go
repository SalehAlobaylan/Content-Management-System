package controllers

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"content-management-system/src/models"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

func isRedundancyRecommendation(rec models.MediaCirculationRecommendation) bool {
	var m struct {
		Origin string `json:"origin"`
	}
	return json.Unmarshal(rec.Metrics, &m) == nil && m.Origin == "redundancy"
}

// Share Circulation's current human exceptions and Storage's hot-protection
// predicate at nomination and application. Reads fail closed on missing evidence.
func redundancyConsequenceQuery(db *gorm.DB, tenant string) (*gorm.DB, error) {
	var policy models.StoragePolicy
	err := db.Where("tenant_id=? OR tenant_id IS NULL", tenant).Order("tenant_id NULLS LAST,updated_at DESC,id DESC").First(&policy).Error
	if err != nil && err != gorm.ErrRecordNotFound {
		return nil, err
	}
	if err == gorm.ErrRecordNotFound {
		policy = models.StoragePolicy{ProtectTopNByViews: 50, ProtectTopNWindowDays: 30}
	}
	hot := protectedStorageItemsQuery(db, tenant, policy).Select("content_items.public_id")
	q := db.Table("redundancy_family_members m").Joins("JOIN redundancy_families f ON f.id=m.family_id AND f.tenant_id=m.tenant_id JOIN content_items ci ON ci.public_id=m.content_item_id AND ci.tenant_id=m.tenant_id").Where("m.tenant_id=? AND m.role='redundant' AND m.ended_at IS NULL AND f.status='active' AND ci.status='READY'", tenant).
		Where("ci.public_id NOT IN (?)", hot).
		Where(`NOT EXISTS (SELECT 1 FROM media_circulation_overrides o WHERE o.tenant_id=m.tenant_id AND o.subject_id=m.content_item_id AND o.subject_kind IN ('item','family') AND o.override_type IN ? AND (o.expires_at IS NULL OR o.expires_at>?))`, []string{models.MediaCirculationOverrideEditorialHold, models.MediaCirculationOverrideNeverArchive, models.MediaCirculationOverrideKeepLatestNHot}, time.Now().UTC())
	return q, nil
}
func validateRedundancyRecommendation(tx *gorm.DB, tenant string, rec models.MediaCirculationRecommendation) error {
	var metrics struct {
		FamilyID  uint      `json:"family_id"`
		Canonical uuid.UUID `json:"canonical_id"`
	}
	if json.Unmarshal(rec.Metrics, &metrics) != nil || metrics.FamilyID == 0 {
		return errors.New("redundancy recommendation has no current identity proof")
	}
	p, err := redundancyPolicyFor(tx, tenant)
	if err != nil {
		return err
	}
	if !p.Enabled || !p.EmitCirculationRecs {
		return errors.New("redundancy consequences are disabled")
	}
	var n int64
	q, err := redundancyConsequenceQuery(tx, tenant)
	if err != nil {
		return err
	}
	err = q.Where("f.id=? AND f.canonical_content_item_id=? AND f.canonical_content_item_id<>? AND m.content_item_id=?", metrics.FamilyID, metrics.Canonical, rec.SubjectID, rec.SubjectID).Count(&n).Error
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("family or protection changed; this recommendation is stale")
	}
	return nil
}
func reconcileRedundancyRecommendations(db *gorm.DB, tenant, actor string, runID *uint) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := lockRedundancyFamilies(tx, tenant); err != nil {
			return err
		}
		p, err := redundancyPolicyFor(tx, tenant)
		if err != nil {
			return err
		}
		if !p.Enabled || !p.EmitCirculationRecs {
			return nil
		}
		// Older emitters had no family/canonical binding. They cannot be applied
		// under the current contract; reconstruct current desires from families.
		if err := tx.Model(&models.MediaCirculationRecommendation{}).Where("tenant_id=? AND status='pending' AND metrics->>'origin'='redundancy' AND (metrics->>'canonical_id' IS NULL OR metrics->>'identity_key' IS NULL)", tenant).Update("status", models.MediaCirculationRecStatusSuperseded).Error; err != nil {
			return err
		}
		type memberRow struct {
			FamilyID               uint
			ContentItemID          uuid.UUID
			CanonicalContentItemID uuid.UUID
			FileSizeBytes          int64
			Confidence             float64
			Verdict                string
		}
		var rows []memberRow
		// Families are reconciled repeatedly; only new, currently justified desires are emitted.
		q, err := redundancyConsequenceQuery(tx, tenant)
		if err != nil {
			return err
		}
		// Apply the bound AFTER excluding satisfied desires, so later members cannot
		// starve behind an already-emitted first batch.
		err = q.Select("m.family_id,m.content_item_id,f.canonical_content_item_id,ci.file_size_bytes,0.9 AS confidence,desire.verdict").
			Joins("CROSS JOIN LATERAL (VALUES ('rank_down'),('recoverable_delete')) AS desire(verdict)").
			Where("desire.verdict='rank_down' OR ci.file_size_bytes>=?", 500<<20).
			Where(`NOT EXISTS (SELECT 1 FROM media_circulation_recommendations rec WHERE rec.tenant_id=m.tenant_id AND rec.metrics->>'origin'='redundancy' AND rec.metrics->>'identity_key'=concat(m.family_id,':',f.canonical_content_item_id,':',m.content_item_id,':',desire.verdict) AND rec.status IN ('pending','processing','applied','dismissed'))`).
			Order("m.id,desire.verdict").Limit(5000).Scan(&rows).Error
		if err != nil {
			return err
		}
		for _, row := range rows {
			verdict := row.Verdict
			key := fmt.Sprintf("%d:%s:%s:%s", row.FamilyID, row.CanonicalContentItemID, row.ContentItemID, verdict)
			var n int64
			if err := tx.Model(&models.MediaCirculationRecommendation{}).Where("tenant_id=? AND metrics->>'origin'='redundancy' AND metrics->>'identity_key'=? AND status IN ('pending','processing','applied','dismissed')", tenant, key).Count(&n).Error; err != nil {
				return err
			}
			if n > 0 {
				continue
			}
			rec := models.MediaCirculationRecommendation{TenantID: tenant, UnitType: models.MediaCirculationUnitItemFamily, SubjectKind: "content_item", SubjectID: row.ContentItemID, Verdict: verdict, Action: verdict, Score: row.Confidence, Reasons: redundancyJSON([]string{"Human-confirmed redundant recording; canonical " + row.CanonicalContentItemID.String()}), Metrics: redundancyJSON(map[string]interface{}{"origin": "redundancy", "family_id": row.FamilyID, "canonical_id": row.CanonicalContentItemID, "identity_key": key, "redundancy_reclaim": verdict == "recoverable_delete", "approval_required": verdict == "recoverable_delete"}), Status: models.MediaCirculationRecStatusPending}
			if err := tx.Create(&rec).Error; err != nil {
				return err
			}
			if err := redundancyAction(tx, tenant, "circulation.emit", actor, "succeeded", runID, nil, &row.FamilyID, verdict, map[string]interface{}{"recommendation_id": rec.PublicID, "subject_id": row.ContentItemID}); err != nil {
				return err
			}
		}
		return nil
	})
}
