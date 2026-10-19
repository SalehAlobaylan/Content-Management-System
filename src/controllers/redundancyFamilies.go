package controllers

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
	"time"

	"content-management-system/src/intelligence"
	"content-management-system/src/models"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func lockRedundancyFamilies(tx *gorm.DB, tenant string) error {
	return tx.Exec("SELECT pg_advisory_xact_lock(hashtextextended(?,0))", "redundancy-family:"+tenant).Error
}
func selectRedundancyFamilyCanonical(db *gorm.DB, tenant string, items []models.ContentItem) (uuid.UUID, []string, error) {
	if len(items) == 0 {
		return uuid.Nil, nil, errors.New("family has no surviving media")
	}
	values := (intelligence.Engine{DB: db}).Values(tenant, items)
	type engagementRow struct {
		ContentItemID uuid.UUID
		Total         int64
	}
	var rows []engagementRow
	ids := []uuid.UUID{}
	for _, item := range items {
		ids = append(ids, item.PublicID)
	}
	if err := db.Model(&models.UserInteraction{}).Select("content_item_id,count(*) AS total").Where("content_item_id IN ? AND type IN ('complete','bookmark','meaningful')", ids).Group("content_item_id").Scan(&rows).Error; err != nil {
		return uuid.Nil, nil, err
	}
	engagement := map[uuid.UUID]int64{}
	for _, row := range rows {
		engagement[row.ContentItemID] = row.Total * 100
	}
	for _, item := range items {
		engagement[item.PublicID] += int64(item.ViewCount*3 + item.LikeCount*10 + item.ShareCount*15)
	}
	sourceIDs := []uuid.UUID{}
	for _, item := range items {
		if item.ContentSourceID != nil {
			sourceIDs = append(sourceIDs, *item.ContentSourceID)
		}
	}
	var sources []models.ContentSource
	if len(sourceIDs) > 0 {
		if err := db.Where("tenant_id=? AND public_id IN ? AND is_active=true", tenant, sourceIDs).Find(&sources).Error; err != nil {
			return uuid.Nil, nil, err
		}
	}
	activeSource := map[uuid.UUID]bool{}
	for _, source := range sources {
		activeSource[source.PublicID] = true
	}
	standing := func(item models.ContentItem) bool {
		return item.ContentSourceID != nil && activeSource[*item.ContentSourceID]
	}
	quality := func(item models.ContentItem) int {
		n := 0
		if item.PlaybackURL != nil && *item.PlaybackURL != "" {
			n += 2
		}
		if item.PlaybackType != nil && *item.PlaybackType == "hls" {
			n += 2
		}
		if item.TranscriptID != nil {
			n++
		}
		if len(item.MediaRenditions) > 2 {
			n++
		}
		if item.CurrentBitrateKbps != nil {
			n += *item.CurrentBitrateKbps / 64
		}
		return n
	}
	sort.SliceStable(items, func(i, j int) bool {
		a, b := items[i], items[j]
		if engagement[a.PublicID] != engagement[b.PublicID] {
			return engagement[a.PublicID] > engagement[b.PublicID]
		}
		if values[a.PublicID] != values[b.PublicID] {
			return values[a.PublicID] > values[b.PublicID]
		}
		if quality(a) != quality(b) {
			return quality(a) > quality(b)
		}
		if standing(a) != standing(b) {
			return standing(a)
		}
		if !a.CreatedAt.Equal(b.CreatedAt) {
			return a.CreatedAt.Before(b.CreatedAt)
		}
		return a.PublicID.String() < b.PublicID.String()
	})
	return items[0].PublicID, []string{fmt.Sprintf("Engagement history: %d weighted signals", engagement[items[0].PublicID]), fmt.Sprintf("Intelligence value: %.3f", values[items[0].PublicID]), "Playback, transcript and rendition completeness; earliest ingest breaks ties"}, nil
}
func invalidateRedundancyRecommendations(tx *gorm.DB, tenant string, familyIDs []uint) error {
	if len(familyIDs) == 0 {
		return nil
	}
	return tx.Model(&models.MediaCirculationRecommendation{}).Where("tenant_id=? AND status='pending' AND metrics->>'origin'='redundancy' AND (metrics->>'family_id')::bigint IN ?", tenant, familyIDs).Update("status", models.MediaCirculationRecStatusSuperseded).Error
}
func confirmRedundancyPair(db *gorm.DB, tenant, actor string, id uuid.UUID, evaluationID uint) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := lockRedundancyFamilies(tx, tenant); err != nil {
			return err
		}
		var pair models.RedundancyPair
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=?", tenant, id).First(&pair).Error; err != nil {
			return err
		}
		if pair.Tombstoned || pair.Verdict == models.RedundancyVerdictRejected {
			return errors.New("this pair was rejected; human tombstones are permanent")
		}
		if pair.Verdict == models.RedundancyVerdictConfirmed {
			return nil
		}
		if pair.LatestEvaluationID == nil || *pair.LatestEvaluationID != evaluationID || !(pair.Verdict == models.RedundancyVerdictProbable || pair.Verdict == models.RedundancyVerdictHighConfidence) {
			return errors.New("proposal evidence changed; reload the pair")
		}
		var inputs []models.ContentItem
		if err := redundancyLibrary(tx, tenant).Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id IN ?", []uuid.UUID{pair.ItemAID, pair.ItemBID}).Order("public_id").Find(&inputs).Error; err != nil {
			return err
		}
		if len(inputs) != 2 {
			return errors.New("pair inputs are no longer eligible media")
		}
		p, err := redundancyPolicyFor(tx, tenant)
		if err != nil {
			return err
		}
		lanes, fingerprint, err := redundancyLanes(tx, tenant, inputs[0], inputs[1], map[uuid.UUID]models.RedundancyFingerprint{})
		err = redundancyEvidenceCheck(p, lanes, err)
		if err != nil {
			return err
		}
		fingerprint = redundancyEvaluationFingerprint(fingerprint, p)
		var evaluation models.RedundancyPairEvaluation
		if err := tx.First(&evaluation, evaluationID).Error; err != nil {
			return err
		}
		if evaluation.InputFingerprint != fingerprint {
			return errors.New("source, transcript, model, or policy evidence changed; run a new scan")
		}
		var memberships []models.RedundancyFamilyMember
		if err := tx.Joins("JOIN redundancy_families f ON f.id=redundancy_family_members.family_id AND f.status='active'").Where("redundancy_family_members.tenant_id=? AND ended_at IS NULL AND content_item_id IN ?", tenant, []uuid.UUID{pair.ItemAID, pair.ItemBID}).Find(&memberships).Error; err != nil {
			return err
		}
		familyIDs := []uint{}
		seen := map[uint]bool{}
		for _, m := range memberships {
			if !seen[m.FamilyID] {
				seen[m.FamilyID] = true
				familyIDs = append(familyIDs, m.FamilyID)
			}
		}
		sort.Slice(familyIDs, func(i, j int) bool { return familyIDs[i] < familyIDs[j] })
		var families []models.RedundancyFamily
		var members []models.RedundancyFamilyMember
		if len(familyIDs) > 0 {
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id IN ?", familyIDs).Order("id").Find(&families).Error; err != nil {
				return err
			}
			if err := tx.Where("family_id IN ? AND ended_at IS NULL", familyIDs).Find(&members).Error; err != nil {
				return err
			}
		}
		lockedID := uuid.Nil
		lockActor := ""
		for _, f := range families {
			if f.CanonicalLockedBy != "" {
				if lockedID != uuid.Nil && lockedID != f.CanonicalContentItemID {
					return errors.New("families have conflicting canonical locks; unlock one family before merging")
				}
				lockedID = f.CanonicalContentItemID
				lockActor = f.CanonicalLockedBy
			}
		}
		all := map[uuid.UUID]bool{pair.ItemAID: true, pair.ItemBID: true}
		for _, m := range members {
			all[m.ContentItemID] = true
		}
		ids := redundancySortedIDs(all)
		var conflicts int64
		if err := tx.Model(&models.RedundancyPair{}).Where("tenant_id=? AND (tombstoned=true OR verdict=?) AND item_a_id IN ? AND item_b_id IN ?", tenant, models.RedundancyVerdictRejected, ids, ids).Count(&conflicts).Error; err != nil {
			return err
		}
		if conflicts > 0 {
			return errors.New("family merge conflicts with a permanent human rejection; review the existing families")
		}
		var items []models.ContentItem
		if err := redundancyCanonicalLibrary(tx, tenant).Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id IN ?", ids).Order("public_id").Find(&items).Error; err != nil {
			return err
		}
		if lockedID != uuid.Nil {
			available := false
			for _, item := range items {
				available = available || item.PublicID == lockedID
			}
			if !available {
				return errors.New("locked canonical is unavailable; select an available canonical or unlock the family before merging")
			}
		}
		canonical, reasons, err := selectRedundancyFamilyCanonical(tx, tenant, items)
		if err != nil {
			return err
		}
		if lockedID != uuid.Nil {
			canonical = lockedID
			reasons = []string{"Human canonical lock retained"}
		}
		now := time.Now().UTC()
		family := models.RedundancyFamily{TenantID: tenant, Status: "active", CanonicalContentItemID: canonical, CanonicalLockedBy: lockActor, CanonicalReasons: redundancyJSON(reasons), FirstConfirmedAt: now, LastConfirmedAt: now}
		if len(families) > 0 {
			family.ID = families[0].ID
			family.PublicID = families[0].PublicID
			family.FirstConfirmedAt = families[0].FirstConfirmedAt
			if err := invalidateRedundancyRecommendations(tx, tenant, familyIDs); err != nil {
				return err
			}
			// End historic memberships before inserting the reconciled single-canonical set.
			if err := tx.Model(&models.RedundancyFamilyMember{}).Where("family_id IN ? AND ended_at IS NULL", familyIDs).Update("ended_at", now).Error; err != nil {
				return err
			}
			if err := tx.Model(&models.RedundancyPair{}).Where("family_id IN ?", familyIDs).Update("family_id", family.ID).Error; err != nil {
				return err
			}
			if len(familyIDs) > 1 {
				if err := tx.Model(&models.RedundancyFamily{}).Where("id IN ? AND id<>?", familyIDs, family.ID).Updates(map[string]interface{}{"status": "dissolved", "dissolved_at": now, "dissolved_by": actor, "dissolve_reason": "merged into confirmed family"}).Error; err != nil {
					return err
				}
			}
			if err := tx.Model(&models.RedundancyFamily{}).Where("id=?", family.ID).Updates(map[string]interface{}{"canonical_content_item_id": canonical, "canonical_locked_by": lockActor, "canonical_reasons": family.CanonicalReasons, "last_confirmed_at": now}).Error; err != nil {
				return err
			}
		} else if err := tx.Create(&family).Error; err != nil {
			return err
		}
		for _, itemID := range redundancySortedIDs(all) {
			role := "redundant"
			if itemID == canonical {
				role = "canonical"
			}
			if err := tx.Create(&models.RedundancyFamilyMember{TenantID: tenant, FamilyID: family.ID, ContentItemID: itemID, Role: role, Since: now}).Error; err != nil {
				return err
			}
		}
		if err := tx.Model(&pair).Updates(map[string]interface{}{"family_id": family.ID, "verdict": models.RedundancyVerdictConfirmed, "reviewed_by": actor, "reviewed_at": now}).Error; err != nil {
			return err
		}
		return redundancyAction(tx, tenant, "pair.confirm", actor, "succeeded", nil, &pair.ID, &family.ID, "Human confirmed recording identity", map[string]interface{}{"evaluation_id": evaluationID, "canonical": canonical, "members": all})
	})
}
func redundancyEvaluationFingerprint(fingerprint string, p models.RedundancyPolicy) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(fingerprint+string(p.LaneSettings)+fmt.Sprint(p.ProposalFloor))))
}
