package controllers

import (
	"content-management-system/src/models"
	"encoding/json"
	"sort"

	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// Media Circulation — evict aggregation (Slice 2).
//
// Produces item/family evict recommendations by REUSING the Storage+Quality
// classification (D10): protection set, candidate query, and per-item role are all
// existing storage logic. The only circulation-owned judgment is rank_down, which
// uses the thin value seam (circulationMediaValue). Reusing storage's protection +
// role queries inherits chapter independence for free — a hot child is never an
// evict target and a weak parent never punishes its children.

const (
	mediaCircVerdictProtect           = "protect"
	mediaCircVerdictRankDown          = "rank_down"
	mediaCircVerdictReEncode          = "re_encode"
	mediaCircVerdictMoveToCold        = "move_to_cold"
	mediaCircVerdictRecoverableDelete = "recoverable_delete"

	// Bounded output (Config Discipline: capacity constants, not env, not policy).
	mediaCircEvictProtectCap   = 100
	mediaCircEvictStorageCap   = 100
	mediaCircEvictRankDownCap  = 50
	mediaCircRankDownScanLimit = 500
)

// circulationRecInput is the shared intermediate for both evict (item_family) and
// intake (source) recommendations before persistence.
type circulationRecInput struct {
	SubjectID   uuid.UUID
	SubjectKind string
	Verdict     string
	Action      string
	Score       float64
	Reasons     []string
	Metrics     map[string]interface{}
}

// evictRecommendation is kept as an alias so Slice-2 code/tests read naturally.
type evictRecommendation = circulationRecInput

// mapRoleToEvictVerdict maps a storage content-role to an evict verdict/action.
// hot/normal feed units are not storage-eviction candidates (ok=false). Pure so it
// is unit-testable. Action follows the same policy intent storageRecommendationsFor
// uses (cold when available, else recoverable-delete for parents).
func mapRoleToEvictVerdict(role string, policy models.StoragePolicy, coldEnabled bool) (verdict, action string, ok bool) {
	switch role {
	case storageRoleAtomizedParentSource:
		if coldEnabled {
			return mediaCircVerdictMoveToCold, mediaCircVerdictMoveToCold, true
		}
		return mediaCircVerdictRecoverableDelete, mediaCircVerdictRecoverableDelete, true
	case storageRoleFailedOrOrphanArtifact:
		return mediaCircVerdictRecoverableDelete, mediaCircVerdictRecoverableDelete, true
	case storageRoleUnsuitableMedia:
		return mediaCircVerdictRecoverableDelete, mediaCircVerdictRecoverableDelete, true
	case storageRoleDormantFeedUnit:
		return mediaCircVerdictReEncode, mediaCircVerdictReEncode, true
	default:
		return "", "", false
	}
}

func subjectKindForRole(role string) string {
	if role == storageRoleAtomizedParentSource {
		return "parent_family"
	}
	return "content_item"
}

// computeEvictRecommendations builds the bounded evict recommendation set. It reads
// three reused sources and never re-derives "what is valuable/hot".
func computeEvictRecommendations(db *gorm.DB, tenantID string, storagePolicy models.StoragePolicy, circPolicy models.MediaCirculationPolicy, coldEnabled bool) []evictRecommendation {
	recs := []evictRecommendation{}
	protectedIDs := map[uuid.UUID]bool{}

	// 1. protect ← hot-protection set (reused). Also the exclusion set for everything below.
	var protectedItems []models.ContentItem
	protectedStorageItemsQuery(db, tenantID, storagePolicy).
		Order("(view_count + like_count * 2 + share_count * 4) DESC").
		Limit(mediaCircEvictProtectCap).
		Find(&protectedItems)
	for _, it := range protectedItems {
		protectedIDs[it.PublicID] = true
		recs = append(recs, evictRecommendation{
			SubjectID:   it.PublicID,
			SubjectKind: "content_item",
			Verdict:     mediaCircVerdictProtect,
			Action:      mediaCircVerdictProtect,
			Score:       float64(it.ViewCount + it.LikeCount*2 + it.ShareCount*4),
			Reasons:     []string{"Hot feed unit — protected from eviction and de-prioritization."},
			Metrics:     map[string]interface{}{"view_count": it.ViewCount, "file_size_bytes": it.FileSizeBytes},
		})
	}

	// 2. storage-action candidates (dormant / parent-source / unsuitable / failed) —
	//    reuse the shared candidate query; classify each with the reused role fn.
	var candidates []models.ContentItem
	buildCandidateQuery(db, filterFromPolicy(storagePolicy, tenantID, "", "")).
		Order("file_size_bytes DESC").
		Limit(mediaCircEvictStorageCap).
		Find(&candidates)
	for _, it := range candidates {
		if protectedIDs[it.PublicID] {
			continue // chapter independence: never evict a protected item
		}
		role, reason := storageRoleForContentItem(it)
		verdict, action, ok := mapRoleToEvictVerdict(role, storagePolicy, coldEnabled)
		if !ok {
			continue
		}
		// Atomization safety guardrail: don't recommend cooling/deleting a parent
		// whose atomization run isn't cleanly completed (stage-3 will deepen this).
		if role == storageRoleAtomizedParentSource && !parentAtomizationComplete(db, tenantID, it.PublicID) {
			continue
		}
		recs = append(recs, evictRecommendation{
			SubjectID:   it.PublicID,
			SubjectKind: subjectKindForRole(role),
			Verdict:     verdict,
			Action:      action,
			Score:       float64(it.FileSizeBytes),
			Reasons:     []string{reason, "Storage role: " + role + "."},
			Metrics:     map[string]interface{}{"role": role, "file_size_bytes": it.FileSizeBytes, "view_count": it.ViewCount},
		})
	}

	// 3. rank_down ← visible low-value feed units not protected (the boredom lever).
	//    Pre-bound the scan to the lowest-engagement visible tail, then filter by the
	//    circulation value floor.
	var visibleTail []models.ContentItem
	db.Where("tenant_id = ?", tenantID).
		Where("is_feed_unit = TRUE").
		Where("feed_visibility = ?", "visible").
		Where("status = ?", models.ContentStatusReady).
		Where("type IN ?", []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}).
		Order("view_count ASC, created_at ASC").
		Limit(mediaCircRankDownScanLimit).
		Find(&visibleTail)

	type rankDownCand struct {
		item  models.ContentItem
		value float64
	}
	rd := make([]rankDownCand, 0)
	for _, it := range visibleTail {
		if protectedIDs[it.PublicID] {
			continue
		}
		v := circulationMediaValue(it)
		if v < circPolicy.ValueFloor {
			rd = append(rd, rankDownCand{item: it, value: v})
		}
	}
	sort.Slice(rd, func(i, j int) bool { return rd[i].value < rd[j].value })
	if len(rd) > mediaCircEvictRankDownCap {
		rd = rd[:mediaCircEvictRankDownCap]
	}
	for _, cand := range rd {
		recs = append(recs, evictRecommendation{
			SubjectID:   cand.item.PublicID,
			SubjectKind: "content_item",
			Verdict:     mediaCircVerdictRankDown,
			Action:      mediaCircVerdictRankDown,
			Score:       circPolicy.ValueFloor - cand.value,
			Reasons:     []string{"Visible feed unit below the circulation value floor; de-prioritize so it stops boring viewers."},
			Metrics:     map[string]interface{}{"value": cand.value, "value_floor": circPolicy.ValueFloor, "view_count": cand.item.ViewCount},
		})
	}

	return recs
}

// parentAtomizationComplete returns true when the parent's latest atomization run
// completed cleanly. A missing run is treated as safe because atomizedParentSourceQuery
// already requires a published child (legacy parents predate run tracking).
func parentAtomizationComplete(db *gorm.DB, tenantID string, parentID uuid.UUID) bool {
	var run models.MediaAtomizationRun
	if err := db.Where("tenant_id = ? AND parent_content_item_id = ?", tenantID, parentID).
		Order("created_at DESC").First(&run).Error; err != nil {
		return true
	}
	return run.Status == "completed"
}

func evictVerdictCounts(recs []evictRecommendation) map[string]int {
	counts := map[string]int{}
	for _, r := range recs {
		counts[r.Verdict]++
	}
	return counts
}

// persistRecommendationsForUnit replaces prior *pending* rows of the given unit
// type with the fresh set in a transaction, preserving applied/dismissed history
// (D11 — the autopilot track record). Shared by the evict (item_family) and intake
// (source) sides.
func persistRecommendationsForUnit(db *gorm.DB, tenantID, unitType string, recs []circulationRecInput) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&models.MediaCirculationRecommendation{}).
			Where("tenant_id = ? AND unit_type = ? AND status = ?",
				tenantID, unitType, models.MediaCirculationRecStatusPending).
			Update("status", models.MediaCirculationRecStatusSuperseded).Error; err != nil {
			return err
		}
		if len(recs) == 0 {
			return nil
		}
		rows := make([]models.MediaCirculationRecommendation, 0, len(recs))
		for _, r := range recs {
			reasonsJSON, _ := json.Marshal(r.Reasons)
			metricsJSON, _ := json.Marshal(r.Metrics)
			rows = append(rows, models.MediaCirculationRecommendation{
				TenantID:    tenantID,
				UnitType:    unitType,
				SubjectID:   r.SubjectID,
				SubjectKind: r.SubjectKind,
				Verdict:     r.Verdict,
				Action:      r.Action,
				Score:       r.Score,
				Reasons:     datatypes.JSON(reasonsJSON),
				Metrics:     datatypes.JSON(metricsJSON),
				Status:      models.MediaCirculationRecStatusPending,
			})
		}
		return tx.Create(&rows).Error
	})
}

// countPendingEvictByVerdict is the cheap health-proof read (counts of the last
// generated evict recs, grouped by verdict).
func countPendingEvictByVerdict(db *gorm.DB, tenantID string) map[string]int64 {
	type row struct {
		Verdict string
		Count   int64
	}
	var rows []row
	db.Model(&models.MediaCirculationRecommendation{}).
		Select("verdict, COUNT(*) as count").
		Where("tenant_id = ? AND unit_type = ? AND status = ?",
			tenantID, models.MediaCirculationUnitItemFamily, models.MediaCirculationRecStatusPending).
		Group("verdict").
		Scan(&rows)
	out := map[string]int64{}
	for _, r := range rows {
		out[r.Verdict] = r.Count
	}
	return out
}
