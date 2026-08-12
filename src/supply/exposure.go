package supply

import (
	"fmt"
	"sync"
	"time"

	"content-management-system/src/feedcontract"
	"content-management-system/src/models"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

const podsSupplyReturnProbeLimit = 48

// PodsReturnedItem is deliberately minimal consumer-boundary evidence. The
// internal probe returns no user/session state and cannot write telemetry.
type PodsReturnedItem struct {
	ID                 uuid.UUID
	PublishedAt        time.Time
	SourceRunRequestID *uint
}

type PodsReturnProbe func(db *gorm.DB, tenant string, limit int) ([]PodsReturnedItem, error)

var (
	podsReturnProbeMu sync.RWMutex
	podsReturnProbe   PodsReturnProbe
)

// RegisterPodsReturnProbe is called by the CMS feed owner. Keeping the probe
// behind this narrow callback prevents the Supply supervisor from importing a
// controller or reconstructing ranking/serving policy on its own.
func RegisterPodsReturnProbe(probe PodsReturnProbe) {
	podsReturnProbeMu.Lock()
	podsReturnProbe = probe
	podsReturnProbeMu.Unlock()
}

func currentPodsReturnProbe() PodsReturnProbe {
	podsReturnProbeMu.RLock()
	probe := podsReturnProbe
	podsReturnProbeMu.RUnlock()
	return probe
}

type PodsExposureProof struct {
	SchemaVersion         string     `json:"schema_version"`
	GeneratedAt           time.Time  `json:"generated_at"`
	Verdict               string     `json:"verdict"`
	EvidenceCompleteness  string     `json:"evidence_completeness"`
	BaseEligibleCount     int64      `json:"base_eligible_count"`
	ReachableCount        int64      `json:"reachable_count"`
	ReturnedCount         int        `json:"returned_count"`
	DistinctReturnedCount int        `json:"distinct_returned_count"`
	EligibleReturnedGap   int64      `json:"eligible_returned_gap"`
	RepeatPressure        float64    `json:"repeat_pressure"`
	ActiveGenerationID    *uuid.UUID `json:"active_generation_id,omitempty"`
	ProbeID               uuid.UUID  `json:"probe_id"`
	NewestEligibleAt      *time.Time `json:"newest_eligible_at,omitempty"`
	NewestReachableAt     *time.Time `json:"newest_reachable_at,omitempty"`
	NewestReturnedAt      *time.Time `json:"newest_returned_at,omitempty"`
	LastFeedRenderedAt    *time.Time `json:"last_feed_rendered_at,omitempty"`
	LastExactViewAt       *time.Time `json:"last_exact_view_at,omitempty"`
	ReturnedIDs           []string   `json:"returned_ids"`
	RenderedIDs           []string   `json:"rendered_ids"`
	ViewedIDs             []string   `json:"viewed_ids"`
	Unknowns              []string   `json:"unknowns"`
}

// BuildPodsExposureProof is a non-perturbing, tenant-scoped internal probe. It
// uses the canonical serving predicate and active generation fence but never
// creates a user session, updates seen state, records demand, or changes rank.
func BuildPodsExposureProof(db *gorm.DB, tenant string, now time.Time) PodsExposureProof {
	proof := PodsExposureProof{SchemaVersion: "pods-exposure/v2", GeneratedAt: now.UTC(), Verdict: "unknown", EvidenceCompleteness: "complete", ProbeID: uuid.New(), ReturnedIDs: []string{}, RenderedIDs: []string{}, ViewedIDs: []string{}, Unknowns: []string{}}
	if db == nil || tenant == "" {
		proof.EvidenceCompleteness = "unavailable"
		proof.Unknowns = append(proof.Unknowns, "Explicit tenant or database evidence is unavailable.")
		return proof
	}
	base := feedcontract.PodsEligibleMediaQuery(db, tenant, feedcontract.SupportsAtomizedPodsSchema(db))
	if err := base.Count(&proof.BaseEligibleCount).Error; err != nil {
		proof.EvidenceCompleteness = "unavailable"
		proof.Unknowns = append(proof.Unknowns, "Base eligibility could not be read.")
		return proof
	}
	var newest models.ContentItem
	if proof.BaseEligibleCount > 0 && base.Order("COALESCE(published_at, created_at) DESC").First(&newest).Error == nil {
		at := newest.CreatedAt
		if newest.PublishedAt != nil {
			at = *newest.PublishedAt
		}
		proof.NewestEligibleAt = &at
	}
	// The public Pods feed uses the historical generation lane/member names
	// `media`/`feed_unit`.  Evidence must use that exact fence; inventing a
	// second `pods`/`content_item` vocabulary would silently report every active
	// generation as empty.
	reachable := feedcontract.ApplyActiveGenerationMembership(db, base.Session(&gorm.Session{}), tenant, "media", "feed_unit", "content_items.public_id")
	generationID, generationSupported, generationActive := feedcontract.ActiveGeneration(db, tenant, "media")
	if generationSupported && !generationActive {
		proof.EvidenceCompleteness = "unavailable"
		proof.Unknowns = append(proof.Unknowns, "Current generation authority is missing; serving is fail-closed.")
		proof.Verdict = "unknown"
		return proof
	}
	if generationActive {
		proof.ActiveGenerationID = &generationID
	}
	if err := reachable.Count(&proof.ReachableCount).Error; err != nil {
		proof.EvidenceCompleteness = "partial"
		proof.Unknowns = append(proof.Unknowns, "Current generation reachability could not be read.")
	}
	var newestReachable models.ContentItem
	if proof.ReachableCount > 0 && reachable.Session(&gorm.Session{}).Order("COALESCE(published_at, created_at) DESC").First(&newestReachable).Error == nil {
		at := newestReachable.CreatedAt
		if newestReachable.PublishedAt != nil {
			at = *newestReachable.PublishedAt
		}
		proof.NewestReachableAt = &at
	}
	probe := currentPodsReturnProbe()
	if probe == nil {
		proof.EvidenceCompleteness = "unavailable"
		proof.Unknowns = append(proof.Unknowns, "The canonical Pods return probe is not registered.")
	} else if returned, err := probe(db, tenant, podsSupplyReturnProbeLimit); err != nil {
		proof.EvidenceCompleteness = "unavailable"
		proof.Unknowns = append(proof.Unknowns, "The canonical Pods return probe could not establish serving evidence.")
	} else {
		seen := map[uuid.UUID]bool{}
		for _, item := range returned {
			if !seen[item.ID] {
				proof.ReturnedIDs = append(proof.ReturnedIDs, item.ID.String())
				seen[item.ID] = true
			}
		}
		proof.ReturnedCount = len(returned)
		proof.DistinctReturnedCount = len(seen)
		proof.EligibleReturnedGap = proof.BaseEligibleCount - int64(proof.DistinctReturnedCount)
		if proof.EligibleReturnedGap < 0 {
			proof.EligibleReturnedGap = 0
		}
		if len(returned) > 0 {
			proof.RepeatPressure = float64(len(returned)-len(seen)) / float64(len(returned))
		}
		if len(returned) > 0 {
			at := returned[0].PublishedAt.UTC()
			for _, item := range returned[1:] {
				if item.PublishedAt.After(at) {
					at = item.PublishedAt.UTC()
				}
			}
			proof.NewestReturnedAt = &at
		}
	}
	returnedIDs := make([]uuid.UUID, 0, len(proof.ReturnedIDs))
	for _, rawID := range proof.ReturnedIDs {
		if id, err := uuid.Parse(rawID); err == nil {
			returnedIDs = append(returnedIDs, id)
		}
	}
	var renderEvents []models.PodsBoundaryObservation
	renderQuery := db.Where("tenant_id=? AND boundary=? AND verdict=? AND content_item_id IN ?", tenant, "page_render", string(VerdictPresent), returnedIDs)
	if proof.ActiveGenerationID != nil {
		renderQuery = renderQuery.Where("generation_id=?", *proof.ActiveGenerationID)
	}
	if len(returnedIDs) > 0 && renderQuery.Order("observed_at DESC").Limit(podsSupplyReturnProbeLimit).Find(&renderEvents).Error == nil && len(renderEvents) > 0 {
		proof.LastFeedRenderedAt = &renderEvents[0].ObservedAt
		seenRendered := map[uuid.UUID]bool{}
		for _, event := range renderEvents {
			if !seenRendered[event.ContentItemID] {
				proof.RenderedIDs = append(proof.RenderedIDs, event.ContentItemID.String())
				seenRendered[event.ContentItemID] = true
			}
		}
	} else {
		proof.Unknowns = append(proof.Unknowns, "No exact returned-item page-render evidence was observed.")
	}
	var viewRows []models.PodsBoundaryObservation
	viewQuery := db.Where("tenant_id=? AND boundary=? AND verdict=? AND content_item_id IN ?", tenant, "exact_view", string(VerdictPresent), returnedIDs)
	if proof.ActiveGenerationID != nil {
		viewQuery = viewQuery.Where("generation_id=?", *proof.ActiveGenerationID)
	}
	if len(returnedIDs) > 0 && viewQuery.Order("observed_at DESC").Limit(podsSupplyReturnProbeLimit).Find(&viewRows).Error == nil && len(viewRows) > 0 {
		proof.LastExactViewAt = &viewRows[0].ObservedAt
		seenViewed := map[uuid.UUID]bool{}
		for _, row := range viewRows {
			if !seenViewed[row.ContentItemID] {
				proof.ViewedIDs = append(proof.ViewedIDs, row.ContentItemID.String())
				seenViewed[row.ContentItemID] = true
			}
		}
	} else {
		proof.Unknowns = append(proof.Unknowns, "No threshold-qualified view of an exact returned item was observed.")
	}
	switch {
	case proof.BaseEligibleCount == 0:
		proof.Verdict = "no_base_eligible_inventory"
	case proof.ReachableCount == 0:
		proof.Verdict = "eligible_not_generation_reachable"
	case proof.EvidenceCompleteness == "unavailable":
		proof.Verdict = "unknown"
	case proof.ReturnedCount == 0:
		proof.Verdict = "eligible_not_returned"
	case proof.NewestReachableAt != nil && proof.NewestReturnedAt != nil && proof.NewestReachableAt.After(proof.NewestReturnedAt.Add(24*time.Hour)):
		// A real return probe can be non-empty while systematically returning
		// old inventory. That is a serving/ranking starvation fact, not a
		// license for Supply to mutate ranking or replay pipeline work.
		proof.Verdict = "eligible_not_returned"
	case proof.NewestReturnedAt != nil && now.UTC().Sub(proof.NewestReturnedAt.UTC()) > 24*time.Hour:
		proof.Verdict = "pods_inventory_stale"
	default:
		proof.Verdict = "return_path_observed"
	}
	if len(proof.Unknowns) > 0 && proof.EvidenceCompleteness == "complete" {
		proof.EvidenceCompleteness = "partial"
	}
	return proof
}

// RecordPodsExposureReturnProof persists the feed-return part of a proof only
// from the bounded Supply evaluator. Status, Console, and Operator reads call
// BuildPodsExposureProof alone and therefore remain non-mutating.
func RecordPodsExposureReturnProof(db *gorm.DB, tenant string, proof PodsExposureProof) error {
	if db == nil || tenant == "" || proof.ProbeID == uuid.Nil || proof.GeneratedAt.IsZero() {
		return fmt.Errorf("invalid Pods return proof persistence input")
	}
	ids := make([]uuid.UUID, 0, len(proof.ReturnedIDs))
	for _, rawID := range proof.ReturnedIDs {
		id, err := uuid.Parse(rawID)
		if err != nil {
			return fmt.Errorf("invalid CMS-derived Pods return item id")
		}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		_, err := PrunePodsBoundaryObservations(db, proof.GeneratedAt, 500)
		return err
	}
	var items []struct {
		PublicID           uuid.UUID
		SourceRunRequestID *uint
	}
	if err := db.Model(&models.ContentItem{}).
		Select("public_id, source_run_request_id").
		Where("tenant_id = ? AND public_id IN ?", tenant, ids).
		Find(&items).Error; err != nil {
		return err
	}
	if len(items) != len(ids) {
		return fmt.Errorf("Pods return proof contains an item outside the current tenant")
	}
	for _, item := range items {
		if err := RecordPodsBoundaryObservation(db, models.PodsBoundaryObservation{
			TenantID: tenant, ContentItemID: item.PublicID, GenerationID: proof.ActiveGenerationID,
			Boundary: "feed_return", ProbeKind: "direct_probe", ProbeID: proof.ProbeID.String(),
			SourceRunRequestID: item.SourceRunRequestID, Verdict: string(VerdictPresent), ObservedAt: proof.GeneratedAt.UTC(),
		}); err != nil {
			return err
		}
	}
	_, err := PrunePodsBoundaryObservations(db, proof.GeneratedAt, 500)
	return err
}

// SetPodsReturnProbeForTest restores a prior registered probe after an
// isolated Supply test. It is not an operational configuration surface.
func SetPodsReturnProbeForTest(probe PodsReturnProbe) func() {
	podsReturnProbeMu.Lock()
	previous := podsReturnProbe
	podsReturnProbe = probe
	podsReturnProbeMu.Unlock()
	return func() {
		podsReturnProbeMu.Lock()
		podsReturnProbe = previous
		podsReturnProbeMu.Unlock()
	}
}
