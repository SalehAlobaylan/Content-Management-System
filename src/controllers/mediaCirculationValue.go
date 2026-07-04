package controllers

import (
	"content-management-system/src/intelligence"
	"content-management-system/src/models"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// The D3 value seam, stage-4 edition. The thin recomposition that used to live
// here is now the intelligence package's FallbackValue (identical math); the
// REAL engine is the persisted six-signal value model in src/intelligence.
//
//   - circulationMediaValue: pure per-item fallback — used where no DB handle
//     exists and as the fallback for never-scored items. Identical behavior to
//     the original thin seam, so pre-telemetry decisions don't shift.
//   - circulationMediaValues: the primary path — batch-reads the persisted
//     durable value (media_intelligence_scores) with per-item fallback.
//
// See docs/media-circulation-engine.md (D3) and
// docs/ranking-intelligence-stage4-plan.md (slice 1).
func circulationMediaValue(item models.ContentItem) float64 {
	return intelligence.FallbackValue(item)
}

// circulationMediaValues resolves durable values for a loaded item set through
// the intelligence engine: persisted score when available, FallbackValue
// otherwise. Callers that loop items should use this instead of per-item
// circulationMediaValue so scored items get the real model.
func circulationMediaValues(db *gorm.DB, tenantID string, items []models.ContentItem) map[uuid.UUID]float64 {
	return intelligence.Engine{DB: db}.Values(tenantID, items)
}
