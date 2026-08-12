package supply

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"time"

	"content-management-system/src/models"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const PodsBoundaryObservationRetention = 30 * 24 * time.Hour

var validPodsBoundaries = map[string]bool{"feed_return": true, "page_render": true, "exact_view": true}
var validPodsProbeKinds = map[string]bool{"direct_probe": true, "frozen_session": true, "authenticated": true, "anonymous": true}

// RecordPodsBoundaryObservation appends idempotent exact evidence. The caller
// supplies server-derived identities only; arbitrary payload data is excluded.
func RecordPodsBoundaryObservation(db *gorm.DB, row models.PodsBoundaryObservation) error {
	if db == nil || strings.TrimSpace(row.TenantID) == "" || row.ContentItemID == uuid.Nil ||
		!validPodsBoundaries[row.Boundary] || !validPodsProbeKinds[row.ProbeKind] ||
		strings.TrimSpace(row.ProbeID) == "" || len(row.ProbeID) > 160 {
		return fmt.Errorf("invalid exact Pods boundary observation")
	}
	if row.Verdict != string(VerdictPresent) && row.Verdict != string(VerdictAbsent) && row.Verdict != string(VerdictUnknown) {
		return fmt.Errorf("invalid Pods boundary verdict")
	}
	if row.ObservedAt.IsZero() {
		return fmt.Errorf("Pods boundary observation time is required")
	}
	identity := fmt.Sprintf("pods-boundary/v1\n%s\n%s\n%s\n%s\n%s\n%s", row.TenantID, row.ContentItemID, row.Boundary, row.ProbeKind, row.ProbeID, row.Verdict)
	row.ProvenanceDigest = fmt.Sprintf("%x", sha256.Sum256([]byte(identity)))
	if row.PublicID == uuid.Nil {
		row.PublicID = uuid.New()
	}
	return db.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "tenant_id"}, {Name: "boundary"}, {Name: "probe_kind"}, {Name: "probe_id"}, {Name: "content_item_id"}}, DoNothing: true}).Create(&row).Error
}

// PrunePodsBoundaryObservations is intentionally bounded and is called only
// by the Supply evaluator/reconciler owner, never by a browser request.
func PrunePodsBoundaryObservations(db *gorm.DB, now time.Time, limit int) (int64, error) {
	if db == nil {
		return 0, nil
	}
	if limit < 1 || limit > 1000 {
		limit = 500
	}
	var ids []uuid.UUID
	if err := db.Model(&models.PodsBoundaryObservation{}).Where("created_at < ?", now.UTC().Add(-PodsBoundaryObservationRetention)).Order("created_at").Limit(limit).Pluck("public_id", &ids).Error; err != nil || len(ids) == 0 {
		return 0, err
	}
	result := db.Where("public_id IN ?", ids).Delete(&models.PodsBoundaryObservation{})
	return result.RowsAffected, result.Error
}
