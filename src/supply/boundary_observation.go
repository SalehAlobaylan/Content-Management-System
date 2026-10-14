package supply

import (
	"crypto/sha256"
	"fmt"
	"sort"
	"strings"
	"time"

	"content-management-system/src/models"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

const PodsBoundaryObservationRetention = 30 * 24 * time.Hour

var validPodsBoundaries = map[string]bool{"feed_return": true, "page_render": true, "exact_view": true}
var validPodsProbeKinds = map[string]bool{"direct_probe": true, "frozen_session": true, "authenticated": true, "anonymous": true}

// RecordPodsBoundaryObservation appends idempotent exact evidence. The caller
// supplies server-derived identities only; arbitrary payload data is excluded.
func RecordPodsBoundaryObservation(db *gorm.DB, row models.PodsBoundaryObservation) error {
	return RecordPodsBoundaryObservations(db, []models.PodsBoundaryObservation{row})
}

// RecordPodsBoundaryObservations preserves the exact-observation owner contract
// in one bounded transaction. Only newly inserted evidence increments yield.
func RecordPodsBoundaryObservations(db *gorm.DB, rows []models.PodsBoundaryObservation) error {
	if db == nil || len(rows) > 50 {
		return fmt.Errorf("invalid exact Pods boundary batch")
	}
	if len(rows) == 0 {
		return nil
	}
	values := make([]string, 0, len(rows))
	args := make([]any, 0, len(rows)*11)
	for _, row := range rows {
		if strings.TrimSpace(row.TenantID) == "" || row.ContentItemID == uuid.Nil ||
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
		values = append(values, "(?,?,?,?,?,?,?,?,?,?,?)")
		args = append(args, row.PublicID, row.TenantID, row.ContentItemID, row.GenerationID, row.Boundary, row.ProbeKind,
			row.ProbeID, row.SourceRunRequestID, row.Verdict, row.ProvenanceDigest, row.ObservedAt.UTC())
	}
	return db.Transaction(func(tx *gorm.DB) error {
		inserted := []models.PodsBoundaryObservation{}
		if err := tx.Raw(`INSERT INTO pods_boundary_observations
   (public_id,tenant_id,content_item_id,generation_id,boundary,probe_kind,probe_id,source_run_request_row_id,verdict,provenance_digest,observed_at)
   VALUES `+strings.Join(values, ",")+`
   ON CONFLICT (tenant_id,boundary,probe_kind,probe_id,content_item_id) DO NOTHING
   RETURNING tenant_id,content_item_id,boundary,verdict,observed_at`, args...).Scan(&inserted).Error; err != nil {
			return err
		}
		presentIDs := []uuid.UUID{}
		tenants := []string{}
		for _, row := range inserted {
			if row.Verdict == string(VerdictPresent) && (row.Boundary == "feed_return" || row.Boundary == "page_render") {
				presentIDs = append(presentIDs, row.ContentItemID)
				tenants = append(tenants, row.TenantID)
			}
		}
		if len(presentIDs) == 0 || !tx.Migrator().HasTable(&models.MediaSourceYieldDaily{}) {
			return nil
		}
		items := []models.ContentItem{}
		if err := tx.Select("tenant_id,public_id,content_source_id").Where("tenant_id IN ? AND public_id IN ?", tenants, presentIDs).Find(&items).Error; err != nil {
			return err
		}
		sources := map[string]uuid.UUID{}
		for _, item := range items {
			if item.ContentSourceID != nil {
				sources[item.TenantID+":"+item.PublicID.String()] = *item.ContentSourceID
			}
		}
		type delta struct {
			tenant, date string
			source       uuid.UUID
			public, page int
		}
		deltas := map[string]*delta{}
		for _, row := range inserted {
			if row.Verdict != string(VerdictPresent) || (row.Boundary != "feed_return" && row.Boundary != "page_render") {
				continue
			}
			source, ok := sources[row.TenantID+":"+row.ContentItemID.String()]
			if !ok {
				continue
			}
			date := row.ObservedAt.UTC().Format("2006-01-02")
			key := fmt.Sprintf("%s:%s:%s", row.TenantID, source, date)
			if deltas[key] == nil {
				deltas[key] = &delta{tenant: row.TenantID, source: source, date: date}
			}
			if row.Boundary == "feed_return" {
				deltas[key].public++
			} else {
				deltas[key].page++
			}
		}
		if len(deltas) == 0 {
			return nil
		}
		keys := make([]string, 0, len(deltas))
		for key := range deltas {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		yieldValues := make([]string, 0, len(keys))
		yieldArgs := make([]any, 0, len(keys)*5)
		for _, key := range keys {
			d := deltas[key]
			yieldValues = append(yieldValues, "(?,?,?,?,?,now())")
			yieldArgs = append(yieldArgs, d.tenant, d.source, d.date, d.public, d.page)
		}
		return tx.Exec(`INSERT INTO media_source_yield_daily
   (tenant_id,content_source_id,yield_date,public_returns,first_page_returns,updated_at)
   VALUES `+strings.Join(yieldValues, ",")+`
   ON CONFLICT (tenant_id,content_source_id,yield_date) DO UPDATE SET
   public_returns=media_source_yield_daily.public_returns+EXCLUDED.public_returns,
   first_page_returns=media_source_yield_daily.first_page_returns+EXCLUDED.first_page_returns,
   updated_at=now()`, yieldArgs...).Error
	})
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
