package economics

import (
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"time"
)

// SnapshotByteTime starts at first observation, never backdates present size into history.
// Public artifact ownership is reported as usage, independently of who benefits or pays.
func SnapshotByteTime(tx *gorm.DB, tenant string, now time.Time) error {
	var exists bool
	if e := tx.Raw("SELECT to_regclass('media_artifact_manifests') IS NOT NULL AND to_regclass('content_items') IS NOT NULL").Scan(&exists).Error; e != nil || !exists {
		return e
	}
	var rows []struct {
		ID             string
		OwnerID        *string
		SizeBytes      int64
		UpdatedAt      time.Time
		State          string
		PriorSize      *int64
		ObservedAt     *time.Time
		PriorUpdatedAt *time.Time
	}
	q := `SELECT m.public_id::text id,c.author_id::text owner_id,m.size_bytes,m.updated_at,m.state,s.size_bytes prior_size,s.observed_at,s.owner_updated_at prior_updated_at FROM media_artifact_manifests m LEFT JOIN content_items c ON c.public_id=m.content_item_id AND c.tenant_id=m.tenant_id LEFT JOIN economics_artifact_snapshots s ON s.artifact_id=m.public_id AND s.tenant_id=m.tenant_id WHERE m.tenant_id=? AND (s.artifact_id IS NULL OR s.observed_at<? OR m.updated_at>s.owner_updated_at) ORDER BY s.observed_at NULLS FIRST,m.id LIMIT 200`
	if e := tx.Raw(q, tenant, now.Add(-time.Hour)).Scan(&rows).Error; e != nil {
		return e
	}
	subjects := []string{}
	for _, r := range rows {
		if r.OwnerID != nil {
			subjects = append(subjects, "account:"+*r.OwnerID)
		}
	}
	barriers, e := loadSubjectBarriers(tx, subjects)
	if e != nil {
		return e
	}
	usage := []Usage{}
	var snapshots []struct {
		TenantID, ArtifactID, OwnerSubject string
		SizeBytes                          int64
		ObservedAt, OwnerUpdatedAt         time.Time
		CoverageGap                        bool
	}
	for _, r := range rows {
		owner := ""
		if r.OwnerID != nil {
			owner = "account:" + *r.OwnerID
			owner, _ = barriers.Resolve(owner, now)
		}
		size := r.SizeBytes
		if r.State == "deleted" || r.State == "failed" || r.State == "uncertain" {
			size = 0
		}
		gap := false
		if r.ObservedAt != nil && r.PriorSize != nil {
			start := *r.ObservedAt
			if now.Sub(start) > time.Hour {
				gap = true
				start = now.Add(-time.Hour)
			}
			if r.PriorUpdatedAt == nil || r.PriorUpdatedAt.Before(r.UpdatedAt) {
				gap = true
			} else if *r.PriorSize > 0 && *r.PriorSize <= 256*1024*1024*1024 {
				for start.Before(now) {
					end := start.UTC().Truncate(24*time.Hour).AddDate(0, 0, 1)
					if end.After(now) {
						end = now
					}
					seconds := int64(end.Sub(start) / time.Second)
					value := *r.PriorSize * seconds
					if value > 0 {
						usage = append(usage, Usage{TenantID: tenant, Day: start.UTC().Truncate(24 * time.Hour), Subject: "system", Operation: "storage", Unit: "byte_seconds", Variant: "observed_snapshot", Value: value})
						if owner != "" && owner != "removed" {
							usage = append(usage, Usage{TenantID: tenant, Day: start.UTC().Truncate(24 * time.Hour), Subject: owner, Operation: "storage", Unit: "owned_byte_seconds", Variant: "observed_snapshot", Value: value})
						}
					}
					start = end
				}
			}
		}
		snapshots = append(snapshots, struct {
			TenantID, ArtifactID, OwnerSubject string
			SizeBytes                          int64
			ObservedAt, OwnerUpdatedAt         time.Time
			CoverageGap                        bool
		}{tenant, r.ID, owner, size, now, r.UpdatedAt, gap})
	}
	if e := addUsage(tx, usage); e != nil {
		return e
	}
	if len(snapshots) == 0 {
		return nil
	}
	return tx.Table("economics_artifact_snapshots").Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "tenant_id"}, {Name: "artifact_id"}}, DoUpdates: clause.Assignments(map[string]any{"owner_subject": gorm.Expr("EXCLUDED.owner_subject"), "size_bytes": gorm.Expr("EXCLUDED.size_bytes"), "observed_at": gorm.Expr("EXCLUDED.observed_at"), "owner_updated_at": gorm.Expr("EXCLUDED.owner_updated_at"), "coverage_gap": gorm.Expr("economics_artifact_snapshots.coverage_gap OR EXCLUDED.coverage_gap")})}).CreateInBatches(snapshots, 200).Error
}
