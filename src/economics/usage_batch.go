package economics

import (
	"fmt"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"math"
	"time"
)

// Coalesce before the SQL upsert so duplicate keys in one sealed batch remain
// additive and never require one database round trip per observation.
func addUsage(tx *gorm.DB, rows []Usage) error {
	grouped := map[usageKey]int64{}
	for _, r := range rows {
		if r.Value < 0 {
			return fmt.Errorf("negative usage")
		}
		key := usageKey{r.TenantID, r.Subject, r.Operation, r.Unit, r.Variant, r.Day.UTC().Truncate(24 * time.Hour).Unix()}
		if grouped[key] > math.MaxInt64-r.Value {
			return fmt.Errorf("usage capacity exceeded")
		}
		grouped[key] += r.Value
	}
	out := make([]Usage, 0, len(grouped))
	for k, n := range grouped {
		if n > 0 {
			out = append(out, Usage{TenantID: k.Tenant, Subject: k.Subject, Operation: k.Operation, Unit: k.Unit, Variant: k.Variant, Day: time.Unix(k.Day, 0).UTC(), Value: n})
		}
	}
	if len(out) == 0 {
		return nil
	}
	return tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "tenant_id"}, {Name: "day"}, {Name: "subject"}, {Name: "operation"}, {Name: "unit"}, {Name: "variant"}}, DoUpdates: clause.Assignments(map[string]any{"value": gorm.Expr("economics_usage_daily.value+EXCLUDED.value")})}).CreateInBatches(out, 500).Error
}

type subjectBarrier struct {
	Erased    bool
	ClearedAt *time.Time
}
type subjectBarriers map[string]subjectBarrier

func loadSubjectBarriers(tx *gorm.DB, subjects []string) (subjectBarriers, error) {
	out := subjectBarriers{}
	hashes := []string{}
	byHash := map[string]string{}
	for _, s := range subjects {
		if len(s) < 8 || s[:8] != "account:" {
			continue
		}
		if _, ok := out[s]; ok {
			continue
		}
		hash, e := SubjectHash(tx, s)
		if e != nil {
			return nil, e
		}
		out[s] = subjectBarrier{}
		hashes = append(hashes, hash)
		byHash[hash] = s
	}
	if len(hashes) == 0 {
		return out, nil
	}
	var rows []struct {
		SubjectHash string
		Erased      bool
		ClearedAt   *time.Time
	}
	if e := tx.Table("economics_lifecycle_barriers").Where("subject_hash IN ?", hashes).Find(&rows).Error; e != nil {
		return nil, e
	}
	for _, r := range rows {
		out[byHash[r.SubjectHash]] = subjectBarrier{r.Erased, r.ClearedAt}
	}
	return out, nil
}
func (b subjectBarriers) Resolve(subject string, at time.Time) (string, bool) {
	r := b[subject]
	if r.Erased {
		return "removed", true
	}
	return subject, r.ClearedAt != nil && !at.After(*r.ClearedAt)
}
