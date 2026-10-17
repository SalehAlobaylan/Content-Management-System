package economics

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// A call has a fixed wall budget; successful chunks commit their cursor with their
// private output. Reports continue reading the last complete publication.
const projectionBudget = 20 * time.Second
const projectionPage = 25
const projectionEvidenceLimit = 100000 // Per expense/pool, not the entire platform.
var ErrProjectionPending = errors.New("economics projection is checkpointed and still building")

type projectionJob struct {
	TenantID  string
	ID        string
	StartedAt time.Time
	Policy    string
	Phase     string
	Cursor    int64
	Rows      int64
}

func (projectionJob) TableName() string { return "economics_projection_jobs" }

func Rebuild(db *gorm.DB, tenant string) error {
	db = privateDB(db)
	if !LifecycleReady(db) {
		return fmt.Errorf("IAM lifecycle synchronization is required before publishing account reports")
	}
	deadline := time.Now().Add(projectionBudget)
	for {
		complete, e := projectionStep(db, tenant)
		if e != nil {
			return e
		}
		if complete {
			return nil
		}
		if time.Now().After(deadline) {
			return ErrProjectionPending
		}
	}
}

func projectionStep(db *gorm.DB, tenant string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	complete := false
	var job projectionJob
	e := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if e := tx.Exec("SET LOCAL statement_timeout='25s'").Error; e != nil {
			return e
		}
		if e := Lock(tx); e != nil {
			return e
		}
		var policy Policy
		if e := tx.Where("tenant_id=?", tenant).Take(&policy).Error; e != nil {
			return e
		}
		if !policy.ReportsEnabled {
			return ErrDisabled
		}
		encoded, _ := json.Marshal(policy)
		e := tx.Where("tenant_id=?", tenant).Take(&job).Error
		if e != nil && !errors.Is(e, gorm.ErrRecordNotFound) {
			return e
		}
		now := time.Now().UTC()
		if job.ID != "" && (job.Policy != string(encoded) || !job.StartedAt.Truncate(24*time.Hour).Equal(now.Truncate(24*time.Hour))) {
			if e := discardProjection(tx, tenant); e != nil {
				return e
			}
			job = projectionJob{}
		}
		if job.ID == "" {
			if e := prepareProjection(tx, tenant, policy); e != nil {
				return fmt.Errorf("prepare: %w", e)
			}
			job = projectionJob{TenantID: tenant, ID: uuid.NewString(), StartedAt: now, Policy: string(encoded), Phase: "invoices"}
			if e := tx.Create(&job).Error; e != nil {
				return e
			}
			if e := tx.Create(&Run{ID: job.ID, TenantID: tenant, Status: "building", StartedAt: now}).Error; e != nil {
				return e
			}
			return snapshotProjection(tx, job)
		}
		if !LifecycleReady(tx) {
			return fmt.Errorf("IAM lifecycle synchronization expired during projection")
		}
		if job.Phase == "invoices" {
			done, e := reconcileProjectionPage(tx, &job)
			if e != nil {
				return e
			}
			if done {
				job.Phase = "expenses"
				job.Cursor = 0
			}
		} else {
			done, e := projectExpensePage(tx, &job, policy)
			if e != nil {
				return e
			}
			if done {
				if e := publishProjection(tx, job, policy); e != nil {
					return fmt.Errorf("publish: %w", e)
				}
				complete = true
				return nil
			}
		}
		if e := tx.Model(&Run{}).Where("id=?", job.ID).Updates(map[string]any{"status": "building", "rows": job.Rows, "error": ""}).Error; e != nil {
			return e
		}
		return tx.Save(&job).Error
	})
	if e != nil && job.ID != "" {
		// A failed chunk rolls back both cursor and output. Retrying resumes the same job.
		message := e.Error()
		if len(message) > 500 {
			message = message[:500]
		}
		_ = db.Transaction(func(tx *gorm.DB) error {
			if e := Lock(tx); e != nil {
				return e
			}
			return tx.Model(&Run{}).Where("id=?", job.ID).Updates(map[string]any{"status": "failed", "error": message}).Error
		})
	}
	return complete, e
}

func prepareProjection(tx *gorm.DB, tenant string, p Policy) error {
	if p.CollectionEnabled {
		if e := SnapshotByteTime(tx, tenant, time.Now().UTC()); e != nil {
			return e
		}
	}
	if e := ProjectContentAttributions(tx, tenant, p.CollectionEnabled); e != nil {
		return e
	}
	if p.CollectionEnabled {
		if e := ProjectConsumption(tx, tenant, time.Now().UTC(), p.CollectionSince); e != nil {
			return e
		}
	}
	if e := ProjectStoryAttributions(tx, tenant); e != nil {
		return e
	}
	if p.CollectionEnabled {
		if e := ProjectJobUsage(tx, tenant, time.Now().UTC(), p.CollectionSince); e != nil {
			return e
		}
		if e := ProjectOwnerUsage(tx, tenant); e != nil {
			return e
		}
	}
	if e := ProjectAI(tx, tenant); e != nil {
		return e
	}
	if e := ProjectAIResiduals(tx, tenant); e != nil {
		return e
	}
	return ProjectAIInvoices(tx, tenant)
}

func snapshotProjection(tx *gorm.DB, j projectionJob) error {
	// INSERT SELECT keeps the platform-sized evidence in PostgreSQL, not Go heap.
	// One locked transaction establishes a coherent build input; work pages never
	// chase live usage or amended invoices partway through a publication.
	for _, q := range []struct {
		kind, scope, query string
		args               []any
	}{
		{"cost", "source_id||':'||logical_key", `SELECT DISTINCT ON(source_id,logical_key) * FROM economics_cost_revisions WHERE tenant_id=? AND period_end>? ORDER BY source_id,logical_key,revision DESC`, []any{j.TenantID, retainedMonth(j.StartedAt)}},
		{"usage", "day::text", `SELECT * FROM economics_usage_daily WHERE tenant_id=? AND day>=?`, []any{j.TenantID, retainedMonth(j.StartedAt)}},
		{"weight", "content_id", `SELECT * FROM economics_content_weights WHERE tenant_id=?`, []any{j.TenantID}},
		{"attribution", "id", `SELECT * FROM economics_attributions WHERE tenant_id=?`, []any{j.TenantID}},
		{"profile", "resource", `SELECT * FROM economics_profiles WHERE tenant_id=?`, []any{j.TenantID}},
		{"rule", "kind", `SELECT * FROM economics_content_rules WHERE tenant_id=?`, []any{j.TenantID}},
		{"previous", "expense_key", `SELECT * FROM economics_cost_summaries WHERE tenant_id=?`, []any{j.TenantID}},
		{"pool", "expense_key", `SELECT * FROM economics_pool_weights WHERE tenant_id=?`, []any{j.TenantID}},
	} {
		args := []any{j.ID, q.kind}
		args = append(args, q.args...)
		if e := tx.Exec(`INSERT INTO economics_projection_inputs(job_id,kind,scope,payload) SELECT ?,?,`+q.scope+`,to_jsonb(s) FROM (`+q.query+`) s`, args...).Error; e != nil {
			return e
		}
	}
	// Retained resource offsets have no active expense to visit.
	return tx.Exec(`INSERT INTO economics_projection_summaries SELECT s.*,? FROM economics_cost_summaries s WHERE tenant_id=? AND source_id IN('retained-resource','cleared-resource-offset')`, j.ID, j.TenantID).Error
}

func discardProjection(tx *gorm.DB, tenant string) error {
	if e := tx.Exec(`UPDATE economics_runs SET status='superseded',completed_at=now() WHERE id IN(SELECT id FROM economics_projection_jobs WHERE tenant_id=?)`, tenant).Error; e != nil {
		return e
	}
	return tx.Where("tenant_id=?", tenant).Delete(&projectionJob{}).Error
}

func inputQuery(tx *gorm.DB, j projectionJob, kind, table string) *gorm.DB {
	return tx.Table("economics_projection_inputs i").Joins("CROSS JOIN LATERAL jsonb_populate_record(NULL::"+table+",i.payload) s").Select("s.*").Where("i.job_id=? AND i.kind=?", j.ID, kind)
}
func projectExpensePage(tx *gorm.DB, j *projectionJob, p Policy, upper ...int64) (bool, error) {
	var page []struct {
		Cost
		Ordinal int64
	}
	query := inputQuery(tx, *j, "cost", "economics_cost_revisions").Select("s.*,i.ordinal").Where("i.ordinal>?", j.Cursor).Order("i.ordinal").Limit(projectionPage)
	if len(upper) > 0 {
		query = query.Where("i.ordinal<=?", upper[0])
	}
	if e := query.Scan(&page).Error; e != nil {
		return false, e
	}

	if len(page) == 0 {
		return true, nil
	}
	// Page-wide immutable evidence is fetched once. Empty evidence must not cost
	// seven remote database round trips per historic receipt.
	var profiles []Profile
	var rules []ContentRule
	var refs []Attribution
	var previous []Summary
	var stored []struct {
		ExpenseKey, RuleVersion string
		Weight                  int64
	}
	refIDs, keys := []string{}, []string{}
	for _, entry := range page {
		if entry.AttributionRef != "" {
			refIDs = append(refIDs, entry.AttributionRef)
		}
		for _, c := range expandInfrastructureDays([]Cost{entry.Cost}) {
			keys = append(keys, c.SourceID+":"+c.LogicalKey)
		}
	}
	if len(refIDs) > 0 {
		if e := inputQuery(tx, *j, "attribution", "economics_attributions").Where("i.scope IN ?", refIDs).Scan(&refs).Error; e != nil {
			return false, e
		}
	}
	attrs := map[string]Attribution{}
	for _, a := range refs {
		attrs[a.ID] = a
	}
	for _, q := range []struct {
		query *gorm.DB
		dest  any
	}{
		{inputQuery(tx, *j, "profile", "economics_profiles").Order("s.effective_from DESC,s.version DESC").Limit(1001), &profiles},
		{inputQuery(tx, *j, "rule", "economics_content_rules").Order("s.effective_from DESC,s.version DESC").Limit(1001), &rules},
		{inputQuery(tx, *j, "previous", "economics_cost_summaries").Where("i.scope IN ?", keys).Limit(projectionEvidenceLimit + 1), &previous},
		{inputQuery(tx, *j, "pool", "economics_pool_weights").Where("i.scope IN ?", keys).Limit(projectionEvidenceLimit + 1), &stored},
	} {
		if e := q.query.Scan(q.dest).Error; e != nil {
			return false, e
		}
	}
	if len(profiles) > 1000 || len(rules) > 1000 {
		return false, fmt.Errorf("allocation versions exceed bounded evidence budget")
	}
	if len(previous) > projectionEvidenceLimit || len(stored) > projectionEvidenceLimit {
		// A large existing pool gets a page of its own instead of expanding heap.
		if len(page) > 1 {
			_, e := projectExpensePage(tx, j, p, page[0].Ordinal)
			return false, e
		}
		return false, fmt.Errorf("one expense exceeds bounded evidence budget")
	}
	prior := map[string][]Summary{}
	for _, r := range previous {
		prior[r.ExpenseKey] = append(prior[r.ExpenseKey], r)
	}
	pools := &poolWeightStore{old: map[poolKey]int64{}, changed: map[poolKey]int64{}}
	for _, r := range stored {
		pools.old[poolKey{r.ExpenseKey, r.RuleVersion}] = r.Weight
	}
	type stagedSummary struct {
		Summary
		JobID string
	}
	staged := make([]stagedSummary, 0, 500)
	flush := func() error {
		if len(staged) == 0 {
			return nil
		}
		if e := tx.Table("economics_projection_summaries").CreateInBatches(staged, 500).Error; e != nil {
			return e
		}
		staged = staged[:0]
		return nil
	}
	for _, entry := range page {
		cost := entry.Cost
		var e error
		cost.Net, e = ParseAmount(string(cost.Net))
		if e != nil {
			return false, fmt.Errorf("snapshot net: %w", e)
		}
		if cost.BeforeBenefits != nil {
			n, e := ParseAmount(string(*cost.BeforeBenefits))
			if e != nil {
				return false, e
			}
			cost.BeforeBenefits = &n
		}
		if a, ok := attrs[cost.AttributionRef]; ok && a.Benefit == "shared_content" {
			cost.ContentID = a.ContentID
		}
		costs := expandInfrastructureDays([]Cost{cost})
		old := []Summary{}
		for _, c := range costs {
			old = append(old, prior[c.SourceID+":"+c.LogicalKey]...)
		}
		var usage []Usage
		var weights []Weight
		if cost.Destination == "infrastructure" {
			if e := inputQuery(tx, *j, "usage", "economics_usage_daily").Where("i.scope>=? AND i.scope<?", cost.PeriodStart.Format("2006-01-02"), cost.PeriodEnd.Format("2006-01-02")).Limit(projectionEvidenceLimit + 1).Scan(&usage).Error; e != nil {
				return false, e
			}
		}
		if cost.ContentID != "" {
			if e := inputQuery(tx, *j, "weight", "economics_content_weights").Where("i.scope=?", cost.ContentID).Limit(projectionEvidenceLimit + 1).Scan(&weights).Error; e != nil {
				return false, e
			}
		}
		if len(usage) > projectionEvidenceLimit || len(weights) > projectionEvidenceLimit {
			return false, fmt.Errorf("one expense exceeds bounded evidence budget")
		}
		out, e := projectExpense(j.TenantID, j.ID, j.StartedAt, p, costs, usage, weights, profiles, rules, attrs, old, pools)
		if e != nil {
			return false, e
		}
		if len(out) > projectionEvidenceLimit {
			return false, fmt.Errorf("one expense exceeds summary budget")
		}
		rows := groupSummaries(out)
		for _, row := range rows {
			staged = append(staged, stagedSummary{row, j.ID})
			if len(staged) == 500 {
				if e := flush(); e != nil {
					return false, e
				}
			}
		}
		j.Cursor = entry.Ordinal
		j.Rows += int64(len(rows))
	}
	if e := flush(); e != nil {
		return false, e
	}
	if e := pools.Save(tx, j.TenantID); e != nil {
		return false, e
	}
	return len(page) < projectionPage, nil
}

func publishProjection(tx *gorm.DB, j projectionJob, p Policy) error {
	// Compare logical groups in SQL so unchanged history is not rewritten.
	match := `s.grain=l.grain AND s.day=l.day AND s.subject=l.subject AND s.resource=l.resource AND s.feature=l.feature AND s.source_id=l.source_id AND s.expense_key=l.expense_key AND s.method=l.method AND s.rule_version=l.rule_version AND s.direct=l.direct AND s.shared=l.shared AND s.before_benefits IS NOT DISTINCT FROM l.before_benefits AND s.reconciled=l.reconciled AND s.expense_revision=l.expense_revision`
	removed := tx.Exec(`DELETE FROM economics_cost_summaries l WHERE l.tenant_id=? AND NOT EXISTS(SELECT 1 FROM economics_projection_summaries s WHERE s.job_id=? AND `+match+`)`, j.TenantID, j.ID)
	if removed.Error != nil {
		return removed.Error
	}
	inserted := tx.Exec(`INSERT INTO economics_cost_summaries SELECT tenant_id,revision,grain,day,subject,resource,feature,direct,shared,before_benefits,reconciled,method,source_id,rule_version,published_at,expense_key,expense_revision FROM economics_projection_summaries s WHERE job_id=? AND NOT EXISTS(SELECT 1 FROM economics_cost_summaries l WHERE l.tenant_id=? AND `+match+`)`, j.ID, j.TenantID)
	if inserted.Error != nil {
		return inserted.Error
	}
	j.Rows = removed.RowsAffected + inserted.RowsAffected

	if e := tx.Exec(`INSERT INTO economics_publications(tenant_id,revision,published_at) VALUES(?,?,?) ON CONFLICT(tenant_id) DO UPDATE SET revision=EXCLUDED.revision,published_at=EXCLUDED.published_at`, j.TenantID, j.ID, j.StartedAt).Error; e != nil {
		return e
	}
	if e := Retention(tx, j.TenantID, time.Now().UTC()); e != nil {
		return e
	}
	if e := updateIssues(tx, j.TenantID, time.Now().UTC(), p.CollectionEnabled); e != nil {
		return e
	}
	if e := tx.Model(&Run{}).Where("id=?", j.ID).Updates(map[string]any{"status": "completed", "completed_at": time.Now().UTC(), "rows": j.Rows, "error": ""}).Error; e != nil {
		return e
	}
	return tx.Where("tenant_id=?", j.TenantID).Delete(&projectionJob{}).Error
}
