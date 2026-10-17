package economics

import "gorm.io/gorm"

// Normalize one invoice in PostgreSQL using integer microdollars and a stable
// remainder order. Signed adjustments conserve the invoice exactly. No receipt
// population is loaded into the application to reconcile a provider month.
func reconcileProjectionPage(tx *gorm.DB, j *projectionJob) (bool, error) {
	var bills []int64
	if e := tx.Table("economics_projection_inputs").Where("job_id=? AND kind='cost' AND payload->>'destination'='reconciliation' AND ordinal>?", j.ID, j.Cursor).Order("ordinal").Limit(5).Pluck("ordinal", &bills).Error; e != nil {
		return false, e
	}
	for _, id := range bills {
		if e := tx.Exec(`WITH bill AS (
   SELECT payload p FROM economics_projection_inputs WHERE ordinal=? AND job_id=?
  ), owners AS (
   SELECT i.ordinal,i.payload,(i.payload->>'net')::numeric n
   FROM economics_projection_inputs i,bill b
   WHERE i.job_id=? AND i.kind='cost' AND i.payload->>'source_id' IN('ai-owner','ai-legacy')
    AND i.payload->>'provider'=b.p->>'provider' AND i.payload->>'resource'=b.p->>'resource'
    AND (i.payload->>'period_start')::timestamptz >= (b.p->>'period_start')::timestamptz
    AND (i.payload->>'period_end')::timestamptz <= (b.p->>'period_end')::timestamptz
  ), totals AS (
   SELECT coalesce(sum(n),0) subtotal,coalesce(bool_or(payload->>'method'='incomplete_owner_evidence'),false) incomplete FROM owners
  ), parts AS (
   SELECT o.*,trunc(((b.p->>'net')::numeric*1000000000000*o.n)::numeric(100,40)/nullif(t.subtotal,0),0) micros,
     row_number() OVER(ORDER BY o.ordinal) rank
   FROM owners o,bill b,totals t
  ), remainders AS (
   SELECT p.*, (b.p->>'net')::numeric*1000000000000-sum(micros) OVER() remainder FROM parts p,bill b
  ), updated AS (
   UPDATE economics_projection_inputs i SET payload=jsonb_set(jsonb_set(i.payload,'{net}',to_jsonb(
     (r.micros+CASE WHEN r.rank<=abs(r.remainder) THEN sign(r.remainder) ELSE 0 END)/1000000000000
   )),'{reconciled}',b.p->'reconciled')
   FROM remainders r,bill b,totals t
   WHERE i.ordinal=r.ordinal AND NOT t.incomplete AND t.subtotal<>0 RETURNING i.ordinal
  )
  UPDATE economics_projection_inputs i SET payload=
   jsonb_set(jsonb_set(jsonb_set(jsonb_set(i.payload,'{net}',to_jsonb(
    CASE WHEN t.incomplete THEN (b.p->>'net')::numeric-t.subtotal
         WHEN t.subtotal<>0 THEN 0 ELSE (b.p->>'net')::numeric END
   )),'{destination}','"unallocated"'),'{method}','"invoice_residual"'),'{before_benefits}',
    CASE WHEN (NOT t.incomplete AND t.subtotal<>0) OR (b.p->>'net')::numeric=CASE WHEN t.incomplete THEN t.subtotal ELSE 0 END THEN '0'::jsonb ELSE coalesce(b.p->'before_benefits','null'::jsonb) END)
  FROM bill b,totals t WHERE i.ordinal=?`, id, j.ID, j.ID, id).Error; e != nil {
			return false, e
		}
		j.Cursor = id
	}
	return len(bills) < 5, nil
}
