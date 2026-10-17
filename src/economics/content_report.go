package economics

import (
	"github.com/gin-gonic/gin"
)

// ContentReport exposes preparation pools, not a list of who consumed content.
func ContentReport(c *gin.Context, s RequestScope) {
	var rows []struct {
		Key              string  `json:"key"`
		Period           *Amount `json:"period"`
		LifetimeEstimate *Amount `json:"lifetime_estimate"`
		Consumption      *string `json:"consumption"`
		Kind             string  `json:"kind"`
	}
	q := `WITH latest AS(SELECT DISTINCT ON(source_id,logical_key) * FROM economics_cost_revisions WHERE tenant_id=? ORDER BY source_id,logical_key,revision DESC),costs AS(SELECT COALESCE(NULLIF(c.content_id,''),NULLIF(a.content_id,'')) content,c.*,COALESCE(a.feature,c.feature) content_feature FROM latest c LEFT JOIN economics_attributions a ON a.id=c.attribution_ref AND a.tenant_id=c.tenant_id WHERE c.destination='content' OR a.benefit='shared_content'),period_totals AS(SELECT expense_key,sum(direct+shared) amount FROM economics_cost_summaries WHERE tenant_id=? AND day>=? AND day<? GROUP BY expense_key) SELECT c.content key,sum(p.amount)::text period,sum(c.net)::text lifetime_estimate,CASE WHEN min(c.content_feature)=max(c.content_feature) THEN min(c.content_feature) ELSE 'mixed' END kind FROM costs c LEFT JOIN period_totals p ON p.expense_key=c.source_id||':'||c.logical_key WHERE c.content IS NOT NULL GROUP BY c.content ORDER BY sum(p.amount) DESC NULLS LAST,c.content LIMIT ? OFFSET ?`
	if fail(c, s.DB.Raw(q, s.Tenant, s.Tenant, s.From, s.To, s.Limit, s.Offset).Scan(&rows).Error) {
		return
	}
	if len(rows) > 0 {
		ids := []string{}
		for _, r := range rows {
			ids = append(ids, r.Key)
		}
		var consumption []struct {
			ContentID string
			Total     string
		}
		if fail(c, s.DB.Raw(`SELECT content_id,sum(value)::text total FROM economics_content_weights WHERE tenant_id=? AND content_id IN ? AND month>=date_trunc('month',?::timestamptz) AND month<? GROUP BY content_id`, s.Tenant, ids, s.From, s.To).Scan(&consumption).Error) {
			return
		}
		values := map[string]string{}
		for _, r := range consumption {
			values[r.ContentID] = r.Total
		}
		for i, r := range rows {
			if v, ok := values[r.Key]; ok {
				value := v
				rows[i].Consumption = &value
			}
		}
	}
	out := meta(s)
	out["content"] = nonnil(rows)
	out["note"] = "Period costs use the selected published basis. Lifetime figures are retained owner-priced preparation estimates, before invoice corrections; never add lifetime figures to period expense. Consumption is a coarse observation, not transfer or attention."
	c.JSON(200, out)
}
