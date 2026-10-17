package economics

import (
	"content-management-system/src/utils"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type RequestScope struct {
	DB            *gorm.DB
	Tenant, Actor string
	From, To      time.Time
	Limit, Offset int
}

func requestScope(c *gin.Context) (RequestScope, bool) {
	s := RequestScope{DB: privateDB(c.MustGet("db").(*gorm.DB)), Limit: 50}
	p, ok := utils.GetAdminPrincipal(c)
	if !ok || !p.HasRole("admin") || p.TenantID == "" {
		c.AbortWithStatusJSON(403, gin.H{"error": "Administrator access required"})
		return s, false
	}
	s.Tenant = p.TenantID
	s.Actor = p.UserID
	path := c.Request.URL.Path
	sensitive := strings.Contains(path, "/accounts") || strings.Contains(path, "/costs") || strings.Contains(path, "/revisions") || strings.Contains(path, "/issues")
	if sensitive && !LifecycleReady(s.DB) {
		c.JSON(503, gin.H{"error": "Account reports await IAM lifecycle synchronization"})
		return s, false
	}
	if c.Request.Method != "GET" {
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, MaxEnvelope)
	}
	if !Available(s.DB) {
		c.JSON(503, gin.H{"error": "Apply the canonical Platform Economics migration"})
		return s, false
	}
	now := time.Now().UTC()
	s.To = now.Truncate(24*time.Hour).AddDate(0, 0, 1)
	s.From = s.To.AddDate(0, 0, -30)
	for name, dest := range map[string]*time.Time{"from": &s.From, "to": &s.To} {
		if v := c.Query(name); v != "" {
			at, e := time.Parse("2006-01-02", v)
			if e != nil {
				c.JSON(400, gin.H{"error": "Use UTC dates YYYY-MM-DD"})
				return s, false
			}
			*dest = at
		}
	}
	if !s.To.After(s.From) || s.To.Sub(s.From) > 366*24*time.Hour || s.From.Before(retainedMonth(now)) || s.To.After(now.AddDate(0, 0, 2)) {
		c.JSON(400, gin.H{"error": "Select a period within the retained twelve months"})
		return s, false
	}
	if s.From.Before(now.AddDate(0, 0, -90).Truncate(24 * time.Hour)) {
		// Fine-grained dates have expired. Return complete calendar months,
		// including the boundary month, with the effective period disclosed.
		s.From = time.Date(s.From.Year(), s.From.Month(), 1, 0, 0, 0, 0, time.UTC)
		if s.To.Day() != 1 && s.To.Before(now.AddDate(0, 0, -90)) {
			s.To = time.Date(s.To.Year(), s.To.Month(), 1, 0, 0, 0, 0, time.UTC).AddDate(0, 1, 0)
		}
		c.Set("economics_monthly_period", true)
	}
	if v, e := strconv.Atoi(c.Query("limit")); e == nil && v > 0 {
		if v > 100 {
			v = 100
		}
		s.Limit = v
	}
	if v, e := strconv.Atoi(c.Query("offset")); e == nil && v >= 0 && v <= 100000 {
		s.Offset = v
	}
	if cursor := c.Query("cursor"); cursor != "" {
		b, e := base64.RawURLEncoding.DecodeString(cursor)
		if e != nil {
			c.JSON(400, gin.H{"error": "Invalid cursor"})
			return s, false
		}
		parts := strings.Split(string(b), ":")
		if len(parts) != 2 {
			c.JSON(400, gin.H{"error": "Invalid cursor"})
			return s, false
		}
		n, e := strconv.Atoi(parts[1])
		if e != nil || n < 0 || n > 100000 {
			c.JSON(400, gin.H{"error": "Invalid cursor"})
			return s, false
		}
		var rev string
		if e = s.DB.Table("economics_publications").Where("tenant_id=?", s.Tenant).Pluck("revision", &rev).Error; e != nil {
			c.JSON(503, gin.H{"error": "Publication unavailable"})
			return s, false
		}
		if parts[0] != rev {
			c.JSON(409, gin.H{"error": "Report revised; restart pagination"})
			return s, false
		}
		s.Offset = n
	}
	c.Header("Cache-Control", "private, no-store")
	return s, true
}
func fail(c *gin.Context, e error) bool {
	if e == nil {
		return false
	}
	code := 500
	if errors.Is(e, ErrConflict) {
		code = 409
	}
	if errors.Is(e, ErrDisabled) {
		code = 409
	}
	c.JSON(code, gin.H{"error": "Economics operation could not complete", "reason": e.Error()})
	return true
}
func meta(s RequestScope) gin.H {
	var publication struct {
		Revision    string
		SnapshotAt  *time.Time
		PublishedAt *time.Time
	}
	_ = s.DB.Raw(`SELECT p.revision,p.published_at snapshot_at,r.completed_at published_at FROM economics_publications p LEFT JOIN economics_runs r ON r.id=p.revision AND r.tenant_id=p.tenant_id AND r.status='completed' WHERE p.tenant_id=?`, s.Tenant).Scan(&publication).Error
	return gin.H{"revision": publication.Revision, "as_of": publication.SnapshotAt, "published_at": publication.PublishedAt, "from": s.From, "to": s.To, "coverage": "partial", "currency": "USD", "rule_version": RuleVersion, "scope": "known source subtotal", "environment": EnvironmentName(), "daily_days": 90, "monthly_months": 12, "period_note": "Older history is retained by calendar month; use the effective from/to dates returned here"}
}
func summaryQuery(s RequestScope) *gorm.DB {
	return s.DB.Table("economics_cost_summaries").Where("tenant_id=? AND day>=? AND day<?", s.Tenant, s.From, s.To)
}

type ReportRow struct {
	Key            string  `json:"key"`
	Resource       string  `json:"resource,omitempty"`
	Feature        string  `json:"feature,omitempty"`
	Direct         *Amount `json:"direct"`
	Shared         *Amount `json:"shared"`
	Total          *Amount `json:"total"`
	Reconciled     bool    `json:"reconciled"`
	Methods        string  `json:"methods,omitempty"`
	BeforeBenefits *Amount `json:"before_benefits"`
}

const sumSelect = "CASE WHEN bool_and(method='incomplete_owner_evidence') THEN NULL ELSE sum(direct)::text END direct, CASE WHEN bool_and(method='incomplete_owner_evidence') THEN NULL ELSE sum(shared)::text END shared,CASE WHEN bool_and(method='incomplete_owner_evidence') THEN NULL ELSE sum(direct+shared)::text END total, COALESCE(bool_and(reconciled),false) reconciled, CASE WHEN count(*)=count(before_benefits) THEN sum(before_benefits)::text END before_benefits"

func Overview(c *gin.Context) {
	s, ok := requestScope(c)
	if !ok {
		return
	}
	var total ReportRow
	var resources []ReportRow
	var destinations []ReportRow
	var trend []ReportRow
	if fail(c, summaryQuery(s).Select(sumSelect).Scan(&total).Error) || fail(c, summaryQuery(s).Select("resource key,"+sumSelect).Group("resource").Order("resource").Scan(&resources).Error) || fail(c, summaryQuery(s).Select("CASE WHEN subject LIKE 'account:%' THEN 'accounts' ELSE subject END key,"+sumSelect).Group("CASE WHEN subject LIKE 'account:%' THEN 'accounts' ELSE subject END").Scan(&destinations).Error) || fail(c, summaryQuery(s).Select("day::text key,"+sumSelect).Group("day").Order("day").Scan(&trend).Error) {
		return
	}
	var distribution struct {
		Count  int64   `json:"count"`
		Mean   *string `json:"mean"`
		Median *string `json:"median"`
		P90    *string `json:"p90"`
		P95    *string `json:"p95"`
	}
	if fail(c, s.DB.Raw(`SELECT count(*) count,avg(total)::text mean,percentile_cont(0.5) WITHIN GROUP(ORDER BY total)::text median,CASE WHEN count(*)>=30 THEN percentile_cont(0.9) WITHIN GROUP(ORDER BY total)::text END p90,CASE WHEN count(*)>=30 THEN percentile_cont(0.95) WITHIN GROUP(ORDER BY total)::text END p95 FROM(SELECT subject,sum(direct+shared) total FROM economics_cost_summaries WHERE tenant_id=? AND day>=? AND day<? AND subject LIKE 'account:%' GROUP BY subject HAVING bool_or(method<>'incomplete_owner_evidence'))x`, s.Tenant, s.From, s.To).Scan(&distribution).Error) {
		return
	}
	out := meta(s)
	out["total"] = total
	out["resources"] = nonnil(resources)
	out["destinations"] = nonnil(destinations)
	out["trend"] = nonnil(trend)
	out["distribution"] = distribution
	out["distribution_note"] = "Observed accounts with supported costs; partial coverage is not complete active-user economics"
	c.JSON(200, out)
}
func nonnil[T any](rows []T) []T {
	if rows == nil {
		return []T{}
	}
	return rows
}

type AccountRow struct {
	Driver     *string `json:"driver"`
	AccountID  string  `json:"account_id"`
	Direct     *Amount `json:"direct"`
	Shared     *Amount `json:"shared"`
	Total      *Amount `json:"total"`
	Requests   int64   `json:"requests"`
	Errors     int64   `json:"errors"`
	Reconciled bool    `json:"reconciled"`
}

func Accounts(c *gin.Context) {
	s, ok := requestScope(c)
	if !ok {
		return
	}
	search := c.Query("q")
	if len(search) > 160 {
		c.JSON(400, gin.H{"error": "Search is too long"})
		return
	}
	order := "total DESC,subject"
	switch c.Query("sort") {
	case "requests":
		order = "requests DESC,subject"
	case "account":
		order = "subject"
	case "", "cost":
	default:
		c.JSON(400, gin.H{"error": "Unsupported sort"})
		return
	}
	base := `WITH cost_resources AS(SELECT subject,resource,sum(direct) direct,sum(shared) shared,bool_and(reconciled) reconciled,bool_or(method<>'incomplete_owner_evidence') known FROM economics_cost_summaries WHERE tenant_id=? AND day>=? AND day<? GROUP BY subject,resource), costs AS(SELECT subject,sum(direct) direct,sum(shared) shared,bool_and(reconciled) reconciled,bool_or(known) known,(array_agg(resource ORDER BY direct+shared DESC,resource))[1] driver FROM cost_resources GROUP BY subject), usage AS(SELECT subject,sum(CASE WHEN unit='requests' THEN value ELSE 0 END) requests,sum(CASE WHEN unit='errors' THEN value ELSE 0 END) errors FROM(SELECT * FROM economics_usage_daily UNION ALL SELECT * FROM economics_usage_monthly)u WHERE tenant_id=? AND day>=? AND day<? GROUP BY subject), all_accounts AS(SELECT COALESCE(c.subject,u.subject) subject,COALESCE(c.direct,0) direct,COALESCE(c.shared,0) shared,COALESCE(c.direct,0)+COALESCE(c.shared,0) total,COALESCE(u.requests,0) requests,COALESCE(u.errors,0) errors,COALESCE(c.reconciled,false) reconciled,c.subject IS NOT NULL AND c.known cost_known,CASE WHEN c.known THEN c.driver END driver FROM costs c FULL OUTER JOIN usage u USING(subject)) `
	args := []any{s.Tenant, s.From, s.To, s.Tenant, s.From, s.To}
	where := ` WHERE subject LIKE 'account:%' AND subject LIKE ?`
	args = append(args, "%"+search+"%")
	if raw := c.Query("account_ids"); raw != "" {
		ids := strings.Split(raw, ",")
		if len(ids) > 100 {
			c.JSON(400, gin.H{"error": "At most 100 account IDs"})
			return
		}
		subjects := []string{}
		for _, id := range ids {
			parsed, e := uuid.Parse(id)
			if e != nil {
				c.JSON(400, gin.H{"error": "Invalid account filter"})
				return
			}
			subjects = append(subjects, "account:"+parsed.String())
		}
		where += " AND subject IN ?"
		args = append(args, subjects)
	}
	var count int64
	if fail(c, s.DB.Raw(base+"SELECT count(*) FROM all_accounts"+where, args...).Scan(&count).Error) {
		return
	}
	var rows []AccountRow
	query := base + `SELECT substring(subject from 9) account_id,driver,CASE WHEN cost_known THEN direct::text END direct,CASE WHEN cost_known THEN shared::text END shared,CASE WHEN cost_known THEN total::text END total,requests,errors,reconciled FROM all_accounts` + where + " ORDER BY " + order + " LIMIT ? OFFSET ?"
	if fail(c, s.DB.Raw(query, append(args, s.Limit, s.Offset)...).Scan(&rows).Error) {
		return
	}
	out := meta(s)
	out["accounts"] = nonnil(rows)
	out["total"] = count
	out["offset"] = s.Offset
	out["next_cursor"] = nil
	if int64(s.Offset+len(rows)) < count {
		out["next_cursor"] = base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprint(out["revision"]) + ":" + strconv.Itoa(s.Offset+len(rows))))
	}
	c.JSON(200, out)
}
func Account(c *gin.Context) {
	s, ok := requestScope(c)
	if !ok {
		return
	}
	id, e := uuid.Parse(c.Param("id"))
	if e != nil {
		c.JSON(400, gin.H{"error": "Invalid account ID"})
		return
	}
	subject := "account:" + id.String()
	resolved, _, err := ResolveSubject(s.DB, subject, time.Now())
	if err != nil {
		fail(c, err)
		return
	}
	if resolved == "removed" {
		c.JSON(404, gin.H{"error": "Account economics unavailable"})
		return
	}
	var total ReportRow
	var resources, activities, trend []ReportRow
	var units []Usage
	q := func() *gorm.DB { return summaryQuery(s).Where("subject=?", subject) }
	if fail(c, q().Select(sumSelect).Scan(&total).Error) || fail(c, q().Select("resource key,"+sumSelect+",string_agg(DISTINCT method,', ') methods").Group("resource").Scan(&resources).Error) || fail(c, q().Select("feature key,"+sumSelect).Group("feature").Scan(&activities).Error) || fail(c, q().Select("day::text key,"+sumSelect).Group("day").Order("day").Scan(&trend).Error) || fail(c, s.DB.Raw(`SELECT operation,unit,variant,sum(value) value FROM(SELECT * FROM economics_usage_daily UNION ALL SELECT * FROM economics_usage_monthly)u WHERE tenant_id=? AND subject=? AND day>=? AND day<? GROUP BY operation,unit,variant`, s.Tenant, subject, s.From, s.To).Scan(&units).Error) {
		return
	}
	for _, r := range Resources {
		found := false
		for _, row := range resources {
			if row.Key == r {
				found = true
			}
		}
		if !found {
			resources = append(resources, ReportRow{Key: r})
		}
	}
	out := meta(s)
	out["account_id"] = id.String()
	out["total"] = total
	out["resources"] = nonnil(resources)
	out["activities"] = nonnil(activities)
	out["trend"] = nonnil(trend)
	out["usage"] = nonnil(units)
	out["comparisons"] = efficiency(total, units)
	metrics, e := AccountEfficiency(s.DB, s.Tenant, subject, s.From, s.To)
	if fail(c, e) {
		return
	}
	out["efficiency_metrics"] = metrics
	var requested []struct {
		Provider   string    `json:"provider"`
		Resource   string    `json:"resource"`
		Feature    string    `json:"feature"`
		ContentID  string    `json:"content_id"`
		Amount     *Amount   `json:"amount"`
		Benefit    string    `json:"benefit"`
		OccurredAt time.Time `json:"occurred_at"`
	}
	if fail(c, s.DB.Raw(`SELECT c.provider,c.resource,a.feature,a.content_id,CASE WHEN c.method='incomplete_owner_evidence' OR c.provider='local' THEN NULL ELSE c.net::text END amount,a.benefit,c.period_start occurred_at FROM economics_cost_revisions c JOIN economics_attributions a ON a.id=c.attribution_ref AND a.tenant_id=c.tenant_id WHERE c.tenant_id=? AND a.subject=? AND c.period_start>=? AND c.period_start<? AND c.source_id='ai-owner' ORDER BY c.period_start DESC,c.id LIMIT 50`, s.Tenant, subject, s.From, s.To).Scan(&requested).Error) {
		return
	}
	out["requested_work"] = nonnil(requested)
	out["note"] = "Shared cost estimates are allocations, not savings from removing this account"
	c.JSON(200, out)
}
func efficiency(total ReportRow, units []Usage) gin.H {
	playback, opens := int64(0), int64(0)
	for _, u := range units {
		if u.Unit == "media_seconds" {
			playback += u.Value
		}
		if u.Unit == "news_opens" {
			opens += u.Value
		}
	}
	return gin.H{"observed_media_seconds": playback, "distinct_story_opens": opens, "cost_per_hour": nil, "cost_per_story": nil, "reason": "Requires matching feature-cost coverage and sufficient comparable observations"}
}
func Breakdown(c *gin.Context) {
	s, ok := requestScope(c)
	if !ok {
		return
	}
	dimension := c.DefaultQuery("dimension", "feature")
	if dimension == "content" {
		ContentReport(c, s)
		return
	}
	column := "feature"
	if dimension == "resource" {
		column = "resource"
	} else if dimension != "feature" {
		c.JSON(400, gin.H{"error": "Use feature or resource; content detail requires retained eligible evidence"})
		return
	}
	var rows []ReportRow
	if fail(c, summaryQuery(s).Select(column+" key,"+sumSelect).Group(column).Order(column).Limit(s.Limit).Offset(s.Offset).Scan(&rows).Error) {
		return
	}
	out := meta(s)
	out["rows"] = nonnil(rows)
	c.JSON(200, out)
}
func PolicyRead(c *gin.Context) {
	s, ok := requestScope(c)
	if !ok {
		return
	}
	p := Policy{TenantID: s.Tenant}
	e := s.DB.Where("tenant_id=?", s.Tenant).Take(&p).Error
	if e != nil && !errors.Is(e, gorm.ErrRecordNotFound) {
		fail(c, e)
		return
	}
	c.JSON(200, p)
}
func PolicyWrite(c *gin.Context) {
	s, ok := requestScope(c)
	if !ok {
		return
	}
	var p Policy
	if c.ShouldBindJSON(&p) != nil {
		c.JSON(400, gin.H{"error": "Invalid policy"})
		return
	}
	var old Policy
	if e := s.DB.Where("tenant_id=?", s.Tenant).Take(&old).Error; e != nil && !errors.Is(e, gorm.ErrRecordNotFound) {
		fail(c, e)
		return
	}
	p.CollectionSince = old.CollectionSince
	if p.CollectionEnabled && !old.CollectionEnabled {
		now := time.Now().UTC()
		p.CollectionSince = &now
	}
	p.TenantID = s.Tenant
	p.UpdatedBy = s.Actor
	p.UpdatedAt = time.Now().UTC()
	if fail(c, s.DB.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "tenant_id"}}, DoUpdates: clause.AssignmentColumns([]string{"collection_enabled", "collection_since", "reports_enabled", "imports_enabled", "allocation_enabled", "updated_by", "updated_at"})}).Create(&p).Error) {
		return
	}
	var ps []Policy
	if s.DB.Limit(100).Find(&ps).Error == nil {
		DefaultCollector.SetPolicies(ps)
	}
	c.JSON(200, p)
}
func Costs(c *gin.Context) {
	s, ok := requestScope(c)
	if !ok {
		return
	}
	var rows []Cost
	q := s.DB.Where("tenant_id=? AND period_start<? AND period_end>?", s.Tenant, s.To, s.From)
	if key := c.Query("logical_key"); key != "" {
		q = q.Where("logical_key=?", key)
	}
	if fail(c, q.Order("created_at DESC,id").Limit(s.Limit).Offset(s.Offset).Find(&rows).Error) {
		return
	}
	out := meta(s)
	out["costs"] = nonnil(rows)
	c.JSON(200, out)
}
func CostWrite(c *gin.Context) {
	s, ok := requestScope(c)
	if !ok {
		return
	}
	var v Cost
	if c.ShouldBindJSON(&v) != nil {
		c.JSON(400, gin.H{"error": "Invalid cost evidence"})
		return
	}
	v.TenantID = s.Tenant
	v.Actor = s.Actor
	if v.SourceID == "ai-owner" || v.SourceID == "ai-invoice" || v.SourceID == "ai-legacy" || v.Resource == "ai" {
		c.JSON(400, gin.H{"error": "Owner receipt source is read-only"})
		return
	}
	if v.Destination != "infrastructure" && v.Destination != "overhead" && v.Destination != "unallocated" {
		c.JSON(400, gin.H{"error": "Manual bills support infrastructure, overhead or unallocated pools; account and content attribution comes from verified owners"})
		return
	}
	v.Subject, v.ContentID = "", ""
	var source Source
	if e := s.DB.Where("id=? AND tenant_id=? AND provider=? AND resource=?", v.SourceID, s.Tenant, v.Provider, v.Resource).Take(&source).Error; e != nil {
		c.JSON(400, gin.H{"error": "Cost source must match the tenant, provider and resource"})
		return
	}
	if e := ValidateCost(&v); e != nil {
		c.JSON(400, gin.H{"error": e.Error()})
		return
	}
	row, e := AppendCost(s.DB, v)
	if fail(c, e) {
		return
	}
	c.JSON(201, row)
}
func Sources(c *gin.Context) {
	s, ok := requestScope(c)
	if !ok {
		return
	}
	var rows []Source
	if fail(c, s.DB.Where("tenant_id=?", s.Tenant).Order("provider,resource").Limit(100).Find(&rows).Error) {
		return
	}
	out := meta(s)
	out["sources"] = nonnil(rows)
	ids := make([]string, 0, len(rows))
	for _, source := range rows {
		ids = append(ids, source.ID)
	}
	evidence, e := sourceEvidence(s.DB, s.Tenant, ids)
	if fail(c, e) {
		return
	}
	out["usage_evidence"] = evidence
	c.JSON(200, out)
}
func SourceWrite(c *gin.Context) {
	s, ok := requestScope(c)
	if !ok {
		return
	}
	var v Source
	if c.ShouldBindJSON(&v) != nil || !ValidResource(v.Resource) || len(v.Provider) > 64 || v.Provider == "" || len(v.Scope) > 160 || v.Scope == "" || v.Currency != "USD" {
		c.JSON(400, gin.H{"error": "Provider, resource, scope and USD currency required"})
		return
	}
	v.ID = uuid.NewSHA1(uuid.NameSpaceOID, []byte(s.Tenant+":"+v.Provider+":"+v.Resource+":"+v.Scope)).String()
	v.TenantID = s.Tenant
	v.Capability = "manual"
	if strings.EqualFold(v.Provider, "neon") && len(v.Config) > 0 && string(v.Config) != "{}" {
		var cfg NeonConfig
		if json.Unmarshal(v.Config, &cfg) != nil || !neonID.MatchString(cfg.OrgID) || !neonID.MatchString(cfg.ProjectID) {
			c.JSON(400, gin.H{"error": "Explicit Neon organization and project IDs required"})
			return
		}
		v.Config, _ = json.Marshal(cfg)
		v.Capability = "neon_consumption"
	} else {
		v.Config = json.RawMessage(`{}`)
	}
	v.Status = "unverified"
	v.UpdatedAt = time.Now().UTC()
	if fail(c, UpsertSource(s.DB, v)) {
		return
	}
	c.JSON(201, v)
}
func Profiles(c *gin.Context) {
	s, ok := requestScope(c)
	if !ok {
		return
	}
	var rows []Profile
	if fail(c, s.DB.Where("tenant_id=?", s.Tenant).Order("resource,operation,effective_from DESC").Limit(1000).Find(&rows).Error) {
		return
	}
	var rules []ContentRule
	if fail(c, s.DB.Where("tenant_id=?", s.Tenant).Order("kind,effective_from DESC,version DESC").Limit(1000).Find(&rules).Error) {
		return
	}
	c.JSON(200, gin.H{"profiles": nonnil(rows), "content_rules": nonnil(rules)})
}
func ProfileWrite(c *gin.Context) {
	s, ok := requestScope(c)
	if !ok {
		return
	}
	var p Profile
	if c.ShouldBindJSON(&p) != nil || !ValidResource(p.Resource) || Operations[p.Operation] == "" || p.Weight < 0 || p.Weight > 1000000 || len(p.Evidence) < 10 || len(p.Evidence) > 4000 || p.EffectiveFrom.Before(time.Now().UTC().Truncate(24*time.Hour)) {
		c.JSON(400, gin.H{"error": "Use a registered operation, measured evidence and a prospective effective date"})
		return
	}
	p.TenantID = s.Tenant
	p.Actor = s.Actor
	e := s.DB.Transaction(func(tx *gorm.DB) error {
		if e := Lock(tx); e != nil {
			return e
		}
		var version int
		if e := tx.Model(&Profile{}).Where("tenant_id=? AND resource=? AND operation=?", s.Tenant, p.Resource, p.Operation).Select("COALESCE(max(version),0)").Scan(&version).Error; e != nil {
			return e
		}
		p.Version = version + 1
		return tx.Create(&p).Error
	})
	if fail(c, e) {
		return
	}
	c.JSON(201, p)
}
func Issues(c *gin.Context) {
	s, ok := requestScope(c)
	if !ok {
		return
	}
	var rows []Issue
	if fail(c, s.DB.Where("tenant_id=? AND status<>'candidate'", s.Tenant).Order("last_seen_at DESC").Limit(s.Limit).Offset(s.Offset).Find(&rows).Error) {
		return
	}
	c.JSON(200, gin.H{"issues": nonnil(rows)})
}
func IssueClose(c *gin.Context) {
	s, ok := requestScope(c)
	if !ok {
		return
	}
	var body struct {
		Reason string `json:"reason"`
	}
	if c.ShouldBindJSON(&body) != nil || len(body.Reason) < 3 || len(body.Reason) > 2000 {
		c.JSON(400, gin.H{"error": "A reason is required"})
		return
	}
	r := s.DB.Model(&Issue{}).Where("tenant_id=? AND id=?", s.Tenant, c.Param("id")).Updates(map[string]any{"status": "resolved", "close_reason": body.Reason})
	if fail(c, r.Error) {
		return
	}
	if r.RowsAffected == 0 {
		c.JSON(404, gin.H{"error": "Issue not found"})
		return
	}
	c.JSON(200, gin.H{"resolved": true})
}
func Runs(c *gin.Context) {
	s, ok := requestScope(c)
	if !ok {
		return
	}
	var rows []Run
	if fail(c, s.DB.Where("tenant_id=?", s.Tenant).Order("started_at DESC").Limit(s.Limit).Offset(s.Offset).Find(&rows).Error) {
		return
	}
	c.JSON(200, gin.H{"runs": nonnil(rows)})
}
func RunNow(c *gin.Context) {
	s, ok := requestScope(c)
	if !ok {
		return
	}
	if e := Rebuild(s.DB, s.Tenant); e == ErrProjectionPending {
		c.JSON(202, gin.H{"published": false, "status": "building"})
		return
	} else if fail(c, e) {
		return
	}
	c.JSON(200, gin.H{"published": true})
}
func Coverage(c *gin.Context) {
	s, ok := requestScope(c)
	if !ok {
		return
	}
	var states []struct {
		Source     string     `json:"source"`
		LastSeenAt *time.Time `json:"last_seen_at"`
		Lost       int64      `json:"known_lost"`
		OpenEpochs int64      `json:"open_epochs"`
	}
	if fail(c, s.DB.Raw(`SELECT source,max(last_seen_at) last_seen_at,sum(lost) lost,count(*) FILTER(WHERE NOT closed) open_epochs FROM economics_collection_state WHERE tenant_id=? GROUP BY source`, s.Tenant).Scan(&states).Error) {
		return
	}
	out := meta(s)
	out["collectors"] = nonnil(states)
	out["lifecycle_ready"] = LifecycleReady(s.DB)
	var lifecycle struct {
		SyncedAt *time.Time `json:"synced_at"`
	}
	_ = s.DB.Table("economics_lifecycle_sync").Where("singleton=true").Take(&lifecycle).Error
	out["lifecycle_synced_at"] = lifecycle.SyncedAt
	out["gaps"] = []string{"Historical receipts without consumer provenance remain unallocated", "Delivery transfer requires provider evidence; playback does not measure bytes", "Database/application dollars require calibrated profiles and a covered cost pool", "RUX identities are independent and are never joined to accounts", "Mobile shared-player evidence includes replay and handoffs; suspended-runtime gaps and physical-device coverage remain unqualified", "Web partial/global playback is observed; keepalive is best effort and browser/native coverage comparability remains unqualified", "Cross-month cumulative playback is counted once; affected content pools are withheld because monthly baselines are uncertain", "Public News counts opens, not distinct people; its unsupported preparation share stays unallocated", "Shared local preparation/FFmpeg work is reported as usage; its unsupported hosting cost is not assigned to the uploader as serving expense"}
	c.JSON(200, out)
}
func RegisterAdmin(admin *gin.RouterGroup) {
	g := admin.Group("/economics", utils.RequireAdminRole("admin"), readSnapshot())
	g.GET("/overview", Overview)
	g.GET("/accounts", Accounts)
	g.GET("/accounts/:id", Account)
	g.GET("/breakdown", Breakdown)
	g.GET("/policy", PolicyRead)
	g.PUT("/policy", PolicyWrite)
	g.GET("/costs", Costs)
	g.POST("/costs", CostWrite)
	g.GET("/revisions", Costs)
	g.GET("/sources", Sources)
	g.POST("/sources", SourceWrite)
	g.POST("/sources/:id/import", SourceImport)
	g.GET("/profiles", Profiles)
	g.POST("/profiles", ProfileWrite)
	g.POST("/content-rules", ContentRuleWrite)
	g.GET("/issues", Issues)
	g.POST("/issues/:id/close", IssueClose)
	g.GET("/runs", Runs)
	g.POST("/run", RunNow)
	g.GET("/coverage", Coverage)
}
func InternalUsage(c *gin.Context) {
	source, ok := c.Get(utils.InternalPrincipalContextKey)
	if !ok {
		c.JSON(http.StatusForbidden, gin.H{"error": "Machine principal required"})
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, MaxEnvelope)
	var b Batch
	if c.ShouldBindJSON(&b) != nil {
		c.JSON(400, gin.H{"error": "Invalid bounded usage envelope"})
		return
	}
	if b.TenantID != PublicTenant() {
		c.JSON(403, gin.H{"error": "Source tenant is outside this deployment"})
		return
	}
	if fail(c, Ingest(c.MustGet("db").(*gorm.DB), fmt.Sprint(source), b)) {
		return
	}
	c.JSON(202, gin.H{"accepted": true, "sequence": b.Sequence})
}

func InternalCollection(c *gin.Context) {
	tenant := PublicTenant()
	var p Policy
	db := c.MustGet("db").(*gorm.DB)
	if tenant == "" || !Available(db) {
		c.JSON(200, gin.H{"tenant_id": tenant, "environment": EnvironmentName(), "collection_enabled": false, "storage_window_version": 0})
		return
	}
	if e := db.Where("tenant_id=?", tenant).Take(&p).Error; e != nil && !errors.Is(e, gorm.ErrRecordNotFound) {
		c.JSON(503, gin.H{"error": "Collection policy unavailable"})
		return
	}
	out := gin.H{"tenant_id": tenant, "environment": EnvironmentName(), "collection_enabled": p.CollectionEnabled, "schema_version": 1, "storage_window_version": 1, "max_rows": MaxRows, "max_bytes": MaxEnvelope}
	if scope := c.Query("storage_scope"); scope != "" {
		if len(scope) > 256 {
			c.JSON(400, gin.H{"error": "Storage scope too long"})
			return
		}
		to := time.Now().UTC().Truncate(time.Hour).Add(-2 * time.Hour)
		var hours []time.Time
		if e := db.Raw(`SELECT hour FROM generate_series(?::timestamptz,?::timestamptz,interval '1 hour') hour WHERE NOT EXISTS(SELECT 1 FROM economics_import_windows w WHERE w.tenant_id=? AND w.source='cloudflare' AND w.window_key=?||':'||to_char(hour AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS"Z"')) ORDER BY hour LIMIT 4`, to.Add(-47*time.Hour), to, tenant, scope).Scan(&hours).Error; e != nil {
			fail(c, e)
			return
		}
		out["storage_missing_hours"] = nonnil(hours)
	}
	c.JSON(200, out)
}
func SourceImport(c *gin.Context) {
	s, ok := requestScope(c)
	if !ok {
		return
	}
	var policy Policy
	if e := s.DB.Where("tenant_id=?", s.Tenant).Take(&policy).Error; e != nil || !policy.ImportsEnabled {
		c.JSON(409, gin.H{"error": "Imports are paused. Enable imports in Diagnostics before fetching provider evidence"})
		return
	}
	var source Source
	if e := s.DB.Where("tenant_id=? AND id=?", s.Tenant, c.Param("id")).Take(&source).Error; e != nil {
		c.JSON(404, gin.H{"error": "Source not found"})
		return
	}
	if source.Capability != "neon_consumption" {
		c.JSON(400, gin.H{"error": "No verified automatic API for this source; use manual evidence"})
		return
	}
	if fail(c, ImportNeon(s.DB, source)) {
		return
	}
	c.JSON(200, gin.H{"imported": true, "valuation": "usage_only"})
}

// Every multi-query GET reads one publication snapshot, even if a background run commits mid-request.
func readSnapshot() gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Request.Method != "GET" {
			c.Next()
			return
		}
		db := c.MustGet("db").(*gorm.DB)
		tx := db.Begin(&sql.TxOptions{Isolation: sql.LevelReadCommitted, ReadOnly: true})
		if tx.Error != nil {
			c.AbortWithStatusJSON(503, gin.H{"error": "Report snapshot unavailable"})
			return
		}
		defer tx.Rollback()
		if e := tx.Exec("SET LOCAL statement_timeout='5s'").Error; e != nil {
			c.AbortWithStatusJSON(503, gin.H{"error": "Report snapshot unavailable"})
			return
		}
		// Acquire before any report read. READ COMMITTED refreshes the snapshot
		// after a waited erasure; the shared lock then prevents mixed publications
		// or a lifecycle acknowledgement overtaking an in-flight account response.
		if e := tx.Exec("SELECT pg_advisory_xact_lock_shared(731903186)").Error; e != nil {
			c.AbortWithStatusJSON(503, gin.H{"error": "Report snapshot unavailable"})
			return
		}
		c.Set("db", tx)
		defer c.Set("db", db)
		c.Next()
	}
}
