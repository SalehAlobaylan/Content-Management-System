package economics

import (
	"content-management-system/src/models"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"strings"
	"time"
)

// ProjectAI reads authoritative events. Its anti-join does not skip late commits.
func ProjectAI(tx *gorm.DB, tenant string) error {
	var events []struct {
		models.AISpendEvent
		AttributionRef string
		ServiceAt      *time.Time
	}
	var exists bool
	if e := tx.Raw("SELECT to_regclass('ai_spend_events') IS NOT NULL").Scan(&exists).Error; e != nil || !exists {
		return e
	}
	e := tx.Raw(`SELECT e.*,COALESCE(l.attribution_ref,'') attribution_ref,j.completed_at service_at FROM ai_spend_events e LEFT JOIN transcription_jobs j ON e.spend_class='stt_api' AND e.source_service='cms_stt_owner' AND j.public_id::text=e.system_run_id AND j.tenant_id=e.tenant_id LEFT JOIN economics_receipt_links l ON l.event_id=e.event_id WHERE e.tenant_id=? AND NOT e.cached AND (e.cost_usd<>0 OR e.spend_class IN('llm','stt_api')) AND NOT EXISTS(SELECT 1 FROM economics_cost_revisions c WHERE c.tenant_id=e.tenant_id AND c.source_id='ai-owner' AND c.logical_key='ai:'||e.event_id::text) ORDER BY e.id LIMIT 1000`, tenant).Scan(&events).Error
	if e != nil {
		return e
	}
	rows := make([]Cost, 0, len(events))
	for _, event := range events {
		if event.CostUSD != event.CostUSD {
			return fmt.Errorf("nonfinite legacy cost")
		}
		amount, e := ParseAmount(fmt.Sprintf("%.12f", event.CostUSD))
		if e != nil {
			return e
		}
		at := event.OccurredAt.UTC()
		day := at.Truncate(24 * time.Hour)
		if event.ServiceAt != nil {
			day = event.ServiceAt.UTC().Truncate(24 * time.Hour)
		}
		posted := event.CreatedAt.UTC()
		c := Cost{OccurredAt: &at, PostedAt: &posted, ID: uuid.NewString(), TenantID: tenant, SourceID: "ai-owner", LogicalKey: "ai:" + event.EventID.String(), Revision: 1, Provider: event.Provider, Resource: "ai", Feature: event.Operation, PeriodStart: day, PeriodEnd: day.AddDate(0, 0, 1), Net: amount, BeforeBenefits: &amount, Currency: "USD", Destination: "unallocated", AttributionRef: event.AttributionRef, Method: "owner_receipt_estimate", Reason: "Canonical AI receipt; legacy float normalized to 12 decimal places", Actor: "cms-projection", CreatedAt: time.Now().UTC()}
		if event.Unpriced || event.UsageUnavailable {
			c.Method = "incomplete_owner_evidence"
			c.BeforeBenefits = nil
		}
		if event.Cached {
			c.Net = Zero
			zero := Zero
			c.BeforeBenefits = &zero
		}
		rows = append(rows, c)
	}
	if len(rows) == 0 {
		return nil
	}
	return tx.CreateInBatches(rows, 500).Error
}

// projectExpense uses only the evidence needed by one original expense.
func projectExpense(tenant, rev string, now time.Time, p Policy, costs []Cost, usage []Usage, weights []Weight, profiles []Profile, contentRules []ContentRule, attributions map[string]Attribution, previous []Summary, pools *poolWeightStore) ([]Summary, error) {
	dailyCutoff := now.AddDate(0, 0, -90).Truncate(24 * time.Hour)
	usageDays := map[int64][]Usage{}
	for _, row := range usage {
		usageDays[row.Day.Unix()] = append(usageDays[row.Day.Unix()], row)
	}
	contentMonths := indexContentWeights(weights)
	prior := map[string][]Summary{}
	for _, row := range previous {
		prior[row.ExpenseKey] = append(prior[row.ExpenseKey], row)
	}
	out := []Summary{}
	for _, c := range costs {
		expenseKey := c.SourceID + ":" + c.LogicalKey
		if a, ok := attributions[c.AttributionRef]; ok {
			// Registration and lifecycle changes serialize under this same lock.
			// Their removable links already reflect erasure/clear barriers.
			subject := a.Subject
			cleared := subject == "cleared" || subject == "expired" || subject == "removed"
			c.Subject = subject
			if a.Benefit == "private" && !cleared {
				c.Destination = "direct"
			}
			if a.Benefit == "shared_content" && !cleared && a.ContentID != "" {
				c.Destination, c.ContentID, c.Metric = "content", a.ContentID, ""
				if a.Feature == "pods" {
					c.Metric = "media_seconds"
				}
				if a.Feature == "news" {
					c.Metric = "news_opens"
				}
			}
			c.Feature = a.Feature
		}
		costUsage := usageDays[c.PeriodStart.Unix()]
		costWeights := contentMonths[c.ContentID+":"+c.PeriodStart.Format("2006-01")]
		if c.Destination == "content" {
			for _, rule := range contentRules {
				if rule.Kind == c.Feature && !rule.EffectiveFrom.After(c.PeriodStart) {
					c.AllocableBPS = rule.AllocableBPS
					c.Observation = rule.Observation
					c.AppliedRule = rule.Kind + ":" + fmt.Sprint(rule.Version)
					break
				}
			}
		}

		if c.Destination == "infrastructure" {
			c.AppliedRule = appliedProfiles(c, profiles)
		}
		old := prior[expenseKey]
		oldSum := Zero
		for _, row := range old {
			oldSum = Add(oldSum, Add(row.Direct, row.Shared))
		}
		frozen := len(old) > 0 && c.PeriodStart.Before(dailyCutoff)
		allocationPaused := len(old) > 0 && !p.AllocationEnabled && c.Destination != "direct"
		dailyShared := len(old) > 0 && c.Destination != "direct" && oldSum == c.Net && old[0].ExpenseRevision == c.Revision && old[0].Reconciled == c.Reconciled && old[0].RuleVersion == RuleVersion+"/"+c.AppliedRule && old[0].PublishedAt.UTC().Truncate(24*time.Hour).Equal(now.Truncate(24*time.Hour))
		if frozen || allocationPaused || dailyShared {
			for _, row := range old {
				row.Revision = rev
				if row.Day.Before(dailyCutoff) {
					row.Grain = "month"
					row.Day = time.Date(row.Day.Year(), row.Day.Month(), 1, 0, 0, 0, 0, time.UTC)
				}
				out = append(out, row)
			}
			delta := Sub(c.Net, oldSum)
			oldGross, allGross := Zero, true
			latestRevision := 0
			for _, r := range old {
				if r.ExpenseRevision > latestRevision {
					latestRevision = r.ExpenseRevision
				}
				if r.BeforeBenefits == nil {
					allGross = false
				} else {
					oldGross = Add(oldGross, *r.BeforeBenefits)
				}
			}
			grossChanged := latestRevision != c.Revision && (c.BeforeBenefits == nil && allGross || c.BeforeBenefits != nil && (!allGross || *c.BeforeBenefits != oldGross))
			if delta != Zero || grossChanged {
				date, grain, method := c.PeriodStart, "day", "correction_while_allocation_paused"
				if frozen {
					date = time.Date(date.Year(), date.Month(), 1, 0, 0, 0, 0, time.UTC)
					grain = "month"
					method = "correction_after_attribution_expiry"
				}
				row := Summary{ExpenseRevision: c.Revision, ExpenseKey: expenseKey, TenantID: tenant, Revision: rev, Grain: grain, Day: date, Subject: "unallocated", Resource: c.Resource, Feature: c.Feature, Direct: Zero, Shared: delta, Reconciled: c.Reconciled, Method: method, SourceID: c.SourceID, RuleVersion: RuleVersion + "/" + c.AppliedRule, PublishedAt: now}
				if c.BeforeBenefits != nil && allGross {
					grossDelta := Sub(*c.BeforeBenefits, oldGross)
					row.BeforeBenefits = &grossDelta
				}
				out = append(out, row)
			}

			continue
		}

		var eligibleContentWeights map[string]int64
		destinations := map[string]Amount{}
		methods := map[string]string{}
		direct := false
		switch c.Destination {
		case "direct":
			subject := c.Subject
			if subject == "" {
				subject = "unallocated"
			}
			destinations[subject] = c.Net
			direct = true
			methods[subject] = "direct_owner_attribution"
		case "infrastructure":
			if p.AllocationEnabled {
				pool := Proportion(c.Net, int64(c.AllocableBPS), 10000)
				w, qualified := infrastructureWeights(c, costUsage, profiles)
				if qualified {
					allocated, e := Allocate(pool, w)
					if e != nil {
						return nil, e
					}
					for s, a := range allocated {
						destinations[s] = a
						methods[s] = "calibrated_resource_profile"
					}
				}
			}
		case "content":
			if p.AllocationEnabled {
				w := contentWeights(c, costWeights)
				if e := pools.Preserve(c, w); e != nil {
					return nil, e
				}
				eligibleContentWeights = w
				allocated, e := Allocate(Proportion(c.Net, int64(c.AllocableBPS), 10000), w)
				if e != nil {
					return nil, e
				}
				for s, a := range allocated {
					destinations[s] = a
					methods[s] = "expense_month_consumption"
				}
			}
		}
		if c.Method == "incomplete_owner_evidence" && c.Net == Zero {
			for subject := range destinations {
				methods[subject] = "incomplete_owner_evidence"
			}
		}
		used := Zero
		for _, a := range destinations {
			used = Add(used, a)
		}
		residual := Sub(c.Net, used)
		if residual != Zero || len(destinations) == 0 {
			scope := "unallocated"
			if c.Destination == "overhead" {
				scope = "system"
			}
			destinations[scope] = Add(nonempty(destinations[scope]), residual)
			methods[scope] = c.Method
			if methods[scope] == "" {
				methods[scope] = "unsupported_or_shared"
			}
		}
		// A multi-day source is spread by calendar day for reporting only, never as a new charge.
		days := int64(c.PeriodEnd.Sub(c.PeriodStart) / (24 * time.Hour))
		if days < 1 {
			days = 1
		}
		dayweights := map[string]int64{}
		for i := int64(0); i < days; i++ {
			dayweights[c.PeriodStart.AddDate(0, 0, int(i)).Format("2006-01-02")] = 1
		}
		grossParts := grossAllocations(c, p.AllocationEnabled, costUsage, profiles, costWeights, eligibleContentWeights)
		for subject, amount := range destinations {
			method := methods[subject]
			gross, hasGross := grossParts[subject]
			grossByDay := map[string]Amount{}
			if hasGross {
				grossByDay, _ = Allocate(gross, dayweights)
			}
			feature := c.Feature
			if parts := strings.SplitN(subject, "\x1f", 2); len(parts) == 2 {
				subject = parts[0]
				feature = parts[1]
			}
			byday, _ := Allocate(amount, dayweights)
			for day, part := range byday {
				date, _ := time.Parse("2006-01-02", day)
				grain := "day"
				if date.Before(now.AddDate(0, 0, -90)) {
					grain = "month"
					date = time.Date(date.Year(), date.Month(), 1, 0, 0, 0, 0, time.UTC)
				}
				s := Summary{ExpenseRevision: c.Revision, ExpenseKey: expenseKey, TenantID: tenant, Revision: rev, Grain: grain, Day: date, Subject: subject, Resource: c.Resource, Feature: feature, Direct: Zero, Shared: Zero, Reconciled: c.Reconciled, Method: method, SourceID: c.SourceID, RuleVersion: RuleVersion + "/" + c.AppliedRule, PublishedAt: now}
				if hasGross {
					g := grossByDay[day]
					s.BeforeBenefits = &g
				}
				if direct {
					s.Direct = part
				} else {
					s.Shared = part
				}
				out = append(out, s)
			}
		}
	}
	return out, nil
}
func nonempty(a Amount) Amount {
	if a == "" {
		return Zero
	}
	return a
}

type Profile struct {
	TenantID      string    `json:"-"`
	Resource      string    `json:"resource"`
	Operation     string    `json:"operation"`
	Version       int       `json:"version"`
	EffectiveFrom time.Time `json:"effective_from"`
	Weight        int64     `json:"weight"`
	Evidence      string    `json:"evidence"`
	Actor         string    `json:"actor"`
}

func (Profile) TableName() string { return "economics_profiles" }

type Weight struct {
	Client     string
	TenantID   string
	Month      time.Time
	ContentID  string
	Subject    string
	Segment    string
	Kind       string
	Value      int64
	ObservedAt time.Time
}

func (Weight) TableName() string { return "economics_content_weights" }
func infrastructureWeights(c Cost, usage []Usage, profiles []Profile) (map[string]int64, bool) {
	w := map[string]int64{}
	qualified := true
	seen := false
	for _, u := range usage {
		if u.Day.Before(c.PeriodStart) || !u.Day.Before(c.PeriodEnd) || u.Unit != c.Metric || u.Subject == "synthetic" || strings.HasPrefix(u.Variant, "preparation_") || strings.HasPrefix(u.Variant, "elapsed:") {
			continue
		}
		seen = true
		weight := int64(-1)
		for _, p := range profiles {
			if p.Resource == c.Resource && p.Operation == u.Operation && !p.EffectiveFrom.After(u.Day) && p.Evidence != "" {
				weight = p.Weight
				break
			}
		}
		if weight < 0 {
			qualified = false
			continue
		}
		if weight > 0 && u.Value > 1e15/weight {
			return w, false
		}
		if w[u.Subject+"\x1f"+Operations[u.Operation]] > 1e15-u.Value*weight {
			return w, false
		}
		w[u.Subject+"\x1f"+Operations[u.Operation]] += u.Value * weight
	}
	return w, qualified && seen
}

// A cumulative segment spanning months has no trustworthy monthly baseline.
// Keep its maximum observed usage once, but withhold the affected content pool
// rather than allocate that uncertain timing to other consumers.
func indexContentWeights(rows []Weight) map[string][]Weight {
	first := map[mediaSegmentKey]int64{}
	ambiguous := map[mediaSegmentKey]bool{}
	for _, w := range rows {
		if w.Kind == "media_seconds" {
			key := mediaSegment(w)
			if month, ok := first[key]; ok && month != w.Month.Unix() {
				ambiguous[key] = true
			} else {
				first[key] = w.Month.Unix()
			}
		}
	}
	out := map[string][]Weight{}
	for _, w := range rows {
		key := w.ContentID + ":" + w.Month.Format("2006-01")
		out[key] = append(out[key], w)
		if w.Kind == "media_seconds" && ambiguous[mediaSegment(w)] {
			marker := w
			marker.Kind = "media_cross_month"
			out[key] = append(out[key], marker)
		}
	}
	return out
}

func contentWeights(c Cost, rows []Weight) map[string]int64 {
	result := map[string]int64{}
	month := time.Date(c.PeriodStart.Year(), c.PeriodStart.Month(), 1, 0, 0, 0, 0, time.UTC)
	for _, r := range rows {
		if r.Kind == "media_cross_month" && r.ContentID == c.ContentID && r.Month.Equal(month) && (c.Observation == "" || r.Client == c.Observation) {
			return result
		}
	}
	for _, r := range rows {
		preparedAt := c.PeriodStart
		if c.OccurredAt != nil {
			preparedAt = *c.OccurredAt
		}
		if r.ContentID != c.ContentID || !r.Month.Equal(month) || r.ObservedAt.Before(preparedAt) || r.Kind != c.Metric || c.Observation != "" && r.Client != c.Observation {
			continue
		}
		if r.Value < 0 || result[r.Subject] > 1e15-r.Value {
			continue
		}
		result[r.Subject] += r.Value
	}
	return result
}

// An invoice replaces matched receipt estimates. Unmatched amount is one residual, not a second bill.
func reconcile(costs []Cost) []Cost {
	for i := range costs {
		bill := &costs[i]
		if bill.Destination != "reconciliation" {
			continue
		}
		indices := []int{}
		weights := map[string]Amount{}
		subtotal := Zero
		complete := true
		for j := range costs {
			c := &costs[j]
			if (c.SourceID == "ai-owner" || c.SourceID == "ai-legacy") && c.Provider == bill.Provider && c.Resource == bill.Resource && !c.PeriodStart.Before(bill.PeriodStart) && !c.PeriodEnd.After(bill.PeriodEnd) {
				indices = append(indices, j)
				subtotal = Add(subtotal, c.Net)
				if c.Method == "incomplete_owner_evidence" {
					complete = false
				}
			}
		}
		if Compare(subtotal, Zero) != 0 && complete {
			for _, j := range indices {
				weights[fmt.Sprint(j)] = costs[j].Net
			}
			parts, _ := ScaleSigned(bill.Net, weights)
			used := Zero
			for _, j := range indices {
				if a, ok := parts[fmt.Sprint(j)]; ok {
					costs[j].Net = a
					costs[j].Reconciled = bill.Reconciled
					used = Add(used, a)
				}
			}
			bill.Net = Sub(bill.Net, used)
		}
		if !complete {
			// Unknown billed attempts cannot donate their invoice share to known users.
			// Retain priced estimates and disclose the unmatched signed remainder.
			bill.Net = Sub(bill.Net, subtotal)
			bill.Reason = "Incomplete priced attempt coverage; invoice remainder retained at platform level"
		}
		bill.Destination = "unallocated"
		if bill.Net == Zero {
			zero := Zero
			bill.BeforeBenefits = &zero
		}
		bill.Method = "invoice_residual"
	}
	return costs
}
func retainedMonth(now time.Time) time.Time {
	return time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC).AddDate(0, -11, 0)
}

func Retention(tx *gorm.DB, tenant string, now time.Time) error {
	var staged bool
	if e := tx.Raw("SELECT to_regclass('economics_projection_jobs') IS NOT NULL").Scan(&staged).Error; e != nil {
		return e
	}
	if staged {
		// Never retain a private build across a UTC retention boundary or a pause.
		var expired int64
		if e := tx.Table("economics_projection_jobs j").Where(`tenant_id=? AND (started_at<? OR EXISTS(SELECT 1 FROM economics_policies p WHERE p.tenant_id=j.tenant_id AND NOT p.reports_enabled))`, tenant, now.UTC().Truncate(24*time.Hour)).Count(&expired).Error; e != nil {
			return e
		}
		if expired > 0 {
			if e := discardProjection(tx, tenant); e != nil {
				return e
			}
		}
	}

	day := now.AddDate(0, 0, -90).UTC().Truncate(24 * time.Hour)
	month := retainedMonth(now)
	if e := tx.Exec(`INSERT INTO economics_usage_monthly SELECT tenant_id,date_trunc('month',day)::date,subject,operation,unit,variant,sum(value) FROM economics_usage_daily WHERE tenant_id=? AND day<? GROUP BY tenant_id,date_trunc('month',day),subject,operation,unit,variant ON CONFLICT(tenant_id,day,subject,operation,unit,variant) DO UPDATE SET value=economics_usage_monthly.value+EXCLUDED.value`, tenant, day).Error; e != nil {
		return e
	}
	for _, q := range []struct {
		SQL  string
		Args []any
	}{{`DELETE FROM economics_usage_daily WHERE tenant_id=? AND day<?`, []any{tenant, day}}, {`DELETE FROM economics_usage_monthly WHERE tenant_id=? AND day<?`, []any{tenant, month}}, {`DELETE FROM economics_content_weights WHERE tenant_id=? AND month<?`, []any{tenant, day.AddDate(0, -1, 0)}}, {`DELETE FROM economics_diagnostic_examples WHERE tenant_id=? AND created_at<?`, []any{tenant, now.AddDate(0, 0, -7)}},
		{`UPDATE economics_cost_summaries SET grain='month',day=date_trunc('month',day)::date WHERE tenant_id=? AND grain='day' AND day<?`, []any{tenant, day}},
		{`DELETE FROM economics_cost_summaries WHERE tenant_id=? AND day<?`, []any{tenant, month}},
		{`UPDATE economics_attributions SET subject=CASE WHEN subject LIKE 'account:%' THEN 'expired' ELSE subject END,owner_subject=CASE WHEN owner_subject LIKE 'account:%' THEN 'expired' ELSE owner_subject END WHERE tenant_id=? AND created_at<? AND (subject LIKE 'account:%' OR owner_subject LIKE 'account:%')`, []any{tenant, day}},
		{`UPDATE economics_cost_revisions SET subject='expired' WHERE tenant_id=? AND period_end<? AND subject LIKE 'account:%'`, []any{tenant, day}},
		{`DELETE FROM economics_interaction_receipts WHERE tenant_id=? AND observed_at<?`, []any{tenant, day}},
		{`DELETE FROM economics_owner_usage_receipts WHERE tenant_id=? AND observed_at<?`, []any{tenant, day}},
		{`DELETE FROM economics_runs WHERE tenant_id=? AND started_at<?`, []any{tenant, now.AddDate(0, 0, -90)}},
		{`DELETE FROM economics_issues WHERE tenant_id=? AND last_seen_at<?`, []any{tenant, month}}} {
		if e := tx.Exec(q.SQL, q.Args...).Error; e != nil {
			return e
		}
	}
	return nil
}
func updateIssues(tx *gorm.DB, tenant string, now time.Time, collectionEnabled bool) error {
	var states []struct {
		Source string
		Lost   int64
		Last   *time.Time
	}
	if e := tx.Raw(`SELECT source,COALESCE(sum(lost),0) lost,max(last_seen_at) last FROM economics_collection_state WHERE tenant_id=? GROUP BY source`, tenant).Scan(&states).Error; e != nil {
		return e
	}
	knownLost := int64(0)
	latest := map[string]*time.Time{}
	for _, r := range states {
		knownLost += r.Lost
		latest[r.Source] = r.Last
	}
	var previous Issue
	if e := tx.Where("tenant_id=? AND kind='coverage' AND scope='platform'", tenant).Take(&previous).Error; e != nil && e != gorm.ErrRecordNotFound {
		return e
	}
	var old struct {
		KnownLost int64 `json:"known_lost"`
	}
	_ = json.Unmarshal(previous.Evidence, &old)
	bad := knownLost > old.KnownLost
	stale := []string{}
	if collectionEnabled {
		for _, source := range []string{"cms", "iam"} {
			at := latest[source]
			if at == nil || at.Before(now.Add(-15*time.Minute)) {
				bad = true
				stale = append(stale, source)
			}
		}
	}
	var unavailable int64
	if e := tx.Model(&Source{}).Where("tenant_id=? AND status='unavailable'", tenant).Count(&unavailable).Error; e != nil {
		return e
	}
	bad = bad || unavailable > 0
	if e := ObserveIssue(tx, tenant, "coverage", "platform", "Usage or cost-source coverage needs review", bad, map[string]any{"known_lost": knownLost, "new_known_lost": knownLost - old.KnownLost, "stale_sources": stale, "unavailable_cost_sources": unavailable, "coverage": "Partial; unknown process loss and missing provider bills are not measured zeros"}, now); e != nil {
		return e
	}
	return EvaluateEfficiencyIssues(tx, tenant, now)
}

func grossAllocations(c Cost, enabled bool, usage []Usage, profiles []Profile, weights []Weight, actualWeights ...map[string]int64) map[string]Amount {
	out := map[string]Amount{}
	if c.BeforeBenefits == nil {
		return out
	}
	gross := *c.BeforeBenefits
	if c.Destination == "direct" {
		out[c.Subject] = gross
		return out
	}
	if enabled {
		var w map[string]int64
		qualified := false
		if c.Destination == "infrastructure" {
			w, qualified = infrastructureWeights(c, usage, profiles)
		}
		if c.Destination == "content" {
			w = contentWeights(c, weights)
			if len(actualWeights) > 0 && actualWeights[0] != nil {
				w = actualWeights[0]
			}
			qualified = true
		}
		if qualified {
			out, _ = Allocate(Proportion(gross, int64(c.AllocableBPS), 10000), w)
		}
	}
	used := Zero
	for _, a := range out {
		used = Add(used, a)
	}
	scope := "unallocated"
	if c.Destination == "overhead" {
		scope = "system"
	}
	remainder := Sub(gross, used)
	if remainder != Zero || len(out) == 0 {
		out[scope] = Add(nonempty(out[scope]), remainder)
	}
	return out
}

// A monthly serving bill is a single expense. Its daily portions use each day's workload,
// so a user who joins later is not allocated earlier days simply by appearing this month.
func expandInfrastructureDays(costs []Cost) []Cost {
	out := []Cost{}
	for _, c := range costs {
		days := int64(c.PeriodEnd.Sub(c.PeriodStart) / (24 * time.Hour))
		if c.Destination != "infrastructure" || days <= 1 {
			out = append(out, c)
			continue
		}
		weights := map[string]int64{}
		for d := int64(0); d < days; d++ {
			weights[c.PeriodStart.AddDate(0, 0, int(d)).Format("2006-01-02")] = 1
		}
		parts, _ := Allocate(c.Net, weights)
		gross := map[string]Amount{}
		if c.BeforeBenefits != nil {
			gross, _ = Allocate(*c.BeforeBenefits, weights)
		}
		for d := int64(0); d < days; d++ {
			day := c.PeriodStart.AddDate(0, 0, int(d))
			key := day.Format("2006-01-02")
			part := c
			part.LogicalKey = c.LogicalKey + "@" + key
			part.PeriodStart = day
			part.PeriodEnd = day.AddDate(0, 0, 1)
			part.Net = parts[key]
			if c.BeforeBenefits != nil {
				a := gross[key]
				part.BeforeBenefits = &a
			}
			out = append(out, part)
		}
	}
	return out
}
