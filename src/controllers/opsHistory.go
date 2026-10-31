package controllers

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/utils"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type opsBriefingItem struct {
	Key       string    `json:"key"`
	System    string    `json:"system"`
	Family    string    `json:"family"`
	Kind      string    `json:"kind"`
	Title     string    `json:"title"`
	Count     int64     `json:"count"`
	Href      string    `json:"href"`
	FirstSeen time.Time `json:"first_seen"`
	LastSeen  time.Time `json:"last_seen"`
	AmountUSD float64   `json:"amount_usd,omitempty"`
}
type opsHistorySource struct{ Table, Time, Outcome, Kind, Where string }
type opsCalendarRun struct {
	Key        string     `json:"key"`
	System     string     `json:"system"`
	LaneKey    string     `json:"lane_key"`
	TenantID   string     `json:"tenant_id"`
	StartedAt  time.Time  `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	Status     string     `json:"status"`
	DurationMS int64      `json:"duration_ms"`
	Href       string     `json:"href"`
	Headline   string     `json:"headline,omitempty"`
}
type opsCalendarEvent struct {
	Key     string    `json:"key"`
	System  string    `json:"system"`
	Kind    string    `json:"kind"`
	Verdict string    `json:"verdict"`
	At      time.Time `json:"at"`
	Count   int64     `json:"count"`
	Href    string    `json:"href"`
}
type opsCalendarOverlap struct {
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
	Systems    []string  `json:"systems"`
	RunKeys    []string  `json:"run_keys"`
}

func opsExtraHistory(m opsFleetMember) []opsHistorySource {
	switch m.Key {
	case "system_health":
		return []opsHistorySource{{"system_incident_episodes", "first_detected_at", "'opened'", "episode", ""}, {"system_incident_episodes", "resolved_at", "'resolved'", "episode", "resolved_at IS NOT NULL"}}
	case "feed_integrity":
		return []opsHistorySource{{"feed_integrity_episodes", "first_detected_at", "'opened'", "episode", ""}, {"feed_integrity_episodes", "resolved_at", "'resolved'", "episode", "resolved_at IS NOT NULL"}, {"feed_integrity_actions", "updated_at", "outcome", "approval_decided", "outcome IN ('approved','rejected','expired')"}}
	case "experience":
		return []opsHistorySource{{"experience_incidents", "first_seen_at", "'opened'", "episode", ""}, {"experience_incidents", "resolved_at", "'resolved'", "episode", "resolved_at IS NOT NULL"}}
	case "ai_spend":
		return []opsHistorySource{{"ai_spend_episodes", "first_seen_at", "kind||':'||status", "budget", ""}, {"ai_spend_episodes", "closed_at", "'resolved'", "episode", "closed_at IS NOT NULL"}, {"ai_spend_events", "occurred_at", "spend_class", "spend_events", ""}}
	case "media_studio":
		return []opsHistorySource{{"media_studio_actions", "human_outcome_at", "human_outcome", "approval_decided", "human_outcome_at IS NOT NULL"}}
	case "media_circulation":
		return []opsHistorySource{{"media_circulation_recommendations", "updated_at", "status", "approval_decided", "status IN ('applied','dismissed')"}}
	case "preferences":
		return []opsHistorySource{{"topic_proposals", "COALESCE(resolved_at,created_at)", "status", "topic_review", ""}}
	case "embedding_lifecycle":
		return []opsHistorySource{{"embedding_campaign_exceptions", "updated_at", "status", "exception", ""}}
	case "supply_continuity":
		return []opsHistorySource{{"media_supply_episode_events", "occurred_at", "event_type", "episode", ""}, {"media_supply_action_events", "created_at", "event_type", "action", ""}}
	case "workload_governor":
		return []opsHistorySource{{"workload_decisions", "created_at", "event||':'||verdict", "capacity", ""}}
	}
	return nil
}
func opsLedgerSources(m opsFleetMember) []opsHistorySource {
	sources := opsExtraHistory(m)
	seen := map[string]bool{}
	for _, lane := range m.Lanes {
		if lane.RunTable != "" && !seen[lane.RunTable] {
			seen[lane.RunTable] = true
			start := lane.RunStart
			if start == "" {
				start = "started_at"
			}
			state := lane.RunState
			if state == "" {
				state = "status"
			}
			sources = append(sources, opsHistorySource{Table: lane.RunTable, Time: "COALESCE(" + lane.RunFinished + "," + start + ")", Outcome: state, Kind: "run"})
		}
		if lane.ActionTable != "" && !seen[lane.ActionTable] {
			seen[lane.ActionTable] = true
			at := "COALESCE(finished_at,created_at)"
			if m.Key == "redundancy" || m.Key == "embedding_lifecycle" || m.Key == "experience" {
				at = "created_at"
			}
			sources = append(sources, opsHistorySource{Table: lane.ActionTable, Time: at, Outcome: lane.ActionKind + "||':'||" + lane.ActionOutcome, Kind: "action"})
		}
	}
	return sources
}

// Aggregate every matching source row. Only finite owner outcomes/classes are
// grouped; no detail LIMIT can silently discard events before marking read.
func collectOpsBriefing(db *gorm.DB, since, through time.Time) ([]opsBriefingItem, []string) {
	items := []opsBriefingItem{}
	errs := []string{}
	var mu sync.Mutex
	opsParallelReadMembers(func(m opsFleetMember) {
		rows, errors := collectOpsBriefingMember(db, m, since, through)
		mu.Lock()
		items = append(items, rows...)
		errs = append(errs, errors...)
		mu.Unlock()
	})
	sort.Slice(items, func(i, j int) bool {
		if !items[i].LastSeen.Equal(items[j].LastSeen) {
			return items[i].LastSeen.After(items[j].LastSeen)
		}
		return items[i].Key < items[j].Key
	})
	return items, errs
}

func collectOpsBriefingMember(db *gorm.DB, m opsFleetMember, since, through time.Time) ([]opsBriefingItem, []string) {
	items := []opsBriefingItem{}
	errs := []string{}
	for _, source := range opsLedgerSources(m) {
		if db.Statement.Context.Err() != nil {
			errs = append(errs, m.Key+": read deadline exceeded")
			break
		}
		var rows []struct {
			Outcome             string
			Count               int64
			FirstSeen, LastSeen time.Time
			AmountUSD           float64
		}
		amount := "0.0"
		if source.Table == "ai_spend_events" {
			amount = "COALESCE(sum(cost_usd),0)"
		}
		query := fmt.Sprintf("SELECT COALESCE(%s,'unknown') AS outcome,count(*) AS count,min(%s) AS first_seen,max(%s) AS last_seen,%s AS amount_usd FROM %s WHERE %s>? AND %s<=?", source.Outcome, source.Time, source.Time, amount, source.Table, source.Time, source.Time)
		if source.Where != "" {
			query += " AND (" + source.Where + ")"
		}
		query += " GROUP BY 1 ORDER BY 1 LIMIT 201"
		if err := db.Raw(query, since.Add(-5*time.Minute), through).Scan(&rows).Error; err != nil {
			errs = append(errs, m.Key+"/"+source.Kind+": "+opsReadError(err))
			continue
		}
		if len(rows) > 200 {
			errs = append(errs, m.Key+": history class capacity exceeded")
			rows = rows[:200]
		}
		for _, r := range rows {
			title := fmt.Sprintf("%s: %d %s — %s", m.Label, r.Count, source.Kind, r.Outcome)
			if source.Table == "ai_spend_events" {
				title += fmt.Sprintf(" ($%.4f)", r.AmountUSD)
			}
			items = append(items, opsBriefingItem{Key: source.Table + ":" + source.Kind + ":" + r.Outcome, System: m.Key, Family: m.Family, Kind: source.Kind, Title: title, Count: r.Count, Href: m.CockpitPath, FirstSeen: r.FirstSeen, LastSeen: r.LastSeen, AmountUSD: r.AmountUSD})
		}
	}
	return items, errs
}

func opsBriefingToken(secret []byte, tenant, admin string, since, through time.Time) string {
	mac := hmac.New(sha256.New, secret)
	fmt.Fprintf(mac, "ops-briefing-v1\x00%s\x00%s\x00%s\x00%s", tenant, admin, since.UTC().Format(time.RFC3339Nano), through.UTC().Format(time.RFC3339Nano))
	return hex.EncodeToString(mac.Sum(nil))
}
func GetOpsBriefing(c *gin.Context) {
	p, ok := utils.GetAdminPrincipal(c)
	if !ok {
		c.JSON(401, gin.H{"error": "unauthorized"})
		return
	}
	db, cancel, through := opsRequestDB(c)
	defer cancel()
	var cursor models.OpsBriefingCursor
	err := db.Where("tenant_id=? AND admin_id=?", p.TenantID, p.UserID).First(&cursor).Error
	if err != nil && err != gorm.ErrRecordNotFound {
		c.JSON(503, gin.H{"error": "briefing cursor unavailable"})
		return
	}
	since := cursor.LastSeenAt
	if since.IsZero() {
		since = through.Add(-24 * time.Hour)
	}
	items, errs := collectOpsBriefing(db, since, through)
	var commands []models.OpsFleetCommand
	commandsErr := db.Where("created_at>? AND created_at<=?", since.Add(-5*time.Minute), through).Order("created_at DESC,id DESC").Limit(51).Find(&commands).Error
	if commandsErr != nil {
		errs = append(errs, "fleet_commands: "+opsReadError(commandsErr))
	}
	truncated := len(commands) > 50
	if truncated {
		commands = commands[:50]
	}
	var commandCount int64
	if err := db.Model(&models.OpsFleetCommand{}).Where("created_at>? AND created_at<=?", since.Add(-5*time.Minute), through).Count(&commandCount).Error; err != nil {
		errs = append(errs, "fleet_commands: "+opsReadError(err))
	}
	if commandCount > 0 {
		items = append(items, opsBriefingItem{Key: "ops_fleet_commands:all", System: "operations", Family: "platform", Kind: "command", Title: fmt.Sprintf("%d fleet commands recorded", commandCount), Count: commandCount, Href: "/platform/operations", FirstSeen: since, LastSeen: through})
	}
	if commands == nil {
		commands = []models.OpsFleetCommand{}
	}
	token := ""
	if len(errs) == 0 {
		secret, e := utils.GetJWTSecret()
		if e != nil {
			errs = append(errs, "briefing signing unavailable")
		} else {
			token = opsBriefingToken(secret, p.TenantID, p.UserID, since, through)
		}
	}
	totals := map[string]int64{}
	for _, item := range items {
		totals[item.Kind] += item.Count
	}
	c.JSON(200, gin.H{"data": gin.H{"since": since, "through": through, "items": items, "totals": totals, "commands": commands, "commands_truncated": truncated, "command_count": commandCount, "adapter_errors": errs, "complete": len(errs) == 0, "read_token": token, "overlap_seconds": 300}})
}
func MarkOpsBriefingSeen(c *gin.Context) {
	p, ok := utils.GetAdminPrincipal(c)
	if !ok {
		c.JSON(401, gin.H{"error": "unauthorized"})
		return
	}
	var req struct {
		Since     time.Time `json:"since"`
		Through   time.Time `json:"through"`
		ReadToken string    `json:"read_token"`
	}
	if c.ShouldBindJSON(&req) != nil || req.Through.IsZero() || req.Since.IsZero() {
		c.JSON(400, gin.H{"error": "since, through and the completed briefing read_token are required"})
		return
	}
	secret, err := utils.GetJWTSecret()
	if err != nil {
		c.JSON(503, gin.H{"error": "briefing signing unavailable"})
		return
	}
	want := opsBriefingToken(secret, p.TenantID, p.UserID, req.Since, req.Through)
	if !hmac.Equal([]byte(want), []byte(req.ReadToken)) {
		c.JSON(409, gin.H{"error": "reload a complete briefing before marking it read"})
		return
	}
	db, cancel, now := opsRequestDB(c)
	defer cancel()
	if req.Through.After(now) {
		req.Through = now
	}
	row := models.OpsBriefingCursor{TenantID: p.TenantID, AdminID: p.UserID, LastSeenAt: req.Through}
	err = db.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "tenant_id"}, {Name: "admin_id"}}, DoUpdates: clause.Assignments(map[string]any{"last_seen_at": gorm.Expr("GREATEST(ops_briefing_cursors.last_seen_at,EXCLUDED.last_seen_at)"), "updated_at": now})}).Create(&row).Error
	if err != nil {
		c.JSON(503, gin.H{"error": "briefing cursor could not be saved"})
		return
	}
	c.JSON(200, gin.H{"data": gin.H{"through": req.Through}})
}

func GetOpsCalendar(c *gin.Context) {
	db, cancel, now := opsRequestDB(c)
	defer cancel()
	runs, events, errs, truncated := collectOpsCalendar(db, now)
	fleet := collectOpsStatus(db, now)
	for _, s := range fleet {
		if s.Error != "" {
			errs = append(errs, s.MemberKey+": "+s.Error)
		}
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"as_of": now, "window_start": now.Add(-24 * time.Hour), "fleet": fleet, "runs": runs, "events": events, "overlaps": opsCalendarOverlaps(runs, now), "adapter_errors": errs, "complete": len(errs) == 0 && !truncated, "truncated": truncated}})
}
func collectOpsCalendar(db *gorm.DB, now time.Time) ([]opsCalendarRun, []opsCalendarEvent, []string, bool) {
	runs := []opsCalendarRun{}
	events := []opsCalendarEvent{}
	errs := []string{}
	truncated := false
	var mu sync.Mutex
	opsParallelReadMembers(func(m opsFleetMember) {
		memberRuns, memberErrors, cut := collectOpsCalendarMember(db, m, now)
		mu.Lock()
		runs = append(runs, memberRuns...)
		errs = append(errs, memberErrors...)
		truncated = truncated || cut
		mu.Unlock()
	})
	var rows []struct {
		Event, Verdict string
		At             time.Time
		Count          int64
	}
	err := db.Raw(`SELECT event,verdict,date_trunc('minute',created_at) AS at,count(*) AS count FROM workload_decisions WHERE created_at>=? AND created_at<=? GROUP BY event,verdict,date_trunc('minute',created_at) ORDER BY at DESC,event,verdict LIMIT 501`, now.Add(-24*time.Hour), now).Scan(&rows).Error
	if err != nil {
		errs = append(errs, "workload_governor/events: "+opsReadError(err))
	}
	if len(rows) > 500 {
		truncated = true
		rows = rows[:500]
	}
	for _, r := range rows {
		events = append(events, opsCalendarEvent{Key: "workload_decisions:" + r.Event + ":" + r.Verdict + ":" + r.At.Format(time.RFC3339), System: "workload_governor", Kind: r.Event, Verdict: r.Verdict, At: r.At, Count: r.Count, Href: "/platform/workload?tab=activity"})
	}
	sort.Slice(runs, func(i, j int) bool { return runs[i].StartedAt.After(runs[j].StartedAt) })
	return runs, events, errs, truncated
}

func collectOpsCalendarMember(db *gorm.DB, m opsFleetMember, now time.Time) ([]opsCalendarRun, []string, bool) {
	runs := []opsCalendarRun{}
	errs := []string{}
	truncated := false
	for _, lane := range m.Lanes {
		if lane.RunTable == "" {
			continue
		}
		var rows []opsRunEvidence
		source := opsRunSelect(m, lane)
		if lane.RunWhere != "" {
			source += " WHERE " + lane.RunWhere
		}
		query := "SELECT * FROM (" + source + ") ops_runs WHERE started_at<=? AND (finished_at>=? OR finished_at IS NULL) ORDER BY started_at DESC,native_id DESC LIMIT 201"
		if err := db.Raw(query, now, now.Add(-24*time.Hour)).Scan(&rows).Error; err != nil {
			errs = append(errs, m.Key+"/"+lane.Key+": "+opsReadError(err))
			continue
		}
		if len(rows) > 200 {
			truncated = true
			rows = rows[:200]
		}
		for _, r := range rows {
			if r.StartedAt.IsZero() {
				continue
			}
			end := now
			if r.FinishedAt != nil && r.FinishedAt.Before(end) {
				end = *r.FinishedAt
			}
			duration := end.Sub(r.StartedAt).Milliseconds()
			if duration < 0 {
				duration = 0
			}
			runs = append(runs, opsCalendarRun{Key: lane.RunTable + ":" + r.NativeID, System: m.Key, LaneKey: lane.Key, TenantID: r.TenantID, StartedAt: r.StartedAt, FinishedAt: r.FinishedAt, Status: r.Status, DurationMS: duration, Href: lane.CockpitPath, Headline: r.Headline})
		}
	}
	return runs, errs, truncated
}

// A sweep line reports actual overlapping intervals, with no scheduler effects.
func opsCalendarOverlaps(runs []opsCalendarRun, now time.Time) []opsCalendarOverlap {
	type edge struct {
		at     time.Time
		key    string
		start  bool
		system string
	}
	edges := []edge{}
	for _, r := range runs {
		end := now
		if r.FinishedAt != nil && r.FinishedAt.Before(end) {
			end = *r.FinishedAt
		}
		start := r.StartedAt
		if start.Before(now.Add(-24 * time.Hour)) {
			start = now.Add(-24 * time.Hour)
		}
		if !start.Before(end) {
			continue
		}
		edges = append(edges, edge{start, r.Key, true, r.System}, edge{end, r.Key, false, r.System})
	}
	sort.Slice(edges, func(i, j int) bool {
		if edges[i].at.Equal(edges[j].at) {
			return !edges[i].start && edges[j].start
		}
		return edges[i].at.Before(edges[j].at)
	})
	active := map[string]string{}
	out := []opsCalendarOverlap{}
	for i := 0; i < len(edges); {
		at := edges[i].at
		for i < len(edges) && edges[i].at.Equal(at) {
			e := edges[i]
			if e.start {
				active[e.key] = e.system
			} else {
				delete(active, e.key)
			}
			i++
		}
		if i == len(edges) || len(active) < 2 {
			continue
		}
		systems := map[string]bool{}
		keys := []string{}
		for k, s := range active {
			systems[s] = true
			keys = append(keys, k)
		}
		if len(systems) < 2 {
			continue
		}
		names := []string{}
		for s := range systems {
			names = append(names, s)
		}
		sort.Strings(names)
		sort.Strings(keys)
		out = append(out, opsCalendarOverlap{at, edges[i].at, names, keys})
	}
	return out
}

// Keep remote SQL fan-out bounded to four readers for every history request.
func opsParallelReadMembers(fn func(opsFleetMember)) {
	jobs := make(chan opsFleetMember, len(opsFleetRegistry))
	for _, member := range opsFleetRegistry {
		jobs <- member
	}
	close(jobs)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for member := range jobs {
				fn(member)
			}
		}()
	}
	wg.Wait()
}
