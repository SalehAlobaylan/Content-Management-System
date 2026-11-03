package controllers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"content-management-system/src/models"
	"gorm.io/gorm"
)

// The original 3s proposal was qualified against remote Neon: warm-pool
// owner reads still timed out before most adapters completed. Keep one bounded
// 15s request budget, four readers, and explicit partial evidence on expiry.
const opsReadDeadline = 15 * time.Second
const opsReadRowLimit = 500

type opsTrustSummary struct {
	State  string `json:"state"`
	Earned int    `json:"earned"`
	Total  int    `json:"total"`
	Detail string `json:"detail"`
}
type opsContainment struct {
	Registered bool   `json:"registered"`
	OptedOut   *bool  `json:"opted_out,omitempty"`
	PauseScope string `json:"pause_scope"`
}
type opsAdapterTiming struct {
	Adapter    string `json:"adapter"`
	DurationMS int64  `json:"duration_ms"`
	Error      string `json:"error,omitempty"`
}
type opsSnapshot struct {
	AsOf      time.Time          `json:"as_of"`
	Fleet     []opsMemberStatus  `json:"fleet"`
	Attention []opsAttentionItem `json:"attention"`
	Errors    []string           `json:"adapter_errors"`
	Timings   []opsAdapterTiming `json:"adapter_timings"`
	Complete  bool               `json:"complete"`
}
type opsRunEvidence struct {
	NativeID   string
	TenantID   string
	Status     string
	Headline   string
	StartedAt  time.Time
	FinishedAt *time.Time
}

func opsReadError(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "read deadline exceeded"
	}
	if errors.Is(err, context.Canceled) {
		return "read cancelled"
	}
	return "owner evidence unavailable"
}
func opsSeverityRank(value string) int {
	switch opsNormalizeSeverity(value) {
	case "critical":
		return 4
	case "major":
		return 3
	case "minor":
		return 2
	default:
		return 1
	}
}
func opsNormalizeSeverity(value string) string {
	switch strings.ToLower(value) {
	case "critical", "broken":
		return "critical"
	case "major", "degraded", "degraded_major", "error":
		return "major"
	case "minor", "warning", "watching", "degraded_minor":
		return "minor"
	case "info", "healthy":
		return "info"
	default:
		return "major"
	}
}
func opsFingerprint(parts ...string) string {
	h := sha256.Sum256([]byte(strings.Join(parts, "|")))
	return hex.EncodeToString(h[:12])
}
func opsTimeFingerprint(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func opsRunSelect(member opsFleetMember, lane opsFleetLane) string {
	tenant := "tenant_id"
	if member.Key == "system_health" {
		tenant = "'platform'"
	}
	start := lane.RunStart
	if start == "" {
		start = "started_at"
	}
	headline := lane.RunHeadline
	if headline == "" {
		headline = "''"
	}
	state := lane.RunState
	if state == "" {
		state = "status"
	}
	finish := lane.RunFinished
	if finish == "" {
		finish = "finished_at"
	}
	return fmt.Sprintf("SELECT id::text AS native_id, %s AS tenant_id, %s AS status, %s AS headline, %s AS started_at, %s AS finished_at FROM %s", tenant, state, headline, start, finish, lane.RunTable)
}

func opsStatusForLane(db *gorm.DB, member opsFleetMember, lane opsFleetLane, now time.Time) []opsMemberStatus {
	if member.Key == "storage" {
		return opsStorageStatuses(db, member, lane)
	}
	base := opsMemberStatus{MemberKey: member.Key, MemberLabel: member.Label, Family: member.Family, Kind: member.Kind, LaneKey: lane.Key, LaneLabel: lane.Label, CockpitPath: lane.CockpitPath, Pausable: lane.PauseColumn != "", State: "unknown", Liveness: "unknown", Trust: opsTrustSummary{State: "not_applicable", Detail: "Owner does not expose normalized trust for this lane"}, Containment: opsContainment{PauseScope: "new owner admissions; in-flight work continues"}}
	if member.Key == "system_health" {
		base.Containment.PauseScope = "containment only; service observation continues"
	}
	if lane.StatusSQL == "" {
		if !lane.Scheduled && member.Key != "ranking_intelligence" {
			base.Liveness = "not_applicable"
		}
		return []opsMemberStatus{base}
	}
	var rows []opsStatusRow
	query := "SELECT p.* FROM (" + lane.StatusSQL + ") p"
	if lane.RunTable != "" {
		runSQL := opsRunSelect(member, lane)
		if lane.RunWhere != "" {
			runSQL += " WHERE " + lane.RunWhere
		}
		query = "SELECT p.*,r.native_id AS latest_run_id,r.status AS latest_run_status,r.headline AS latest_run_headline,r.started_at AS latest_started_at,r.finished_at AS latest_finished_at FROM (" + lane.StatusSQL + ") p LEFT JOIN LATERAL (SELECT * FROM (" + runSQL + ") candidate WHERE candidate.tenant_id=p.tenant_id ORDER BY CASE WHEN status IN ('running','claimed','verifying','processing') THEN 0 ELSE 1 END,started_at DESC NULLS LAST,native_id::bigint DESC LIMIT 1) r ON true"
	}
	if err := db.Raw(query+" LIMIT ?", opsReadRowLimit+1).Scan(&rows).Error; err != nil {
		base.State = "errored"
		base.Error = opsReadError(err)
		return []opsMemberStatus{base}
	}
	if len(rows) > opsReadRowLimit {
		base.State = "errored"
		base.Error = "policy scope exceeds bounded read capacity"
		return []opsMemberStatus{base}
	}
	if len(rows) == 0 {
		base.State = "unconfigured"
		return []opsMemberStatus{base}
	}
	out := make([]opsMemberStatus, 0, len(rows))
	for _, row := range rows {
		status := base
		status.TenantID = row.TenantID
		status.Enabled = row.Enabled
		status.Mode = row.Mode
		status.PausedUntil = row.PausedUntil
		status.LastRunAt = row.LastRunAt
		if member.Key == "pods_pipeline" {
			out = append(out, opsPodsPipelineStatus(status, row))
			continue
		}
		found := row.LatestRunID != nil
		run := opsRunEvidence{TenantID: row.TenantID, Status: row.LatestRunStatus, Headline: row.LatestRunHeadline, FinishedAt: row.LatestFinishedAt}
		if row.LatestRunID != nil {
			run.NativeID = *row.LatestRunID
		}
		if row.LatestStartedAt != nil {
			run.StartedAt = *row.LatestStartedAt
		}
		if row.OwnerOutcome != "" {
			status.LastRunStatus = row.OwnerOutcome
			status.LastRunHeadline = "Owner evaluator outcome: " + row.OwnerOutcome
			if row.OwnerOutcome == "record_failed" || row.OwnerOutcome == "control_unavailable" {
				status.LastRunStatus = "failed"
			}
		}
		if found {
			status.LastRunStatus = run.Status
			status.LastRunHeadline = run.Headline
			if !run.StartedAt.IsZero() {
				// Keep the displayed time paired with this run's status. Cadence
				// still uses the separate policy watermark in row.LastRunAt.
				status.LastRunAt = &run.StartedAt
			}
		}
		status = opsLaneLiveness(status, lane, row, run, found, now)
		if member.Key == "system_health" && row.PausedUntil != nil && row.PausedUntil.After(now) {
			status.Evidence = "Containment paused; observation remains scheduled"
		}
		if member.TrustCapable && (member.Kind == "autopilot" || lane.Key == "autopilot") {
			status.Trust = opsTrustSummary{State: "unknown", Detail: "Inspect the owner cockpit for class-specific trust"}
		}
		out = append(out, status)
	}
	return out
}

func opsLaneLiveness(status opsMemberStatus, lane opsFleetLane, row opsStatusRow, run opsRunEvidence, found bool, now time.Time) opsMemberStatus {
	paused := row.PausedUntil != nil && row.PausedUntil.After(now) && status.MemberKey != "system_health"
	if !row.Enabled {
		status.State = "disabled"
		status.Liveness = "not_applicable"
		return status
	}
	if paused {
		status.State = "paused"
		status.Liveness = "not_applicable"
		return status
	}
	active := found && (run.Status == "running" || run.Status == "claimed" || run.Status == "verifying" || run.Status == "processing")
	if active && !run.StartedAt.IsZero() {
		status.State = "running"
		status.Liveness = "observable"
		maxRun := lane.MaxRun
		if maxRun == 0 {
			maxRun = 2 * time.Hour
		}
		if status.MemberKey == "feed_integrity" {
			maxRun = 6 * time.Hour
		}
		if status.MemberKey == "embedding_lifecycle" && lane.Key == "campaigns" {
			status.Liveness = "not_applicable"
			return status
		}
		if now.After(run.StartedAt.Add(maxRun)) {
			status.State = "run_stuck"
			at := run.StartedAt.Add(maxRun)
			status.StalledSince = &at
		}
		return status
	}
	if lane.ManualOnly || !lane.Scheduled || row.IntervalMinutes <= 0 {
		status.State = "idle"
		status.Liveness = "not_applicable"
		return status
	}
	status.Liveness = "observable"
	status.State = "idle"
	watermark := row.CreatedAt
	if row.LastRunAt != nil {
		watermark = *row.LastRunAt
	}
	due := watermark.Add(time.Duration(row.IntervalMinutes) * time.Minute)
	status.NextDueAt = &due
	grace := lane.Grace
	if grace == 0 {
		grace = 30 * time.Minute
	}
	if status.MemberKey == "embedding_lifecycle" {
		grace = 2 * time.Hour
	}
	threshold := due.Add(grace)
	if row.DeepIntervalHours > 0 {
		baseline := row.CreatedAt
		if row.DeepLastRunAt != nil {
			baseline = *row.DeepLastRunAt
		}
		deepDue := baseline.Add(time.Duration(row.DeepIntervalHours) * time.Hour)
		status.DeepDueAt = &deepDue
		if deepThreshold := deepDue.Add(6 * time.Hour); deepThreshold.Before(threshold) {
			threshold = deepThreshold
		}
		if deepDue.Before(due) {
			status.NextDueAt = &deepDue
		}
	}
	if now.After(threshold) {
		status.State = "stalled"
		status.StalledSince = &threshold
	}
	return status
}

// One task per registry member; each task owns its local result. A deadline
// returns explicit missing adapters while cancelled SQL work drains safely.
func collectOpsSnapshot(db *gorm.DB, now time.Time, attention bool) opsSnapshot {
	return collectOpsSnapshotMembers(db, now, attention, opsFleetRegistry)
}
func collectOpsSnapshotMembers(db *gorm.DB, now time.Time, attention bool, members []opsFleetMember) opsSnapshot {
	ctx, cancel := context.WithTimeout(db.Statement.Context, opsReadDeadline)
	defer cancel()
	db = db.WithContext(ctx)
	type result struct {
		index  int
		fleet  []opsMemberStatus
		items  []opsAttentionItem
		err    []string
		timing opsAdapterTiming
	}
	jobs := make(chan int, len(members))
	done := make(chan result, len(members))
	for i := range members {
		jobs <- i
	}
	close(jobs)
	for worker := 0; worker < 4; worker++ {
		go func() {
			for i := range jobs {
				r := result{index: i}
				member := members[i]
				start := time.Now()
				r.timing.Adapter = member.Key
				func() {
					defer func() {
						if recover() != nil {
							r.err = append(r.err, member.Key+": adapter failed")
						}
					}()
					for _, lane := range member.Lanes {
						for _, status := range opsStatusForLane(db, member, lane, now) {
							r.fleet = append(r.fleet, status)
							if status.Error != "" {
								r.err = append(r.err, member.Key+"/"+lane.Key+": "+status.Error)
							}
						}
					}
					if attention {
						var err error
						r.items, err = opsAttentionForMember(db, member, now)
						if err != nil {
							r.err = append(r.err, member.Key+": "+opsReadError(err))
						}
						trust, trustItems, trustErr := opsTrustForMember(db, member, now)
						if trustErr != nil {
							r.err = append(r.err, member.Key+"/trust: "+opsReadError(trustErr))
						}
						r.items = append(r.items, trustItems...)
						for j := range r.fleet {
							if t, ok := trust[r.fleet[j].TenantID]; ok && (r.fleet[j].Kind == "autopilot" || r.fleet[j].LaneKey == "autopilot") {
								r.fleet[j].Trust = t
							}
						}
					}
				}()
				for _, lane := range member.Lanes {
					found := false
					for _, status := range r.fleet {
						if status.LaneKey == lane.Key {
							found = true
							break
						}
					}
					if !found {
						r.fleet = append(r.fleet, opsMemberStatus{MemberKey: member.Key, MemberLabel: member.Label, Family: member.Family, Kind: member.Kind, LaneKey: lane.Key, LaneLabel: lane.Label, Pausable: lane.PauseColumn != "", State: "errored", Liveness: "unknown", Error: "adapter failed before returning evidence", CockpitPath: lane.CockpitPath})
					}
				}
				r.timing.DurationMS = time.Since(start).Milliseconds()
				if len(r.err) > 0 {
					r.timing.Error = strings.Join(r.err, "; ")
				}
				done <- r
			}
		}()
	}
	snapshot := opsSnapshot{AsOf: now, Fleet: []opsMemberStatus{}, Attention: []opsAttentionItem{}, Errors: []string{}, Timings: []opsAdapterTiming{}, Complete: true}
	results := make(map[int]result)
	for len(results) < len(members) {
		select {
		case r := <-done:
			results[r.index] = r
		case <-ctx.Done():
			goto collected
		}
	}
collected:
	for i, member := range members {
		r, ok := results[i]
		if !ok {
			snapshot.Errors = append(snapshot.Errors, member.Key+": read deadline exceeded")
			for _, lane := range member.Lanes {
				snapshot.Fleet = append(snapshot.Fleet, opsMemberStatus{MemberKey: member.Key, MemberLabel: member.Label, LaneKey: lane.Key, LaneLabel: lane.Label, Family: member.Family, Kind: member.Kind, State: "errored", Liveness: "unknown", Pausable: lane.PauseColumn != "", Error: "read deadline exceeded", CockpitPath: lane.CockpitPath})
			}
			continue
		}
		snapshot.Fleet = append(snapshot.Fleet, r.fleet...)
		snapshot.Attention = append(snapshot.Attention, r.items...)
		snapshot.Errors = append(snapshot.Errors, r.err...)
		snapshot.Timings = append(snapshot.Timings, r.timing)
	}
	if attention {
		for _, status := range snapshot.Fleet {
			if status.MemberKey == "storage" && status.State == "attention" {
				severity := "major"
				if status.LastRunStatus == "critical" {
					severity = "critical"
				}
				snapshot.Attention = append(snapshot.Attention, opsAttentionItem{Key: "storage:budget:" + status.TenantID, Fingerprint: opsFingerprint("storage", status.TenantID, status.LastRunStatus), System: "storage", TenantID: status.TenantID, Kind: "budget", Severity: severity, Title: "Storage requires attention", Detail: status.Evidence, Count: 1, FirstSeen: now, Href: status.CockpitPath})
			}
			if status.LastRunStatus == "failed" || status.LastRunStatus == "partial" {
				at := now
				if status.LastRunAt != nil {
					at = *status.LastRunAt
				}
				snapshot.Attention = append(snapshot.Attention, opsAttentionItem{Key: status.MemberKey + ":exception:" + status.LaneKey + ":" + status.TenantID, Fingerprint: opsFingerprint(status.MemberKey, status.LaneKey, status.LastRunStatus, opsTimeFingerprint(status.LastRunAt)), System: status.MemberKey, TenantID: status.TenantID, Kind: "exception", Severity: "major", Title: status.MemberLabel + " latest run is " + status.LastRunStatus, Detail: status.LastRunHeadline, Count: 1, FirstSeen: at, Href: status.CockpitPath})
			}
			if status.State != "stalled" && status.State != "run_stuck" {
				continue
			}
			at := now
			if status.StalledSince != nil {
				at = *status.StalledSince
			}
			snapshot.Attention = append(snapshot.Attention, opsAttentionItem{Key: status.MemberKey + ":stalled:" + status.LaneKey + ":" + status.TenantID, Fingerprint: opsFingerprint(status.MemberKey, status.LaneKey, status.State, opsTimeFingerprint(status.LastRunAt)), System: status.MemberKey, TenantID: status.TenantID, Kind: "stalled", Severity: "major", Title: status.MemberLabel + " " + status.LaneLabel + " is " + status.State, Detail: status.Evidence, Count: 1, FirstSeen: at, Href: status.CockpitPath})
		}
	}
	snapshot.Complete = len(snapshot.Errors) == 0
	return snapshot
}
func collectOpsStatus(db *gorm.DB, now time.Time) []opsMemberStatus {
	return collectOpsSnapshot(db, now, false).Fleet
}
func opsOpenAttention(db *gorm.DB, now time.Time) ([]opsAttentionItem, []string) {
	s := collectOpsSnapshot(db, now, true)
	return s.Attention, s.Errors
}

func opsHeadline(items []opsAttentionItem, statuses []opsMemberStatus) (string, string) {
	return opsHeadlineAt(items, statuses, time.Now().UTC())
}
func opsHeadlineAt(items []opsAttentionItem, statuses []opsMemberStatus, asOf time.Time) (string, string) {
	stalled := map[string]bool{}
	observable := map[string]bool{}
	unknown := false
	major := false
	watching := false
	for _, s := range statuses {
		if s.Liveness == "observable" {
			observable[s.MemberKey] = true
		}
		if s.State == "errored" || s.State == "attention" {
			major = true
		}
		if s.State == "unknown" || (s.Enabled && s.Liveness == "unknown") {
			unknown = true
		}
		if s.State == "stalled" || s.State == "run_stuck" {
			major = true
			if s.Liveness == "observable" {
				stalled[s.MemberKey] = true
			}
		}
	}
	for _, item := range items {
		watching = true
		if item.Snoozed {
			continue
		}
		if item.System == "system_health" && item.Kind == "episode" {
			return "incident", opsHeadlineSummary("incident", items, statuses, asOf)
		}
		if opsSeverityRank(item.Severity) >= 3 || (item.Kind == "approval" && !item.FirstSeen.IsZero() && asOf.Sub(item.FirstSeen) >= 12*time.Hour) {
			major = true
		}
		if item.Kind == "stalled" && (len(statuses) == 0 || observable[item.System]) {
			stalled[item.System] = true
		}
	}
	if len(stalled) >= 3 {
		return "incident", opsHeadlineSummary("incident", items, statuses, asOf)
	}
	if major {
		return "attention", opsHeadlineSummary("attention", items, statuses, asOf)
	}
	if watching || unknown {
		return "watching", opsHeadlineSummary("watching", items, statuses, asOf)
	}
	return "all_clear", opsHeadlineSummary("all_clear", items, statuses, asOf)
}

func opsSortAttention(items []opsAttentionItem) {
	priority := map[string]int{"approval": 0, "episode": 1, "stalled": 2, "budget": 3, "exception": 4, "attention": 5}
	sort.SliceStable(items, func(i, j int) bool {
		a, b := items[i], items[j]
		if opsSeverityRank(a.Severity) != opsSeverityRank(b.Severity) {
			return opsSeverityRank(a.Severity) > opsSeverityRank(b.Severity)
		}
		if priority[a.Kind] != priority[b.Kind] {
			return priority[a.Kind] < priority[b.Kind]
		}
		if !a.FirstSeen.Equal(b.FirstSeen) {
			return a.FirstSeen.Before(b.FirstSeen)
		}
		return a.Key < b.Key
	})
}

func opsApplyContainment(db *gorm.DB, statuses []opsMemberStatus) error {
	var policy models.SystemAutopilotPolicy
	err := db.Where("scope='platform'").First(&policy).Error
	if err != nil && err != gorm.ErrRecordNotFound {
		return err
	}
	disabled := containmentDisabledSet(policy)
	for i := range statuses {
		for _, sibling := range systemSiblingAutopilots {
			if sibling.Key == statuses[i].MemberKey {
				statuses[i].Containment.Registered = true
				if err == nil {
					v := disabled[sibling.Key]
					statuses[i].Containment.OptedOut = &v
				}
			}
		}
	}
	return nil
}

func opsHeadlineSummary(headline string, items []opsAttentionItem, statuses []opsMemberStatus, now time.Time) string {
	approvals, visible, suppressed, observable, current, unknown := 0, 0, 0, 0, 0, 0
	oldest := time.Duration(0)
	stalled := map[string]bool{}
	for _, item := range items {
		if item.Snoozed {
			suppressed++
			continue
		}
		visible++
		if item.Kind == "approval" {
			approvals++
			if !item.FirstSeen.IsZero() && now.Sub(item.FirstSeen) > oldest {
				oldest = now.Sub(item.FirstSeen)
			}
		}
	}
	for _, s := range statuses {
		if s.Liveness == "observable" {
			observable++
			if s.State == "idle" || s.State == "running" {
				current++
			}
		}
		if s.Liveness == "unknown" && s.State != "unconfigured" {
			unknown++
		}
		if s.State == "stalled" || s.State == "run_stuck" {
			stalled[s.MemberKey] = true
		}
	}
	parts := []string{fmt.Sprintf("%d fleet items need attention", visible)}
	if headline == "all_clear" {
		parts[0] = "No open fleet attention"
	}
	if approvals > 0 {
		parts = append(parts, fmt.Sprintf("%d approvals waiting (oldest %.0fh)", approvals, oldest.Hours()))
	}
	if len(stalled) > 0 {
		parts = append(parts, fmt.Sprintf("%d members stalled or stuck", len(stalled)))
	}
	parts = append(parts, fmt.Sprintf("%d/%d observable lanes current", current, observable))
	if unknown > 0 {
		parts = append(parts, fmt.Sprintf("%d lanes lack liveness evidence", unknown))
	}
	if suppressed > 0 {
		parts = append(parts, fmt.Sprintf("%d items snoozed", suppressed))
	}
	return strings.Join(parts, " · ") + "."
}
