package workload

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"content-management-system/src/models"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func GetStatus(db *gorm.DB) (Status, error) { return NewEngine(db).Status() }

func (e *Engine) Status() (Status, error) {
	status := Status{Resources: []ResourceStatus{}, Attention: []Attention{}, Coverage: Registry().Coverage, Reasons: []string{}, UnavailableCount: UnavailableCount()}
	status.DisabledJournalLoss = disabledJournalLoss.Load()
	if status.DisabledJournalLoss > 0 {
		status.Reasons = append(status.Reasons, "disabled_admission_journal_incomplete")
	}
	err := isolatedFor(e.DB, 8*time.Second, func(tx *gorm.DB) error {
		p, err := policyIn(tx)
		if err != nil {
			return err
		}
		now := e.now()
		status.Policy = p
		status.Mode = p.Mode
		status.EffectiveMode = effectiveMode(p, now)
		status.Available = true
		latest, err := latestSnapshots(tx, resourceKeys())
		if err != nil {
			return err
		}
		reserved, err := reservedUnits(tx, resourceKeys(), now)
		if err != nil {
			return err
		}
		limits := limitsFor(p)
		for _, r := range resources {
			s, ok := latest[r.Key]
			state := "unknown"
			isFresh := ok && fresh(s.ObservedAt, now, p.SnapshotMaxAgeSeconds)
			if ok && isFresh && s.Available {
				state = s.State
			}
			var observed *time.Time
			if !s.ObservedAt.IsZero() {
				copy := s.ObservedAt
				observed = &copy
			}
			status.Resources = append(status.Resources, ResourceStatus{Key: r.Key, Label: r.Label, Kind: r.Kind, State: state, Fresh: isFresh, Available: isFresh && s.Available, Healthy: isFresh && s.Available && s.Healthy, ObservedAt: observed, Pressure: s.Pressure, Reserved: reserved[r.Key], Limit: limits[r.Key], Reason: s.Reason})
			if ok && (status.HeartbeatAt == nil || s.CreatedAt.After(*status.HeartbeatAt)) {
				copy := s.CreatedAt
				status.HeartbeatAt = &copy
			}
		}
		if err = tx.Model(&models.WorkloadPermit{}).Where("state IN ?", []string{StateAcquired, StateRunning, StateHandedOff, StateUncertain}).Count(&status.Summary.ActivePermits).Error; err != nil {
			return err
		}
		if err = tx.Model(&models.WorkloadPermit{}).Where("state='uncertain'").Count(&status.Summary.UncertainPermits).Error; err != nil {
			return err
		}
		if err = tx.Model(&models.WorkloadPermit{}).Where("state IN ? AND expires_at<=?", []string{StateRunning, StateHandedOff, StateUncertain}, now).Count(&status.Summary.OverduePermits).Error; err != nil {
			return err
		}
		var totals struct {
			Decisions   int64
			WouldDefer  int64
			Unavailable int64
		}
		if err = tx.Raw(`SELECT COUNT(*) AS decisions, COUNT(*) FILTER(WHERE would_verdict IN ('defer','blocked')) AS would_defer, COUNT(*) FILTER(WHERE would_verdict='unknown') AS unavailable FROM workload_decisions WHERE event='request' AND created_at>=?`, now.Add(-24*time.Hour)).Scan(&totals).Error; err != nil {
			return err
		}
		status.Summary.Decisions24h = totals.Decisions
		status.Summary.WouldDefer24h = totals.WouldDefer
		status.Summary.Unavailable24h = totals.Unavailable
		var callers []struct {
			System          string
			Requests        int64
			LastRequestedAt *time.Time
		}
		if err = tx.Raw(`SELECT system,COUNT(*) AS requests,MAX(created_at) AS last_requested_at FROM workload_decisions WHERE event='request' AND created_at>=? GROUP BY system`, now.Add(-24*time.Hour)).Scan(&callers).Error; err != nil {
			return err
		}
		for i := range status.Coverage {
			for _, caller := range callers {
				if status.Coverage[i].System == caller.System {
					status.Coverage[i].Requests24h = caller.Requests
					status.Coverage[i].LastRequestedAt = caller.LastRequestedAt
					status.Coverage[i].RuntimeObserved = caller.Requests > 0
					break
				}
			}
		}
		// Continuous deferral starts after the most recent successful enforced
		// grant; observation-only hypothetical deferrals never claim starvation.
		if err = tx.Raw(`SELECT d.tenant_id,d.system,d.class,MIN(d.created_at) AS since,COUNT(*) AS count,'continuous_deferral' AS reason
FROM workload_decisions d WHERE d.event='request' AND d.mode='enforce' AND d.granted_units=0 AND d.created_at>=?
AND d.created_at>COALESCE((SELECT MAX(g.created_at) FROM workload_decisions g WHERE g.event='request' AND g.mode='enforce' AND g.tenant_id=d.tenant_id AND g.system=d.system AND g.class=d.class AND g.granted_units>0),'epoch'::timestamptz)
GROUP BY d.tenant_id,d.system,d.class HAVING MIN(d.created_at)<=? ORDER BY MIN(d.created_at) LIMIT 100`, now.Add(-7*24*time.Hour), now.Add(-time.Duration(p.StarvationMinutes)*time.Minute)).Scan(&status.Attention).Error; err != nil {
			return err
		}
		if status.Summary.OverduePermits > 0 {
			status.Reasons = append(status.Reasons, "unresolved_owner_effects_require_terminal_proof")
		}
		for _, r := range status.Resources {
			if r.State == "unknown" {
				status.Reasons = append(status.Reasons, "resource_evidence_unavailable")
				break
			}
		}
		return nil
	})
	return status, err
}

func boundedLimit(n int) int {
	if n < 1 {
		return 50
	}
	return min(n, 200)
}

func ListPermits(db *gorm.DB, f ListFilter) ([]models.WorkloadPermit, error) {
	rows := []models.WorkloadPermit{}
	err := isolatedFor(db, 8*time.Second, func(tx *gorm.DB) error {
		q := tx.Model(&models.WorkloadPermit{})
		if f.TenantID != "" {
			q = q.Where("tenant_id=?", f.TenantID)
		}
		if f.System != "" {
			q = q.Where("system=?", f.System)
		}
		if f.Class != "" {
			q = q.Where("class=?", f.Class)
		}
		if f.State != "" {
			q = q.Where("state=?", f.State)
		}
		if f.Verdict != "" {
			q = q.Where("verdict=?", f.Verdict)
		}
		if f.BeforeTime != nil && f.BeforePermitID != "" {
			q = q.Where("(created_at,id)<(?,?::uuid)", *f.BeforeTime, f.BeforePermitID)
		}
		return q.Order("created_at DESC, id DESC").Limit(boundedLimit(f.Limit)).Find(&rows).Error
	})
	return rows, err
}

func ListDecisions(db *gorm.DB, f ListFilter) ([]models.WorkloadDecision, error) {
	rows := []models.WorkloadDecision{}
	err := isolatedFor(db, 8*time.Second, func(tx *gorm.DB) error {
		q := tx.Model(&models.WorkloadDecision{})
		if f.TenantID != "" {
			q = q.Where("tenant_id=?", f.TenantID)
		}
		if f.System != "" {
			q = q.Where("system=?", f.System)
		}
		if f.Class != "" {
			q = q.Where("class=?", f.Class)
		}
		if f.Verdict != "" {
			q = q.Where("verdict=?", f.Verdict)
		}
		if f.BeforeID > 0 {
			q = q.Where("id<?", f.BeforeID)
		}
		return q.Order("id DESC").Limit(boundedLimit(f.Limit)).Find(&rows).Error
	})
	return rows, err
}

func ListSnapshots(db *gorm.DB, limit int, resource ...string) ([]models.WorkloadCapacitySnapshot, error) {
	rows := []models.WorkloadCapacitySnapshot{}
	err := isolatedFor(db, 8*time.Second, func(tx *gorm.DB) error {
		if len(resource) > 0 && resource[0] != "" {
			if _, ok := resourceByKey(resource[0]); !ok {
				return ErrInvalid
			}
			tx = tx.Where("resource=?", resource[0])
		}
		return tx.Order("id DESC").Limit(boundedLimit(limit)).Find(&rows).Error
	})
	return rows, err
}

func GetQualification(db *gorm.DB) (Qualification, error) { return NewEngine(db).Qualification() }
func (e *Engine) Qualification() (Qualification, error) {
	var q Qualification
	err := isolatedFor(e.DB, 8*time.Second, func(tx *gorm.DB) error {
		p, err := policyIn(tx)
		if err != nil {
			return err
		}
		q, err = qualificationIn(tx, p, e.now())
		return err
	})
	return q, err
}

func qualificationIn(tx *gorm.DB, p Policy, now time.Time) (Qualification, error) {
	// Closed UTC hours produce a stable, reviewable evidence digest while live
	// requests continue. Promotion separately rechecks fresh current resources.
	end := now.Truncate(time.Hour)
	start := end.Add(-7 * 24 * time.Hour)
	q := Qualification{WindowStart: start, WindowEnd: end, RequiredHours: p.ObservationHours, RequiredRequests: p.MinClassRequests, Classes: []ClassQualification{}, Resources: []ResourceEvidence{}, TotalResources: len(resources), Evidence: []string{"closed_utc_hours", "minimum_24h_observe_window", "minimum_20_requests_per_class", "minimum_5_verified_completions_per_class", "every_registered_caller_exercised", "observed_capacity_collision", "zero_unresolved_effects", "current_fresh_resource_evidence", "native_owner_guards_remain_authoritative", "human_review_of_delivery_and_service_reliability_required"}}
	latest, err := latestSnapshots(tx, resourceKeys())
	if err != nil {
		return q, err
	}
	for _, r := range resources {
		s := latest[r.Key]
		if s.Available && fresh(s.ObservedAt, now, p.SnapshotMaxAgeSeconds) {
			q.FreshResources++
		}
	}
	if err = tx.Raw(`SELECT resource,COUNT(*) AS samples,COUNT(*) FILTER(WHERE state='unknown') AS unknown,COUNT(*) FILTER(WHERE state='saturated') AS saturated,COUNT(*) FILTER(WHERE reason='sustained_queue_growth') AS growth_alarms,COALESCE(MAX(pressure),0) AS max_pressure FROM workload_capacity_snapshots WHERE created_at>=? AND created_at<? GROUP BY resource ORDER BY resource`, start, end).Scan(&q.Resources).Error; err != nil {
		return q, err
	}
	var callerCounts []struct {
		System   string
		Class    string
		Requests int64
	}
	if err = tx.Raw(`SELECT system,class,COUNT(*) AS requests FROM workload_decisions WHERE event='request' AND mode='observe' AND COALESCE(evidence->>'origin','automation')<>'human' AND created_at>=? AND created_at<? GROUP BY system,class`, start, end).Scan(&callerCounts).Error; err != nil {
		return q, err
	}
	var totals []struct {
		Class           string
		Requests        int64
		WouldDefer      int64
		Unavailable     int64
		FirstObservedAt *time.Time
	}
	if err = tx.Raw(`SELECT class,COUNT(*) AS requests,COUNT(*) FILTER(WHERE would_verdict IN ('defer','blocked')) AS would_defer,COUNT(*) FILTER(WHERE would_verdict='unknown') AS unavailable,MIN(created_at) AS first_observed_at FROM workload_decisions WHERE event='request' AND mode='observe' AND COALESCE(evidence->>'origin','automation')<>'human' AND created_at>=? AND created_at<? GROUP BY class`, start, end).Scan(&totals).Error; err != nil {
		return q, err
	}
	byClass := map[string]ClassQualification{}
	for _, v := range totals {
		byClass[v.Class] = ClassQualification{Class: v.Class, Requests: v.Requests, WouldDefer: v.WouldDefer, Unavailable: v.Unavailable, FirstObservedAt: v.FirstObservedAt}
	}
	var completions []struct {
		Class                 string
		Count                 int64
		MeanCompletionSeconds float64
	}
	if err = tx.Raw(`SELECT class,COUNT(*) AS count,COALESCE(AVG(GREATEST(0,EXTRACT(EPOCH FROM released_at-COALESCE(began_at,created_at)))),0) AS mean_completion_seconds FROM workload_permits WHERE state='released' AND mode='observe' AND origin<>'human' AND actual_units>0 AND released_at>=? AND released_at<? GROUP BY class`, start, end).Scan(&completions).Error; err != nil {
		return q, err
	}
	for _, v := range completions {
		c := byClass[v.Class]
		c.Class = v.Class
		c.Settled = v.Count
		c.MeanCompletionSeconds = v.MeanCompletionSeconds
		byClass[v.Class] = c
	}
	var unresolved []struct {
		Class string
		Count int64
	}
	if err = tx.Raw(`SELECT class,COUNT(*) AS count FROM workload_permits WHERE state='uncertain' OR (state IN ('running','handed_off') AND expires_at<?) GROUP BY class`, now).Scan(&unresolved).Error; err != nil {
		return q, err
	}
	for _, v := range unresolved {
		c := byClass[v.Class]
		c.Class = v.Class
		c.Unresolved = v.Count
		byClass[v.Class] = c
	}
	for _, class := range actionClasses {
		c := byClass[class.Key]
		c.Class = class.Key
		c.Missing = []string{}
		c.Callers = []CallerEvidence{}
		if c.Requests < int64(p.MinClassRequests) {
			c.Missing = append(c.Missing, "insufficient_observed_requests")
		}
		if c.FirstObservedAt == nil || end.Sub(*c.FirstObservedAt) < time.Duration(p.ObservationHours)*time.Hour {
			c.Missing = append(c.Missing, "insufficient_observation_window")
		}
		if c.Settled < 5 {
			c.Missing = append(c.Missing, "insufficient_verified_completions")
		}
		if c.WouldDefer < 1 {
			c.Missing = append(c.Missing, "no_observed_capacity_collision")
		}
		for _, entry := range coverage {
			if entry.Relationship != "governed" {
				continue
			}
			governed := false
			for _, key := range entry.Classes {
				if key == class.Key {
					governed = true
					break
				}
			}
			if !governed {
				continue
			}
			caller := CallerEvidence{System: entry.System}
			for _, v := range callerCounts {
				if v.System == entry.System && v.Class == class.Key {
					caller.Requests = v.Requests
					caller.Observed = v.Requests > 0
					break
				}
			}
			c.Callers = append(c.Callers, caller)
			if !caller.Observed {
				c.Missing = append(c.Missing, "caller:"+entry.System+":unexercised")
			}
		}
		if c.Unavailable > 0 {
			c.Missing = append(c.Missing, "unknown_admission_evidence")
		}
		if c.Unresolved > 0 {
			c.Missing = append(c.Missing, "unresolved_owner_effects")
		}
		for _, key := range class.Resources {
			s := latest[key]
			if !s.Available || !fresh(s.ObservedAt, now, p.SnapshotMaxAgeSeconds) || s.State == "unknown" || s.State == "saturated" {
				c.Missing = append(c.Missing, key+":not_ready")
			}
		}
		c.Eligible = len(c.Missing) == 0
		q.Classes = append(q.Classes, c)
	}
	// Include live readiness and unresolved counts. A newly unavailable resource
	// invalidates a prior review; ordinary refresh timestamps do not.
	q.Digest = hashValue(struct {
		Revision int64
		End      time.Time
		Classes  []ClassQualification
	}{p.Revision, end, q.Classes})
	return q, nil
}

func UpdatePolicy(db *gorm.DB, u PolicyUpdate, actor string) (Policy, error) {
	var result Policy
	if strings.TrimSpace(actor) == "" || len(actor) > 120 || strings.TrimSpace(u.ReviewNote) == "" || len(u.ReviewNote) > 2000 {
		return result, ErrInvalid
	}
	err := isolatedFor(db, 8*time.Second, func(tx *gorm.DB) error {
		if err := lockPolicy(tx, true); err != nil {
			return err
		}
		old, err := policyIn(tx)
		if err != nil {
			return err
		}
		if old.Revision != u.Revision {
			return ErrConflict
		}
		now := time.Now().UTC()
		next := old
		next.Mode = u.Mode
		next.EnforcedClasses = jsonValue(u.EnforcedClasses)
		next.ResourceLimits = jsonValue(u.ResourceLimits)
		next.SnapshotMaxAgeSeconds = u.SnapshotMaxAgeSeconds
		next.RecoverySeconds = u.RecoverySeconds
		next.LeaseSeconds = u.LeaseSeconds
		next.StarvationMinutes = u.StarvationMinutes
		next.UpdatedAt = now
		next.UpdatedBy = actor
		next.Revision++
		if err = validateStoredPolicy(next); err != nil {
			return err
		}
		if next.Mode == ModeEnforce {
			if len(u.EnforcedClasses) == 0 || u.Confirmation != "ENFORCE CAPACITY" {
				return fmt.Errorf("%w: explicit class selection and confirmation required", ErrInvalid)
			}
			q, err := qualificationIn(tx, old, now)
			if err != nil {
				return err
			}
			if u.QualificationDigest == "" || u.QualificationDigest != q.Digest {
				return fmt.Errorf("%w: evidence digest changed", ErrConflict)
			}
			for _, key := range u.EnforcedClasses {
				// Existing enforce classes keep their explicit approval while the
				// unchanged capacity rules remain in force. Any tightening or newly
				// selected class must earn observation evidence.
				already := old.Mode == ModeEnforce && enforced(old, key) && reflect.DeepEqual(limitsFor(old), limitsFor(next))
				if already {
					continue
				}
				eligible := false
				for _, c := range q.Classes {
					if c.Class == key {
						eligible = c.Eligible
						break
					}
				}
				if !eligible {
					return fmt.Errorf("%w: %s", ErrNotQualified, key)
				}
			}
		}
		if err = tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "scope"}}, UpdateAll: true}).Create(&next).Error; err != nil {
			return err
		}
		p := models.WorkloadPermit{TenantID: "platform", System: "workload_governor", Class: "policy", Mode: next.Mode, PolicyRevision: next.Revision, Verdict: "admit", WouldVerdict: "admit", Reasons: jsonValue([]string{"human_policy_review"})}
		if err = ledger(tx, p, "policy_changed", map[string]any{"before": old, "after": next, "review_note": u.ReviewNote, "qualification_digest": u.QualificationDigest}, actor, now); err != nil {
			return err
		}
		result = next
		return nil
	})
	if err == nil {
		cacheUpdatedPolicy(result)
	}
	return result, err
}

func Pause(db *gorm.DB, revision int64, minutes int, actor, reason string) (Policy, error) {
	var result Policy
	if minutes < 0 || minutes > 1440 || strings.TrimSpace(actor) == "" || len(actor) > 120 || strings.TrimSpace(reason) == "" || len(reason) > 2000 {
		return result, ErrInvalid
	}
	err := isolatedFor(db, 8*time.Second, func(tx *gorm.DB) error {
		if err := lockPolicy(tx, true); err != nil {
			return err
		}
		p, err := policyIn(tx)
		if err != nil {
			return err
		}
		if p.Revision != revision {
			return ErrConflict
		}
		now := time.Now().UTC()
		p.PausedUntil = nil
		if minutes > 0 {
			until := now.Add(time.Duration(minutes) * time.Minute)
			p.PausedUntil = &until
		}
		p.Revision++
		p.UpdatedAt = now
		p.UpdatedBy = actor
		if err = tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "scope"}}, UpdateAll: true}).Create(&p).Error; err != nil {
			return err
		}
		if err = ledger(tx, models.WorkloadPermit{TenantID: "platform", System: "workload_governor", Class: "policy", Mode: p.Mode, PolicyRevision: p.Revision, Verdict: "admit", WouldVerdict: "admit", Reasons: jsonValue([]string{"human_emergency_bypass"})}, "policy_paused", map[string]any{"minutes": minutes, "reason": reason, "paused_until": p.PausedUntil}, actor, now); err != nil {
			return err
		}
		result = p
		return nil
	})
	if err == nil {
		cacheUpdatedPolicy(result)
	}
	return result, err
}

// EnabledClasses returns an independent sorted policy projection.
func EnabledClasses(p Policy) []string {
	var result []string
	_ = jsonUnmarshalClasses(p.EnforcedClasses, &result)
	sort.Strings(result)
	return result
}

func jsonUnmarshalClasses(data []byte, target *[]string) error {
	if len(data) == 0 {
		return errors.New("empty classes")
	}
	return json.Unmarshal(data, target)
}
