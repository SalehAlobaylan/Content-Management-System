package workload

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"content-management-system/src/models"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

func RequestPermit(db *gorm.DB, r Request) Decision { return NewEngine(db).RequestPermit(r) }

func (e *Engine) RequestPermit(r Request) (decision Decision) {
	if r.Origin == "" {
		r.Origin = OriginAutomation
	}
	if err := validateRequest(r); err != nil {
		return Decision{Verdict: "blocked", WouldVerdict: "blocked", Reasons: []string{"invalid_request"}, GrantedUnits: 0}
	}
	if cached, ok := e.cachedDisabled(r); ok {
		return cached
	}
	decision = unavailableDecisionWithoutMetric(r)
	defer func() {
		if p := recover(); p != nil {
			decision = unavailable(r, "evaluation_panic")
		}
	}()
	class, _ := Class(r.Class)
	err := isolated(e.DB, func(tx *gorm.DB) error {
		if err := lockPolicy(tx, false); err != nil {
			return err
		}
		policy, err := policyIn(tx)
		if err != nil {
			return err
		}
		now := e.now()
		mode := effectiveMode(policy, now)
		if mode == ModeOff {
			decision = Decision{Verdict: "admit", WouldVerdict: "admit", GrantedUnits: r.Units, Mode: ModeOff, Bypass: true, Reasons: []string{"bypass:disabled"}}
			// Off mode has no active reservation but still leaves bounded useful
			// admission evidence when the ledger is available.
			return ledger(tx, models.WorkloadPermit{TenantID: r.TenantID, System: r.System, Class: r.Class, RequestedUnits: r.Units, GrantedUnits: r.Units, Mode: mode, Verdict: decision.Verdict, WouldVerdict: decision.WouldVerdict, Reasons: jsonValue(decision.Reasons), PolicyRevision: policy.Revision}, "request", map[string]any{"origin": r.Origin, "bypass": true}, "", now)
		}
		resourcesLocked, lockErr := tryLockResources(tx, class.Resources)
		if lockErr != nil {
			return lockErr
		}
		if !resourcesLocked && mode == ModeEnforce && enforced(policy, r.Class) && r.Origin != OriginHuman {
			decision = Decision{Verdict: "defer", WouldVerdict: "defer", GrantedUnits: 0, Mode: ModeEnforce, Reasons: []string{"capacity_allocation_busy"}}
			return ledger(tx, models.WorkloadPermit{TenantID: r.TenantID, System: r.System, Class: r.Class, RequestedUnits: r.Units, Mode: mode, Verdict: "defer", WouldVerdict: "defer", Reasons: jsonValue(decision.Reasons), PolicyRevision: policy.Revision}, "request", map[string]any{"origin": r.Origin, "run_id": r.RunID, "owner_kind": r.OwnerKind, "owner_id": r.OwnerID}, "", now)
		}
		key := requestIdentity(r)
		if err = tx.Exec("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))", "workload-request/v1/"+r.TenantID+"/"+key).Error; err != nil {
			return err
		}
		var previous models.WorkloadPermit
		var reusable *models.WorkloadPermit
		err = tx.Where("tenant_id=? AND request_key=?", r.TenantID, key).First(&previous).Error
		if err == nil {
			if previous.System != r.System || previous.Class != r.Class || previous.OwnerKind != r.OwnerKind || previous.OwnerID != r.OwnerID || previous.RequestedUnits != r.Units {
				return fmt.Errorf("%w: request key identity mismatch", ErrInvalid)
			}
			if previous.BeganAt == nil && (previous.State == "expired" || (previous.State == StateAcquired && (!previous.ExpiresAt.After(now) || previous.PolicyRevision != policy.Revision))) {
				// An authoritative native owner may reacquire safely unstarted work
				// after downtime. Begun, uncertain, settled and human-revoked rows
				// never use this path. The request lock prevents duplicate renewal.
				reusable = &previous
				if err = tx.Model(&previous).Update("state", "expired").Error; err != nil {
					return err
				}
			} else {
				decision = decisionFromPermit(previous)
				return nil
			}
		}
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		snapshots, err := e.ensureSnapshots(tx, class.Resources, policy, now)
		if err != nil {
			return err
		}
		reserved, err := reservedUnits(tx, class.Resources, now)
		if err != nil {
			return err
		}
		spend := SpendSignal{Available: true, State: "normal", ObservedAt: now}
		if class.SpendOwner != "" {
			spend = SpendSignal{State: "unknown", Reason: "tenant_spend_unavailable"}
			if e.SpendProvider != nil {
				var providerErr error
				spend, providerErr = e.SpendProvider(tx.Statement.Context, tx, r)
				if providerErr != nil {
					spend = SpendSignal{State: "unknown", Reason: "tenant_spend_unavailable"}
				}
			}
		}
		would, units, reasons, unknown := evaluate(r, class, policy, snapshots, reserved, spend, now)
		if !resourcesLocked && !unknown {
			would = "defer"
			units = 0
			reasons = append(reasons, "capacity_allocation_busy")
		}
		actualMode := mode
		if mode == ModeEnforce && !enforced(policy, r.Class) {
			actualMode = ModeObserve
			reasons = append(reasons, "class_observe")
		}
		granted, verdict, bypass := units, would, false
		if actualMode == ModeObserve {
			granted = r.Units
			verdict = "admit"
		}
		if r.Origin == OriginHuman {
			granted = r.Units
			verdict = "admit"
			bypass = true
			reasons = append(reasons, "bypass:human")
		}
		if unknown {
			granted = r.Units
			verdict = "admit"
			bypass = true
			reasons = append(reasons, "governor_unavailable")
		}
		decision = Decision{GrantedUnits: granted, Verdict: verdict, WouldVerdict: would, Reasons: reasons, Mode: actualMode, Bypass: bypass, Unavailable: unknown}
		p := models.WorkloadPermit{ID: uuid.NewString(), TenantID: r.TenantID, RequestKey: key, System: r.System, Class: r.Class, RunID: r.RunID, Origin: r.Origin, OwnerKind: r.OwnerKind, OwnerID: r.OwnerID, RequestedUnits: r.Units, GrantedUnits: granted, State: StateAcquired, Mode: actualMode, Verdict: verdict, WouldVerdict: would, Reasons: jsonValue(reasons), Bypass: bypass, PolicyRevision: policy.Revision, CreatedAt: now, UpdatedAt: now, ExpiresAt: now.Add(time.Duration(policy.LeaseSeconds) * time.Second)}
		weights := map[string]int{}
		for key, weight := range class.Weights {
			weights[key] = weight * granted
		}
		p.Reservations = jsonValue(weights)
		if granted > 0 {
			if reusable != nil {
				p.ID = reusable.ID
				err = tx.Save(&p).Error
			} else {
				err = tx.Create(&p).Error
			}
			if err != nil {
				return err
			}
			decision.PermitID = p.ID
			decision.ExpiresAt = &p.ExpiresAt
		} else {
			p.ID = ""
		}
		return ledger(tx, p, "request", map[string]any{"origin": r.Origin, "run_id": r.RunID, "owner_kind": r.OwnerKind, "owner_id": r.OwnerID, "resources": snapshots, "reserved": reserved, "spend": spend, "unavailable": unknown}, "", now)
	})
	if err != nil {
		if e.failureObserver != nil {
			e.failureObserver(err)
		}
		if errors.Is(err, ErrInvalid) {
			return Decision{Verdict: "blocked", WouldVerdict: "blocked", Reasons: []string{"invalid_request_identity"}}
		}
		return unavailable(r, "admission_store_failed")
	}
	if decision.Unavailable {
		recordUnavailable()
	}
	return decision
}

func unavailableDecisionWithoutMetric(r Request) Decision {
	return Decision{Verdict: "admit", WouldVerdict: "unknown", GrantedUnits: r.Units, Reasons: []string{"governor_unavailable"}, Bypass: true, Unavailable: true}
}

func lockResources(tx *gorm.DB, keys []string) error {
	ordered := append([]string{}, keys...)
	sort.Strings(ordered)
	for _, key := range ordered {
		if err := tx.Exec("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))", "workload-resource/v1/"+key).Error; err != nil {
			return err
		}
	}
	return nil
}

func tryLockResources(tx *gorm.DB, keys []string) (bool, error) {
	ordered := append([]string{}, keys...)
	sort.Strings(ordered)
	for _, key := range ordered {
		var locked bool
		if err := tx.Raw("SELECT pg_try_advisory_xact_lock(hashtextextended(?, 0))", "workload-resource/v1/"+key).Scan(&locked).Error; err != nil {
			return false, err
		}
		if !locked {
			return false, nil
		}
	}
	return true, nil
}

func reservedUnits(tx *gorm.DB, keys []string, now time.Time) (map[string]int, error) {
	// jsonb_each_text reads only the bounded code-owned resource map. Running,
	// handed-off and uncertain effects retain capacity beyond their lease TTL.
	var totals []struct {
		Resource string
		Units    int
	}
	err := tx.Raw(`SELECT r.key AS resource, COALESCE(SUM(r.value::bigint),0)::integer AS units
FROM workload_permits p CROSS JOIN LATERAL jsonb_each_text(p.reservations) r
WHERE r.key IN ? AND (p.state IN ('running','handed_off','uncertain') OR (p.state='acquired' AND p.expires_at>?))
GROUP BY r.key`, keys, now).Scan(&totals).Error
	result := map[string]int{}
	for _, v := range totals {
		result[v.Resource] = v.Units
	}
	return result, err
}

func evaluate(r Request, class ActionClass, p Policy, snapshots map[string]models.WorkloadCapacitySnapshot, reserved map[string]int, spend SpendSignal, now time.Time) (string, int, []string, bool) {
	units := r.Units
	reasons := []string{}
	unknown := false
	limits := limitsFor(p)
	for _, key := range class.Resources {
		s, ok := snapshots[key]
		if !ok || !fresh(s.ObservedAt, now, p.SnapshotMaxAgeSeconds) || !s.Available || s.State == "unknown" {
			unknown = true
			reasons = append(reasons, key+":unknown")
			continue
		}
		resource, _ := resourceByKey(key)
		switch s.State {
		case "saturated":
			units = 0
			reasons = append(reasons, key+":saturated")
		case "constrained", "recovering":
			units = min(units, max(1, r.Units/2))
			reasons = append(reasons, key+":"+s.State)
		}
		if resource.Kind == "slots" {
			available := max(0, limits[key].Capacity-reserved[key])
			weight := class.Weights[key]
			if weight > 0 {
				units = min(units, available/weight)
				if available < r.Units*weight {
					reasons = append(reasons, key+":reserved_capacity")
				}
			}
		}
	}
	if class.SpendOwner != "" {
		if !spend.Available || !fresh(spend.ObservedAt, now, p.SnapshotMaxAgeSeconds) || spend.State == "unknown" || spend.State == "" {
			unknown = true
			reasons = append(reasons, "tenant_spend:unknown")
		} else if spend.State == "capped" {
			units = 0
			reasons = append(reasons, "tenant_spend:capped")
		} else if spend.State == "warning" {
			units = min(units, max(1, r.Units/2))
			reasons = append(reasons, "tenant_spend:warning")
		} else if spend.State != "normal" {
			unknown = true
			reasons = append(reasons, "tenant_spend:unknown")
		}
	}
	if unknown {
		return "unknown", r.Units, reasons, true
	}
	if len(reasons) == 0 {
		reasons = append(reasons, "capacity_available")
	}
	if units <= 0 {
		return "defer", 0, reasons, false
	}
	if units < r.Units {
		return "admit_reduced", units, reasons, false
	}
	return "admit", units, reasons, false
}

func fresh(observed, now time.Time, maxAge int) bool {
	return !observed.IsZero() && !observed.After(now.Add(5*time.Second)) && now.Sub(observed) <= time.Duration(maxAge)*time.Second
}

func latestSnapshots(tx *gorm.DB, keys []string) (map[string]models.WorkloadCapacitySnapshot, error) {
	var rows []models.WorkloadCapacitySnapshot
	err := tx.Raw("SELECT DISTINCT ON (resource) * FROM workload_capacity_snapshots WHERE resource IN ? ORDER BY resource, id DESC", keys).Scan(&rows).Error
	result := map[string]models.WorkloadCapacitySnapshot{}
	for _, row := range rows {
		result[row.Resource] = row
	}
	return result, err
}

func (e *Engine) ensureSnapshots(tx *gorm.DB, keys []string, p Policy, now time.Time) (map[string]models.WorkloadCapacitySnapshot, error) {
	latest, err := latestSnapshots(tx, keys)
	if err != nil {
		return nil, err
	}
	stale := []string{}
	for _, key := range keys {
		s, ok := latest[key]
		if !ok || !fresh(s.ObservedAt, now, p.SnapshotMaxAgeSeconds) {
			stale = append(stale, key)
		}
	}
	if len(stale) == 0 || e.SampleProvider == nil {
		return latest, nil
	}
	ctx, cancel := context.WithTimeout(tx.Statement.Context, 500*time.Millisecond)
	defer cancel()
	samples, providerErr := e.SampleProvider(ctx, stale)
	if ctx.Err() != nil {
		providerErr = ctx.Err()
	}
	for _, key := range stale {
		s, ok := samples[key]
		if providerErr != nil || !ok {
			s = Sample{Reason: "sample_unavailable"}
		}
		row := snapshotFromSample(key, s, latest[key], p, now)
		if err = tx.Create(&row).Error; err != nil {
			return nil, err
		}
		latest[key] = row
	}
	return latest, nil
}

func snapshotFromSample(key string, s Sample, previous models.WorkloadCapacitySnapshot, p Policy, now time.Time) models.WorkloadCapacitySnapshot {
	state := "normal"
	reason := s.Reason
	resource, _ := resourceByKey(key)
	limit := limitsFor(p)[key]
	if !s.Available || !fresh(s.ObservedAt, now, p.SnapshotMaxAgeSeconds) || math.IsNaN(s.Pressure) || math.IsInf(s.Pressure, 0) || s.Pressure < 0 {
		state = "unknown"
		if reason == "" {
			reason = "observation_unavailable"
		}
	} else if !s.Healthy || s.Incident || (resource.Kind == "pressure" && s.Pressure >= limit.Hard) {
		state = "saturated"
	} else if resource.Kind == "pressure" && s.Pressure >= limit.Soft {
		state = "constrained"
	}
	var recovering *time.Time
	evidence := map[string]any{}
	for k, v := range s.Evidence {
		evidence[k] = v
	}
	// Three consecutive materially growing queue observations constrain new
	// admission before the absolute floor is reached. It is deterministic and
	// never treats missing/stale observations as a healthy growth baseline.
	if resource.Kind == "pressure" && state != "unknown" {
		streak := 0
		if previous.Available && fresh(previous.ObservedAt, now, p.SnapshotMaxAgeSeconds*2) && s.Pressure-previous.Pressure >= math.Max(1, limit.Hard*0.05) {
			var prior map[string]any
			_ = json.Unmarshal(previous.Evidence, &prior)
			if n, ok := prior["growth_streak"].(float64); ok {
				streak = int(n)
			}
			streak++
		}
		evidence["growth_streak"] = streak
		if streak >= 3 && state == "normal" {
			state = "constrained"
			reason = "sustained_queue_growth"
		}
	}
	if state != "saturated" && state != "unknown" {
		if previous.State == "saturated" || previous.State == "unknown" {
			since := now
			recovering = &since
		} else if previous.State == "recovering" {
			recovering = previous.RecoverySince
			if !fresh(previous.ObservedAt, now, p.SnapshotMaxAgeSeconds*2) {
				since := now
				recovering = &since
			}
		}
		if recovering != nil && now.Sub(*recovering) < time.Duration(p.RecoverySeconds)*time.Second {
			state = "recovering"
		} else {
			recovering = nil
		}
	}
	pressure := s.Pressure
	if math.IsNaN(pressure) || math.IsInf(pressure, 0) || pressure < 0 {
		pressure = 0
	}
	return models.WorkloadCapacitySnapshot{Resource: key, State: state, Available: s.Available && state != "unknown", Healthy: s.Available && s.Healthy && state != "unknown", Pressure: pressure, Reason: reason, ObservedAt: s.ObservedAt, CreatedAt: now, RecoverySince: recovering, Evidence: jsonValue(evidence)}
}

func RefreshSnapshots(db *gorm.DB) error { return NewEngine(db).RefreshSnapshots() }
func (e *Engine) RefreshSnapshots() error {
	if e.DB == nil {
		return errors.New("governor database unavailable")
	}
	if e.SampleProvider == nil {
		return errors.New("workload sample provider unavailable")
	}
	// Collection and persistence share one background deadline. Sampling uses
	// several reads against the remote database; a separate two-second cutoff
	// used to discard whole observations during otherwise recoverable latency.
	ctx, cancel := boundedContext(e.DB, 8*time.Second)
	defer cancel()
	keys := resourceKeys()
	samples, err := e.SampleProvider(ctx, keys)
	if err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	// Do not hold allocation locks while collecting evidence. Re-read policy
	// and prior snapshots under the locks before deriving the new states.
	return isolatedFor(e.DB.WithContext(ctx), 8*time.Second, func(tx *gorm.DB) error {
		if err := lockPolicy(tx, false); err != nil {
			return err
		}
		p, err := policyIn(tx)
		if err != nil {
			return err
		}
		if err = lockResources(tx, keys); err != nil {
			return err
		}
		previous, err := latestSnapshots(tx, keys)
		if err != nil {
			return err
		}
		now := e.now()
		rows := make([]models.WorkloadCapacitySnapshot, 0, len(keys))
		for _, key := range keys {
			s := samples[key]
			// A native admission may have refreshed this resource during
			// collection. Never replace its newer observation with older data.
			if !s.ObservedAt.IsZero() && previous[key].ObservedAt.After(s.ObservedAt) {
				continue
			}
			row := snapshotFromSample(key, s, previous[key], p, now)
			rows = append(rows, row)
		}
		if len(rows) == 0 {
			return nil
		}
		return tx.Create(&rows).Error
	})
}

func parseReservations(p models.WorkloadPermit) map[string]int {
	var weights map[string]int
	_ = json.Unmarshal(p.Reservations, &weights)
	return weights
}
