package workload

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"content-management-system/src/models"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Off is a deliberate bypass, not a reservation transaction. A short policy
// cache and bounded asynchronous journal keep the dormant integration cheap.
// This is explicitly installed for the single runtime database, never inferred
// from native transaction handles or silently shared across test databases.
type disabledPolicyCache struct {
	sync.RWMutex
	policy    Policy
	checkedAt time.Time
	available bool
	journal   chan models.WorkloadDecision
}

var disabledJournalLoss atomic.Uint64

func InstallOffPolicyCache(db *gorm.DB) {
	db = db.Session(&gorm.Session{Logger: db.Logger.LogMode(logger.Silent)})
	cache := &disabledPolicyCache{journal: make(chan models.WorkloadDecision, 256)}
	configured.Lock()
	configured.offCache = cache
	configured.Unlock()
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			cache.RLock()
			due := time.Since(cache.checkedAt) >= 2*time.Second
			cache.RUnlock()
			if due {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				policy, err := policyIn(db.WithContext(ctx))
				cancel()
				cache.Lock()
				cache.checkedAt = time.Now().UTC()
				cache.available = err == nil
				if err == nil && policy.Revision >= cache.policy.Revision {
					cache.policy = policy
				}
				cache.Unlock()
			}
			batch := make([]models.WorkloadDecision, 0, 100)
			for len(batch) < 100 {
				select {
				case row := <-cache.journal:
					batch = append(batch, row)
				default:
					goto flush
				}
			}
		flush:
			if len(batch) > 0 {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				err := db.WithContext(ctx).Session(&gorm.Session{SkipDefaultTransaction: true}).Create(&batch).Error
				cancel()
				if err != nil {
					disabledJournalLoss.Add(uint64(len(batch)))
				}
			}
			<-ticker.C
		}
	}()
}

func cacheUpdatedPolicy(policy Policy) {
	configured.RLock()
	cache := configured.offCache
	configured.RUnlock()
	if cache == nil {
		return
	}
	cache.Lock()
	if policy.Revision >= cache.policy.Revision {
		cache.policy = policy
		cache.checkedAt = time.Now().UTC()
		cache.available = true
	}
	cache.Unlock()
}

func (e *Engine) cachedDisabled(r Request) (Decision, bool) {
	cache := e.offCache
	if cache == nil {
		return Decision{}, false
	}
	cache.RLock()
	p, checked, available := cache.policy, cache.checkedAt, cache.available
	cache.RUnlock()
	now := e.now()
	if checked.IsZero() || checked.After(now.Add(time.Second)) || now.Sub(checked) > 5*time.Second {
		return Decision{}, false
	}
	if !available {
		return unavailable(r, "disabled_policy_store_unavailable"), true
	}
	if effectiveMode(p, now) != ModeOff {
		return Decision{}, false
	}
	d := Decision{Verdict: "admit", WouldVerdict: "admit", GrantedUnits: r.Units, Mode: ModeOff, Bypass: true, Reasons: []string{"bypass:disabled"}}
	row := models.WorkloadDecision{TenantID: r.TenantID, System: r.System, Class: r.Class, Event: "request", Verdict: d.Verdict, WouldVerdict: d.WouldVerdict, RequestedUnits: r.Units, GrantedUnits: r.Units, Reasons: jsonValue(d.Reasons), Evidence: jsonValue(map[string]any{"origin": r.Origin, "run_id": r.RunID, "owner_kind": r.OwnerKind, "owner_id": r.OwnerID, "bypass": true}), Mode: ModeOff, PolicyRevision: p.Revision, CreatedAt: now}
	select {
	case cache.journal <- row:
	default:
		disabledJournalLoss.Add(1)
	}
	return d, true
}
