package workload

import (
	"errors"
	"testing"
	"time"

	"content-management-system/src/models"
)

func TestDisabledCacheNeverUsesStaleOrEnforcingPolicy(t *testing.T) {
	now := time.Now().UTC()
	cache := &disabledPolicyCache{policy: defaultPolicy(), checkedAt: now, available: true, journal: make(chan models.WorkloadDecision, 1)}
	e := &Engine{Clock: func() time.Time { return now }, offCache: cache}
	r := validRequest("text_embedding", 1)
	// There is deliberately no database. A fresh Off policy must be cheap.
	d := e.RequestPermit(r)
	if !d.Allowed() || !d.Bypass || d.Unavailable || d.PermitID != "" || d.Mode != ModeOff {
		t.Fatalf("off bypass: %+v", d)
	}
	entry := <-cache.journal
	if entry.Event != "request" || entry.System != r.System || entry.Mode != ModeOff {
		t.Fatalf("lost off provenance: %+v", entry)
	}
	cache.checkedAt = now.Add(-6 * time.Second)
	if _, ok := e.cachedDisabled(r); ok {
		t.Fatal("stale Off policy bypassed durable read")
	}
	cache.checkedAt = now
	cache.policy.Mode = ModeEnforce
	if _, ok := e.cachedDisabled(r); ok {
		t.Fatal("Enforce policy used Off bypass")
	}
	cache.policy.Mode = ModeOff
	cache.available = false
	if d, ok := e.cachedDisabled(r); !ok || !d.Unavailable || !d.Allowed() {
		t.Fatalf("unavailable policy was not explicit fail-open: %+v", d)
	}
}

func TestDisabledCacheOverflowIsVisibleAndCommittedPolicyInvalidatesBypass(t *testing.T) {
	now := time.Now().UTC()
	cache := &disabledPolicyCache{policy: defaultPolicy(), checkedAt: now, available: true, journal: make(chan models.WorkloadDecision, 1)}
	e := &Engine{Clock: func() time.Time { return now }, offCache: cache}
	before := disabledJournalLoss.Load()
	e.RequestPermit(validRequest("image_embedding", 1))
	e.RequestPermit(validRequest("image_embedding", 1))
	if disabledJournalLoss.Load() != before+1 {
		t.Fatal("bounded journal lost entries invisibly")
	}
	configured.Lock()
	previous := configured.offCache
	configured.offCache = cache
	configured.Unlock()
	t.Cleanup(func() { configured.Lock(); configured.offCache = previous; configured.Unlock() })
	policy := defaultPolicy()
	policy.Mode = ModeObserve
	policy.Revision = 2
	cacheUpdatedPolicy(policy)
	if _, ok := e.cachedDisabled(validRequest("image_embedding", 1)); ok {
		t.Fatal("committed Observe policy still bypassed")
	}
	cacheUpdatedPolicy(defaultPolicy())
	if cache.policy.Revision != 2 {
		t.Fatal("older refresh replaced a committed policy")
	}
}

func TestDisabledPolicyAllowsExpiredUnstartedOwnerButPreservesRevocation(t *testing.T) {
	now := time.Now().UTC()
	for _, state := range []string{"expired", "revoked", "released"} {
		p := models.WorkloadPermit{State: state, ExpiresAt: now.Add(-time.Hour)}
		err := begin(nil, &p, defaultPolicy(), now)
		if state == "expired" && err != nil {
			t.Fatalf("disabled governor stranded native backlog: %v", err)
		}
		if state != "expired" && !errors.Is(err, ErrNotAdmitted) {
			t.Fatalf("disabled governor ignored terminal %s: %v", state, err)
		}
	}
}
