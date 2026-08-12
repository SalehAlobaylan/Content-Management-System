package supply

import (
	"testing"
	"time"
)

func TestReconcileSourceRunWorkRejectsInvalidBatch(t *testing.T) {
	if _, err := ReconcileSourceRunWork(nil, 1); err == nil {
		t.Fatal("expected nil database to be rejected")
	}
}

func TestReconcilerWorkerHealthRequiresCurrentHeartbeat(t *testing.T) {
	original := reconcilerWorkerLastHeartbeat.Load()
	t.Cleanup(func() { reconcilerWorkerLastHeartbeat.Store(original) })

	now := time.Now().UTC()
	reconcilerWorkerLastHeartbeat.Store(0)
	if ReconcilerWorkerHealthy(now) {
		t.Fatal("worker without heartbeat must be unhealthy")
	}
	reconcilerWorkerLastHeartbeat.Store(now.Add(-reconcilerWorkerLease * 2).UnixNano())
	if !ReconcilerWorkerHealthy(now) {
		t.Fatal("heartbeat at the bounded freshness edge must be healthy")
	}
	reconcilerWorkerLastHeartbeat.Store(now.Add(-reconcilerWorkerLease*2 - time.Nanosecond).UnixNano())
	if ReconcilerWorkerHealthy(now) {
		t.Fatal("stale heartbeat must be unhealthy")
	}
	reconcilerWorkerLastHeartbeat.Store(now.Add(time.Second).UnixNano())
	if ReconcilerWorkerHealthy(now) {
		t.Fatal("future heartbeat must not mask worker clock skew")
	}
}
