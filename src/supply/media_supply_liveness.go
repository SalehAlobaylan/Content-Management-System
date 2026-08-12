package supply

import (
	"sync/atomic"
	"time"
)

const MediaSupplyEvaluatorHeartbeatGrace = 15 * time.Minute

var mediaSupplyEvaluatorLastHeartbeat atomic.Int64

// MediaSupplyEvaluatorWorkerStatus is process-local liveness evidence for the
// bounded CMS scheduler. It proves only that a discovery pass completed; it
// cannot prove a tenant's source admission or Pods delivery state.
type MediaSupplyEvaluatorWorkerStatus struct {
	State         string     `json:"state"`
	LastHeartbeat *time.Time `json:"last_heartbeat_at,omitempty"`
	StaleAfterAt  *time.Time `json:"stale_after_at,omitempty"`
}

func MarkMediaSupplyEvaluatorHeartbeat(at time.Time) {
	mediaSupplyEvaluatorLastHeartbeat.Store(at.UTC().UnixNano())
}

func MediaSupplyEvaluatorWorkerHealthy(now time.Time) bool {
	return MediaSupplyEvaluatorWorkerStatusAt(now).State == "ready"
}

func MediaSupplyEvaluatorWorkerStatusAt(now time.Time) MediaSupplyEvaluatorWorkerStatus {
	lastNanos := mediaSupplyEvaluatorLastHeartbeat.Load()
	if lastNanos <= 0 {
		return MediaSupplyEvaluatorWorkerStatus{State: "not_started"}
	}
	last := time.Unix(0, lastNanos).UTC()
	staleAfter := last.Add(MediaSupplyEvaluatorHeartbeatGrace)
	state := "ready"
	if now.UTC().After(staleAfter) {
		state = "stale"
	}
	return MediaSupplyEvaluatorWorkerStatus{State: state, LastHeartbeat: &last, StaleAfterAt: &staleAfter}
}

// SetMediaSupplyEvaluatorHeartbeatForTest temporarily replaces process-local
// state for deterministic CMS tests. Production callers must mark actual
// completed scheduler passes with MarkMediaSupplyEvaluatorHeartbeat.
func SetMediaSupplyEvaluatorHeartbeatForTest(at time.Time) func() {
	previous := mediaSupplyEvaluatorLastHeartbeat.Load()
	mediaSupplyEvaluatorLastHeartbeat.Store(at.UTC().UnixNano())
	return func() { mediaSupplyEvaluatorLastHeartbeat.Store(previous) }
}
