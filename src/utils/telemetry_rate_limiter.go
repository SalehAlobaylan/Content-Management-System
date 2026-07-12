package utils

import (
	"sync"
	"time"
)

// TelemetryRateLimiter is a bounded, self-cleaning fixed-window limiter for the
// public RUX telemetry ingest path. Unlike LoginRateLimiter (whose entries map
// grows one key per IP forever), telemetry has vastly more distinct keys
// (per-session, per-forwarded-key), so an unbounded map would leak memory.
//
// Two defenses: (1) a background sweep evicts expired windows on an interval,
// and (2) a hard MaxKeys cap rejects new keys rather than allowing an abuse
// flood to bypass the budget by rotating session identifiers.
type TelemetryRateLimiter struct {
	mu          sync.Mutex
	entries     map[string]*telemetryWindow
	maxRequests int
	window      time.Duration
	maxKeys     int
	stop        chan struct{}
}

type telemetryWindow struct {
	count   int
	resetAt time.Time
}

// NewTelemetryRateLimiter starts the background sweeper immediately. maxKeys
// bounds memory; when exceeded the limiter allows requests instead of tracking
// new keys until the sweep drains expired windows.
func NewTelemetryRateLimiter(maxRequests int, window time.Duration, maxKeys int) *TelemetryRateLimiter {
	if maxKeys <= 0 {
		maxKeys = 50000
	}
	l := &TelemetryRateLimiter{
		entries:     make(map[string]*telemetryWindow),
		maxRequests: maxRequests,
		window:      window,
		maxKeys:     maxKeys,
		stop:        make(chan struct{}),
	}
	go l.sweepLoop()
	return l
}

// Allow reports whether the key is within its window budget and how many
// requests remain. An empty key is always allowed (nothing to key on).
func (l *TelemetryRateLimiter) Allow(key string) (bool, int) {
	return l.AllowN(key, 1)
}

// AllowN consumes units from a key's fixed-window budget. Batch ingestion uses
// this so the budget represents events, not merely HTTP requests.
func (l *TelemetryRateLimiter) AllowN(key string, units int) (bool, int) {
	if key == "" {
		return false, 0
	}
	if units <= 0 {
		return true, l.maxRequests
	}
	if units > l.maxRequests {
		return false, 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	entry, ok := l.entries[key]
	if !ok || now.After(entry.resetAt) {
		// New or expired window. Respect the hard cap rather than growing the map.
		if !ok && len(l.entries) >= l.maxKeys {
			return false, 0
		}
		l.entries[key] = &telemetryWindow{count: units, resetAt: now.Add(l.window)}
		return true, l.maxRequests - units
	}
	if entry.count+units > l.maxRequests {
		return false, 0
	}
	entry.count += units
	return true, l.maxRequests - entry.count
}

func (l *TelemetryRateLimiter) sweepLoop() {
	// Sweep at the window cadence (min 30s) so expired entries never linger.
	interval := l.window
	if interval < 30*time.Second {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-ticker.C:
			l.sweep()
		}
	}
}

func (l *TelemetryRateLimiter) sweep() {
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	for k, e := range l.entries {
		if now.After(e.resetAt) {
			delete(l.entries, k)
		}
	}
}

// Stop halts the background sweeper. Optional; used in tests.
func (l *TelemetryRateLimiter) Stop() { close(l.stop) }

// Size returns the current tracked-key count (test/observability helper).
func (l *TelemetryRateLimiter) Size() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries)
}
