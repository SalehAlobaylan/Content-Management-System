package utils

import (
	"testing"
	"time"
)

func TestTelemetryRateLimiter_BlocksOverBudget(t *testing.T) {
	l := NewTelemetryRateLimiter(3, time.Minute, 1000)
	defer l.Stop()
	for i := 0; i < 3; i++ {
		if ok, _ := l.Allow("k1"); !ok {
			t.Fatalf("request %d should be allowed", i)
		}
	}
	if ok, _ := l.Allow("k1"); ok {
		t.Fatal("4th request in window should be blocked")
	}
	// Different key has its own budget.
	if ok, _ := l.Allow("k2"); !ok {
		t.Fatal("distinct key should have its own budget")
	}
}

func TestTelemetryRateLimiter_EmptyKeyIsRejected(t *testing.T) {
	l := NewTelemetryRateLimiter(1, time.Minute, 1000)
	defer l.Stop()
	if ok, _ := l.Allow(""); ok {
		t.Fatal("empty key must be rejected")
	}
}

func TestTelemetryRateLimiter_RejectsAtKeyCap(t *testing.T) {
	l := NewTelemetryRateLimiter(1, time.Minute, 2)
	defer l.Stop()
	l.Allow("a")
	l.Allow("b")
	if l.Size() != 2 {
		t.Fatalf("expected 2 tracked keys, got %d", l.Size())
	}
	if ok, _ := l.Allow("c"); ok {
		t.Fatal("new key past cap should be rejected")
	}
	if l.Size() != 2 {
		t.Fatalf("map must not grow past cap, got %d", l.Size())
	}
}

func TestTelemetryRateLimiter_ConsumesBatchUnits(t *testing.T) {
	l := NewTelemetryRateLimiter(5, time.Minute, 1000)
	defer l.Stop()
	if ok, remaining := l.AllowN("k1", 3); !ok || remaining != 2 {
		t.Fatalf("first batch: got allowed=%v remaining=%d", ok, remaining)
	}
	if ok, _ := l.AllowN("k1", 3); ok {
		t.Fatal("batch exceeding remaining budget should be blocked")
	}
}

func TestTelemetryRateLimiter_SweepEvictsExpired(t *testing.T) {
	l := NewTelemetryRateLimiter(1, 10*time.Millisecond, 1000)
	defer l.Stop()
	l.Allow("x")
	if l.Size() != 1 {
		t.Fatalf("expected 1 key, got %d", l.Size())
	}
	time.Sleep(20 * time.Millisecond)
	l.sweep()
	if l.Size() != 0 {
		t.Fatalf("expired key should be swept, got %d", l.Size())
	}
}
