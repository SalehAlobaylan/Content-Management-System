package utils

import (
	"sync"
	"time"
)

type loginRateEntry struct {
	Count   int
	ResetAt time.Time
}

type LoginRateLimiter struct {
	mu          sync.Mutex
	entries     map[string]*loginRateEntry
	maxAttempts int
	window      time.Duration
}

func NewLoginRateLimiter(maxAttempts int, window time.Duration) *LoginRateLimiter {
	return &LoginRateLimiter{
		entries:     make(map[string]*loginRateEntry),
		maxAttempts: maxAttempts,
		window:      window,
	}
}

func (l *LoginRateLimiter) Allow(ip string) (bool, int) {
	if ip == "" {
		return true, 0
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	entry, exists := l.entries[ip]
	if !exists || now.After(entry.ResetAt) {
		l.entries[ip] = &loginRateEntry{Count: 1, ResetAt: now.Add(l.window)}
		return true, 0
	}

	if entry.Count >= l.maxAttempts {
		retryAfter := int(entry.ResetAt.Sub(now).Seconds())
		if retryAfter < 1 {
			retryAfter = 1
		}
		return false, retryAfter
	}

	entry.Count++
	return true, 0
}
