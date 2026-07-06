package controllers

import (
	"testing"
	"time"

	"content-management-system/src/models"
)

// Slice 3 — pure trigger decision (S7 pause, S8 debounce, chain vs interval).
func TestDecideStudioTrigger(t *testing.T) {
	now := time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC)
	base := models.DefaultMediaStudioAutopilotPolicy("default") // interval 360, debounce 15
	ago := func(min int) *time.Time { t := now.Add(-time.Duration(min) * time.Minute); return &t }
	future := func(min int) *time.Time { t := now.Add(time.Duration(min) * time.Minute); return &t }

	t.Run("paused blocks all triggers", func(t *testing.T) {
		p := base
		p.PausedUntil = future(60)
		if _, run := decideStudioTrigger(now, ago(1000), p, true); run {
			t.Fatal("paused must not run even with chain available")
		}
	})

	t.Run("chain fires past debounce", func(t *testing.T) {
		if trig, run := decideStudioTrigger(now, ago(20), base, true); !run || trig != models.StudioRunTriggerChained {
			t.Fatalf("expected chained run, got trig=%q run=%v", trig, run)
		}
	})

	t.Run("chain debounced within window", func(t *testing.T) {
		// last run 5 min ago, debounce 15 → chain suppressed; interval not due
		// either (5 < 360), so no run.
		if trig, run := decideStudioTrigger(now, ago(5), base, true); run {
			t.Fatalf("expected no run within debounce, got trig=%q", trig)
		}
	})

	t.Run("interval fires when due and no chain", func(t *testing.T) {
		if trig, run := decideStudioTrigger(now, ago(400), base, false); !run || trig != models.StudioRunTriggerInterval {
			t.Fatalf("expected interval run, got trig=%q run=%v", trig, run)
		}
	})

	t.Run("interval not due, no chain → no run", func(t *testing.T) {
		if _, run := decideStudioTrigger(now, ago(100), base, false); run {
			t.Fatal("expected no run: interval not elapsed and no chain")
		}
	})

	t.Run("never-run tenant with chain fires chained", func(t *testing.T) {
		if trig, run := decideStudioTrigger(now, nil, base, true); !run || trig != models.StudioRunTriggerChained {
			t.Fatalf("expected chained run on first-ever with chain, got trig=%q run=%v", trig, run)
		}
	})

	t.Run("never-run tenant no chain fires interval", func(t *testing.T) {
		if trig, run := decideStudioTrigger(now, nil, base, false); !run || trig != models.StudioRunTriggerInterval {
			t.Fatalf("expected interval run on first-ever, got trig=%q run=%v", trig, run)
		}
	})
}
