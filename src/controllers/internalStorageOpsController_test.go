package controllers

import "testing"

// classifyBudget is the load-bearing function — it decides whether the
// sweepers run or pause. A regression here either silently lets a runaway
// sweep blow the budget (bad) or pauses everything when we still have
// headroom (worse — operator confusion). Lock the truth table down.

func TestClassifyBudget_ZeroBudgetAlwaysOk(t *testing.T) {
	// budget=0 means "no cap" — used for AWS S3 paid plans where there's no
	// free tier to track against. Any usage value must return "ok".
	cases := []int64{0, 1, 1000, 1_000_000_000}
	for _, used := range cases {
		if got := classifyBudget(used, 0, 80, 95); got != "ok" {
			t.Errorf("classifyBudget(used=%d, budget=0) = %q, want ok", used, got)
		}
	}
}

func TestClassifyBudget_BelowWarn(t *testing.T) {
	// 79% of 1M = 790000 — below the 80% warn threshold, status is ok.
	if got := classifyBudget(790_000, 1_000_000, 80, 95); got != "ok" {
		t.Errorf("79%% utilisation should be ok, got %q", got)
	}
}

func TestClassifyBudget_AtWarnExact(t *testing.T) {
	// Exactly at the warn threshold — boundary semantics: warn (>= warnPct).
	if got := classifyBudget(800_000, 1_000_000, 80, 95); got != "warn" {
		t.Errorf("exactly 80%% should be warn, got %q", got)
	}
}

func TestClassifyBudget_BetweenWarnAndCap(t *testing.T) {
	// 85% — between 80 and 95.
	if got := classifyBudget(850_000, 1_000_000, 80, 95); got != "warn" {
		t.Errorf("85%% should be warn, got %q", got)
	}
}

func TestClassifyBudget_AtCapExact(t *testing.T) {
	// Exactly at the cap threshold — boundary semantics: cap (>= capPct).
	if got := classifyBudget(950_000, 1_000_000, 80, 95); got != "cap" {
		t.Errorf("exactly 95%% should be cap, got %q", got)
	}
}

func TestClassifyBudget_OverCap(t *testing.T) {
	// Over the cap — including over-budget (>100%).
	if got := classifyBudget(1_500_000, 1_000_000, 80, 95); got != "cap" {
		t.Errorf("150%% should be cap, got %q", got)
	}
}

func TestClassifyBudget_WarnEqualsCap(t *testing.T) {
	// Pathological config: warn == cap. Behaviour: 'cap' wins (it's checked
	// first in the switch). Operator gets the safer outcome.
	if got := classifyBudget(900_000, 1_000_000, 90, 90); got != "cap" {
		t.Errorf("warn=cap=90, used=90%% should be cap, got %q", got)
	}
}

func TestClassifyBudget_NegativeBudgetTreatedAsNoCap(t *testing.T) {
	// Defensive: negative budget shouldn't crash. Treated as "no cap".
	if got := classifyBudget(500, -1, 80, 95); got != "ok" {
		t.Errorf("negative budget should be ok, got %q", got)
	}
}
