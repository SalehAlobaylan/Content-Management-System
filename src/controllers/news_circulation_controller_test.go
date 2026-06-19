package controllers

import (
	"content-management-system/src/models"
	"testing"
	"time"
)

func TestCirculationWindowForUsesRiyadhCalendarBoundaries(t *testing.T) {
	policy := models.DefaultNewsCirculationPolicy("default")
	loc, err := time.LoadLocation("Asia/Riyadh")
	if err != nil {
		t.Fatalf("load Riyadh timezone: %v", err)
	}
	now := time.Date(2026, 6, 19, 9, 15, 0, 0, loc).UTC()

	today := circulationWindowFor(policy, models.NewsWindowToday, now)
	wantToday := time.Date(2026, 6, 19, 0, 0, 0, 0, loc).UTC()
	if !today.PrimaryStart.Equal(wantToday) {
		t.Fatalf("today primary start = %s, want %s", today.PrimaryStart, wantToday)
	}
	if !today.QueryStart.Equal(wantToday.Add(-72 * time.Hour)) {
		t.Fatalf("today query start = %s, want carryover start", today.QueryStart)
	}

	week := circulationWindowFor(policy, models.NewsWindowWeek, now)
	wantWeek := time.Date(2026, 6, 14, 0, 0, 0, 0, loc).UTC()
	if !week.PrimaryStart.Equal(wantWeek) {
		t.Fatalf("week primary start = %s, want %s", week.PrimaryStart, wantWeek)
	}
	if !week.QueryStart.Equal(wantWeek) {
		t.Fatalf("week query start = %s, want %s", week.QueryStart, wantWeek)
	}

	month := circulationWindowFor(policy, models.NewsWindowMonth, now)
	wantMonth := time.Date(2026, 6, 1, 0, 0, 0, 0, loc).UTC()
	if !month.PrimaryStart.Equal(wantMonth) {
		t.Fatalf("month primary start = %s, want %s", month.PrimaryStart, wantMonth)
	}
}

func TestStoryLifecycle(t *testing.T) {
	policy := models.DefaultNewsCirculationPolicy("default")
	window := circulationWindow{
		Name:         models.NewsWindowToday,
		PrimaryStart: time.Date(2026, 6, 19, 0, 0, 0, 0, time.UTC),
		Now:          time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC),
	}

	if got := storyLifecycle(policy, window, window.Now.Add(-30*time.Minute), 3, false); got != models.NewsLifecycleBreaking {
		t.Fatalf("breaking lifecycle = %s", got)
	}
	if got := storyLifecycle(policy, window, window.Now.Add(-4*time.Hour), 1, false); got != models.NewsLifecycleActive {
		t.Fatalf("active lifecycle = %s", got)
	}
	if got := storyLifecycle(policy, window, window.PrimaryStart.Add(-time.Hour), 4, true); got != models.NewsLifecycleCooling {
		t.Fatalf("carryover lifecycle = %s", got)
	}
	if got := storyLifecycle(policy, window, window.Now.AddDate(0, 0, -10), 4, false); got != models.NewsLifecycleHistorical {
		t.Fatalf("historical lifecycle = %s", got)
	}
}

func TestGuardedIntervalRespectsPolicyBoundsAndMaxChange(t *testing.T) {
	policy := models.DefaultNewsCirculationPolicy("default")

	if got := guardedInterval(60, 5, policy); got != 30 {
		t.Fatalf("guarded decrease = %d, want 30", got)
	}
	if got := guardedInterval(60, 240, policy); got != 90 {
		t.Fatalf("guarded increase = %d, want 90", got)
	}
	if got := guardedInterval(300, 900, policy); got != 360 {
		t.Fatalf("max bound = %d, want 360", got)
	}
}
