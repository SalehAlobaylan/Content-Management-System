package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"testing"
	"time"

	"github.com/google/uuid"
)

func cachedSlide(storyID uuid.UUID, leadPublishedAt, lastMemberAt time.Time) StorySlide {
	return StorySlide{
		Featured: StoryFeatured{
			StorySummary: StorySummary{
				StoryID:      storyID,
				PublishedAt:  leadPublishedAt,
				LastMemberAt: lastMemberAt,
			},
		},
	}
}

func TestPaginateStorySlidesUsesLastMemberAtForCursor(t *testing.T) {
	storyID := uuid.New()
	leadPublishedAt := time.Date(2026, 6, 19, 8, 0, 0, 0, time.UTC)
	lastMemberAt := time.Date(2026, 6, 19, 11, 30, 0, 0, time.UTC)

	_, cursor := paginateStorySlides([]StorySlide{
		cachedSlide(storyID, leadPublishedAt, lastMemberAt),
	}, time.Time{}, uuid.Nil, 1, nil)
	if cursor == nil || *cursor == "" {
		t.Fatal("expected cursor")
	}
	gotTime, gotID, err := utils.DecodeCursor(*cursor)
	if err != nil {
		t.Fatalf("decode cursor: %v", err)
	}
	if gotID != storyID {
		t.Fatalf("cursor story = %s, want %s", gotID, storyID)
	}
	if !gotTime.Equal(lastMemberAt) {
		t.Fatalf("cursor time = %s, want last_member_at %s", gotTime, lastMemberAt)
	}
}

func TestPaginateStorySlidesFallbackUsesLastMemberAt(t *testing.T) {
	cursorStory := uuid.New()
	first := uuid.New()
	second := uuid.New()
	cursorTime := time.Date(2026, 6, 19, 11, 0, 0, 0, time.UTC)
	slides := []StorySlide{
		cachedSlide(first, time.Date(2026, 6, 19, 7, 0, 0, 0, time.UTC), time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)),
		cachedSlide(second, time.Date(2026, 6, 19, 10, 45, 0, 0, time.UTC), time.Date(2026, 6, 19, 10, 30, 0, 0, time.UTC)),
	}

	page, _ := paginateStorySlides(slides, cursorTime, cursorStory, 1, nil)
	if len(page) != 1 {
		t.Fatalf("page len = %d, want 1", len(page))
	}
	if page[0].Featured.StoryID != second {
		t.Fatalf("resumed story = %s, want %s", page[0].Featured.StoryID, second)
	}
}

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

func TestCirculationContextFromPolicyUsesProvidedPolicy(t *testing.T) {
	policy := models.DefaultNewsCirculationPolicy("default")
	policy.CarryoverHours = 24
	now := time.Date(2026, 6, 19, 9, 15, 0, 0, time.UTC)

	ctx := circulationContextFromPolicy(policy, models.NewsWindowToday, now)
	if ctx.Policy.CarryoverHours != 24 {
		t.Fatalf("policy carryover = %d, want override", ctx.Policy.CarryoverHours)
	}
	if got, want := ctx.Window.QueryStart, ctx.Window.PrimaryStart.Add(-24*time.Hour); !got.Equal(want) {
		t.Fatalf("query start = %s, want %s", got, want)
	}
}

func TestSanitizeCirculationPolicyEnforcesCadenceGuardrails(t *testing.T) {
	policy := models.DefaultNewsCirculationPolicy("default")
	policy.CarryoverMinScore = 4
	policy.RecencyWeight = -1
	policy.ImportanceWeight = 2
	policy.SourceMinIntervalMinutes = 5
	policy.SourceMaxIntervalMinutes = 900
	policy.SourceMaxChangePercent = 90

	got := sanitizeCirculationPolicy(policy)
	if got.CarryoverMinScore != 1 {
		t.Fatalf("carryover min score = %f, want 1", got.CarryoverMinScore)
	}
	if got.RecencyWeight != 0 || got.ImportanceWeight != 1 {
		t.Fatalf("weights = recency %f importance %f, want 0 and 1", got.RecencyWeight, got.ImportanceWeight)
	}
	if got.SourceMinIntervalMinutes != 10 {
		t.Fatalf("source min interval = %d, want 10", got.SourceMinIntervalMinutes)
	}
	if got.SourceMaxIntervalMinutes != 360 {
		t.Fatalf("source max interval = %d, want 360", got.SourceMaxIntervalMinutes)
	}
	if got.SourceMaxChangePercent != 50 {
		t.Fatalf("source max change = %d, want 50", got.SourceMaxChangePercent)
	}
}

func TestSanitizeCirculationPolicyEnforcesAutopilotGuardrails(t *testing.T) {
	policy := models.DefaultNewsCirculationPolicy("default")
	policy.AutopilotMode = "wild"
	policy.AutopilotIntervalMinutes = 1
	policy.AutopilotMaxQueueDepth = -10
	policy.AutopilotMaxActionsPerRun = 500

	got := sanitizeCirculationPolicy(policy)
	if got.AutopilotMode != models.NewsAutopilotModeSafeAuto {
		t.Fatalf("autopilot mode = %s, want safe_auto", got.AutopilotMode)
	}
	if got.AutopilotIntervalMinutes != 5 {
		t.Fatalf("autopilot interval = %d, want 5", got.AutopilotIntervalMinutes)
	}
	if got.AutopilotMaxQueueDepth != 100 {
		t.Fatalf("autopilot max queue = %d, want default 100", got.AutopilotMaxQueueDepth)
	}
	if got.AutopilotMaxActionsPerRun != 50 {
		t.Fatalf("autopilot max actions = %d, want ceiling 50", got.AutopilotMaxActionsPerRun)
	}
}

func TestSanitizeOverrideBoostBoundsManualExceptions(t *testing.T) {
	tests := []struct {
		name  string
		input float64
		want  float64
	}{
		{name: "zero resets to neutral", input: 0, want: 1},
		{name: "negative resets to neutral", input: -4, want: 1},
		{name: "tiny clamps to floor", input: 0.01, want: 0.1},
		{name: "normal passes through", input: 1.4, want: 1.4},
		{name: "huge clamps to ceiling", input: 50, want: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sanitizeOverrideBoost(tt.input); got != tt.want {
				t.Fatalf("boost = %f, want %f", got, tt.want)
			}
		})
	}
}

func TestAutopilotStateAndToolAccessRespectSafety(t *testing.T) {
	policy := models.DefaultNewsCirculationPolicy("default")
	policy.AutopilotEnabled = true
	policy.AutopilotMaxQueueDepth = 10
	boostUntil := time.Now().Add(time.Hour)
	policy.AutopilotBoostUntil = &boostUntil
	health := autopilotHealthSignal{
		AggregationReachable: true,
		QueueDepth:           25,
		MaxQueueDepth:        10,
	}

	if got := autopilotStateForPolicy(policy, health, time.Now()); got != models.NewsAutopilotStateSafety {
		t.Fatalf("state = %s, want safety to take precedence over active boost window", got)
	}
	allowed, blocked := autopilotToolAccess(policy, health, time.Now())
	for _, tool := range allowed {
		if tool == "circulation.sweep" {
			t.Fatalf("circulation sweep should be blocked while queue depth is unsafe")
		}
	}
	found := false
	for _, tool := range blocked {
		if tool.Name == "circulation.sweep" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected circulation.sweep in blocked tools")
	}
}

func TestRelevantQueueDepthOnlyCountsNewsToolbeltQueues(t *testing.T) {
	stats := []autopilotQueueStat{
		{Queue: "fetch-queue", Waiting: 2, Active: 1, Delayed: 3},
		{Queue: "news-circulation-queue", Waiting: 4},
		{Queue: "media-queue", Waiting: 100},
	}
	if got := relevantQueueDepth(stats); got != 10 {
		t.Fatalf("queue depth = %d, want 10", got)
	}
}

func baseFreshnessHealth(policy models.NewsCirculationPolicy) autopilotHealthSignal {
	return autopilotHealthSignal{
		AggregationReachable: true,
		QueueDepth:           4,
		MaxQueueDepth:        policy.AutopilotMaxQueueDepth,
		TodayStoryCount:      int64(policy.MinTodayStories + 6),
		ActiveSources:        20,
		DueSources:           2,
		Snapshots: []autopilotSnapshotSignal{
			{Window: models.NewsWindowToday, Dirty: false, AgeSeconds: 10},
			{Window: models.NewsWindowWeek, Dirty: false, AgeSeconds: 10},
			{Window: models.NewsWindowMonth, Dirty: false, AgeSeconds: 10},
		},
	}
}

func TestAutopilotFreshnessVerdictHealthyFeed(t *testing.T) {
	policy := models.DefaultNewsCirculationPolicy("default")
	policy.AutopilotMaxQueueDepth = 100

	got := computeAutopilotFreshness(policy, baseFreshnessHealth(policy))
	if got.Verdict != "fresh" {
		t.Fatalf("verdict = %s, want fresh", got.Verdict)
	}
	if got.Score < 85 {
		t.Fatalf("score = %d, want >= 85", got.Score)
	}
	if got.RecommendedAction != "none" {
		t.Fatalf("recommended action = %s, want none", got.RecommendedAction)
	}
}

func TestAutopilotFreshnessVerdictThinToday(t *testing.T) {
	policy := models.DefaultNewsCirculationPolicy("default")
	policy.MinTodayStories = 10
	health := baseFreshnessHealth(policy)
	health.TodayStoryCount = 5

	got := computeAutopilotFreshness(policy, health)
	if got.Verdict != "thin" {
		t.Fatalf("verdict = %s, want thin", got.Verdict)
	}
	if got.RecommendedAction != "boost_freshness" {
		t.Fatalf("recommended action = %s, want boost_freshness", got.RecommendedAction)
	}
}

func TestAutopilotFreshnessVerdictHighCarryoverLowersScore(t *testing.T) {
	policy := models.DefaultNewsCirculationPolicy("default")
	health := baseFreshnessHealth(policy)
	health.TodayStoryCount = 10
	health.TodayCarryoverCount = 7
	health.TodayCarryoverRatio = 0.7

	got := computeAutopilotFreshness(policy, health)
	if got.Score >= 85 {
		t.Fatalf("score = %d, want below fresh threshold", got.Score)
	}
	if got.Verdict == "fresh" {
		t.Fatalf("verdict = fresh, want carryover to prevent fresh verdict")
	}
}

func TestAutopilotFreshnessVerdictStaleSnapshots(t *testing.T) {
	policy := models.DefaultNewsCirculationPolicy("default")
	health := baseFreshnessHealth(policy)
	health.Snapshots = []autopilotSnapshotSignal{
		{Window: models.NewsWindowToday, Dirty: true, AgeSeconds: 180},
		{Window: models.NewsWindowWeek, Dirty: false, AgeSeconds: 120},
		{Window: models.NewsWindowMonth, Dirty: false, AgeSeconds: 10},
	}

	got := computeAutopilotFreshness(policy, health)
	if got.Verdict != "stale" {
		t.Fatalf("verdict = %s, want stale", got.Verdict)
	}
	if got.RecommendedAction != "run_once" {
		t.Fatalf("recommended action = %s, want run_once", got.RecommendedAction)
	}
}

func TestAutopilotFreshnessVerdictRecentDirtySnapshotsAreWatching(t *testing.T) {
	policy := models.DefaultNewsCirculationPolicy("default")
	health := baseFreshnessHealth(policy)
	health.Snapshots = []autopilotSnapshotSignal{
		{Window: models.NewsWindowToday, Dirty: true, AgeSeconds: 15},
		{Window: models.NewsWindowWeek, Dirty: true, AgeSeconds: 15},
		{Window: models.NewsWindowMonth, Dirty: true, AgeSeconds: 15},
	}

	got := computeAutopilotFreshness(policy, health)
	if got.Verdict != "watching" {
		t.Fatalf("verdict = %s, want watching", got.Verdict)
	}
	if got.RecommendedAction != "none" {
		t.Fatalf("recommended action = %s, want none", got.RecommendedAction)
	}
}

func TestAutopilotFreshnessVerdictSafetyBlocks(t *testing.T) {
	policy := models.DefaultNewsCirculationPolicy("default")
	policy.AutopilotMaxQueueDepth = 10
	health := baseFreshnessHealth(policy)
	health.QueueDepth = 11

	got := computeAutopilotFreshness(policy, health)
	if got.Verdict != "blocked" {
		t.Fatalf("verdict = %s, want blocked", got.Verdict)
	}
	if got.RecommendedAction != "pause" {
		t.Fatalf("recommended action = %s, want pause", got.RecommendedAction)
	}
}

func TestAutopilotFreshnessVerdictDegraded(t *testing.T) {
	policy := models.DefaultNewsCirculationPolicy("default")
	health := baseFreshnessHealth(policy)
	health.AggregationReachable = false

	got := computeAutopilotFreshness(policy, health)
	if got.Verdict != "degraded" {
		t.Fatalf("verdict = %s, want degraded", got.Verdict)
	}
	if got.Score != 0 {
		t.Fatalf("score = %d, want 0", got.Score)
	}
}

func TestAutopilotFreshnessVerdictSourceReviewRecommendation(t *testing.T) {
	policy := models.DefaultNewsCirculationPolicy("default")
	health := baseFreshnessHealth(policy)
	health.SourceErrorRate = 0.3

	got := computeAutopilotFreshness(policy, health)
	if got.RecommendedAction != "review_sources" {
		t.Fatalf("recommended action = %s, want review_sources", got.RecommendedAction)
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
