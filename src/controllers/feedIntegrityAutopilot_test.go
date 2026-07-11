package controllers

import (
	"testing"

	"content-management-system/src/models"
)

func TestSanitizeFeedIntegrityAutopilotPolicy(t *testing.T) {
	p := models.FeedIntegrityPolicy{
		AutopilotMode:                  "unsafe",
		AutopilotActionHourlyCap:       100,
		AutopilotDiagnosticHourlyCap:   0,
		AutopilotCooldownMinutes:       1,
		AutopilotEvidenceMaxAgeMinutes: 1000,
		AutopilotRetryLimit:            10,
		AutopilotTrustMinDecisions:     0,
		AutopilotTrustMinAgreementPct:  2,
	}
	sanitizeFeedIntegrityAutopilotPolicy(&p)
	if p.AutopilotMode != models.FeedIntegrityAutopilotModeObserve {
		t.Fatalf("unsafe mode must clamp to observe, got %q", p.AutopilotMode)
	}
	if p.AutopilotActionHourlyCap != 2 || p.AutopilotDiagnosticHourlyCap != 4 || p.AutopilotCooldownMinutes != 60 || p.AutopilotEvidenceMaxAgeMinutes != 10 || p.AutopilotRetryLimit != 1 || p.AutopilotTrustMinDecisions != 20 || p.AutopilotTrustMinAgreementPct != 95 {
		t.Fatalf("policy defaults were not restored: %+v", p)
	}
}

func TestFeedIntegrityActionModeNeverElevates(t *testing.T) {
	p := models.DefaultFeedIntegrityPolicy("default")
	p.AutopilotMode = models.FeedIntegrityAutopilotModeAssist
	p.AutopilotActionModes = []byte(`{"news_snapshot.refresh_window":"safe_auto"}`)
	if got := feedIntegrityActionMode(p, models.FeedIntegrityActionRefreshWindow); got != models.FeedIntegrityAutopilotModeAssist {
		t.Fatalf("per-action mode must not elevate global mode, got %q", got)
	}
	p.AutopilotMode = models.FeedIntegrityAutopilotModeSafeAuto
	p.AutopilotActionModes = []byte(`{"news_snapshot.refresh_window":"observe"}`)
	if got := feedIntegrityActionMode(p, models.FeedIntegrityActionRefreshWindow); got != models.FeedIntegrityAutopilotModeObserve {
		t.Fatalf("restrictive action mode must win, got %q", got)
	}
}

func TestFeedIntegrityScopeAndFingerprintAreStable(t *testing.T) {
	f := models.FeedIntegrityFinding{CheckKey: "probe_url_dead", Feed: "foryou", Variant: "default", TargetType: "content_item", TargetRef: "item-1", Evidence: []byte(`{"url":"https://cdn.example/a.mp4?token=one"}`)}
	first := feedIntegrityFingerprint(f)
	f.Evidence = []byte(`{"url":"https://cdn.example/a.mp4?token=two"}`)
	if got := feedIntegrityFingerprint(f); got != first {
		t.Fatalf("expiring evidence must not change the fingerprint: %q != %q", got, first)
	}
	if got := feedIntegrityScope(f); got != "content_item:item-1" {
		t.Fatalf("unexpected scope %q", got)
	}
}

func TestFeedIntegrityRegistryKeepsMutationClassesHumanOwned(t *testing.T) {
	playback := feedIntegrityActionSpecFor(models.FeedIntegrityFinding{CheckKey: "probe_url_dead", Feed: "foryou", Variant: "default", TargetType: "content_item", TargetRef: "item-1"})
	if playback.AutoEligible || playback.ActionClass != "storage.inspect" {
		t.Fatalf("playback damage must remain human-owned: %+v", playback)
	}
	cache := feedIntegrityActionSpecFor(models.FeedIntegrityFinding{CheckKey: "edge_news_cache_stale", Feed: "news", Variant: "window:today", TargetType: "snapshot", TargetRef: "today"})
	if !cache.AutoEligible || cache.ActionClass != models.FeedIntegrityActionRefreshWindow {
		t.Fatalf("only the one-window cache repair should be auto-eligible: %+v", cache)
	}
}

func TestFeedIntegrityOnlyOneWindowRefreshIsExecutable(t *testing.T) {
	// V1's single executable repair. If this ever expands silently, the
	// decision loop would start offering human approvals that dead-end in
	// owner_tool_unregistered, and Safe Auto could fire an unregistered tool.
	if !feedIntegrityActionExecutable(models.FeedIntegrityActionRefreshWindow) {
		t.Fatal("the one-window news refresh must be executable")
	}
	for _, class := range []string{models.FeedIntegrityActionConfirm, "storage.inspect", "media_studio.inspect", "news_snapshot.inspect", "enrichment.review"} {
		if feedIntegrityActionExecutable(class) {
			t.Fatalf("class %q must not be executable in V1", class)
		}
	}
}

func TestFeedIntegrityPriorityAndDecisionPromotion(t *testing.T) {
	criticalReadiness := models.FeedIntegrityFinding{Severity: "critical", Axis: models.FeedIntegrityAxisReadiness}
	majorConsumer := models.FeedIntegrityFinding{Severity: "major", Axis: models.FeedIntegrityAxisConsumer}
	if feedIntegrityPriority(criticalReadiness) <= feedIntegrityPriority(majorConsumer) {
		t.Fatal("critical evidence must sort before major evidence")
	}
	if got := promoteFeedIntegrityDecision(models.FeedIntegrityDecisionBlocked, models.FeedIntegrityDecisionApprovalRequired); got != models.FeedIntegrityDecisionApprovalRequired {
		t.Fatalf("higher-value operator decision must win, got %q", got)
	}
}
