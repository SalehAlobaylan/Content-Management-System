package controllers

import (
	"content-management-system/src/models"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"testing"
	"time"
)

func TestTerminalReceiptPreservesLegacyDigestAndBindsFrozenSession(t *testing.T) {
	for _, platform := range []string{"web", "ios"} {
		ev := terminalFixture("feed", "success")
		ev.ClientPlatform = platform
		ev.OSMajor = 26
		legacy := `["pods","feed","success","r2","safari",18,"mobile","unknown",false,null,null,null,null,null]`
		if platform == "ios" {
			legacy = legacy[:len(legacy)-1] + `,26]`
		}
		before := terminalReceipt(&ev)
		if before.Digest != fmt.Sprintf("%x", sha256.Sum256([]byte(legacy))) {
			t.Fatalf("%s legacy replay digest changed", platform)
		}
		id := uuid.New()
		ev.FeedSessionID = &id
		after := terminalReceipt(&ev)
		if after.Identity != before.Identity || after.Digest == before.Digest {
			t.Fatal("session correlation must affect comparison without changing terminal identity")
		}
	}
}

func terminalFixture(phase, outcome string) models.ExperienceEvent {
	visible := true
	duration := 1000
	id := uuid.New()
	return models.ExperienceEvent{EventID: uuid.New(), TenantID: "default", SchemaVersion: 2, EventType: "journey_terminal", Phase: phase, Outcome: outcome, TerminalStatus: "accepted", Visible: &visible, DurationMS: &duration, JourneyID: &id, SessionID: "long-lived-tab", PageLoadID: uuid.New(), Release: "r2", Surface: "pods", BrowserFamily: "safari", BrowserMajor: 18, DeviceClass: "mobile", NetworkClass: "unknown"}
}
func TestContributions_EmptyUsesSuccessfulResponses(t *testing.T) {
	rendered := terminalFixture("feed", "success")
	empty := terminalFixture("feed", "empty")
	failed := terminalFixture("feed", "failed")
	var numerator, denominator int64
	for _, example := range []struct {
		event models.ExperienceEvent
		count int64
	}{{rendered, 9950}, {empty, 50}, {failed, 100}} {
		for _, c := range contributionsFor(&example.event) {
			if c.metric == mFeedEmptyRate {
				numerator += c.num * example.count
				denominator += c.denom * example.count
			}
		}
	}
	if numerator != 50 || denominator != 10000 {
		t.Fatalf("empty rate got %d/%d", numerator, denominator)
	}
}
func TestContributions_LegacyAndIneligibleUncovered(t *testing.T) {
	for _, outcome := range []string{"cancelled", "backgrounded", "end_of_feed"} {
		ev := terminalFixture("startup", outcome)
		if len(contributionsFor(&ev)) > 0 {
			t.Fatalf("%s entered eligibility", outcome)
		}
	}
	ev := terminalFixture("startup", "success")
	ev.SchemaVersion = 1
	if len(contributionsFor(&ev)) > 0 {
		t.Fatal("legacy event entered v2 SLIs")
	}
}
func TestContributions_HandoffDriftFailureMeasured(t *testing.T) {
	ev := terminalFixture("handoff", "failed")
	drift := 3000
	ev.PositionDriftMS = &drift
	for _, c := range contributionsFor(&ev) {
		if c.metric == mHandoffDrift && c.num == 1 && c.denom == 1 {
			return
		}
	}
	t.Fatal("wrong-position failure omitted drift")
}
func TestContributions_ContinuationRegistry(t *testing.T) {
	ev := terminalFixture("continuation", "failed")
	observed := 60000
	stall := 30000
	count := 2
	ev.ObservedPlaybackMS = &observed
	ev.StallDurationMS = &stall
	ev.StallCount = &count
	found := map[string]contribution{}
	for _, c := range contributionsFor(&ev) {
		found[c.metric] = c
	}
	if found[mRebufferRatio].num != 30000 || found[mRebufferRatio].denom != 60000 || found[mContinuationFatalRate].num != 1 || found[mStallIncidence].num != 1 {
		t.Fatalf("continuation contributions %+v", found)
	}
}

func TestContributions_ListeningFallbackHasItsOwnPopulation(t *testing.T) {
	ev := terminalFixture("continuation", "success")
	used := true
	ev.FallbackUsed = &used
	found := map[string]contribution{}
	for _, c := range contributionsFor(&ev) {
		found[c.metric] = c
	}
	if found[mContinuationFallbackUse].num != 1 || found[mContinuationFallbackUse].denom != 1 {
		t.Fatal("listening fallback omitted")
	}
	if _, mixed := found[mFallbackUse]; mixed {
		t.Fatal("startup and listening populations mixed")
	}
}
func TestEvaluateSLI_InclusiveLatencyBoundaries(t *testing.T) {
	for _, example := range []struct {
		metric     string
		ms, target int
	}{{mPlaybackStartSuccess, 2000, 2000}, {mFeedRenderSuccess, 2500, 2500}, {mFeedRenderSuccess, 2100, 2500}, {mFeedRenderSuccess, 2499, 2500}} {
		b := [8]int64{}
		b[latencyBucketIndex(example.ms)] = 100
		st := evaluateSLI(spec(example.metric, "min", .99, false, example.target), 100, 100, 100, 0, b, 50)
		if st.Status != "ok" {
			t.Fatalf("%dms target%d got%s", example.ms, example.target, st.Status)
		}
	}
}
func TestSurfaceVerdict_RequiredCoverageNotGreen(t *testing.T) {
	if got := surfaceVerdict([]sliStatus{{Status: "ok", Required: true}, {Status: "insufficient", Required: true}}, true); got != "insufficient_data" {
		t.Fatal(got)
	}
}
func TestExperienceTransition_ConfirmationRecoveryAndReplay(t *testing.T) {
	p := models.DefaultExperiencePolicy("default")
	inc := models.ExperienceIncident{}
	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	step := func(hours int, status string) string {
		return advanceExperienceIncident(&inc, p, status, start.Add(time.Duration(hours)*time.Hour))
	}
	if step(0, "breach") != "incident_watching" || inc.Severity != "watching" || inc.Status != "confirming" {
		t.Fatal("first window incorrectly confirmed")
	}
	if step(1, "breach") != "incident_opened" || inc.ConfirmedAt == nil {
		t.Fatal("adjacent windows not confirmed")
	}
	if step(1, "breach") != "" || inc.ViolationStreak != 2 {
		t.Fatal("replay advanced")
	}
	step(2, "ok")
	step(3, "insufficient")
	if inc.CleanStreak != 0 || inc.Status != "open" {
		t.Fatal("missing evidence continued recovery")
	}
	step(4, "ok")
	step(6, "ok")
	if inc.CleanStreak != 1 {
		t.Fatal("gap advanced recovery")
	}
	step(7, "ok")
	if step(8, "ok") != "incident_resolved" {
		t.Fatal("three adjacent clean windows did not resolve")
	}
}
func TestExperienceTransition_SuppressionInterruptsConfirmation(t *testing.T) {
	p := models.DefaultExperiencePolicy("default")
	inc := models.ExperienceIncident{}
	start := time.Now().UTC().Truncate(time.Hour)
	advanceExperienceIncident(&inc, p, "breach", start)
	advanceExperienceIncident(&inc, p, "suppressed", start.Add(time.Hour))
	advanceExperienceIncident(&inc, p, "breach", start.Add(2*time.Hour))
	if inc.ConfirmedAt != nil || inc.ViolationStreak != 1 {
		t.Fatal("suppression bridged confirmation")
	}
}
func TestValidateRuxEvent_V2TerminalAndStrictDimensions(t *testing.T) {
	base := validRuxEvent()
	base.EventType = "journey_terminal"
	base.Phase = "startup"
	base.Outcome = "success"
	j := uuid.NewString()
	base.JourneyID = &j
	raw := jsonBytes(map[string]any{"duration_ms": 2000, "visible": true})
	if err := json.Unmarshal(raw, &base.Measurements); err != nil {
		t.Fatal(err)
	}
	if _, ok := validateAndNormalizeRuxEvent(&base, time.Now()); !ok {
		t.Fatal("valid terminal rejected")
	}
	for _, change := range []func(*incomingRuxEvent){func(in *incomingRuxEvent) { in.Phase = "ranking" }, func(in *incomingRuxEvent) { bad := "bad"; in.ContentID = &bad }, func(in *incomingRuxEvent) { bad := "secret url"; in.PlaybackType = &bad }, func(in *incomingRuxEvent) { bad := "arbitrary-locale"; in.Locale = &bad }, func(in *incomingRuxEvent) { in.Release = "private message !" }, func(in *incomingRuxEvent) { in.JourneyID = nil }} {
		example := base
		change(&example)
		if _, ok := validateAndNormalizeRuxEvent(&example, time.Now()); ok {
			t.Fatal("malformed terminal accepted")
		}
	}
}
func TestExperienceReceipt_TransportIndependentAndMetadataStrict(t *testing.T) {
	ev := terminalFixture("startup", "success")
	ev.Measurements = jsonBytes(map[string]any{"duration_ms": 1000, "visible": true})
	a := terminalReceipt(&ev)
	ev.EventID = uuid.New()
	ev.Sequence = 999
	ev.OccurredAt = time.Now().Add(24 * time.Hour)
	b := terminalReceipt(&ev)
	if a != b {
		t.Fatal("transport retry changed receipt")
	}
	ev.Release = "r3"
	b = terminalReceipt(&ev)
	if a.Identity != b.Identity || a.Digest == b.Digest {
		t.Fatal("metadata conflict not detected")
	}
}
func TestExperiencePolicy_UnsupportedThresholdsRejected(t *testing.T) {
	for _, raw := range []string{`{"private":{"target":1}}`, `{"feed_render_success":{"target":2}}`, `{"playback_start_success":{"latency_p75_ms":0}}`} {
		if validExperienceThresholds([]byte(raw)) {
			t.Fatal(raw)
		}
	}
}

func TestEvaluateSLI_IndependentLatencyFloor(t *testing.T) {
	b := [8]int64{}
	b[latencyBucketIndex(3000)] = 60
	st := evaluateSLI(spec(mFeedRenderSuccess, "min", .995, false, 2500), 100, 100, 60, 0, b, 50, 100)
	if st.Status != "insufficient" {
		t.Fatalf("latency floor bypassed: %s", st.Status)
	}
	b = [8]int64{}
	b[latencyBucketIndex(1000)] = 10
	st = evaluateSLI(spec(mFeedRenderSuccess, "min", .995, false, 2500), 100, 100, 10, 0, b, 50, 10)
	if st.Status != "ok" {
		t.Fatalf("lower independently qualified latency floor ignored: %s", st.Status)
	}
}

func TestEvaluateSLI_WithinBinBudgetWithholdsLatencyVerdict(t *testing.T) {
	b := [8]int64{}
	b[latencyBucketIndex(3000)] = 100
	st := evaluateSLI(spec(mFeedRenderSuccess, "min", .995, false, 3500), 100, 100, 100, 0, b, 50)
	if st.Status != "insufficient" || st.EvidenceReason != "latency_histogram_resolution" {
		t.Fatalf("imprecise histogram established a breach: %+v", st)
	}
	st = evaluateSLI(spec(mFeedRenderSuccess, "min", .995, false, 2500), 100, 100, 100, 0, b, 50)
	if st.Status != "latency_breach" {
		t.Fatalf("exact boundary failed to classify: %+v", st)
	}
	st = evaluateSLI(spec(mFeedRenderSuccess, "min", .995, false, 4000), 100, 100, 101, 0, b, 50)
	if st.Status != "insufficient" || st.EvidenceReason != "latency_histogram_incomplete" {
		t.Fatalf("incomplete histogram qualified: %+v", st)
	}
}
