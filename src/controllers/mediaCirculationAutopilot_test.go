package controllers

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"

	"content-management-system/src/intelligence"
	"content-management-system/src/models"
	"content-management-system/src/utils"
)

// ----------------------------------------------------------------
// Slice 1 — policy defaults + sanitize clamps (plan §11, grill G2/G3/G7/G8)
// ----------------------------------------------------------------

func TestDefaultMediaCirculationPolicyAutopilotDefaults(t *testing.T) {
	p := models.DefaultMediaCirculationPolicy("default")
	if p.AutopilotEnabled {
		t.Fatal("autopilot must be disabled by default")
	}
	if p.AutopilotMode != models.MediaAutopilotModeObserve {
		t.Fatalf("default mode must be observe, got %q", p.AutopilotMode)
	}
	if p.AutopilotIntervalMinutes != 360 {
		t.Fatalf("default cadence must be 360m (G2), got %d", p.AutopilotIntervalMinutes)
	}
	if p.AutopilotMaxActionsPerRun != 8 || p.AutopilotMaxAtomizePerRun != 3 {
		t.Fatalf("default caps wrong: actions=%d atomize=%d", p.AutopilotMaxActionsPerRun, p.AutopilotMaxAtomizePerRun)
	}
	if p.AutopilotMaxBytesPerRun != 1<<30 {
		t.Fatalf("default byte cap must be 1GiB, got %d", p.AutopilotMaxBytesPerRun)
	}
	if p.AutopilotEvictConfidenceFloor != 0.5 {
		t.Fatalf("default confidence floor must be 0.5 (G7), got %f", p.AutopilotEvictConfidenceFloor)
	}
	if p.AutopilotTrustMinDecisions != 20 || p.AutopilotTrustMaxRevertPct != 10 {
		t.Fatalf("default trust knobs wrong: %d/%d", p.AutopilotTrustMinDecisions, p.AutopilotTrustMaxRevertPct)
	}
}

func TestSanitizeAutopilotClampsRanges(t *testing.T) {
	p := models.DefaultMediaCirculationPolicy("default")
	p.AutopilotIntervalMinutes = 1        // below 15
	p.AutopilotMaxActionsPerRun = 500     // above 50
	p.AutopilotMaxAtomizePerRun = -3      // below 0
	p.AutopilotMaxQueueDepth = 0          // unset → default
	p.AutopilotMaxBytesPerRun = 1024      // below 64MiB
	p.AutopilotEvictConfidenceFloor = 2.0 // above 0.95
	p.AutopilotTrustMinDecisions = 0      // unset → default
	p.AutopilotTrustMaxRevertPct = 900    // above 100

	got := sanitizeMediaAutopilotFields(p)
	if got.AutopilotIntervalMinutes != 15 {
		t.Fatalf("interval clamp: got %d", got.AutopilotIntervalMinutes)
	}
	if got.AutopilotMaxActionsPerRun != 50 {
		t.Fatalf("actions clamp: got %d", got.AutopilotMaxActionsPerRun)
	}
	if got.AutopilotMaxAtomizePerRun != 0 {
		t.Fatalf("atomize clamp (0 is legal — disables atomize): got %d", got.AutopilotMaxAtomizePerRun)
	}
	if got.AutopilotMaxQueueDepth != 100 {
		t.Fatalf("queue depth default: got %d", got.AutopilotMaxQueueDepth)
	}
	if got.AutopilotMaxBytesPerRun != int64(64)<<20 {
		t.Fatalf("byte floor clamp: got %d", got.AutopilotMaxBytesPerRun)
	}
	if got.AutopilotEvictConfidenceFloor != 0.95 {
		t.Fatalf("confidence ceiling clamp: got %f", got.AutopilotEvictConfidenceFloor)
	}
	if got.AutopilotTrustMinDecisions != 20 {
		t.Fatalf("trust decisions default: got %d", got.AutopilotTrustMinDecisions)
	}
	if got.AutopilotTrustMaxRevertPct != 100 {
		t.Fatalf("revert pct clamp: got %d", got.AutopilotTrustMaxRevertPct)
	}
}

func TestSanitizeAutopilotZeroBytesTakesDefault(t *testing.T) {
	p := models.DefaultMediaCirculationPolicy("default")
	p.AutopilotMaxBytesPerRun = 0
	got := sanitizeMediaAutopilotFields(p)
	if got.AutopilotMaxBytesPerRun != 1<<30 {
		t.Fatalf("zero byte cap must take 1GiB default, got %d", got.AutopilotMaxBytesPerRun)
	}
}

func TestSanitizeAutopilotInvalidModeFallsBackToObserve(t *testing.T) {
	p := models.DefaultMediaCirculationPolicy("default")
	p.AutopilotMode = "full_agent" // never a thing
	got := sanitizeMediaAutopilotFields(p)
	if got.AutopilotMode != models.MediaAutopilotModeObserve {
		t.Fatalf("invalid mode must fall back to observe, got %q", got.AutopilotMode)
	}
}

func TestSanitizeAutopilotExpiredElevationClears(t *testing.T) {
	past := time.Now().Add(-time.Hour)
	p := models.DefaultMediaCirculationPolicy("default")
	p.AutopilotElevatedMode = models.MediaAutopilotElevatedStorageRelief
	p.AutopilotElevatedUntil = &past
	got := sanitizeMediaAutopilotFields(p)
	if got.AutopilotElevatedMode != "" || got.AutopilotElevatedUntil != nil {
		t.Fatalf("expired elevation must clear, got %q / %v", got.AutopilotElevatedMode, got.AutopilotElevatedUntil)
	}

	// And an elevation with no expiry at all is not an elevation.
	p2 := models.DefaultMediaCirculationPolicy("default")
	p2.AutopilotElevatedMode = models.MediaAutopilotElevatedQualityRepair
	got2 := sanitizeMediaAutopilotFields(p2)
	if got2.AutopilotElevatedMode != "" {
		t.Fatalf("expiry-less elevation must clear, got %q", got2.AutopilotElevatedMode)
	}
}

func TestSanitizeAutopilotValidElevationSurvives(t *testing.T) {
	future := time.Now().Add(2 * time.Hour)
	p := models.DefaultMediaCirculationPolicy("default")
	p.AutopilotElevatedMode = models.MediaAutopilotElevatedAtomizationCatchup
	p.AutopilotElevatedUntil = &future
	got := sanitizeMediaAutopilotFields(p)
	if got.AutopilotElevatedMode != models.MediaAutopilotElevatedAtomizationCatchup {
		t.Fatalf("valid elevation must survive sanitize, got %q", got.AutopilotElevatedMode)
	}
}

func TestSanitizeFullPolicyRunsAutopilotSanitize(t *testing.T) {
	// sanitizeMediaCirculationPolicy must chain into the autopilot sanitizer.
	p := models.DefaultMediaCirculationPolicy("default")
	p.AutopilotMode = "bogus"
	got := sanitizeMediaCirculationPolicy(p)
	if got.AutopilotMode != models.MediaAutopilotModeObserve {
		t.Fatalf("full-policy sanitize must fix autopilot fields, got %q", got.AutopilotMode)
	}
}

// ----------------------------------------------------------------
// Slice 2 — pure gate ladder (decideMediaAutopilotRec)
// ----------------------------------------------------------------

func autopilotTestRec(unitType, verdict string) models.MediaCirculationRecommendation {
	return models.MediaCirculationRecommendation{
		PublicID:  uuid.New(),
		TenantID:  "default",
		UnitType:  unitType,
		SubjectID: uuid.New(),
		Verdict:   verdict,
		Action:    verdict,
		Status:    models.MediaCirculationRecStatusPending,
		CreatedAt: time.Now(),
	}
}

func earnedTrust(verdicts ...string) map[string]mediaAutopilotTrustStat {
	m := map[string]mediaAutopilotTrustStat{}
	for _, v := range verdicts {
		m[v] = mediaAutopilotTrustStat{Verdict: v, Decisions: 30, Applied: 30, Earned: true}
	}
	return m
}

func autopilotTestGates(trust map[string]mediaAutopilotTrustStat) *mediaAutopilotGates {
	return &mediaAutopilotGates{
		Trust:           trust,
		Scores:          map[uuid.UUID]intelligence.ScoreSnapshot{},
		ConfidenceFloor: 0.5,
		QueueDepth:      0,
		MaxQueueDepth:   100,
		MaxAtomize:      3,
		MaxBytes:        1 << 30,
		Now:             time.Now(),
		StalenessWindow: mediaAutopilotStalenessWindow,
	}
}

func TestGateRecoverableDeleteIsHardApprovalOnly(t *testing.T) {
	// G3: even with earned trust, recoverable_delete never auto-applies.
	rec := autopilotTestRec(models.MediaCirculationUnitItemFamily, mediaCircVerdictRecoverableDelete)
	g := autopilotTestGates(earnedTrust(mediaCircVerdictRecoverableDelete))
	d := decideMediaAutopilotRec(rec, g)
	if d.Kind != mediaAutopilotDecisionApproval || d.Guardrail != models.MediaAutopilotGuardApprovalTier {
		t.Fatalf("recoverable_delete must be approval-only, got %+v", d)
	}
}

func TestGateApprovalTierVerdicts(t *testing.T) {
	for _, v := range []string{mediaCircVerdictPauseSource, mediaCircVerdictNeedsAdminReview} {
		rec := autopilotTestRec(models.MediaCirculationUnitSource, v)
		d := decideMediaAutopilotRec(rec, autopilotTestGates(earnedTrust(v)))
		if d.Kind != mediaAutopilotDecisionApproval {
			t.Fatalf("%s must be approval tier, got %+v", v, d)
		}
	}
}

func TestGateTrustGateBlocksUnearnedVerdicts(t *testing.T) {
	rec := autopilotTestRec(models.MediaCirculationUnitSource, mediaCircVerdictPullNow)
	d := decideMediaAutopilotRec(rec, autopilotTestGates(nil)) // no trust at all
	if d.Kind != mediaAutopilotDecisionSkip || d.Guardrail != models.MediaAutopilotGuardTrustGate {
		t.Fatalf("unearned pull_now must skip on trust_gate, got %+v", d)
	}
}

func TestGateAcknowledgeOnlyBypassesTrust(t *testing.T) {
	// protect / skip_source / blocked_transcript have no side effect — they
	// auto-acknowledge without trust or confidence.
	for _, tc := range []struct{ unit, verdict string }{
		{models.MediaCirculationUnitItemFamily, mediaCircVerdictProtect},
		{models.MediaCirculationUnitSource, mediaCircVerdictSkipSource},
		{models.MediaCirculationUnitItemFamily, mediaCircVerdictBlockedTranscript},
	} {
		rec := autopilotTestRec(tc.unit, tc.verdict)
		d := decideMediaAutopilotRec(rec, autopilotTestGates(nil))
		if d.Kind != mediaAutopilotDecisionApply || d.Executes {
			t.Fatalf("%s must auto-acknowledge without executing, got %+v", tc.verdict, d)
		}
	}
}

func TestGateEvictRequiresFreshScore(t *testing.T) {
	// G13: trusted rank_down with no score row after refresh → stale_score.
	rec := autopilotTestRec(models.MediaCirculationUnitItemFamily, mediaCircVerdictRankDown)
	g := autopilotTestGates(earnedTrust(mediaCircVerdictRankDown))
	d := decideMediaAutopilotRec(rec, g)
	if d.Kind != mediaAutopilotDecisionSkip || d.Guardrail != models.MediaAutopilotGuardStaleScore {
		t.Fatalf("missing score must skip stale_score, got %+v", d)
	}

	// Present but stale → same guardrail.
	g.Scores[rec.SubjectID] = intelligence.ScoreSnapshot{Value: 0.8, Confidence: 0.9, ExplorationState: intelligence.ExplorationEstablished, Fresh: false}
	d = decideMediaAutopilotRec(rec, g)
	if d.Guardrail != models.MediaAutopilotGuardStaleScore {
		t.Fatalf("stale score must skip stale_score, got %+v", d)
	}
}

func TestGateEvictConfidenceLadder(t *testing.T) {
	rec := autopilotTestRec(models.MediaCirculationUnitItemFamily, mediaCircVerdictReEncode)
	g := autopilotTestGates(earnedTrust(mediaCircVerdictReEncode))

	// Exploring → exploration_guard.
	g.Scores[rec.SubjectID] = intelligence.ScoreSnapshot{ExplorationState: intelligence.ExplorationExploring, Confidence: 0.9, Fresh: true}
	if d := decideMediaAutopilotRec(rec, g); d.Guardrail != models.MediaAutopilotGuardExplorationGuard {
		t.Fatalf("exploring item must skip exploration_guard, got %+v", d)
	}

	// Established but low confidence → low_confidence (G7).
	g.Scores[rec.SubjectID] = intelligence.ScoreSnapshot{ExplorationState: intelligence.ExplorationEstablished, Confidence: 0.3, Fresh: true}
	if d := decideMediaAutopilotRec(rec, g); d.Guardrail != models.MediaAutopilotGuardLowConfidence {
		t.Fatalf("low-confidence item must skip low_confidence, got %+v", d)
	}

	// Established + confident + fresh → apply.
	g.Scores[rec.SubjectID] = intelligence.ScoreSnapshot{ExplorationState: intelligence.ExplorationEstablished, Confidence: 0.8, Fresh: true}
	if d := decideMediaAutopilotRec(rec, g); d.Kind != mediaAutopilotDecisionApply || !d.Executes {
		t.Fatalf("established+confident+fresh must apply, got %+v", d)
	}
}

func TestGateQueueDepthBlocksQueueLoadVerdicts(t *testing.T) {
	rec := autopilotTestRec(models.MediaCirculationUnitSource, mediaCircVerdictPullNow)
	g := autopilotTestGates(earnedTrust(mediaCircVerdictPullNow))
	g.QueueDepth = 500
	g.MaxQueueDepth = 100
	d := decideMediaAutopilotRec(rec, g)
	if d.Guardrail != models.MediaAutopilotGuardQueueDepth {
		t.Fatalf("over-cap queue must skip queue_depth, got %+v", d)
	}
	// But a non-queue verdict (rank_down with a good score) is unaffected.
	rd := autopilotTestRec(models.MediaCirculationUnitItemFamily, mediaCircVerdictRankDown)
	g.Trust = earnedTrust(mediaCircVerdictRankDown)
	g.Scores[rd.SubjectID] = intelligence.ScoreSnapshot{ExplorationState: intelligence.ExplorationEstablished, Confidence: 0.9, Fresh: true}
	if d := decideMediaAutopilotRec(rd, g); d.Kind != mediaAutopilotDecisionApply {
		t.Fatalf("rank_down must not be queue-gated, got %+v", d)
	}
}

func TestGateAtomizeCap(t *testing.T) {
	g := autopilotTestGates(earnedTrust(mediaCircVerdictAtomizeNow))
	g.MaxAtomize = 1
	first := autopilotTestRec(models.MediaCirculationUnitItemFamily, mediaCircVerdictAtomizeNow)
	second := autopilotTestRec(models.MediaCirculationUnitItemFamily, mediaCircVerdictAtomizeNow)
	if d := decideMediaAutopilotRec(first, g); d.Kind != mediaAutopilotDecisionApply {
		t.Fatalf("first atomize must apply, got %+v", d)
	}
	if d := decideMediaAutopilotRec(second, g); d.Guardrail != models.MediaAutopilotGuardAtomizeLimit {
		t.Fatalf("second atomize must skip atomize_limit (G8), got %+v", d)
	}
}

func TestGateByteBudget(t *testing.T) {
	g := autopilotTestGates(earnedTrust(mediaCircVerdictMoveToCold))
	g.MaxBytes = 1_000_000
	rec := autopilotTestRec(models.MediaCirculationUnitItemFamily, mediaCircVerdictMoveToCold)
	rec.Metrics = datatypes.JSON([]byte(`{"file_size_bytes": 2000000}`))
	g.Scores[rec.SubjectID] = intelligence.ScoreSnapshot{ExplorationState: intelligence.ExplorationEstablished, Confidence: 0.9, Fresh: true}
	if d := decideMediaAutopilotRec(rec, g); d.Guardrail != models.MediaAutopilotGuardBudget {
		t.Fatalf("over-byte-budget cold move must skip budget, got %+v", d)
	}
}

func TestGateStaleness(t *testing.T) {
	g := autopilotTestGates(earnedTrust(mediaCircVerdictPullNow))
	old := autopilotTestRec(models.MediaCirculationUnitSource, mediaCircVerdictPullNow)
	old.CreatedAt = time.Now().Add(-48 * time.Hour)
	if d := decideMediaAutopilotRec(old, g); d.Guardrail != models.MediaAutopilotGuardStaleness {
		t.Fatalf("stale rec must skip staleness, got %+v", d)
	}
	applied := autopilotTestRec(models.MediaCirculationUnitSource, mediaCircVerdictPullNow)
	applied.Status = models.MediaCirculationRecStatusApplied
	if d := decideMediaAutopilotRec(applied, g); d.Guardrail != models.MediaAutopilotGuardStaleness {
		t.Fatalf("non-pending rec must skip staleness, got %+v", d)
	}
}

func TestDeepPullCountsDouble(t *testing.T) {
	if mediaAutopilotActionCost(mediaCircVerdictDeepPull) != 2 {
		t.Fatal("deep_pull must cost 2 against the action cap")
	}
	if mediaAutopilotActionCost(mediaCircVerdictPullNow) != 1 {
		t.Fatal("pull_now must cost 1")
	}
}

// ----------------------------------------------------------------
// Slice 2 — trust earn rule (pure)
// ----------------------------------------------------------------

func TestTrustEarnRule(t *testing.T) {
	policy := models.DefaultMediaCirculationPolicy("default") // min 20 decisions, <10% reverts

	earned := mediaAutopilotTrustStat{Verdict: mediaCircVerdictPullNow, Decisions: 25, Applied: 25, Reverts: 1, RevertPct: 4}
	if !evaluateMediaAutopilotTrust(earned, policy) {
		t.Fatal("25 successful human applies at 4% reverts must earn trust")
	}

	thin := mediaAutopilotTrustStat{Verdict: mediaCircVerdictPullNow, Decisions: 5, Applied: 5, RevertPct: 0}
	if evaluateMediaAutopilotTrust(thin, policy) {
		t.Fatal("5 successful human applies must not earn trust")
	}

	dismissedOnly := mediaAutopilotTrustStat{Verdict: mediaCircVerdictPullNow, Decisions: 25, Applied: 0, RevertPct: 0}
	if evaluateMediaAutopilotTrust(dismissedOnly, policy) {
		t.Fatal("dismissals alone must not earn execution trust")
	}

	churny := mediaAutopilotTrustStat{Verdict: mediaCircVerdictRankDown, Decisions: 40, Applied: 40, Reverts: 8, RevertPct: 20}
	if evaluateMediaAutopilotTrust(churny, policy) {
		t.Fatal("20% revert rate must not earn trust")
	}

	// G3: approval-tier verdicts are structurally untrustable.
	deleteStat := mediaAutopilotTrustStat{Verdict: mediaCircVerdictRecoverableDelete, Decisions: 500, Applied: 500, RevertPct: 0}
	if evaluateMediaAutopilotTrust(deleteStat, policy) {
		t.Fatal("recoverable_delete must never earn trust regardless of history")
	}
}

// ----------------------------------------------------------------
// Slice 3 — elevated modes (G6)
// ----------------------------------------------------------------

func TestElevatedCapsStorageReliefPausesIntake(t *testing.T) {
	future := time.Now().Add(time.Hour)
	p := models.DefaultMediaCirculationPolicy("default")
	p.AutopilotElevatedMode = models.MediaAutopilotElevatedStorageRelief
	p.AutopilotElevatedUntil = &future
	adjusted, intakePaused := mediaAutopilotElevatedCaps(p)
	if !intakePaused {
		t.Fatal("storage_relief must pause intake")
	}
	if adjusted.AutopilotMaxActionsPerRun != 16 || adjusted.AutopilotMaxBytesPerRun != int64(4)<<30 {
		t.Fatalf("storage_relief caps wrong: actions=%d bytes=%d", adjusted.AutopilotMaxActionsPerRun, adjusted.AutopilotMaxBytesPerRun)
	}
}

func TestElevatedCapsAtomizationCatchup(t *testing.T) {
	future := time.Now().Add(time.Hour)
	p := models.DefaultMediaCirculationPolicy("default")
	p.AutopilotElevatedMode = models.MediaAutopilotElevatedAtomizationCatchup
	p.AutopilotElevatedUntil = &future
	adjusted, intakePaused := mediaAutopilotElevatedCaps(p)
	if intakePaused {
		t.Fatal("atomization_catchup must not pause intake")
	}
	if adjusted.AutopilotMaxAtomizePerRun != 9 {
		t.Fatalf("atomization_catchup atomize cap: got %d", adjusted.AutopilotMaxAtomizePerRun)
	}
}

func TestElevatedCapsExpiredElevationIsInert(t *testing.T) {
	past := time.Now().Add(-time.Hour)
	p := models.DefaultMediaCirculationPolicy("default")
	p.AutopilotElevatedMode = models.MediaAutopilotElevatedStorageRelief
	p.AutopilotElevatedUntil = &past
	adjusted, intakePaused := mediaAutopilotElevatedCaps(p)
	if intakePaused || adjusted.AutopilotElevatedMode != "" ||
		adjusted.AutopilotMaxActionsPerRun != 8 {
		t.Fatalf("expired elevation must be inert, got %+v paused=%v", adjusted.AutopilotElevatedMode, intakePaused)
	}
}

func TestGateIntakePausedUnderStorageRelief(t *testing.T) {
	g := autopilotTestGates(earnedTrust(mediaCircVerdictPullNow))
	g.IntakePaused = true
	rec := autopilotTestRec(models.MediaCirculationUnitSource, mediaCircVerdictPullNow)
	d := decideMediaAutopilotRec(rec, g)
	if d.Kind != mediaAutopilotDecisionSkip || d.Guardrail != models.MediaAutopilotGuardElevatedMode {
		t.Fatalf("intake must pause under storage_relief, got %+v", d)
	}
	// Evict side is unaffected.
	rd := autopilotTestRec(models.MediaCirculationUnitItemFamily, mediaCircVerdictRankDown)
	g.Trust = earnedTrust(mediaCircVerdictRankDown)
	g.Scores[rd.SubjectID] = intelligence.ScoreSnapshot{ExplorationState: intelligence.ExplorationEstablished, Confidence: 0.9, Fresh: true}
	if d := decideMediaAutopilotRec(rd, g); d.Kind != mediaAutopilotDecisionApply {
		t.Fatalf("evict must proceed under storage_relief, got %+v", d)
	}
}

// ----------------------------------------------------------------
// Slice 2 — observe/safe-auto status mapping
// ----------------------------------------------------------------

func TestObserveModeMapsStatuses(t *testing.T) {
	// The dispatch mapping is exercised without a DB by checking the intended
	// status pairs: apply→would_apply, skip→would_skip in Observe.
	r := &mediaAutopilotRunner{observe: true}
	if got := r.terminalStatus(mediaAutopilotDecisionApply); got != models.MediaAutopilotActionStatusWouldApply {
		t.Fatalf("observe apply status: got %s", got)
	}
	if got := r.terminalStatus(mediaAutopilotDecisionSkip); got != models.MediaAutopilotActionStatusWouldSkip {
		t.Fatalf("observe skip status: got %s", got)
	}
	r.observe = false
	if got := r.terminalStatus(mediaAutopilotDecisionApply); got != models.MediaAutopilotActionStatusSuccess {
		t.Fatalf("safe-auto apply status: got %s", got)
	}
	if got := r.terminalStatus(mediaAutopilotDecisionSkip); got != models.MediaAutopilotActionStatusSkipped {
		t.Fatalf("safe-auto skip status: got %s", got)
	}
}

// ----------------------------------------------------------------
// Review-fix regression tests
// ----------------------------------------------------------------

// The service token the runner mints for CMS→Aggregation calls must carry the
// identity Aggregation's admin-auth plugin trusts (iss=cms-service,
// aud=platform-console, role=admin) and a future expiry.
func TestMintServiceAdminTokenClaims(t *testing.T) {
	t.Setenv("JWT_SECRET", "test-secret-value")
	tok, err := utils.MintServiceAdminToken("default", 10*time.Minute)
	if err != nil {
		t.Fatalf("mint failed: %v", err)
	}
	secret, _ := utils.GetJWTSecret()
	claims, err := utils.ParseJWT(tok, secret)
	if err != nil {
		t.Fatalf("minted token failed to parse: %v", err)
	}
	if claims.Issuer != "cms-service" {
		t.Fatalf("issuer: got %q, want cms-service", claims.Issuer)
	}
	if len(claims.Audience) == 0 || claims.Audience[0] != "platform-console" {
		t.Fatalf("audience: got %v, want [platform-console]", claims.Audience)
	}
	if claims.Role != "admin" {
		t.Fatalf("role: got %q, want admin", claims.Role)
	}
	if claims.ExpiresAt == nil || !claims.ExpiresAt.After(time.Now()) {
		t.Fatal("token must have a future expiry")
	}
}

func TestMintServiceAdminTokenRequiresSecret(t *testing.T) {
	t.Setenv("JWT_SECRET", "")
	if _, err := utils.MintServiceAdminToken("default", time.Minute); err == nil {
		t.Fatal("minting without JWT_SECRET must fail, not sign with an empty key")
	}
}

// The trust-gate skip reason must interpolate the actual thresholds (regression:
// it once read "need ≥ human decisions" with no number).
func TestTrustGateReasonIncludesThreshold(t *testing.T) {
	g := &mediaAutopilotGates{
		Trust:             map[string]mediaAutopilotTrustStat{},
		Scores:            map[uuid.UUID]intelligence.ScoreSnapshot{},
		TrustMinDecisions: 20,
		TrustMaxRevertPct: 10,
		Now:               time.Now(),
		StalenessWindow:   mediaAutopilotStalenessWindow,
	}
	rec := models.MediaCirculationRecommendation{
		PublicID:  uuid.New(),
		UnitType:  models.MediaCirculationUnitSource,
		Verdict:   mediaCircVerdictPullNow,
		SubjectID: uuid.New(),
		CreatedAt: time.Now(),
		Status:    models.MediaCirculationRecStatusPending,
	}
	d := decideMediaAutopilotRec(rec, g)
	if d.Guardrail != models.MediaAutopilotGuardTrustGate {
		t.Fatalf("expected trust_gate, got %q", d.Guardrail)
	}
	if !strings.Contains(d.Reason, "20") {
		t.Fatalf("trust-gate reason must name the 20-decision threshold, got: %s", d.Reason)
	}
	if !strings.Contains(d.Reason, "applies") {
		t.Fatalf("trust-gate reason must explain successful applies, got: %s", d.Reason)
	}
}
