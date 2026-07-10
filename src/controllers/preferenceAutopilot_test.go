package controllers

import (
	"errors"
	"math"
	"regexp"
	"strings"
	"testing"

	"content-management-system/src/models"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// Policy sanitize clamps every knob into range and forces a valid mode.
func TestSanitizePreferenceAutopilotPolicy(t *testing.T) {
	p := models.PreferenceAutopilotPolicy{
		Mode:                "wat",
		IntervalMinutes:     0,     // → default 15
		MaxItemCandidates:   99999, // → clamp 5000
		HighConfidence:      2.0,   // → clamp 1.0
		AdvisoryRejectFloor: 0.35,
		DuplicateCosine:     0.1, // → clamp 0.5
		FailureBreakerPct:   0,   // → default 25
	}
	got := sanitizePreferenceAutopilotPolicy(p)
	if got.Mode != models.PreferenceAutopilotModeObserve {
		t.Fatalf("mode = %q, want observe", got.Mode)
	}
	if got.IntervalMinutes != 15 {
		t.Fatalf("interval = %d, want 15", got.IntervalMinutes)
	}
	if got.MaxItemCandidates != 5000 {
		t.Fatalf("item cap = %d, want 5000", got.MaxItemCandidates)
	}
	if got.HighConfidence != 1.0 {
		t.Fatalf("high conf = %v, want 1.0", got.HighConfidence)
	}
	if got.DuplicateCosine != 0.5 {
		t.Fatalf("dup cosine = %v, want 0.5", got.DuplicateCosine)
	}
	if got.FailureBreakerPct != 25 {
		t.Fatalf("breaker = %d, want 25", got.FailureBreakerPct)
	}
}

// Flip gate colors follow coverage vs floor, and a green-but-no-boost-data gate is
// capped at amber pre-flip.
func TestBuildFlipGate(t *testing.T) {
	cases := []struct {
		name     string
		enabled  bool
		coverage float64
		floor    int
		boost    string
		want     string
	}{
		{"above floor → green", true, 80, 70, "ok", "green"},
		{"within 80% band → amber", true, 60, 70, "ok", "amber"},
		{"well below → red", true, 30, 70, "ok", "red"},
		{"green but pre-flip no boost data → amber", false, 90, 70, "unknown", "amber"},
		{"green pre-flip with boost data → green", false, 90, 70, "ok", "green"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := buildFlipGate("foryou_enabled", tc.enabled, tc.coverage, tc.floor, tc.boost)
			if g.State != tc.want {
				t.Fatalf("state = %q, want %q", g.State, tc.want)
			}
		})
	}
}

func gates(foryouEnabled bool, foryouCov float64, newsEnabled bool, newsCov float64) map[string]preferenceFlipGate {
	return map[string]preferenceFlipGate{
		"foryou_enabled": buildFlipGate("foryou_enabled", foryouEnabled, foryouCov, 70, "ok"),
		"news_enabled":   buildFlipGate("news_enabled", newsEnabled, newsCov, 60, "ok"),
	}
}

// Verdict headline follows the §7 precedence.
func TestComputePreferenceVerdict(t *testing.T) {
	policy := models.DefaultPreferenceAutopilotPolicy("default")
	cases := []struct {
		name   string
		snap   preferenceSnapshot
		runner *preferenceAutopilotRunner
		want   string
	}{
		{
			name:   "run failed → degraded",
			snap:   preferenceSnapshot{FlipGates: gates(true, 90, true, 90)},
			runner: &preferenceAutopilotRunner{errored: 1, success: 0},
			want:   models.PreferenceAutopilotHeadlineDegraded,
		},
		{
			name:   "mute violation → integrity_alert",
			snap:   preferenceSnapshot{MuteViolations: 2, FlipGates: gates(true, 90, true, 90)},
			runner: &preferenceAutopilotRunner{},
			want:   models.PreferenceAutopilotHeadlineIntegrityAlert,
		},
		{
			name:   "null centroid → integrity_alert",
			snap:   preferenceSnapshot{NullCentroidTopics: 1, FlipGates: gates(true, 90, true, 90)},
			runner: &preferenceAutopilotRunner{},
			want:   models.PreferenceAutopilotHeadlineIntegrityAlert,
		},
		{
			name:   "pending over ceiling → backlog",
			snap:   preferenceSnapshot{PendingProposals: 150, FlipGates: gates(true, 90, true, 90)},
			runner: &preferenceAutopilotRunner{},
			want:   models.PreferenceAutopilotHeadlineBacklog,
		},
		{
			name:   "enabled surface below floor → coverage_gap",
			snap:   preferenceSnapshot{FlipGates: gates(true, 40, true, 90)},
			runner: &preferenceAutopilotRunner{},
			want:   models.PreferenceAutopilotHeadlineCoverageGap,
		},
		{
			name:   "pending waiting → review_ready",
			snap:   preferenceSnapshot{PendingProposals: 5, FlipGates: gates(true, 90, true, 90)},
			runner: &preferenceAutopilotRunner{},
			want:   models.PreferenceAutopilotHeadlineReviewReady,
		},
		{
			name:   "flag off + green → flip_eligible",
			snap:   preferenceSnapshot{FlipGates: gates(false, 90, true, 90)},
			runner: &preferenceAutopilotRunner{},
			want:   models.PreferenceAutopilotHeadlineFlipEligible,
		},
		{
			name:   "all healthy → curation_current",
			snap:   preferenceSnapshot{FlipGates: gates(true, 90, true, 90)},
			runner: &preferenceAutopilotRunner{},
			want:   models.PreferenceAutopilotHeadlineCurationCurrent,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			headline, _, _ := computePreferenceVerdict(tc.snap, nil, tc.runner, policy)
			if headline != tc.want {
				t.Fatalf("headline = %q, want %q", headline, tc.want)
			}
		})
	}
}

func TestFailureBreakerUsesAllAttempts(t *testing.T) {
	if failureBreakerTripped(10, 2, 25) {
		t.Fatal("20% failures must not trip a 25% breaker")
	}
	if !failureBreakerTripped(8, 2, 25) {
		t.Fatal("25% failures must trip a 25% breaker")
	}
	if failureBreakerTripped(3, 3, 25) {
		t.Fatal("breaker requires the minimum four attempts")
	}
}

func TestProposalPredictionFreezesOnce(t *testing.T) {
	if !shouldFreezeProposalPrediction(&models.TopicProposal{}) {
		t.Fatal("an unpredicted proposal should freeze its first prediction")
	}
	if shouldFreezeProposalPrediction(&models.TopicProposal{
		PredictionVersion: models.PreferencePredictionVersion,
		PredictedVerdict:  models.PreferenceVerdictReview,
	}) {
		t.Fatal("an existing prediction must remain immutable")
	}
}

func TestUpsertItemTopicsRollsBackBeforePruneOnInsertFailure(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO "content_item_topics"`)).
		WillReturnError(errors.New("write failed"))
	mock.ExpectRollback()

	err = upsertItemTopics(db, uuid.New(), []models.ContentItemTopic{{TopicID: uuid.New(), Score: 0.8}})
	if err == nil {
		t.Fatal("insert failure must be returned")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestReplaceItemTopicMappingsBatchesWholePage(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{SkipDefaultTransaction: true})
	if err != nil {
		t.Fatal(err)
	}

	first, second, topic := uuid.New(), uuid.New(), uuid.New()
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM "content_item_topics" WHERE content_item_id IN ($1,$2)`)).
		WithArgs(first, second).WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO "content_item_topics"`)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	err = replaceItemTopicMappings(db, []itemTopicMapping{
		{ItemID: first, Matches: []models.ContentItemTopic{{TopicID: topic, Score: 0.9}}},
		{ItemID: second}, // Empty matches intentionally clear this subject's stale rows.
	})
	if err != nil {
		t.Fatalf("replace item mappings: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// Label heuristics: a label equal to the slug words is sluggish; Arabic detection
// keys off script range.
func TestLabelHeuristics(t *testing.T) {
	if !isSluggishLabel("saudi arabia", "saudi-arabia") {
		t.Fatal("slug-derived EN should be sluggish")
	}
	if isSluggishLabel("Kingdom of Saudi Arabia", "saudi-arabia") {
		t.Fatal("real label should not be sluggish")
	}
	if !isSluggishLabel("", "x") {
		t.Fatal("empty label should be sluggish")
	}
	if !hasArabicScript("السعودية") {
		t.Fatal("arabic text should be detected")
	}
	if hasArabicScript("Saudi") {
		t.Fatal("latin text is not arabic")
	}
}

// Confidence math building blocks.
func TestScorerMath(t *testing.T) {
	if got := clampRatio(3, 10); got != 0.3 {
		t.Fatalf("clampRatio = %v, want 0.3", got)
	}
	if got := clampRatio(5, 0); got != 0 {
		t.Fatalf("clampRatio zero-den = %v, want 0", got)
	}
	// weightedMean drops missing components by not being passed.
	got := weightedMean([]float64{1.0, 0.5}, []float64{0.5, 0.3})
	want := (1.0*0.5 + 0.5*0.3) / 0.8
	if math.Abs(got-want) > 1e-9 {
		t.Fatalf("weightedMean = %v, want %v", got, want)
	}
	// l2Normalize yields unit length.
	v := l2Normalize([]float32{3, 4})
	var norm float64
	for _, x := range v {
		norm += float64(x) * float64(x)
	}
	if math.Abs(norm-1) > 1e-6 {
		t.Fatalf("l2 norm = %v, want 1", norm)
	}
}

// Derived category vectors need at least two member centroids.
func TestBuildDerivedCategoryVectors(t *testing.T) {
	topics := []topicVector{
		{CategorySlug: "sports", Vec: []float32{1, 0}},
		{CategorySlug: "sports", Vec: []float32{0, 1}},
		{CategorySlug: "lonely", Vec: []float32{1, 1}}, // only one → excluded
	}
	got := buildDerivedCategoryVectors(topics)
	if _, ok := got["sports"]; !ok {
		t.Fatal("sports should have a derived vector (2 members)")
	}
	if _, ok := got["lonely"]; ok {
		t.Fatal("single-member category must be excluded → category_unknown")
	}
}

// Observe never persists predicted_verdict, so the verdict must fall back to the
// in-memory shadow scores for its high-confidence count.
func TestVerdictUsesShadowHighConfidence(t *testing.T) {
	policy := models.DefaultPreferenceAutopilotPolicy("default")
	snap := preferenceSnapshot{PendingProposals: 3, HighConfidencePending: 0, FlipGates: gates(true, 90, true, 90)}
	scored := []proposalScore{
		{Verdict: models.PreferenceVerdictHighConf},
		{Verdict: models.PreferenceVerdictHighConf},
		{Verdict: models.PreferenceVerdictReview},
	}
	_, _, recommended := computePreferenceVerdict(snap, scored, &preferenceAutopilotRunner{}, policy)
	if !strings.Contains(recommended, "2 high-confidence") {
		t.Fatalf("recommendation should surface 2 shadow high-confidence, got %q", recommended)
	}
}

// The earned auto-approve gate: every guard in precedence order (§15 tier).
func TestPreferenceAutoApproveGate(t *testing.T) {
	basePolicy := models.DefaultPreferenceAutopilotPolicy("default")
	basePolicy.AutoApproveEnabled = true
	eligible := preferenceTrustBanner{Eligible: true, Decisions: 40, AgreementPct: 95}
	good := proposalScore{
		Verdict: models.PreferenceVerdictHighConf, Confidence: 0.95, EmbeddingReady: true,
	}

	cases := []struct {
		name          string
		policy        func() models.PreferenceAutopilotPolicy
		trust         preferenceTrustBanner
		score         proposalScore
		approvedSoFar int
		wantOK        bool
		wantGuardrail string
	}{
		{
			name:   "disabled → silent no",
			policy: func() models.PreferenceAutopilotPolicy { p := basePolicy; p.AutoApproveEnabled = false; return p },
			trust:  eligible, score: good, wantOK: false, wantGuardrail: "",
		},
		{
			name:   "trust ineligible at run time → trust_gate",
			policy: func() models.PreferenceAutopilotPolicy { return basePolicy },
			trust:  preferenceTrustBanner{Eligible: false, Decisions: 5},
			score:  good, wantOK: false, wantGuardrail: models.PreferenceGuardTrustGate,
		},
		{
			name:   "below tier threshold → silent no",
			policy: func() models.PreferenceAutopilotPolicy { return basePolicy },
			trust:  eligible,
			score: proposalScore{
				Verdict: models.PreferenceVerdictHighConf, Confidence: 0.88, EmbeddingReady: true,
			},
			wantOK: false, wantGuardrail: "",
		},
		{
			name:   "review verdict → silent no even at high confidence",
			policy: func() models.PreferenceAutopilotPolicy { return basePolicy },
			trust:  eligible,
			score: proposalScore{
				Verdict: models.PreferenceVerdictReview, Confidence: 0.97, EmbeddingReady: true,
			},
			wantOK: false, wantGuardrail: "",
		},
		{
			name:   "duplicate blocker → blocker_flag",
			policy: func() models.PreferenceAutopilotPolicy { return basePolicy },
			trust:  eligible,
			score: proposalScore{
				Verdict: models.PreferenceVerdictHighConf, Confidence: 0.95, EmbeddingReady: true, Duplicate: true,
			},
			wantOK: false, wantGuardrail: models.PreferenceGuardBlockerFlag,
		},
		{
			name:   "needs_label blocker → blocker_flag",
			policy: func() models.PreferenceAutopilotPolicy { return basePolicy },
			trust:  eligible,
			score: proposalScore{
				Verdict: models.PreferenceVerdictHighConf, Confidence: 0.95, EmbeddingReady: true, NeedsLabel: true,
			},
			wantOK: false, wantGuardrail: models.PreferenceGuardBlockerFlag,
		},
		{
			name:          "run cap reached → run_cap",
			policy:        func() models.PreferenceAutopilotPolicy { return basePolicy },
			trust:         eligible,
			score:         good,
			approvedSoFar: 3, // default MaxAutoApprovals
			wantOK:        false, wantGuardrail: models.PreferenceGuardRunCap,
		},
		{
			name:   "happy path → approved",
			policy: func() models.PreferenceAutopilotPolicy { return basePolicy },
			trust:  eligible, score: good, wantOK: true, wantGuardrail: "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ok, guardrail, _ := preferenceAutoApproveGate(tc.policy(), tc.trust, tc.score, tc.approvedSoFar)
			if ok != tc.wantOK || guardrail != tc.wantGuardrail {
				t.Fatalf("gate = (%v, %q), want (%v, %q)", ok, guardrail, tc.wantOK, tc.wantGuardrail)
			}
		})
	}
}

// Auto-approve sanitize: the tier keeps its OWN floor (0.85) and a meaningful-0 cap.
func TestSanitizeAutoApprovePolicy(t *testing.T) {
	p := models.PreferenceAutopilotPolicy{
		AutoApproveMinConfidence: 0.5, // below floor → clamp to 0.85
		MaxAutoApprovals:         0,   // meaningful: disables executions, must stay 0
	}
	got := sanitizePreferenceAutopilotPolicy(p)
	if got.AutoApproveMinConfidence != 0.85 {
		t.Fatalf("min confidence = %v, want 0.85 floor", got.AutoApproveMinConfidence)
	}
	if got.MaxAutoApprovals != 0 {
		t.Fatalf("max approvals = %d, want meaningful 0", got.MaxAutoApprovals)
	}
	// Zero-value (unset) confidence takes the 0.92 default.
	p2 := models.PreferenceAutopilotPolicy{}
	if got2 := sanitizePreferenceAutopilotPolicy(p2); got2.AutoApproveMinConfidence != 0.92 {
		t.Fatalf("default min confidence = %v, want 0.92", got2.AutoApproveMinConfidence)
	}
}

// Insights bucket folding: would_* folds into the class bucket, errors/skips are
// class-independent.
func TestBucketPreferenceAction(t *testing.T) {
	var b prefRunBuckets
	bucketPreferenceAction(&b, models.PreferenceActionMapSweep, models.PreferenceActionStatusSuccess, 3)
	bucketPreferenceAction(&b, models.PreferenceActionAutoApprove, models.PreferenceActionStatusWouldTrigger, 2) // observe folds in
	bucketPreferenceAction(&b, models.PreferenceActionSnapshot, models.PreferenceActionStatusBaselineSuccess, 1)
	bucketPreferenceAction(&b, models.PreferenceActionMine, models.PreferenceActionStatusSkipped, 4)
	bucketPreferenceAction(&b, models.PreferenceActionRecompute, models.PreferenceActionStatusError, 2)
	bucketPreferenceAction(&b, models.PreferenceActionProposalEnrich, models.PreferenceActionStatusWouldSkip, 1)

	if b.MapSweep != 3 {
		t.Fatalf("map_sweep = %d, want 3", b.MapSweep)
	}
	if b.AutoApprove != 2 {
		t.Fatalf("auto_approve = %d (would_trigger must fold in), want 2", b.AutoApprove)
	}
	if b.Baseline != 1 {
		t.Fatalf("baseline = %d, want 1", b.Baseline)
	}
	if b.Skipped != 5 {
		t.Fatalf("skipped = %d (skipped+would_skip), want 5", b.Skipped)
	}
	if b.Errored != 2 {
		t.Fatalf("errored = %d, want 2", b.Errored)
	}
}
