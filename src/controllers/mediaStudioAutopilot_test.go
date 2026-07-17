package controllers

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"content-management-system/src/models"

	"github.com/google/uuid"
)

func TestStudioRunnerDoesNotEmitReatomizeAtSTTAdmission(t *testing.T) {
	data, err := os.ReadFile("mediaStudioAutopilotRunner.go")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "r.maybeEmitReatomize(&item)") {
		t.Fatal("STT admission must not emit a re-atomization recommendation before verified completion")
	}
}

func TestStudioTranscriptRepairSnapshotCarriesImmutableIdentity(t *testing.T) {
	quality := models.TranscriptQuality{PublicID: uuid.New(), ContentItemID: uuid.New(), TranscriptID: uuid.New(), Status: models.TranscriptQualityAutoRepair}
	snapshot := studioTranscriptRepairSnapshotFromQuality(quality)
	if snapshot.QualityID != quality.PublicID || snapshot.ContentItemID != quality.ContentItemID || snapshot.TranscriptID != quality.TranscriptID || snapshot.ObservedState != quality.Status {
		t.Fatalf("snapshot lost immutable quality identity: %+v", snapshot)
	}
}

func TestStudioAutopilotPolicyPatchPreservesOmittedFields(t *testing.T) {
	policy := models.DefaultMediaStudioAutopilotPolicy("default")
	policy.MaxSTTPerRun = 0
	policy.PausedUntil = func() *time.Time { t := time.Now().Add(time.Hour); return &t }()
	mode := models.StudioAutopilotModeSafeAuto
	if err := (mediaStudioAutopilotPolicyPatch{AutopilotMode: &mode}).applyTo(&policy); err != nil {
		t.Fatal(err)
	}
	if policy.MaxSTTPerRun != 0 || policy.PausedUntil == nil {
		t.Fatalf("omitted fields changed: stt=%d paused=%v", policy.MaxSTTPerRun, policy.PausedUntil)
	}
	null := json.RawMessage("null")
	if err := (mediaStudioAutopilotPolicyPatch{PausedUntil: &null}).applyTo(&policy); err != nil {
		t.Fatal(err)
	}
	if policy.PausedUntil != nil {
		t.Fatal("explicit null must clear paused_until")
	}
}

// ----------------------------------------------------------------
// Slice 1 — review-reason code derivation (S4/S5)
// ----------------------------------------------------------------

func strptr(s string) *string { return &s }
func fptr(f float64) *float64 { return &f }

func TestDeriveStudioReviewCodes_SingleConstants(t *testing.T) {
	cases := []struct {
		name   string
		reason string
		want   string
	}{
		{"short_unmergeable", "Chapter below 4:30 and cannot merge without exceeding hard max.", models.StudioReviewCodeShortUnmergeable},
		{"below_min", "Chapter is below the 4:30 minimum feed duration.", models.StudioReviewCodeBelowMin},
		{"above_hard_max", "Chapter exceeds hard maximum duration.", models.StudioReviewCodeAboveHardMax},
		{"planner_fallback", "Fallback single chapter; planner returned no usable chapters.", models.StudioReviewCodePlannerFallback},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			primary, codes := deriveStudioReviewCodes(strptr(tc.reason), false, nil, false, 0.82)
			if primary == nil || *primary != tc.want {
				t.Fatalf("primary: got %v want %s", primary, tc.want)
			}
			if len(codes) != 1 || codes[0] != tc.want {
				t.Fatalf("codes: got %v want [%s]", codes, tc.want)
			}
		})
	}
}

func TestDeriveStudioReviewCodes_SponsorAndLowConfidence(t *testing.T) {
	primary, codes := deriveStudioReviewCodes(nil, false, fptr(0.5), true, 0.82)
	// sponsor_intro outranks low_confidence (S5 precedence).
	if primary == nil || *primary != models.StudioReviewCodeSponsorIntro {
		t.Fatalf("primary: got %v want sponsor_intro", primary)
	}
	if len(codes) != 2 {
		t.Fatalf("expected 2 codes, got %v", codes)
	}
}

func TestDeriveStudioReviewCodes_MergedShortRequiresDeterministicProvenance(t *testing.T) {
	// LLM-authored prose cannot authorize merged_short.
	primary, codes := deriveStudioReviewCodes(nil, false, fptr(0.5), false, 0.82)
	if primary == nil || *primary != models.StudioReviewCodeLowConfidence || len(codes) != 1 {
		t.Fatalf("unproven merge must remain low confidence: primary=%v codes=%v", primary, codes)
	}
	primary, codes = deriveStudioReviewCodes(nil, true, fptr(0.5), false, 0.82)
	if primary == nil || *primary != models.StudioReviewCodeLowConfidence || len(codes) != 2 {
		t.Fatalf("low-confidence merge must be multi-code approval-only: primary=%v codes=%v", primary, codes)
	}
	primary, codes = deriveStudioReviewCodes(nil, true, fptr(0.95), false, 0.82)
	if primary == nil || *primary != models.StudioReviewCodeMergedShort || len(codes) != 1 {
		t.Fatalf("high-confidence deterministic merge should be sole merged_short: primary=%v codes=%v", primary, codes)
	}
}

func TestApplyStudioReviewCodesRejectsUnprovenEmittedMerge(t *testing.T) {
	confidence := 0.95
	chapter := models.Chapter{Confidence: &confidence, MergedShortProvenance: false}
	emittedPrimary := models.StudioReviewCodeMergedShort
	applyStudioReviewCodes(&chapter, &emittedPrimary, []string{models.StudioReviewCodeMergedShort}, 0.82)
	if chapter.NeedsReviewCode != nil || len(chapter.NeedsReviewCodes) != 0 {
		t.Fatalf("unproven emitted merge must not authorize review code: %+v", chapter)
	}
}

func TestDeriveStudioReviewCodes_Unclassified(t *testing.T) {
	primary, codes := deriveStudioReviewCodes(strptr("some free text a human wrote"), false, nil, false, 0.82)
	if primary != nil || codes != nil {
		t.Fatalf("unmatched text must be unclassified, got primary=%v codes=%v", primary, codes)
	}
}

func TestStudioReviewPrimaryCode_Precedence(t *testing.T) {
	got := models.StudioReviewPrimaryCode([]string{
		models.StudioReviewCodeBelowMin,
		models.StudioReviewCodeLowConfidence,
		models.StudioReviewCodeSponsorIntro,
	})
	if got != models.StudioReviewCodeSponsorIntro {
		t.Fatalf("precedence: got %s want sponsor_intro", got)
	}
}

func TestSameStudioReviewCodeSet(t *testing.T) {
	if !sameStudioReviewCodeSet([]string{models.StudioReviewCodeMergedShort}, []string{models.StudioReviewCodeMergedShort}) {
		t.Fatal("matching single-code set rejected")
	}
	if sameStudioReviewCodeSet([]string{models.StudioReviewCodeMergedShort, models.StudioReviewCodeLowConfidence}, []string{models.StudioReviewCodeMergedShort}) {
		t.Fatal("new second review code must invalidate mechanical clearance")
	}
}

// ----------------------------------------------------------------
// Slice 1 — policy defaults + sanitize clamps (H8)
// ----------------------------------------------------------------

func TestDefaultMediaStudioAutopilotPolicy(t *testing.T) {
	p := models.DefaultMediaStudioAutopilotPolicy("default")
	if p.AutopilotEnabled {
		t.Fatal("autopilot must be disabled by default")
	}
	if p.AutopilotMode != models.StudioAutopilotModeObserve {
		t.Fatalf("default mode must be observe, got %q", p.AutopilotMode)
	}
	if p.IntervalMinutes != 360 || p.ChainDebounceMinutes != 15 {
		t.Fatalf("cadence defaults wrong: %d/%d", p.IntervalMinutes, p.ChainDebounceMinutes)
	}
	if p.MaxClearsPerRun != 10 || p.MaxPublishesPerRun != 5 || p.MaxRejectsPerRun != 10 {
		t.Fatalf("clear caps wrong: %d/%d/%d", p.MaxClearsPerRun, p.MaxPublishesPerRun, p.MaxRejectsPerRun)
	}
	if p.MaxSTTPerRun != 3 || p.MaxProposalsPerRun != 15 {
		t.Fatalf("stt/proposal caps wrong: %d/%d", p.MaxSTTPerRun, p.MaxProposalsPerRun)
	}
	if p.TrustMinDecisions != 20 || p.TrustMinApprovePct != 90 || p.TrustMaxReversalPct != 5 {
		t.Fatalf("trust defaults wrong (H5): %d/%d/%d", p.TrustMinDecisions, p.TrustMinApprovePct, p.TrustMaxReversalPct)
	}
	if p.AgedThresholdDays != 7 {
		t.Fatalf("aged threshold must be 7 (H6), got %d", p.AgedThresholdDays)
	}
}

func TestSanitizeMediaStudioAutopilotPolicy_Clamps(t *testing.T) {
	p := models.DefaultMediaStudioAutopilotPolicy("default")
	p.AutopilotMode = "wild"
	p.IntervalMinutes = 1
	p.MaxClearsPerRun = 9999
	p.MaxPublishesPerRun = -3
	p.MaxSTTPerRun = 999
	p.TrustMinApprovePct = 250
	p.TrustMaxReversalPct = -5
	p.AgedThresholdDays = 0

	got := sanitizeMediaStudioAutopilotPolicy(p)
	if got.AutopilotMode != models.StudioAutopilotModeObserve {
		t.Fatalf("invalid mode must fall back to observe, got %q", got.AutopilotMode)
	}
	if got.IntervalMinutes != 15 {
		t.Fatalf("interval clamp low: got %d", got.IntervalMinutes)
	}
	if got.MaxClearsPerRun != 50 {
		t.Fatalf("clears clamp high: got %d", got.MaxClearsPerRun)
	}
	if got.MaxPublishesPerRun != 0 {
		t.Fatalf("publishes clamp low: got %d", got.MaxPublishesPerRun)
	}
	if got.MaxSTTPerRun != 20 {
		t.Fatalf("stt clamp high: got %d", got.MaxSTTPerRun)
	}
	if got.TrustMinApprovePct != 100 {
		t.Fatalf("approve pct clamp high: got %d", got.TrustMinApprovePct)
	}
	if got.TrustMaxReversalPct != 0 {
		t.Fatalf("reversal pct clamp low: got %d", got.TrustMaxReversalPct)
	}
	// 0 means "unset" → falls back to the default (clampIntRange semantics).
	if got.AgedThresholdDays != 7 {
		t.Fatalf("aged unset → default: got %d", got.AgedThresholdDays)
	}
}

// ----------------------------------------------------------------
// Slice 2 — pure classification (S2/S3/S5/S16)
// ----------------------------------------------------------------

func TestDecideStudioChapterPath(t *testing.T) {
	const shortDur = forYouMinDurationSec - 1
	const okDur = forYouMinDurationSec + 60

	cases := []struct {
		name    string
		primary string
		codes   []string
		dur     int
		want    studioChapterPath
	}{
		{
			name:    "short_unmergeable single below-min → auto_reject (S3 structural)",
			primary: models.StudioReviewCodeShortUnmergeable,
			codes:   []string{models.StudioReviewCodeShortUnmergeable},
			dur:     shortDur,
			want:    studioPathAutoReject,
		},
		{
			name:    "short_unmergeable but duration legal → approval (no false reject)",
			primary: models.StudioReviewCodeShortUnmergeable,
			codes:   []string{models.StudioReviewCodeShortUnmergeable},
			dur:     okDur,
			want:    studioPathApproval,
		},
		{
			name:    "merged_short single → auto_publish candidate (S16)",
			primary: models.StudioReviewCodeMergedShort,
			codes:   []string{models.StudioReviewCodeMergedShort},
			dur:     okDur,
			want:    studioPathAutoPublish,
		},
		{
			name:    "merged_short multi-code → approval (S5 single-code only)",
			primary: models.StudioReviewCodeMergedShort,
			codes:   []string{models.StudioReviewCodeMergedShort, models.StudioReviewCodeLowConfidence},
			dur:     okDur,
			want:    studioPathApproval,
		},
		{
			name:    "sponsor_intro → approval (editorial, LLM-proposed only)",
			primary: models.StudioReviewCodeSponsorIntro,
			codes:   []string{models.StudioReviewCodeSponsorIntro},
			dur:     okDur,
			want:    studioPathApproval,
		},
		{
			name:    "planner_fallback → approval",
			primary: models.StudioReviewCodePlannerFallback,
			codes:   []string{models.StudioReviewCodePlannerFallback},
			dur:     okDur,
			want:    studioPathApproval,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := decideStudioChapterPath(tc.primary, tc.codes, tc.dur); got != tc.want {
				t.Fatalf("path: got %d want %d", got, tc.want)
			}
		})
	}
}
