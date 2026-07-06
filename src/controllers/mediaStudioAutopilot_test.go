package controllers

import (
	"testing"

	"content-management-system/src/models"
)

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
			primary, codes := deriveStudioReviewCodes(strptr(tc.reason), nil, nil, false, 0.82)
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
	primary, codes := deriveStudioReviewCodes(nil, nil, fptr(0.5), true, 0.82)
	// sponsor_intro outranks low_confidence (S5 precedence).
	if primary == nil || *primary != models.StudioReviewCodeSponsorIntro {
		t.Fatalf("primary: got %v want sponsor_intro", primary)
	}
	if len(codes) != 2 {
		t.Fatalf("expected 2 codes, got %v", codes)
	}
}

func TestDeriveStudioReviewCodes_MergedShortSuppressesLowConfidence(t *testing.T) {
	// A merged-short chapter is coded merged_short, not low_confidence, even at
	// low confidence — so it can be a single-code auto-publish candidate.
	primary, codes := deriveStudioReviewCodes(nil, strptr("merged_short_chapter"), fptr(0.5), false, 0.82)
	if primary == nil || *primary != models.StudioReviewCodeMergedShort {
		t.Fatalf("primary: got %v want merged_short", primary)
	}
	if len(codes) != 1 {
		t.Fatalf("expected single merged_short code, got %v", codes)
	}
}

func TestDeriveStudioReviewCodes_Unclassified(t *testing.T) {
	primary, codes := deriveStudioReviewCodes(strptr("some free text a human wrote"), nil, nil, false, 0.82)
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
