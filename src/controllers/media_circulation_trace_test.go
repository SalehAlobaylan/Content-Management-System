package controllers

import "testing"

func TestMediaSourceRunTraceRequestID(t *testing.T) {
	valid := " 8f39d643-4dd6-4f32-a4bc-8e97f2fb2b10 "
	id, err := mediaSourceRunTraceRequestID(valid)
	if err != nil || id.String() != "8f39d643-4dd6-4f32-a4bc-8e97f2fb2b10" {
		t.Fatalf("expected trimmed UUID to parse, got id=%q err=%v", id, err)
	}
	if _, err := mediaSourceRunTraceRequestID("not-a-source-run"); err == nil {
		t.Fatal("expected malformed request id to be rejected")
	}
}

func TestMediaSourceRunTraceBoundsArePositive(t *testing.T) {
	for name, value := range map[string]int{
		"attempts":              mediaSourceRunTraceAttemptLimit,
		"units":                 mediaSourceRunTraceUnitLimit,
		"receipts":              mediaSourceRunTraceReceiptLimit,
		"verification tasks":    mediaSourceRunTraceVerificationLimit,
		"reconciliation events": mediaSourceRunTraceReconciliationLimit,
		"items":                 mediaSourceRunTraceItemLimit,
	} {
		if value <= 0 {
			t.Fatalf("%s trace bound must be positive", name)
		}
	}
}

func TestTraceContentTitleBoundsRunes(t *testing.T) {
	long := make([]rune, mediaSourceRunTraceTitleRuneLimit+1)
	for i := range long {
		long[i] = 'م'
	}
	value := string(long)
	got := traceContentTitle(&value)
	if want := mediaSourceRunTraceTitleRuneLimit + 1; len([]rune(got)) != want || got[len(got)-len("…"):] != "…" {
		t.Fatalf("title was not safely capped: %q", got)
	}
}
