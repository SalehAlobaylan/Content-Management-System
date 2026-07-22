package controllers

import "testing"

func TestEvaluateCommentPolicyBilingualSafetyAndSpam(t *testing.T) {
	cases := []struct {
		name    string
		text    string
		outcome commentPolicyOutcome
		reason  string
	}{
		{"normal English", "This explains the story clearly.", commentPolicyAllow, ""},
		{"normal Arabic", "تحليل هادئ ومفيد للخبر.", commentPolicyAllow, ""},
		{"Arabic direct threat with tashkil", "سَأَقْتُلُكَ", commentPolicyReject, "direct_threat"},
		{"English direct threat", "I will kill you", commentPolicyReject, "direct_threat"},
		{"repeated token spam", "buy buy buy buy now", commentPolicyReject, "spam"},
		{"repeated rune spam", "so cooooooooolllllllll", commentPolicyReject, "spam"},
		{"sensitive English reference reviews", "This report discusses porn regulation.", commentPolicyReview, "sensitive_reference"},
		{"sensitive Arabic reference reviews", "هذا نقاش عن إباحية الإنترنت.", commentPolicyReview, "sensitive_reference"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := evaluateCommentPolicy(tc.text)
			if got.Outcome != tc.outcome || got.Reason != tc.reason {
				t.Fatalf("evaluateCommentPolicy(%q) = %#v", tc.text, got)
			}
		})
	}
}
