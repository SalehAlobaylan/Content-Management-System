package controllers

import (
	"strings"
	"unicode"

	"golang.org/x/text/unicode/norm"
)

type commentPolicyOutcome string

const (
	commentPolicyAllow  commentPolicyOutcome = "allow"
	commentPolicyReject commentPolicyOutcome = "reject"
	commentPolicyReview commentPolicyOutcome = "review"
)

type commentPolicyDecision struct {
	Outcome commentPolicyOutcome
	Reason  string
}

// normalizeCommentPolicyText makes deterministic rules resilient to harmless
// Unicode presentation variation without retaining a second copy of the text.
func normalizeCommentPolicyText(value string) string {
	value = strings.ToLower(norm.NFKC.String(value))
	value = strings.Map(func(r rune) rune {
		if unicode.Is(unicode.Mn, r) || r == '\u0640' { // Arabic tashkil/tatweel
			return -1
		}
		return r
	}, value)
	return strings.Join(strings.Fields(value), " ")
}

func hasRepeatedToken(value string) bool {
	counts := make(map[string]int)
	for _, token := range strings.Fields(value) {
		counts[token]++
		if len([]rune(token)) > 1 && counts[token] >= 4 {
			return true
		}
	}
	return false
}

func hasRepeatedRune(value string) bool {
	var previous rune
	runLength := 0
	for _, r := range value {
		if r == previous && !unicode.IsSpace(r) {
			runLength++
			if runLength >= 8 {
				return true
			}
			continue
		}
		previous, runLength = r, 1
	}
	return false
}

// evaluateCommentPolicy is intentionally local, deterministic, and bilingual.
// It rejects direct abusive threats, routes ambiguous adult/illicit references
// to human review, and catches mechanical spam. It does not call an LLM or
// transmit comment text beyond CMS.
func evaluateCommentPolicy(text string) commentPolicyDecision {
	normalized := normalizeCommentPolicyText(text)
	for _, phrase := range []string{
		"kill yourself", "go kill yourself", "i will kill you",
		"اقتل نفسك", "ساقتلك", "سأقتلك", "انتحر",
	} {
		if strings.Contains(normalized, phrase) {
			return commentPolicyDecision{Outcome: commentPolicyReject, Reason: "direct_threat"}
		}
	}
	if strings.Count(normalized, "http://")+strings.Count(normalized, "https://") >= 2 ||
		hasRepeatedToken(normalized) || hasRepeatedRune(normalized) {
		return commentPolicyDecision{Outcome: commentPolicyReject, Reason: "spam"}
	}
	for _, phrase := range []string{
		"porn", "xxx", "explicit sex", "إباحية", "اباحيه", "مخدرات للبيع",
	} {
		if strings.Contains(normalized, phrase) {
			return commentPolicyDecision{Outcome: commentPolicyReview, Reason: "sensitive_reference"}
		}
	}
	return commentPolicyDecision{Outcome: commentPolicyAllow}
}
