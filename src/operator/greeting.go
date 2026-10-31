package operator

import (
	"strings"
	"unicode"
)

// IsGreeting lets CMS admission avoid suggesting actions for conversation-only
// replies, including when the dock has an explicit record selection attached.
func IsGreeting(message string) bool {
	_, ok := greetingReply(message, "en")
	return ok
}

// Match the entire message, never just its opening words. An operational
// question prefixed with "hello" must still receive a grounded investigation.
func greetingReply(message, locale string) (string, bool) {
	normalized := strings.Map(func(r rune) rune {
		if unicode.IsMark(r) {
			return -1
		}
		if unicode.IsPunct(r) || unicode.IsSpace(r) {
			return ' '
		}
		return unicode.ToLower(r)
	}, message)
	normalized = strings.Join(strings.Fields(normalized), " ")
	switch normalized {
	case "hello", "hello there", "hi", "hey", "good morning", "good evening", "مرحبا", "أهلا", "اهلا", "أهلا وسهلا", "اهلا وسهلا", "السلام عليكم", "وعليكم السلام", "👋":
		if locale == "ar" {
			return "أهلاً! ما الذي تودّ مراجعته؟ يمكنني مساعدتك في فهم حالة النظام، والتحقيق في مشكلة، ومراجعة الخطوة التالية بأمان.", true
		}
		return "Hello! What would you like me to look into? I can help explain system status, investigate an issue, or review a safe next step.", true
	}
	return "", false
}
