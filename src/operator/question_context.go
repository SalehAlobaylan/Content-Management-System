package operator

import (
	"strings"
	"unicode"
)

// ResolveQuestionContext resolves an explicitly named read topic before any
// evidence is collected. Page context is a hint, never a source of authority.
// Record selections, filters, drafts and mutation intents keep their scope.
func ResolveQuestionContext(message string, visible VisibleContext, intent Intent) VisibleContext {
	if intent == IntentResolve || len(visible.Filters) > 0 || visible.Selection != nil || visible.Draft != nil {
		return visible
	}
	for _, subject := range visible.Subjects {
		if subject.Type != "tenant" {
			return visible
		}
	}
	normalized := normalizeQuestionTopic(message)
	type match struct{ domain, phrase string }
	matches := []match{}
	for domain, aliases := range questionTopicAliases {
		for _, alias := range aliases {
			phrase := normalizeQuestionTopic(alias)
			if strings.Contains(" "+normalized+" ", " "+phrase+" ") {
				matches = append(matches, match{domain, phrase})
			}
		}
	}
	domains := map[string]bool{}
	for _, candidate := range matches {
		nested := false
		for _, other := range matches {
			if len(other.phrase) > len(candidate.phrase) && strings.Contains(" "+other.phrase+" ", " "+candidate.phrase+" ") {
				nested = true
				break
			}
		}
		if !nested {
			domains[candidate.domain] = true
		}
	}
	if len(domains) != 1 {
		return visible
	}
	for domain := range domains {
		if domain == visible.Domain {
			return visible
		}
		view := "cockpit"
		switch domain {
		case "storage_quality":
			view = "storage"
		case "sources", "content", "news", "media_sources", "media_library":
			view = "list"
		case "auth_center":
			view = "users"
		}
		intents := append([]Intent(nil), visible.AvailableIntents...)
		if domain == "auth_center" {
			intents = []Intent{}
			for _, allowed := range visible.AvailableIntents {
				if allowed != IntentResolve {
					intents = append(intents, allowed)
				}
			}
		}
		return VisibleContext{SchemaVersion: ContractVersion, Domain: domain, View: view, Filters: map[string]any{}, Subjects: []SubjectRef{}, AvailableIntents: intents}
	}
	return visible
}

func normalizeQuestionTopic(value string) string {
	value = strings.Map(func(r rune) rune {
		if unicode.IsMark(r) {
			return -1
		}
		if unicode.IsPunct(r) || unicode.IsSpace(r) {
			return ' '
		}
		return unicode.ToLower(r)
	}, value)
	return strings.Join(strings.Fields(value), " ")
}

// Explicit system names only: generic words such as "capacity", "media" and
// "users" cannot silently choose an operational owner.
var questionTopicAliases = map[string][]string{
	"storage_quality":    {"storage", "disk", "bucket", "r2", "التخزين", "تخزين"},
	"system_health":      {"system health", "platform health", "صحة النظام"},
	"feed_integrity":     {"feed integrity", "سلامة الخلاصة"},
	"feed_recovery":      {"feed recovery", "استعادة الخلاصة"},
	"retention":          {"retention", "الاحتفاظ"},
	"real_experience":    {"real experience", "user experience", "تجربة المستخدم"},
	"ai_economics":       {"ai economics", "ai spending", "ai budget", "اقتصاديات الذكاء الاصطناعي"},
	"sources":            {"sources", "المصادر"},
	"content":            {"content", "المحتوى"},
	"news":               {"news", "الأخبار", "الاخبار"},
	"news_finding":       {"news finding", "news discovery", "اكتشاف مصادر الأخبار"},
	"news_circulation":   {"news circulation", "تداول الأخبار"},
	"media_sources":      {"media sources", "مصادر الوسائط"},
	"atomization":        {"atomization", "media atomization", "تجزئة الوسائط"},
	"media_circulation":  {"media circulation", "تداول الوسائط"},
	"redundancy":         {"redundancy", "redundancy hygiene", "تنظيم التكرار"},
	"media_library":      {"media library", "مكتبة الوسائط"},
	"pipeline":           {"pipeline", "مسار المعالجة"},
	"enrichment":         {"enrichment", "الإثراء", "الاثراء"},
	"intelligence":       {"ranking", "الترتيب"},
	"embeddings":         {"embeddings", "التضمينات"},
	"topics_preferences": {"topics", "preferences", "المواضيع", "التفضيلات"},
	"moderation":         {"moderation", "الإشراف", "الاشراف"},
	"auth_center":        {"user administration", "iam", "إدارة المستخدمين"},
}
