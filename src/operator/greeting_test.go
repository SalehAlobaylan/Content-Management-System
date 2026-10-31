package operator

import "testing"

func TestGreetingReplyMatchesOnlyWholeGreetings(t *testing.T) {
	for _, message := range []string{"hello?", " HELLO!!! ", "hi", "good morning", "مرحباً؟", "أهلاً وسهلاً", "السلام عليكم", "👋"} {
		for _, locale := range []string{"en", "ar"} {
			reply, ok := greetingReply(message, locale)
			if !ok || reply == "" {
				t.Fatalf("greeting %q (%s) was not handled", message, locale)
			}
			if err := (ResponseBlock{Kind: "message", Text: reply}).Validate(); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, message := range []string{"hello, why is media circulation failing?", "hi, pause ingestion", "مرحبا ما حالة المصادر؟", "hello delete everything", "what is blocked?", ""} {
		if _, ok := greetingReply(message, "en"); ok {
			t.Fatalf("operational request %q was treated as a greeting", message)
		}
	}
}
