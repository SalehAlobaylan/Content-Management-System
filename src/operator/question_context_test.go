package operator

import (
	"reflect"
	"testing"
)

func TestQuestionContextResolvesNamedReadSystems(t *testing.T) {
	visible := VisibleContext{SchemaVersion: ContractVersion, Domain: "global_ops", View: "operations", Filters: map[string]any{}, Subjects: []SubjectRef{}, AvailableIntents: []Intent{IntentExplain, IntentInvestigate, IntentResolve}}
	for _, tc := range []struct{ question, domain, view string }{
		{"what the status of the storage", "storage_quality", "storage"},
		{"What's my storage usage?", "storage_quality", "storage"},
		{"ما حالة التخزين؟", "storage_quality", "storage"},
		{"What is the status of news circulation?", "news_circulation", "cockpit"},
		{"Check media sources", "media_sources", "list"},
		{"Why is enrichment delayed?", "enrichment", "cockpit"},
	} {
		t.Run(tc.question, func(t *testing.T) {
			got := ResolveQuestionContext(tc.question, visible, IntentExplain)
			if got.Domain != tc.domain || got.View != tc.view {
				t.Fatalf("topic resolved as %+v", got)
			}
			if err := got.Validate(); err != nil {
				t.Fatal(err)
			}
		})
	}
	for _, question := range []string{"hello?", "compare news and storage", "what is the overall status?", "storageful"} {
		if got := ResolveQuestionContext(question, visible, IntentExplain); !reflect.DeepEqual(got, visible) {
			t.Fatalf("ambiguous or unrelated topic %q changed context", question)
		}
	}
	for _, scope := range []VisibleContext{
		{Selection: &ExplicitSelection{Mode: "explicit", IDs: []string{"one"}, Count: 1}},
		{Subjects: []SubjectRef{{Type: "content_item", ID: "one"}}},
		{Filters: map[string]any{"status": "ready"}},
		{Draft: &DraftContext{Kind: "source"}},
	} {
		scope.Domain = "content"
		if got := ResolveQuestionContext("storage status", scope, IntentExplain); !reflect.DeepEqual(got, scope) {
			t.Fatal("a record selection or draft was retargeted")
		}
	}
	if got := ResolveQuestionContext("pause storage", visible, IntentResolve); !reflect.DeepEqual(got, visible) {
		t.Fatal("a mutation intent was retargeted")
	}
}
